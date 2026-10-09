#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股历史数据初始化脚本（baostock主源·双轮版 v9）
架构：
  第一轮（主源）：baostock 单线程批量拉取
  第二轮（补源）：baostock 失败的股票用 5并发裸接口补拉
"""
import csv
import os
import re
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# ===================== 路径定位 =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")

# ===================== 可配置参数区 =====================
HISTORY_YEARS = 2
DATALEN = 550
MAX_WORKERS = 5
RETRY_TIMES = 2
PROGRESS_EVERY = 500
PROGRESS_EVERY_2 = 100
# 修复：适当放宽并发熔断阈值，避免多线程环境下的瞬间“误杀”
CIRCUIT_LIMIT = 50
EXCLUDE_BJ = True
BJ_PREFIXES = ("43", "83", "87", "88", "92")

FORCE_OVERWRITE = os.environ.get("FORCE_OVERWRITE", "0") == "1"

os.makedirs(HISTORY_DIR, exist_ok=True)

now = datetime.now()
START_DATE_BS = (now - timedelta(days=365 * HISTORY_YEARS)).strftime("%Y-%m-%d")
END_DATE_BS = now.strftime("%Y-%m-%d")

STD_COLS = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]

UA = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://quote.eastmoney.com/',
}
TIMEOUT = 15

LOCK = threading.Lock()
STATE = {"consec_fail": {"东财": 0, "新浪": 0, "腾讯": 0},
         "dead": {"东财": False, "新浪": False, "腾讯": False}}
SRC_COUNT = {"baostock": 0, "东财": 0, "新浪": 0, "腾讯": 0}


def _fmt(x, nd=2):
    try:
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return ""


def save_rows(code, rows):
    if not rows:
        return 0
    path = os.path.join(HISTORY_DIR, f"{code}.csv")
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(STD_COLS)
        for d in sorted(rows):
            w.writerow(rows[d])
    os.replace(tmp, path)
    return len(rows)


def normalize_symbol(raw):
    s = re.sub(r'[^0-9]', '', str(raw or ''))
    if len(s) != 6:
        return None, None
    if EXCLUDE_BJ and s.startswith(BJ_PREFIXES):
        return None, None
    if s.startswith('6'):
        return s, 'sh' + s
    if s.startswith(('0', '3')):
        return s, 'sz' + s
    return None, None


def run_baostock_batch(pairs):
    import baostock as bs

    # 加固：登录重试3次×10秒——手动跑时碰上服务端抖动不再一次即败
    ok = False
    for i in range(3):
        try:
            lg = bs.login()
            ok = (lg.error_code == '0')
        except Exception:
            ok = False
        if ok:
            break
        print(f"【baostock】登录失败({i + 1}/3)，10秒后重试...")
        time.sleep(10)
    if not ok:
        print("【baostock】❌ 登录失败（服务端可能夜间不可用），全体转第二轮裸接口补拉")
        return {}, [c for c, _ in pairs]

    success = {}
    failed = []
    total = len(pairs)
    start_time = time.time()

    try:
        for i, (code, symbol) in enumerate(pairs, 1):
            bs_code = f"{symbol[:2]}.{symbol[2:]}"
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=START_DATE_BS, end_date=END_DATE_BS,
                    frequency="d", adjustflag="2")
                rows = {}
                while rs.error_code == '0' and rs.next():
                    r = rs.get_row_data()
                    if not r[4] or not r[5]:
                        continue
                    try:
                        vol = int(round(float(r[5]) / 100))
                    except ValueError:
                        continue
                    rows[r[0]] = [r[0], _fmt(r[1]), _fmt(r[4]), _fmt(r[2]), _fmt(r[3]),
                                  vol, _fmt(r[6]), _fmt(r[7]), _fmt(r[8])]
                if rows:
                    success[code] = rows
                else:
                    failed.append(code)
            except Exception:
                failed.append(code)

            if i % PROGRESS_EVERY == 0:
                elapsed = time.time() - start_time
                avg = elapsed / i
                remain = avg * (total - i)
                print(f"  【baostock】进度：{i}/{total} "
                      f"（{round(i/total*100, 1)}%），成功{len(success)} 失败{len(failed)}，"
                      f"已用 {round(elapsed/60, 1)} 分钟，预计剩余 {round(remain/60, 1)} 分钟")
    finally:
        bs.logout()

    print(f"【baostock】第一轮完成：成功 {len(success)}/{total}，失败 {len(failed)}")
    return success, failed


def fetch_em(symbol):
    mkt = '1' if symbol.startswith('sh') else '0'
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get?'
           f'secid={mkt}.{symbol[2:]}&fields1=f1,f2,f3,f4,f5,f6'
           f'&fields2=f51,f52,f53,f54,f55,f56,f57,f59,f61'
           f'&klt=101&fqt=1&end=20500101&lmt={DATALEN}')
    j = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    rows = (j.get('data') or {}).get('klines') or []
    out = {}
    for line in rows:
        p = line.split(',')
        if len(p) < 9:
            continue
        try:
            vol = int(round(float(p[5])))
        except ValueError:
            continue
        out[p[0]] = [p[0], _fmt(p[1]), _fmt(p[2]), _fmt(p[3]), _fmt(p[4]),
                     vol, _fmt(p[6]), _fmt(p[7]), _fmt(p[8])]
    return out


def fetch_tx(symbol):
    url = ('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?'
           f'param={symbol},day,,,{DATALEN},qfq')
    j = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    node = (j.get('data') or {}).get(symbol) or {}
    rows = node.get('qfqday') or node.get('day') or []
    out = {}
    prev_c = None
    for r in rows:
        if len(r) < 6:
            continue
        try:
            vol = int(round(float(r[5])))
            c = float(r[2])
        except (TypeError, ValueError):
            continue
        pct = _fmt((c / prev_c - 1) * 100) if prev_c else ""
        out[r[0]] = [r[0], _fmt(r[1]), _fmt(r[2]), _fmt(r[3]), _fmt(r[4]),
                     vol, "", pct, ""]
        prev_c = c
    return out


def fetch_sina(symbol):
    url = ('https://quotes.sina.cn/cn/api/jsonp_v2.php/var/CN_MarketDataService.'
           f'getKLineData?symbol={symbol}&scale=240&ma=no&datalen={DATALEN}')
    t = requests.get(url, headers=UA, timeout=TIMEOUT).text
    m = re.search(r'\[.*\]', t, re.S)
    if not m:
        return {}
    out = {}
    prev_c = None
    for d in json.loads(m.group(0)):
        try:
            vol = int(round(float(d['volume']) / 100))
            c = float(d['close'])
        except (KeyError, TypeError, ValueError):
            continue
        pct = _fmt((c / prev_c - 1) * 100) if prev_c else ""
        out[d['day']] = [d['day'], _fmt(d['open']), _fmt(d['close']),
                         _fmt(d['high']), _fmt(d['low']), vol, "", pct, ""]
        prev_c = c
    return out


def fetch_one_round2(code, symbol):
    with LOCK:
        sources = [(n, f) for n, f in (("东财", fetch_em), ("腾讯", fetch_tx), ("新浪", fetch_sina))
                   if not STATE["dead"][n]]

    for name, fn in sources:
        for i in range(RETRY_TIMES + 1):
            try:
                rows = fn(symbol)
                if not rows:
                    return None
                with LOCK:
                    STATE["consec_fail"][name] = 0
                    SRC_COUNT[name] += 1
                return rows
            except Exception:
                if i < RETRY_TIMES:
                    time.sleep(1)
        with LOCK:
            STATE["consec_fail"][name] += 1
            if STATE["consec_fail"][name] >= CIRCUIT_LIMIT and not STATE["dead"][name]:
                STATE["dead"][name] = True
                print(f"\n【⚡熔断】{name}源连续失败{CIRCUIT_LIMIT}只，本次运行弃用该源\n")
    return "FAIL"


def get_all_stock_codes():
    def to_pairs(df, col):
        return sorted({(c, s) for c, s in (normalize_symbol(x) for x in df[col].tolist()) if c})

    import akshare as ak
    print("【股票清单】正在获取全市场A股代码...")

    for i in range(2):
        try:
            df = ak.stock_zh_a_spot_em()
            pairs = to_pairs(df, "代码")
            print(f"【股票清单】✅ 数据源①（东财）获取成功，共 {len(pairs)} 只")
            return pairs
        except Exception as e:
            print(f"【股票清单】数据源①（东财）第{i+1}次失败：{e}")
            time.sleep(5)

    try:
        print("【股票清单】切换数据源②（交易所官网）...")
        df = ak.stock_info_a_code_name()
        pairs = to_pairs(df, "code")
        print(f"【股票清单】✅ 数据源②（交易所官网）获取成功，共 {len(pairs)} 只")
        return pairs
    except Exception as e:
        print(f"【股票清单】数据源②失败：{e}")

    try:
        print("【股票清单】切换数据源③（新浪）...")
        df = ak.stock_zh_a_spot()
        pairs = to_pairs(df, "代码")
        print(f"【股票清单】✅ 数据源③（新浪）获取成功，共 {len(pairs)} 只")
        return pairs
    except Exception as e:
        print(f"【股票清单】数据源③失败：{e}")

    print("【股票清单】❌ 三个数据源全部失败，无法继续！")
    return None


def get_dir_size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return round(total / 1024 / 1024, 1)


def clean_overwrite_dir():
    print("【覆盖模式】检测到 FORCE_OVERWRITE=1，将全量重拉覆盖旧数据...")
    removed = 0
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv"):
            os.remove(os.path.join(HISTORY_DIR, f))
            removed += 1
    print(f"【覆盖模式】已清空旧股票文件 {removed} 个，开始全量重拉")


if __name__ == "__main__":
    print("=" * 60)
    print("===== A股历史数据初始化开始（baostock主源·双轮版 v9） =====")
    print(f"===== 当前时间：{now} =====")
    print(f"===== 拉取范围：{HISTORY_YEARS} 年（{START_DATE_BS.replace('-', '')} ~ {END_DATE_BS.replace('-', '')}）=====")
    print(f"===== 复权方式：前复权（qfq）=====")
    print(f"===== 数据目录：{HISTORY_DIR} =====")
    print(f"===== 运行模式：{'🔁 覆盖模式（全量重拉）' if FORCE_OVERWRITE else '📦 续传模式（跳过已下载）'} =====")
    print("=" * 60)

    if FORCE_OVERWRITE:
        clean_overwrite_dir()

    all_pairs = get_all_stock_codes()
    if all_pairs is None:
        exit(1)

    already = set()
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv"):
            already.add(f.replace(".csv", ""))
    todo_pairs = [(c, s) for c, s in all_pairs if c not in already]

    print(f"\n【进度统计】")
    print(f"  全部股票（不含北交所）：{len(all_pairs)} 只")
    print(f"  已下载：{len(already)} 只（自动跳过）")
    print(f"  本次待下载：{len(todo_pairs)} 只")
    print(f"  策略：第一轮 baostock 单线程批量（约11分钟）→ 第二轮裸接口5并发补拉失败者\n")

    success_count = 0
    empty_count = 0
    fail_list = []
    start_time = time.time()

    print("【第一轮】baostock 单线程批量拉取中...")
    bs_success, bs_failed = run_baostock_batch(todo_pairs)

    for code, rows in bs_success.items():
        if save_rows(code, rows):
            success_count += 1
            SRC_COUNT["baostock"] += 1

    round2_pairs = [(c, s) for c, s in todo_pairs if c in set(bs_failed)]
    if round2_pairs:
        print(f"\n【第二轮】补拉 {len(round2_pairs)} 只（东财/腾讯/新浪，并发{MAX_WORKERS}）...")
        done2 = 0
        r2_start = time.time()

        def worker(pair):
            code, symbol = pair
            if os.path.exists(os.path.join(HISTORY_DIR, f"{code}.csv")):
                return "skip"
            result = fetch_one_round2(code, symbol)
            if isinstance(result, dict):
                save_rows(code, result)
                return "ok"
            elif result is None:
                return "empty"
            return "fail"

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
 
