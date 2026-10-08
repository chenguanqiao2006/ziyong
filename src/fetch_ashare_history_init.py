"""
A股历史数据初始化脚本（baostock主源·双轮版 v7，2026-10-08）
架构（借鉴用户旧脚本 fetch_quotes.py 的成熟经验）：
  第一轮（主源）：baostock 单线程批量拉取
    - 免费、无需注册、无限流，走私有协议不受海外IP拦截影响
    - 一次 bs.login() 长连接，逐只 query，约130ms/只，全市场约11分钟
    - ⚠️ baostock 非线程安全，必须单线程使用
    - 字段直接支持全部9列：date/open/close/high/low/volume/amount/pctChg/turn
  第二轮（补源）：baostock 失败的股票用 5并发裸接口补拉
    - 东财 push2his（带Referer，海外IP可通）→ 腾讯 → 新浪，逐源降级+熔断

保持不变：
- 输出：data/history/股票代码.csv，9列
  date/open/close/high/low/volume/amount/pct_chg/turnover
- 断点续传、覆盖模式(FORCE_OVERWRITE=1)、北交所剔除、北向资金存档、体积监控

v7修复：股票清单筛选逻辑bug——v6中北交所规范化返回(None,None)元组，
  旧写法filter(None,...)无法过滤元组导致排序报错
  'NoneType' and 'str'，改用集合推导式 if c 显式剔除。
"""
import os
import re
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd

# ===================== 路径定位（基于脚本自身位置） =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
# ======================================================================

# ===================== 可配置参数区 =====================
HISTORY_YEARS = 2        # 拉取几年历史
DATALEN = 550            # 裸接口单次请求K线根数（2年约490根，留余量）
MAX_WORKERS = 5          # 第二轮补拉并发数
RETRY_TIMES = 2          # 第二轮单源单只重试次数
PROGRESS_EVERY = 500     # baostock 主源进度打印间隔（它很快，500只一打）
PROGRESS_EVERY_2 = 100   # 第二轮补拉进度打印间隔
CIRCUIT_LIMIT = 15       # 第二轮单源连续失败熔断阈值
EXCLUDE_BJ = True        # 剔除北交所
BJ_PREFIXES = ("43", "83", "87", "88", "92")
# =======================================================

FORCE_OVERWRITE = os.environ.get("FORCE_OVERWRITE", "0") == "1"

os.makedirs(HISTORY_DIR, exist_ok=True)

now = datetime.now()
START_DATE_BS = (now - timedelta(days=365 * HISTORY_YEARS)).strftime("%Y-%m-%d")  # baostock用横杠
END_DATE_BS = now.strftime("%Y-%m-%d")

STD_COLS = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]

UA = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Referer': 'https://quote.eastmoney.com/',
}
TIMEOUT = 15

# 第二轮并发安全状态
LOCK = threading.Lock()
STATE = {"consec_fail": {"东财": 0, "新浪": 0, "腾讯": 0},
         "dead": {"东财": False, "新浪": False, "腾讯": False}}
SRC_COUNT = {"baostock": 0, "东财": 0, "新浪": 0, "腾讯": 0}


# ===================== 代码规范化 =====================
def normalize_symbol(raw):
    """'600519'/'sh600519' → ('600519','sh600519')；北交所或异常代码返回(None,None)"""
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


# ===================== 第一轮：baostock 批量拉取（单线程） =====================
def run_baostock_batch(pairs):
    """
    baostock 单线程批量拉取全部股票。
    返回 (成功DataFrame字典, 失败代码列表)
    """
    import baostock as bs

    lg = bs.login()
    if lg.error_code != '0':
        print(f"【baostock】登录失败：{lg.error_msg}")
        return {}, [c for c, _ in pairs]

    success = {}
    failed = []
    total = len(pairs)
    start_time = time.time()

    try:
        for i, (code, symbol) in enumerate(pairs, 1):
            # baostock 代码格式：sh.600519 / sz.000688
            bs_code = f"{symbol[:2]}.{symbol[2:]}"
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=START_DATE_BS, end_date=END_DATE_BS,
                    frequency="d", adjustflag="2")  # 2=前复权
                rows = []
                while rs.error_code == '0' and rs.next():
                    rows.append(rs.get_row_data())
                if rows:
                    df = pd.DataFrame(rows, columns=rs.fields)
                    df = df.rename(columns={
                        "date": "date", "open": "open", "close": "close",
                        "high": "high", "low": "low", "volume": "volume",
                        "amount": "amount", "pctChg": "pct_chg", "turn": "turnover"})
                    df = df[STD_COLS]
                    for c in STD_COLS[1:]:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    # baostock volume 单位是"股"，统一为"手"
                    df["volume"] = df["volume"] / 100
                    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
                    success[code] = df
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


# ===================== 第二轮：裸接口补拉（5并发） =====================
def fetch_em(symbol):
    """补源①：东财 push2his（带Referer，海外IP实测可通）"""
    mkt = '1' if symbol.startswith('sh') else '0'
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get?'
           f'secid={mkt}.{symbol[2:]}&fields1=f1,f2,f3,f4,f5,f6'
           f'&fields2=f51,f52,f53,f54,f55,f56,f57,f59,f61'
           f'&klt=101&fqt=1&end=20500101&lmt={DATALEN}')
    j = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    rows = (j.get('data') or {}).get('klines') or []
    out = []
    for line in rows:
        p = line.split(',')
        if len(p) >= 9:
            out.append({"date": p[0], "open": p[1], "close": p[3],
                        "high": p[4], "low": p[5], "volume": p[6],
                        "amount": p[7], "pct_chg": p[8], "turnover": None})
    return out


def fetch_tx(symbol):
    """补源②：腾讯"""
    url = ('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?'
           f'param={symbol},day,,,{DATALEN},qfq')
    j = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    node = (j.get('data') or {}).get(symbol) or {}
    rows = node.get('qfqday') or node.get('day') or []
    out = []
    for r in rows:
        if len(r) >= 6:
            out.append({"date": r[0], "open": r[1], "close": r[2],
                        "high": r[3], "low": r[4], "volume": r[5],
                        "amount": r[6] if len(r) > 6 and r[6] else None,
                        "pct_chg": None, "turnover": None})
    for i in range(1, len(out)):
        try:
            prev_c = float(out[i-1]["close"])
            cur_c = float(out[i]["close"])
            out[i]["pct_chg"] = round((cur_c - prev_c) / prev_c * 100, 2)
        except (ValueError, ZeroDivisionError):
            pass
    return out


def fetch_sina(symbol):
    """补源③：新浪"""
    url = ('https://quotes.sina.cn/cn/api/jsonp_v2.php/var/CN_MarketDataService.'
           f'getKLineData?symbol={symbol}&scale=240&ma=no&datalen={DATALEN}')
    t = requests.get(url, headers=UA, timeout=TIMEOUT).text
    m = re.search(r'\[.*\]', t, re.S)
    if not m:
        return []
    out = []
    for d in json.loads(m.group(0)):
        try:
            out.append({"date": d['day'], "open": d['open'], "close": d['close'],
                        "high": d['high'], "low": d['low'], "volume": d['volume'],
                        "amount": None, "pct_chg": None, "turnover": None})
        except (KeyError, TypeError, ValueError):
            continue
    for i in range(1, len(out)):
        try:
            prev_c = float(out[i-1]["close"])
            cur_c = float(out[i]["close"])
            out[i]["pct_chg"] = round((cur_c - prev_c) / prev_c * 100, 2)
        except (ValueError, ZeroDivisionError):
            pass
    return out


def fetch_one_round2(code, symbol):
    """第二轮逐源降级抓取单只。返回 DataFrame / None(空) / 'FAIL'"""
    with LOCK:
        sources = [(n, f) for n, f in (("东财", fetch_em), ("腾讯", fetch_tx), ("新浪", fetch_sina))
                   if not STATE["dead"][n]]

    last_err = None
    for name, fn in sources:
        for i in range(RETRY_TIMES + 1):
            try:
                rows = fn(symbol)
                if not rows:
                    return None
                df = pd.DataFrame(rows)
                for c in STD_COLS[1:]:
                    if c not in df.columns:
                        df[c] = None
                df = df[STD_COLS]
                for c in STD_COLS[1:]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                if name == "新浪":
                    df["volume"] = df["volume"] / 100
                df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
                with LOCK:
                    STATE["consec_fail"][name] = 0
                    SRC_COUNT[name] += 1
                return df
            except Exception as e:
                last_err = e
                if i < RETRY_TIMES:
                    time.sleep(1)
        with LOCK:
            STATE["consec_fail"][name] += 1
            if STATE["consec_fail"][name] >= CIRCUIT_LIMIT and not STATE["dead"][name]:
                STATE["dead"][name] = True
                print(f"\n【⚡熔断】{name}源连续失败{CIRCUIT_LIMIT}只，本次运行弃用该源\n")
    return "FAIL"


# ===================== 股票清单（三级降级，剔除北交所） =====================
def get_all_stock_codes():
    """返回 [(纯代码, 带前缀代码), ...]。降级：东财→交易所官网→新浪"""

    def to_pairs(df, col):
        """★v7修复★ 先规范化出(代码,前缀代码)，再 if c 剔除北交所的(None,None)"""
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


# ===================== 北向资金 / 工具 =====================
def save_north_fund_history():
    import akshare as ak
    print("\n【北向资金】正在拉取全部历史...")
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is not None and len(df) > 0:
            save_path = os.path.join(HISTORY_DIR, "north_fund_ALL_HISTORY.csv")
            df.to_csv(save_path, index=False, encoding="utf-8-sig")
            print(f"【北向资金】✅ 历史存档保存成功：{save_path}，共 {len(df)} 条")
        else:
            print("【北向资金】⚠️ 接口返回空数据，跳过")
    except Exception as e:
        print(f"【北向资金】拉取失败（不影响股票下载）：{e}")


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
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
            os.remove(os.path.join(HISTORY_DIR, f))
            removed += 1
    print(f"【覆盖模式】已清空旧股票文件 {removed} 个，开始全量重拉")


# ===================== 主流程 =====================
if __name__ == "__main__":
    print("=" * 60)
    print("===== A股历史数据初始化开始（baostock主源·双轮版） =====")
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
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
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

    # ============ 第一轮：baostock 批量 ============
    print("【第一轮】baostock 单线程批量拉取中...")
    bs_success, bs_failed = run_baostock_batch(todo_pairs)

    # 落盘第一轮成功的
    for code, df in bs_success.items():
        save_path = os.path.join(HISTORY_DIR, f"{code}.csv")
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        success_count += 1
        SRC_COUNT["baostock"] += 1

    # ============ 第二轮：裸接口补拉 baostock 失败的 ============
    round2_pairs = [(c, s) for c, s in todo_pairs if c in set(bs_failed)]
    if round2_pairs:
        print(f"\n【第二轮】补拉 {len(round2_pairs)} 只（东财/腾讯/新浪，并发{MAX_WORKERS}）...")
        done2 = 0
        r2_start = time.time()

        def worker(pair):
            code, symbol = pair
            save_path = os.path.join(HISTORY_DIR, f"{code}.csv")
            if os.path.exists(save_path):
                return "skip"
            result = fetch_one_round2(code, symbol)
            if isinstance(result, pd.DataFrame):
                result.to_csv(save_path, index=False, encoding="utf-8-sig")
                return "ok"
            elif result is None:
                return "empty"
            return "fail"

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(worker, p): p[0] for p in round2_pairs}
            for fut in as_completed(futs):
                code = futs[fut]
                try:
                    r = fut.result()
                except Exception as e:
                    print(f"  {code} 异常: {type(e).__name__}: {e}")
                    r = "fail"
                done2 += 1
                if r == "ok":
                    success_count += 1
                elif r == "empty":
                    empty_count += 1
                elif r == "fail":
                    fail_list.append(code)
                if done2 % PROGRESS_EVERY_2 == 0:
                    elapsed = time.time() - r2_start
                    print(f"  【第二轮】进度：{done2}/{len(round2_pairs)}，"
                          f"成功{success_count} 空{empty_count} 败{len(fail_list)}")
    else:
        print("\n【第二轮】baostock 全部成功，无需补拉 🎉")

    save_north_fund_history()

    total_time = round((time.time() - start_time) / 60, 1)
    dir_size = get_dir_size_mb(os.path.join(PROJECT_ROOT, "data"))
    print("\n" + "=" * 60)
    print("===== ✅ 历史数据初始化完成 =====")
    print(f"  本次新下载：{success_count} 只")
    print(f"  各数据源：baostock{SRC_COUNT['baostock']} 东财{SRC_COUNT['东财']} "
          f"腾讯{SRC_COUNT['腾讯']} 新浪{SRC_COUNT['新浪']}")
    print(f"  空数据（退市/长期停牌，正常）：{empty_count} 只")
    print(f"  之前已下载（跳过）：{len(already)} 只")
    print(f"  失败：{len(fail_list)} 只")
    if fail_list:
        print(f"  失败清单（重跑本脚本会自动重试）：{fail_list[:50]}{'...' if len(fail_list) > 50 else ''}")
    print(f"  总耗时：{total_time} 分钟")
    print(f"  📦 data目录总体积：{dir_size} MB")
    if dir_size > 800:
        print("  ⚠️⚠️⚠️ 警告：data目录已超 800MB，接近GitHub仓库1GB软限制！")
    print("=" * 60)
    print("💡 提示：如有失败，重跑一次即可（自动跳过成功的，只重试失败的）")
