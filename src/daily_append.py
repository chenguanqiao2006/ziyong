#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
src/daily_append.py — A股每日增量（v1，2026-10-08）
定位：每个交易日收盘后，给 data/history/ 全部存量CSV追加最新日线
架构（基于历史初始化v7实测 + 老库fetch_quotes.py情报）：
  第0轮 baostock 机会登录：login一次（约5秒），成则单线程批量拉全市场
        （老库实测约130ms/只，且带 amount/turn 字段补齐换手率）
        败则5秒内切走，几乎零损失
  第1轮 新浪5并发补拉 baostock 失败者（v7实测贡献4431/5223，主兜底）
  第2轮 腾讯兜底（v7实测贡献753）
  东财不参与（Actions上长期不稳定）
安全设计：
  - 只追加不改历史行，drop_duplicates(date,keep=last) 后按日期排序
  - 拉最近15根K线，"日期大于CSV最后日期"过滤 → 自动跨节假日/补跑
  - 单源连续失败15只熔断
单位口径（与v7历史库一致）：
  - volume 统一为"手"：新浪/baostock 原始单位是股 → ÷100；腾讯原生就是手
  - 新浪/腾讯的 amount/turnover 拉不到 → 留空，由baostock或每月全量刷新补齐
"""
import os
import sys
import json
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# ---------- 路径（路径宪法：一律基于本文件定位） ----------
SRC_DIR = Path(__file__).resolve().parent          # src/
PROJECT_ROOT = SRC_DIR.parent                      # 仓库根
HISTORY_DIR = PROJECT_ROOT / "data" / "history"
NORTH_FILE = "north_fund_ALL_HISTORY.csv"

MAX_WORKERS = 5
RETRY_TIMES = 2
PROGRESS_EVERY = 500
CIRCUIT_LIMIT = 15
FETCH_DAYS = 15          # 每次拉最近N根，覆盖长假+补跑
BS_TIMEOUT = 30          # baostock 登录等待上限（秒）

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
      'Referer': 'https://finance.sina.com.cn/'}
TIMEOUT = 15
STD_COLS = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]

LOCK = threading.Lock()
SRC_COUNT = {"baostock": 0, "新浪": 0, "腾讯": 0}
CIRCUIT = {"新浪": 0, "腾讯": 0}
DEAD = {"新浪": False, "腾讯": False}
CST = timezone(timedelta(hours=8))
TODAY_STR = datetime.now(CST).strftime("%Y-%m-%d")


# ==================== 数据源 ====================
def code_to_bs(code):
    """600519 → sh.600519"""
    return ("sh." if code.startswith("6") else "sz.") + code


def code_to_sym(code):
    """600519 → sh600519"""
    return ("sh" if code.startswith("6") else "sz") + code


def baostock_batch(codes):
    """baostock 单线程批量。返回 (success_dict, failed_list)。
    success_dict: {code: [row_dict,...]}，row含 amount/turnover。
    登录失败或未安装 → ({}, 全部codes)。"""
    try:
        import baostock as bs
    except ImportError:
        print("【baostock】未安装，跳过第0轮")
        return {}, list(codes)
    print("【baostock】尝试登录...")
    try:
        lg = bs.login()
    except Exception as e:
        print(f"【baostock】登录异常: {type(e).__name__}: {e} → 切换新浪/腾讯")
        return {}, list(codes)
    if lg.error_code != '0':
        print(f"【baostock】登录失败: {lg.error_msg} → 切换新浪/腾讯")
        return {}, list(codes)
    print("【baostock】✅ 登录成功，单线程批量拉取中（约130ms/只）...")
    success, failed = {}, []
    total = len(codes)
    try:
        for i, code in enumerate(codes):
            try:
                rs = bs.query_history_k_data_plus(
                    code_to_bs(code),
                    "date,open,high,low,close,volume,amount,turn",
                    start_date="", end_date="",   # 空=全部，取回后尾部截取
                    frequency="d", adjustflag="2")
                rows = []
                while rs.error_code == '0' and rs.next():
                    rows.append(rs.get_row_data())
                if rows:
                    tail = rows[-FETCH_DAYS:]
                    out = []
                    for j, r in enumerate(tail):
                        d = {"date": r[0], "open": r[1], "close": r[4],
                             "high": r[2], "low": r[3], "volume": r[5],
                             "amount": r[6] if r[6] else None,
                             "turnover": r[7] if r[7] else None,
                             "pct_chg": None}
                        if j > 0:
                            try:
                                pc = float(tail[j-1][4])
                                out[j-1]["pct_chg"] = round(
                                    (float(r[4]) - pc) / pc * 100, 2)
                            except (ValueError, ZeroDivisionError):
                                pass
                        out.append(d)
                    success[code] = out
                else:
                    failed.append(code)
            except Exception:
                failed.append(code)
            if (i + 1) % 500 == 0:
                print(f"【baostock】进度: {i+1}/{total}"
                      f"（成功{len(success)} 失败{len(failed)}）")
    finally:
        try:
            bs.logout()
        except Exception:
            pass
    print(f"【baostock】批量完成: 成功{len(success)}，转交补拉{len(failed)}")
    return success, failed


def fetch_sina(code):
    """新浪日线（v7实测最稳接口）。volume原始单位=股。"""
    sym = code_to_sym(code)
    url = ('https://quotes.sina.cn/cn/api/jsonp_v2.php/var/CN_MarketDataService.'
           f'getKLineData?symbol={sym}&scale=240&ma=no&datalen={FETCH_DAYS}')
    t = requests.get(url, headers=UA, timeout=TIMEOUT).text
    m = None
    import re
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
            pc = float(out[i-1]["close"])
            out[i]["pct_chg"] = round((float(out[i]["close"]) - pc) / pc * 100, 2)
        except (ValueError, ZeroDivisionError):
            pass
    return out


def fetch_tx(code):
    """腾讯日线（兜底）。volume原生单位=手。"""
    sym = code_to_sym(code)
    url = ('https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?'
           f'param={sym},day,,,{FETCH_DAYS},qfq')
    j = requests.get(url, headers=UA, timeout=TIMEOUT).json()
    node = (j.get('data') or {}).get(sym) or {}
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
            pc = float(out[i-1]["close"])
            out[i]["pct_chg"] = round((float(out[i]["close"]) - pc) / pc * 100, 2)
        except (ValueError, ZeroDivisionError):
            pass
    return out


def append_one(path):
    """给单只CSV追增量。返回 'skip'/'ok'/'none'/'fail'。"""
    code = path.stem
    try:
        old_df = pd.read_csv(path, dtype={"date": str})
    except Exception:
        return "fail"
    if old_df.empty:
        return "fail"
    last_date = str(old_df["date"].iloc[-1])
    if last_date >= TODAY_STR:
        return "skip"

    rows = None
    src = None
    for name, fn in (("新浪", fetch_sina), ("腾讯", fetch_tx)):
        if DEAD[name]:
            continue
        got = None
        for k in range(RETRY_TIMES + 1):
            try:
                got = [r for r in fn(code) if r["date"] > last_date]
                if got:
                    break
                break           # 拉到但无新数据（停牌），不重试
            except Exception:
                if k < RETRY_TIMES:
                    time.sleep(1)
        with LOCK:
            if got:
                CIRCUIT[name] = 0
                rows, src = got, name
                break
            CIRCUIT[name] += 1
            if CIRCUIT[name] >= CIRCUIT_LIMIT and not DEAD[name]:
                DEAD[name] = True
                print(f"\n【⚡熔断】{name}源连续失败{CIRCUIT_LIMIT}只，本次弃用\n")

    if rows is None:
        return "none"
    new_df = pd.DataFrame(rows)
    for c in STD_COLS[1:]:
        if c not in new_df.columns:
            new_df[c] = None
    new_df = new_df[STD_COLS]
    for c in STD_COLS[1:]:
        new_df[c] = pd.to_numeric(new_df[c], errors="coerce")
    if src == "新浪":
        new_df["volume"] = new_df["volume"] / 100.0    # 股→手
    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="last")
    merged = merged.sort_values("date").reset_index(drop=True)
    merged.to_csv(path, index=False, encoding="utf-8-sig")
    with LOCK:
        SRC_COUNT[src] += 1
    return "ok"


# ==================== 主流程 ====================
def main():
    now = datetime.now(CST)
    print("=" * 60)
    print(f"===== A股每日增量开始（北京时间 {now:%Y-%m-%d %H:%M}） =====")
    print("=" * 60)

    paths = sorted(p for p in HISTORY_DIR.glob("*.csv") if p.name != NORTH_FILE)
    codes = [p.stem for p in paths]
    print(f"【统计】存量CSV共 {len(paths)} 只\n")

    start = time.time()
    stats = {"ok": 0, "skip": 0, "none": 0, "fail": 0}
    fail_list = []

    # ---- 第0轮：baostock 机会登录 ----
    bs_success, rest = baostock_batch(codes)
    for code, rows in bs_success.items():
        try:
            path = HISTORY_DIR / f"{code}.csv"
            old_df = pd.read_csv(path, dtype={"date": str})
            last_date = str(old_df["date"].iloc[-1])
            rows = [r for r in rows if r["date"] > last_date]
            if not rows:
                stats["skip"] += 1
                continue
            new_df = pd.DataFrame(rows)
            for c in STD_COLS[1:]:
                if c not in new_df.columns:
                    new_df[c] = None
            new_df = new_df[STD_COLS]
            for c in STD_COLS[1:]:
                new_df[c] = pd.to_numeric(new_df[c], errors="coerce")
            new_df["volume"] = new_df["volume"] / 100.0    # 股→手
            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["date"], keep="last")
            merged = merged.sort_values("date").reset_index(drop=True)
            merged.to_csv(path, index=False, encoding="utf-8-sig")
            stats["ok"] += 1
            SRC_COUNT["baostock"] += 1
        except Exception as e:
            print(f"  {code} baostock落盘异常: {type(e).__name__}: {e}")
            rest.append(code)

    # ---- 第1+2轮：新浪→腾讯 5并发补拉 ----
    rest_paths = [HISTORY_DIR / f"{c}.csv" for c in rest]
    if rest_paths:
        print(f"\n【补拉】新浪→腾讯 5并发处理 {len(rest_paths)} 只...\n")
        done = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(append_one, p): p for p in rest_paths}
            for fut in as_completed(futs):
                code = futs[fut].stem
                try:
                    r = fut.result()
                except Exception as e:
                    print(f"  {code} 异常: {type(e).__name__}: {e}")
                    r = "fail"
                done += 1
                stats[r] = stats.get(r, 0) + 1
                if r == "fail":
                    fail_list.append(code)
                if done % PROGRESS_EVERY == 0:
                    m = round((time.time() - start) / 60, 1)
                    print(f"【补拉进度】{done}/{len(rest_paths)}，"
                          f"更新{stats['ok']} 最新{stats['skip']} "
                          f"无新数据{stats['none']} 失败{stats['fail']}"
                          f"（已用{m}分钟）")

    total_min = round((time.time() - start) / 60, 1)
    print("\n" + "=" * 60)
    print("===== ✅ 每日增量完成 =====")
    print(f"  追加数据：{stats['ok']} 只"
          f"（baostock {SRC_COUNT['baostock']} / 新浪 {SRC_COUNT['新浪']} / 腾讯 {SRC_COUNT['腾讯']}）")
    print(f"  已是最新（跳过）：{stats['skip']} 只")
    print(f"  无新数据（停牌/假期，正常）：{stats['none']} 只")
    print(f"  失败：{stats['fail']} 只")
    if fail_list:
        print(f"  失败清单(前20): {', '.join(fail_list[:20])}")
    print(f"  总耗时：{total_min} 分钟")
    # data目录体积监控
    total_mb = sum(f.stat().st_size for f in HISTORY_DIR.rglob('*') if f.is_file()) / 1024 / 1024
    print(f"  📦 data/history 体积：{total_mb:.1f} MB"
          + (" ⚠️ 超过警戒线800MB！" if total_mb > 800 else "（健康）"))
    print("=" * 60)
    return 0 if stats["fail"] == 0 else 0   # 失败不阻断提交，明天自动重试


if __name__ == "__main__":
    sys.exit(main())
