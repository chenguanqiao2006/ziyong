#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股每日增量更新
================
由 .github/workflows/daily.yml 每个交易日自动触发，也可手动 Run。

流程：
  1. 扫描 data/history/ 下全部存量CSV，记录每只股票的最后日期/收盘价
  2. 主源 baostock 单线程批量查询（只查最近30个自然日，避免全量历史传输）
  3. baostock 失败/超时的股票 → 新浪 → 腾讯 逐级兜底（5线程并发）
  4. 全部拉完后统一写盘：只追加 date > 存量最后日期 的新行（幂等，重跑不重复）

数据格式（与历史初始化一致）：
  date,open,close,high,low,volume(手),amount(元),pct_chg(%),turnover(%)
"""

import csv
import glob
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

# ============================== 配置 ==============================
FETCH_DAYS = 15            # 每只股票取回最近15根K线（覆盖长假+补跑场景）
LOOKBACK_DAYS = 30         # baostock查询起点：最近30个自然日（≈20个交易日）
PROGRESS_EVERY = 100       # baostock阶段每100只报一次进度
FALLBACK_PROGRESS = 500    # 兜底阶段每500只报一次
THREADS = 5                # 新浪/腾讯兜底并发线程数
REQ_TIMEOUT = 10           # HTTP超时（秒）
BS_TIME_BUDGET = 20 * 60   # baostock时间预算（秒）：超时后剩余全部转兜底

# 成交量单位换算（baostock为股需/100转手；新浪/腾讯接口已是手）
VOL_DIV_BS = 100
VOL_DIV_SINA = 1
VOL_DIV_TX = 1

CST = timezone(timedelta(hours=8))  # 北京时间

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST_DIR = os.path.join(ROOT, "data", "history")

CSV_HEADER = ["date", "open", "close", "high", "low",
              "volume", "amount", "pct_chg", "turnover"]

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


# ============================== 工具 ==============================
def code_to_bs(code: str) -> str:
    """600519 -> sh.600519"""
    if code.startswith(("6", "9")):
        return "sh." + code
    if code.startswith(("4", "8")):
        return "bj." + code
    return "sz." + code


def code_to_sym(code: str) -> str:
    """600519 -> sh600519"""
    if code.startswith(("6", "9")):
        return "sh" + code
    if code.startswith(("4", "8")):
        return "bj" + code
    return "sz" + code


def _fmt(x, nd=2):
    try:
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return ""


def _to_float(x):
    try:
        v = float(x)
        return v if v == v else None  # 过滤NaN
    except (TypeError, ValueError):
        return None


# ============================== 存量扫描 ==============================
def scan_existing() -> dict:
    """扫描全部存量CSV -> {code: {path, last_date, last_close}}"""
    stocks = {}
    for path in sorted(glob.glob(os.path.join(HIST_DIR, "*.csv"))):
        code = os.path.splitext(os.path.basename(path))[0]
        info = {"path": path, "last_date": "", "last_close": ""}
        try:
            df = pd.read_csv(path, dtype=str)
            if not df.empty and "date" in df.columns and str(df.iloc[-1]["date"]).strip():
                info["last_date"] = str(df.iloc[-1]["date"]).strip()
                info["last_close"] = str(df.iloc[-1].get("close", "")).strip()
        except Exception:
            pass  # 读不了的按"无新数据"跳过，月度全量刷新会修复
        stocks[code] = info
    return stocks


# ============================== 行构建 ==============================
def build_new_rows(raw_rows, last_date, last_close, vol_div=1):
    """
    raw_rows: [(date, open, high, low, close, volume, amount, turn)]
    vol_div: 成交量除数（baostock传100，新浪/腾讯传1）
    返回 (new_rows, status)
      status: "new"(有追加) / "latest"(已含最新) / "none"(停牌/无数据)
    """
    if not last_date:  # 存量CSV损坏/为空 → 交给月度全量修复
        return [], "none"

    valid = []
    for r in raw_rows:
        try:
            d = str(r[0]).strip()
            datetime.strptime(d, "%Y-%m-%d")
            valid.append(r)
        except Exception:
            continue
    if not valid:
        return [], "none"

    valid.sort(key=lambda r: r[0])
    if valid[-1][0] < last_date:      # 源里最新日期比存量还早 → 停牌/长期无交易
        return [], "none"
    if valid[-1][0] == last_date:     # 已含源里最新一根 → 已是最新
        return [], "latest"

    new = []
    prev_close = _to_float(last_close)
    for r in valid:
        if r[0] <= last_date:         # 旧数据只用来接力前收盘
            pc = _to_float(r[4])
            if pc:
                prev_close = pc
            continue
        d, o, h, l, c = r[0], _fmt(r[1]), _fmt(r[2]), _fmt(r[3]), _fmt(r[4])
        try:
            vol = int(round(float(r[5]) / vol_div))
        except (TypeError, ValueError):
            vol = ""
        amt = _fmt(r[6]) if len(r) > 6 else ""
        turn = _fmt(r[7]) if len(r) > 7 else ""
        cf = _to_float(c)
        pct = _fmt((cf / prev_close - 1) * 100) if (cf and prev_close) else ""
        new.append({"date": d, "open": o, "close": c, "high": h, "low": l,
                    "volume": vol, "amount": amt,
                    "pct_chg": pct, "turnover": turn})
        if cf:
            prev_close = cf
    return new, ("new" if new else "none")


# ============================== 主源 baostock ==============================
def baostock_batch(codes, stocks):
    """主源：登录后单线程批量查询，返回 (结果dict, 失败code列表)"""
    result, failed = {}, []
    try:
        import baostock as bs
    except ImportError:
        print("【baostock】未安装，全部转兜底源")
        return result, list(codes)

    # 登录（重试3次，间隔5秒）
    ok = False
    for i in range(3):
        try:
            lg = bs.login()
            ok = (lg.error_code == "0")
        except Exception:
            ok = False
        if ok:
            break
        print(f"【baostock】登录失败({i + 1}/3)，5秒后重试...")
        time.sleep(5)
    if not ok:
        print("【baostock】❌ 登录失败，全部转兜底源（新浪→腾讯）")
        return result, list(codes)

    # ★ 核心修复：只查最近30个自然日，不再空日期拉全部历史
    start = (datetime.now(CST) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    total = len(codes)
    t0 = time.time()
    print(f"【baostock】✅ 登录成功，单线程批量拉取中"
          f"（只查近{LOOKBACK_DAYS}天，预计3~5分钟）...")

    for i, code in enumerate(codes, 1):
        # ★ 时间预算：超时后剩余全部转兜底，绝不无限挂起
        if time.time() - t0 > BS_TIME_BUDGET:
            print(f"【baostock】⏰ 时间预算{BS_TIME_BUDGET // 60}分钟已到，"
                  f"剩余 {total - i + 1} 只转兜底源")
            failed.extend(codes[i - 1:])
            break
        try:
            rs = bs.query_history_k_data_plus(
                code_to_bs(code),
                "date,open,high,low,close,volume,amount,turn",
                start_date=start, end_date="",
                frequency="d", adjustflag="2")
            raw = []
            while rs.error_code == "0" and rs.next():
                raw.append(rs.get_row_data())
            if rs.error_code != "0":
                raise RuntimeError(rs.error_msg)
            new, status = build_new_rows(
                raw, stocks[code]["last_date"], stocks[code]["last_close"],
                vol_div=VOL_DIV_BS)
            result[code] = {"rows": new, "status": status, "source": "baostock"}
        except Exception:
            failed.append(code)

        # ★ 进度：每100只报一次（约13秒一行）
        if i % PROGRESS_EVERY == 0:
            el = time.time() - t0
            print(f"【baostock】进度: {i}/{total}"
                  f"（成功{len(result)} 失败{len(failed)}，用时{el:.0f}s）")

    try:
        bs.logout()
    except Exception:
        pass
    print(f"【baostock】批量完成: 成功 {len(result)}，失败转兜底 {len(failed)}")
    return result, failed


# ============================== 兜底源 ==============================
def fetch_sina(code, stocks):
    """新浪（无成交额/换手率，置空）"""
    sym = code_to_sym(code)
    url = ("https://quotes.sina.cn/cn/api/jsonp_v2.php/_=/"
           "CN_MarketDataService.getKLineData"
           f"?symbol={sym}&scale=240&ma=no&datalen={FETCH_DAYS}")
    headers = dict(UA, Referer="https://finance.sina.com.cn")
    r = requests.get(url, headers=headers, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    m = re.search(r"\[.*\]", r.text, re.S)
    if not m:
        raise ValueError("sina响应格式异常")
    data = json.loads(m.group(0))
    raw = [(d["day"], d["open"], d["high"], d["low"], d["close"], d["volume"], "", "")
           for d in data]
    new, status = build_new_rows(
        raw, stocks[code]["last_date"], stocks[code]["last_close"],
        vol_div=VOL_DIV_SINA)
    return {"rows": new, "status": status, "source": "sina"}


def fetch_tencent(code, stocks):
    """腾讯（无成交额/换手率，置空）"""
    sym = code_to_sym(code)
    url = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
           f"?param={sym},day,,,{FETCH_DAYS},qfq")
    j = requests.get(url, headers=UA, timeout=REQ_TIMEOUT).json()
    node = j["data"][sym]
    day = node.get("qfqday") or node.get("day") or []
    # 腾讯行格式: [date, open, close, high, low, volume, ...]
    raw = [(r[0], r[1], r[3], r[4], r[2], r[5], "", "") for r in day]
    new, status = build_new_rows(
        raw, stocks[code]["last_date"], stocks[code]["last_close"],
        vol_div=VOL_DIV_TX)
    return {"rows": new, "status": status, "source": "tencent"}


def run_fallback(codes, stocks, fetcher, name):
    """并发兜底，返回 (结果dict, 仍失败code列表)"""
    result, failed = {}, []
    if not codes:
        return result, failed
    total = len(codes)
    print(f"【{name}】开始兜底 {total} 只（{THREADS}线程并发）...")
    done = 0
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futs = {ex.submit(fetcher, c, stocks): c for c in codes}
        for fut in as_completed(futs):
            code = futs[fut]
            try:
                result[code] = fut.result()
            except Exception:
                failed.append(code)
            done += 1
            if done % FALLBACK_PROGRESS == 0:
                print(f"【{name}】进度: {done}/{total}"
                      f"（成功{len(result)} 失败{len(failed)}）")
    print(f"【{name}】兜底完成: 成功 {len(result)}，仍失败 {len(failed)}")
    return result, failed


# ============================== 写盘 ==============================
def append_to_csv(info, rows):
    """把新行追加到CSV末尾（调用前已保证 date > 存量最后日期，不重不漏）"""
    with open(info["path"], "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for r in rows:
            w.writerow([r[k] for k in CSV_HEADER])


# ============================== 主流程 ==============================
def main():
    now = datetime.now(CST)
    print("=" * 60)
    print(f"===== A股每日增量开始（北京时间 {now:%Y-%m-%d %H:%M}） =====")
    print("=" * 60)

    stocks = scan_existing()
    if not stocks:
        print("❌ 未找到任何存量CSV，请先运行历史数据初始化")
        sys.exit(1)
    print(f"【统计】存量CSV共 {len(stocks)} 只")
    codes = sorted(stocks)

    final = {}

    # ---------- 1. 主源 baostock ----------
    bs_res, bs_failed = baostock_batch(codes, stocks)
    final.update(bs_res)

    # ---------- 2. 兜底：新浪 ----------
    sina_res, sina_failed = run_fallback(bs_failed, stocks, fetch_sina, "新浪")
    final.update(sina_res)

    # ---------- 3. 兜底：腾讯 ----------
    tx_res, tx_failed = run_fallback(sina_failed, stocks, fetch_tencent, "腾讯")
    final.update(tx_res)

    # ---------- 4. 统一写盘（全部成功后才动文件，中途挂掉零污染） ----------
    appended = {}
    for code, res in final.items():
        if res["status"] == "new" and res["rows"]:
            append_to_csv(stocks[code], res["rows"])
            appended[code] = res["source"]
    print(f"【写盘】完成，共追加 {len(appended)} 只")

    # ---------- 5. 收尾报告 ----------
    n_new = len(appended)
    n_latest = sum(1 for r in final.values() if r["status"] == "latest")
    n_none = sum(1 for r in final.values() if r["status"] == "none")
    n_fail = len(tx_failed)
    src = {}
    for s in appended.values():
        src[s] = src.get(s, 0) + 1
    src_str = " / ".join(f"{k} {v}" for k, v in sorted(src.items())) or "-"

    print()
    print("=" * 60)
    print("✅ 每日增量完成")
    print(f"  追加数据：{n_new} 只（{src_str}）")
    print(f"  已是最新（跳过）：{n_latest} 只")
    print(f"  无新数据（停牌/假期，正常）：{n_none} 只")
    print(f"  失败：{n_fail} 只" + ("（明日自动重试补齐）" if n_fail else ""))
    if n_fail:
        print(f"  失败清单（前20只）：{tx_failed[:20]}")
    print("=" * 60)


if __name__ == "__main__":
    main()
