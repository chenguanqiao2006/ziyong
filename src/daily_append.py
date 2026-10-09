#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股每日增量更新
================
由 .github/workflows/daily.yml 每个交易日自动触发（北京时间17:45），也可手动 Run。

流程：
  1. 扫描 data/history/ 下全部存量CSV，记录每只股票的最后日期/收盘价
  2. 主源 baostock 单线程批量查询（只查最近30个自然日，避免全量历史传输）
     —— 双层看门狗：socket默认30秒超时 + 单只120秒硬超时，绝不挂死
     —— 50分钟总时间预算：超时后剩余股票自动转兜底
  3. baostock 失败/超时的股票 -> 新浪 -> 腾讯 逐级兜底（5线程并发）
  4. 全部拉完后统一写盘：只追加 date > 存量最后日期 的新行（幂等，重跑不重复）

数据口径（与月度刷新严格一致，保证"每日追加的行"和"月度重写的行"字节级相同，
  未发生除权事件的股票月度重写后 diff 为零，不产生 git 体积膨胀）：
  date,open,close,high,low,volume(手),amount(元),pct_chg(%官方),turnover(%)
  - 成交量：baostock/新浪单位是"股"需/100，腾讯已是"手"
  - 涨跌幅：优先用baostock官方pctChg；新浪/腾讯兜底行才本地接力计算
  - 停牌日（close/volume为空）跳过，与月度刷新口径一致
  - 读取兼容带/不带BOM的CSV（utf-8-sig两种都能读）
"""

import csv
import glob
import json
import os
import re
import signal
import socket
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
BS_TIME_BUDGET = 50 * 60   # baostock总时间预算50分钟（job超时90分钟，留足余量）
SOCK_TIMEOUT = 30          # 看门狗层1：socket默认超时（秒）
PER_STOCK_TIMEOUT = 120    # 看门狗层2：单只股票硬超时（秒），卡死即放弃转兜底
HANG_ABORT = 3             # 连续挂死3次 → 判定baostock连接已坏，整体转兜底

# 成交量单位换算：统一转为"手"
VOL_DIV_BS = 100
VOL_DIV_SINA = 100
VOL_DIV_TX = 1

CST = timezone(timedelta(hours=8))  # 北京时间

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST_DIR = os.path.join(ROOT, "data", "history")

CSV_HEADER = ["date", "open", "close", "high", "low",
              "volume", "amount", "pct_chg", "turnover"]

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


# ============================== 工具 ==============================
def code_to_bs(code: str) -> str:
    if code.startswith(("6", "9")):
        return "sh." + code
    if code.startswith(("4", "8")):
        return "bj." + code
    return "sz." + code


def code_to_sym(code: str) -> str:
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
        return v if v == v else None
    except (TypeError, ValueError):
        return None


# ============================== 存量扫描 ==============================
def scan_existing() -> dict:
    stocks = {}
    for path in sorted(glob.glob(os.path.join(HIST_DIR, "*.csv"))):
        code = os.path.splitext(os.path.basename(path))[0]
        info = {"path": path, "last_date": "", "last_close": ""}
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
            if not df.empty and "date" in df.columns and str(df.iloc[-1]["date"]).strip():
                info["last_date"] = str(df.iloc[-1]["date"]).strip()
                info["last_close"] = str(df.iloc[-1].get("close", "")).strip()
        except Exception:
            pass
        stocks[code] = info
    return stocks


# ============================== 行构建 ==============================
def build_new_rows(raw_rows, last_date, last_close, vol_div=1):
    if not last_date:
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
    if valid[-1][0] < last_date:
        return [], "none"
    if valid[-1][0] == last_date:
        return [], "latest"

    new = []
    prev_close = _to_float(last_close)
    for r in valid:
        if r[0] <= last_date:
            pc = _to_float(r[4])
            if pc:
                prev_close = pc
            continue
        cf = _to_float(r[4])
        vf = _to_float(r[5])
        if cf is None or vf is None:
            continue
        d, o, h, l, c = r[0], _fmt(r[1]), _fmt(r[2]), _fmt(r[3]), _fmt(cf)
        vol = int(round(vf / vol_div))
        amt = _fmt(r[6]) if len(r) > 6 else ""

        pct = ""
        if len(r) > 7:
     
