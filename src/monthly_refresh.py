#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股月度全量刷新
================
由 .github/workflows/monthly_refresh.yml 每月1日北京时间18:00自动触发（也可手动Run）。
全库重写（近2年窗口、baostock官方pctChg/turnover），与每日增量字节级一致。
安全设计：断点续跑（月度进度文件）+ 原子写盘（tmp+os.replace）
  + 双层看门狗（socket 30秒 + 单只180秒硬超时）+ checkpoint中途提交。
"""

import csv
import glob
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import baostock as bs

# ============================== 配置 ==============================
PROGRESS_EVERY = 100
CHECKPOINT_EVERY = 2000    # 每2000只checkpoint提交一次（防job中途崩丢全部进度；
                           # git按内容去重，字节未变的文件不产生体积膨胀）
TIME_BUDGET = 240 * 60
SOCK_TIMEOUT = 30
PER_STOCK_TIMEOUT = 180
LOGIN_RETRY = 5            # 5次×60秒：扛住服务端最长5分钟的瞬时故障
                           # （凌晨/维护时段连5分钟都扛不住的话，本来就该放弃）
KEEP_YEARS = 2
QUERY_BUFFER_DAYS = 15
CST = timezone(timedelta(hours=8))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
HIST_DIR = os.path.join(DATA_DIR, "history")
os.makedirs(HIST_DIR, exist_ok=True)

CSV_HEADER = ["date", "open", "close", "high", "low",
              "volume", "amount", "pct_chg", "turnover"]


def code_to_bs(code: str) -> str:
    if code.startswith(("6", "9")):
        return "sh." + code
    if code.startswith(("4", "8")):
        return "bj." + code
    return "sz." + code


def _fmt(x, nd=2):
    try:
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return ""


def keep_window() -> tuple:
    if not KEEP_YEARS:
        return "", ""
    now = datetime.now(CST)
    cutoff = (now - timedelta(days=365 * KEEP_YEARS)).strftime("%Y-%m-%d")
    start = (now - timedelta(days=365 * KEEP_YEARS + QUERY_BUFFER_DAYS)).strftime("%Y-%m-%d")
    return start, cutoff


def _alarm_handler(signum, frame):
    raise TimeoutError("per-stock watchdog fired")


def get_all_stocks() -> set:
    codes = set()
    rs = bs.query_stock_basic()
    if rs.error_code != "0":
        raise RuntimeError(f"查询股票清单失败: {rs.error_msg}")
    while rs.next():
        r = rs.get_row_data()
        if r[4] != "1" or r[3]:      # 非股票类型 / 已退市 → 跳过
            continue
        codes.add(r[0].split(".")[-1])
    return codes


def refresh_one(code: str) -> int:
    start, cutoff = keep_window()
    rs = bs.query_history_k_data_plus(
        code_to_bs(code),
        "date,open,high,low,close,volume,amount,pctChg,turn",
        start_date=start, end_date="",
        frequency="d", adjustflag="2")
    if rs.error_code != "0":
        raise RuntimeError(rs.error_msg)

    rows = []
    while rs.next():
        r = rs.get_row_data()
        if not r[4] or not r[5]:             # 停牌日（close/volume为空）跳过
            continue
        try:
            vol = int(round(float(r[5]) / 100))   # 股 → 手
        except ValueError:
            continue
        rows.append([r[0], _fmt(r[1]), _fmt(r[4]), _fmt(r[2]), _fmt(r[3]),
                     vol, _fmt(r[6]), _fmt(r[7]), _fmt(r[8])])
    if not rows:
        return 0

    if cutoff:
        rows = [r for r in rows if r[0] >= cutoff]
        if not rows:
            return 0

    path = os.path.join(HIST_DIR, code + ".csv")
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8-sig") as f:   # 原子写盘
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        w.writerows(rows)
    os.replace(tmp, path)
    return len(rows)


def checkpoint_commit(msg: str):
    try:
        subprocess.run(["git", "add", "data"], cwd=ROOT,
                       check=True, capture_output=True)
        r = subprocess.run(["git", "commit", "-m", msg], cwd=ROOT,
                           capture_output=True, text=True)
        if r.returncode == 0:
            subprocess.run(["git", "pull", "--rebase"], cwd=ROOT,
                           capture_output=True, timeout=120)
            subprocess.run(["git", "push"], cwd=ROOT,
                           capture_output=True, timeout=120)
    except Exception as e:
        print(f"【checkpoint】git提交跳过（不影响数据，收尾步骤会再提交）: {e}")


def main():
    now = datetime.now(CST)
    month_tag = f"{now:%Y-%m}"
    done_path = os.path.join(DATA_DIR, f".monthly_refresh_{month_tag}.done")

    print("=" * 60)
    print(f"===== A股月度全量刷新开始（北京时间 {now:%Y-%m-%d %H:%M}） =====")
    print(f"===== 数据深度：{'近%d年' % KEEP_YEARS if KEEP_YEARS else '上市以来全部'} =====")
    print("=" * 60)

    socket.setdefaulttimeout(SOCK_TIMEOUT)

    ok = False
    for i in range(LOGIN_RETRY):
        try:
            lg = bs.login()
            ok = (lg.error_code == "0")
        except Exception:
            ok = False
        if ok:
            break
        print(f"【baostock】登录失败({i + 1}/{LOGIN_RETRY})，60秒后重试...")
        time.sleep(60)
    if not ok:
        print("【baostock】❌ 登录失败（服务端可能处于夜间不可用时段），本次放弃")
        sys.exit(1)
    print("【baostock】✅ 登录成功")

    use_alarm = False
    old_handler = None
    try:
        old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        use_alarm = True
    except (ValueError, AttributeError):
        pass

    try:
        all_codes = get_all_stocks()
    except Exception as e:
        print(f"❌ 获取股票清单失败: {e}")
        sys.exit(1)
    existing = {os.path.splitext(os.path.basename(p))[0]
                for p in glob.glob(os.path.join(HIST_DIR, "*.csv"))}
    codes = sorted(all_codes | existing)
    new_codes = sorted(all_codes - existing)
    print(f"【统计】baostock在市 {len(all_codes)} 只 | 存量 {len(existing)} 只 | "
          f"合并去重 {len(codes)} 只（新股 {len(new_codes)} 只将新建档）")

    done = set()
    if os.path.exists(done_path):
        with open(done_path, encoding="utf-8") as f:
            done = {ln.strip() for ln in f if ln.strip()}
        print(f"【续跑】发现本月进度文件，已完成 {len(done)} 只，跳过继续")
    todo = [c for c in codes if c not in done]
    total = len(todo)
    if not todo:
        print("✅ 本月已全部刷新完成，无需重跑")
        try:
            bs.logout()
        except Exception:
            pass
        return

    t0 = time.time()
    ok_cnt, fail_list, hang_cnt, rows_total = 0, [], 0, 0
    since_ckpt = 0
    done_f = open(done_path, "a", encoding="utf-8")

    try:
        for i, code in enumerate(todo, 1):
            if time.time() - t0 > TIME_BUDGET:
                print(f"【刷新】⏰ 时间预算{TIME_BUDGET // 60}分钟已到，"
                      f"剩余 {total - i + 1} 只（进度已存盘，重新Run即可断点续跑）")
                break
            if use_alarm:
                signal.alarm(PER_STOCK_TIMEOUT)
            got = None
            try:
                for attempt in range(2):          # 普通异常重试1次
                    try:
                        got = refresh_one(code)
                        break
                    except TimeoutError:          # 挂死不重试，直接放弃
                        hang_cnt += 1
                        print(f"【刷新】⚠️ {code} 查询挂死{PER_STOCK_TIMEOUT}s，"
                              f"放弃（本次运行第{hang_cnt}次挂死）")
                        break
                    except Exception:
                        if attempt == 1:
                            fail_list.append(code)
                        else:
                            time.sleep(3)
            finally:
                if use_alarm:
                    signal.alarm(0)
            if got is not None:
                ok_cnt += 1
                rows_total += got
                done_f.write(code + "\n")
                done_f.flush()
                since_ckpt += 1
                if since_ckpt >= CHECKPOINT_EVERY:
                    checkpoint_commit(
                        f"🔄 月度刷新checkpoint {month_tag} "
                        f"{len(done) + ok_cnt}/{len(codes)}")
                    since_ckpt = 0
            if i % PROGRESS_EVERY == 0:
                el = time.time() - t0
                eta = el / i * (total - i)
                print(f"【进度】{i}/{total}"
                      f"（成功{ok_cnt} 失败{len(fail_list)} 挂死{hang_cnt}，"
                      f"用时{el / 60:.0f}分钟，预计还需{eta / 60:.0f}分钟）")
    finally:
        if use_alarm:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
        done_f.close()
        try:
            bs.logout()
        except Exception:
            pass

    unfinished = total - ok_cnt - len(fail_list)
    if not fail_list and unfinished == 0:
        try:
            os.remove(done_path)              # 全部完成 → 清进度文件
        except OSError:
            pass
        checkpoint_commit(f"🔄 月度全量刷新 {month_tag} 完成")
    else:
        checkpoint_commit(f"🔄 月度刷新 {month_tag}: "
                          f"成功{ok_cnt} 失败{len(fail_list)} 挂死{hang_cnt} "
                          f"未跑{unfinished}")

    print()
    print("=" * 60)
    print(f"✅ 月度全量刷新结束（用时 {(time.time() - t0) / 60:.0f} 分钟）")
    print(f"  刷新成功：{ok_cnt} 只，共写入 {rows_total:,} 根K线")
    print(f"  新建档：{len(new_codes)} 只")
    if hang_cnt:
        print(f"  挂死放弃：{hang_cnt} 只（网络异常，下月自动再试）")
    if fail_list:
        print(f"  失败：{len(fail_list)} 只（前20：{fail_list[:20]}）")
    if unfinished > 0:
        print(f"  未跑到：{unfinished} 只 → 手动Run本workflow可断点续跑")
    print("=" * 60)


if __name__ == "__main__":
    main()
