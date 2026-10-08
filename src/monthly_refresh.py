#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股月度全量刷新
================
由 .github/workflows/monthly_refresh.yml 每月1日自动触发，也可手动 Run。

用途（与每日增量互补）：
  1. 用 baostock 重拉每只股票【上市以来全部】前复权日线，整文件覆盖重写
     → 补齐新浪/腾讯兜底留下的空字段（amount / turnover / pct_chg）
     → 修正除权除息后的复权基准（前复权数据在除权后整个历史都会重算，
        所以必须全量重写，不能只刷最近一段）
  2. 全量股票清单与存量比对，自动收编新股建档
  3. 退市股查询返回空 → 文件保持原样不动（历史仍有分析价值）

断点续跑：每成功一只，代码记入 data/.monthly_refresh_<年-月>.done；
  中断后重新 Run（手动即可），自动跳过已完成的从断点继续。
  每 2000 只做一次 git checkpoint 提交，防止长时间运行后意外丢进度。

预计耗时：5224只 × 全量历史 ≈ 2~3.5小时（数据量是每日增量的百倍级，正常）
"""

import csv
import glob
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import baostock as bs

# ============================== 配置 ==============================
PROGRESS_EVERY = 100      # 每100只报一次进度
CHECKPOINT_EVERY = 2000   # 每2000只 git checkpoint 提交一次
TIME_BUDGET = 240 * 60    # 时间预算4小时（Actions单job上限6小时，留收尾余量）
CST = timezone(timedelta(hours=8))  # 北京时间

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
HIST_DIR = os.path.join(DATA_DIR, "history")
os.makedirs(HIST_DIR, exist_ok=True)

CSV_HEADER = ["date", "open", "close", "high", "low",
              "volume", "amount", "pct_chg", "turnover"]


# ============================== 工具 ==============================
def code_to_bs(code: str) -> str:
    """600519 -> sh.600519"""
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


# ============================== 股票清单 ==============================
def get_all_stocks() -> set:
    """baostock 全量在市A股清单（type=1 且无退市日期）"""
    codes = set()
    rs = bs.query_stock_basic()
    if rs.error_code != "0":
        raise RuntimeError(f"查询股票清单失败: {rs.error_msg}")
    while rs.next():
        r = rs.get_row_data()
        # 字段顺序: code, code_name, ipoDate, outDate, type, status
        if r[4] != "1" or r[3]:      # 非股票类型 / 已退市 → 跳过
            continue
        codes.add(r[0].split(".")[-1])
    return codes


# ============================== 单只刷新 ==============================
def refresh_one(code: str) -> int:
    """
    重拉单只股票全部历史（前复权），整文件原子覆盖重写。
    返回写入的K线根数；无数据（长期停牌/退市/源不覆盖）返回0且不动文件。
    """
    rs = bs.query_history_k_data_plus(
        code_to_bs(code),
        "date,open,high,low,close,volume,amount,pctChg,turn",
        start_date="", end_date="",          # ★ 故意全量：前复权基准需整体重算
        frequency="d", adjustflag="2")
    if rs.error_code != "0":
        raise RuntimeError(rs.error_msg)

    rows = []
    while rs.next():
        r = rs.get_row_data()
        if not r[4] or not r[5]:             # close/volume为空 → 停牌日，跳过
            continue
        try:
            vol = int(round(float(r[5]) / 100))   # 股 → 手（与每日增量一致）
        except ValueError:
            continue
        # baostock字段 → CSV列序 date,open,close,high,low,volume,amount,pct,turn
        rows.append([r[0], _fmt(r[1]), _fmt(r[4]), _fmt(r[2]), _fmt(r[3]),
                     vol, _fmt(r[6]), _fmt(r[7]), _fmt(r[8])])
    if not rows:
        return 0

    path = os.path.join(HIST_DIR, code + ".csv")
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        w.writerows(rows)
    os.replace(tmp, path)                    # 原子替换，不会留半截文件
    return len(rows)


# ============================== git checkpoint ==============================
def checkpoint_commit(msg: str):
    """提交推送当前进度（含已刷新CSV），失败不影响数据流程"""
    try:
        subprocess.run(["git", "add", "data"], cwd=ROOT,
                       check=True, capture_output=True)
        r = subprocess.run(["git", "commit", "-m", msg], cwd=ROOT,
                           capture_output=True, text=True)
        if r.returncode == 0:
            subprocess.run(["git", "push"], cwd=ROOT,
                           capture_output=True, timeout=120)
    except Exception as e:
        print(f"【checkpoint】git提交跳过（不影响数据，收尾步骤会再提交）: {e}")


# ============================== 主流程 ==============================
def main():
    now = datetime.now(CST)
    month_tag = f"{now:%Y-%m}"
    done_path = os.path.join(DATA_DIR, f".monthly_refresh_{month_tag}.done")

    print("=" * 60)
    print(f"===== A股月度全量刷新开始（北京时间 {now:%Y-%m-%d %H:%M}） =====")
    print("=" * 60)

    # ---------- 登录（重试3次） ----------
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
        print("【baostock】❌ 登录失败，本次放弃（下月自动重试，或手动Run）")
        sys.exit(1)
    print("【baostock】✅ 登录成功")

    # ---------- 股票清单：baostock在市股 ∪ 存量CSV ----------
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

    # ---------- 断点续跑 ----------
    done = set()
    if os.path.exists(done_path):
        with open(done_path, encoding="utf-8") as f:
            done = {ln.strip() for ln in f if ln.strip()}
        print(f"【续跑】发现本月进度文件，已完成 {len(done)} 只，跳过继续")
    todo = [c for c in codes if c not in done]
    total = len(todo)
    if not todo:
        print("✅ 本月已全部刷新完成，无需重跑")
        bs.logout()
        return

    # ---------- 逐只刷新 ----------
    t0 = time.time()
    ok_cnt, fail_list, rows_total = 0, [], 0
    since_ckpt = 0
    done_f = open(done_path, "a", encoding="utf-8")

    for i, code in enumerate(todo, 1):
        if time.time() - t0 > TIME_BUDGET:
            print(f"⏰ 时间预算{TIME_BUDGET // 60}分钟已到，剩余 {total - i + 1} 只"
                  f"（进度已存盘，重新Run即可断点续跑）")
            break
        got = None
        for attempt in range(2):             # 失败重试1次
            try:
                got = refresh_one(code)
                break
            except Exception:
                if attempt == 1:
                    fail_list.append(code)   # 重试仍失败 → 记录，不写入done（下月/续跑再试）
                else:
                    time.sleep(3)
        if got is not None:
            ok_cnt += 1
            rows_total += got
            done_f.write(code + "\n")
            done_f.flush()                   # 立即落盘，进程被杀也不丢进度
            since_ckpt += 1
            if since_ckpt >= CHECKPOINT_EVERY:
                checkpoint_commit(
                    f"🔄 月度刷新checkpoint {month_tag} {len(done) + ok_cnt}/{len(codes)}")
                since_ckpt = 0
        if i % PROGRESS_EVERY == 0:
            el = time.time() - t0
            eta = el / i * (total - i)
            print(f"【进度】{i}/{total}"
                  f"（成功{ok_cnt} 失败{len(fail_list)}，"
                  f"用时{el / 60:.0f}分钟，预计还需{eta / 60:.0f}分钟）")

    done_f.close()
    try:
        bs.logout()
    except Exception:
        pass

    # ---------- 收尾 ----------
    unfinished = total - ok_cnt - len(fail_list)
    if not fail_list and unfinished == 0:
        try:
            os.remove(done_path)             # 全部完成 → 删除进度文件
        except OSError:
            pass
        checkpoint_commit(f"🔄 月度全量刷新 {month_tag} 完成")
    else:
        checkpoint_commit(f"🔄 月度刷新 {month_tag}: "
                          f"成功{ok_cnt} 失败{len(fail_list)} 未跑{unfinished}")

    print()
    print("=" * 60)
    print(f"✅ 月度全量刷新结束（用时 {(time.time() - t0) / 60:.0f} 分钟）")
    print(f"  刷新成功：{ok_cnt} 只，共写入 {rows_total:,} 根K线")
    print(f"  新建档：{len(new_codes)} 只")
    if fail_list:
        print(f"  失败：{len(fail_list)} 只（前20：{fail_list[:20]}）")
    if unfinished > 0:
        print(f"  未跑到：{unfinished} 只 → 手动Run本workflow可断点续跑")
    print("=" * 60)


if __name__ == "__main__":
    main()
