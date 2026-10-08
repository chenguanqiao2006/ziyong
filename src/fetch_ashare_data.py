"""
A股每日数据拉取脚本（快照版）
功能：
1. 每日全市场快照：全部A股当日收盘行情 → data/ashare_spot_日期.csv
2. 龙虎榜：当日榜单 → data/longhubang_日期.csv
3. 北向资金：当日资金流向 → data/north_fund_日期.csv
4. 断档检查：检查最近N个交易日快照是否缺失，缺失则写报警文件
5. 三道防线：
   - 时间闸门：北京时间15:30之前触发一律拦截（防盘中/盘前脏数据）
   - 交易日判断：akshare官方日历优先，本地节假日表兜底
   - 接口重试 + 记录akshare版本（便于排查接口改版问题）

路径说明：本脚本位于 src/ 目录，通过自身位置定位仓库根目录下的 data/
"""
import os
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

# ===================== 路径定位（基于脚本自身位置） =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
# ======================================================================

# ===================== 可配置参数区 =====================
RETRY_TIMES = 3          # 接口重试次数
RETRY_WAIT = 5           # 重试间隔秒数
GATE_HOUR = 15           # 时间闸门：15:30之前拦截
GATE_MINUTE = 30
MISSING_CHECK_DAYS = 10  # 断档检查：往回检查多少个自然日内的交易日
# =======================================================

os.makedirs(DATA_DIR, exist_ok=True)

now = datetime.now()
TODAY = now.strftime("%Y-%m-%d")

# ===================== 本地备用节假日表（兜底用） =====================
LOCAL_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-02",
    "2026-02-16", "2026-02-17", "2026-02-18",
    "2026-02-19", "2026-02-20", "2026-02-23",
    "2026-04-06",
    "2026-05-01",
    "2026-06-19",
    "2026-09-25",
    "2026-10-01", "2026-10-02", "2026-10-05",
    "2026-10-06", "2026-10-07", "2026-10-08",
}
WORKDAY_LIST = set()  # 调休补班开市白名单


def is_trade_day(date_obj=None) -> bool:
    """判断某天是否为A股交易日（akshare日历优先，本地表兜底）"""
    d = date_obj if date_obj else now
    date_str = d.strftime("%Y-%m-%d")
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_days = set(pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d"))
        return date_str in trade_days
    except Exception:
        pass
    if date_str in WORKDAY_LIST:
        return True
    if d.weekday() >= 5:
        return False
    if date_str in LOCAL_HOLIDAYS_2026:
        return False
    return True


def get_recent_trade_days(n_days=MISSING_CHECK_DAYS):
    """返回最近n_days个自然日内的交易日列表（含今天，如今天是交易日）"""
    days = []
    for i in range(n_days):
        d = now - timedelta(days=i)
        if is_trade_day(d):
            days.append(d.strftime("%Y-%m-%d"))
    return days


def fetch_with_retry(fetch_func, name: str, *args, **kwargs):
    """带重试的接口调用封装"""
    for i in range(RETRY_TIMES + 1):
        try:
            df = fetch_func(*args, **kwargs)
            return df
        except Exception as e:
            print(f"【{name}】第{i+1}次失败：{e}")
            if i < RETRY_TIMES:
                time.sleep(RETRY_WAIT)
    print(f"【{name}】❌ 重试{RETRY_TIMES}次后仍失败，放弃本项")
    return None


def save_snapshot():
    """拉取并保存全市场快照"""
    print("\n【快照】正在拉取全市场A股行情...")
    df = fetch_with_retry(ak.stock_zh_a_spot_em, "快照")
    if df is not None and len(df) > 0:
        path = os.path.join(DATA_DIR, f"ashare_spot_{TODAY}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"【快照】✅ 保存成功：{path}，共 {len(df)} 条")
    else:
        print("【快照】⚠️ 返回空数据，本日快照缺失（23:00兜底任务会重试）")


def save_longhubang():
    """拉取并保存当日龙虎榜"""
    print("\n【龙虎榜】正在拉取...")
    trade_date = now.strftime("%Y%m%d")
    df = fetch_with_retry(ak.stock_lhb_detail_em, "龙虎榜", start_date=trade_date, end_date=trade_date)
    if df is not None and len(df) > 0:
        path = os.path.join(DATA_DIR, f"longhubang_{TODAY}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"【龙虎榜】✅ 保存成功：{path}，共 {len(df)} 条")
    else:
        # 龙虎榜可能当天没有（普通交易日也可能无榜），属正常
        print("【龙虎榜】⚠️ 当日无龙虎榜数据（可能尚未发布或当日无榜），跳过")


def save_north_fund():
    """拉取并保存当日北向资金"""
    print("\n【北向资金】正在拉取...")
    try:
        # 东财沪股通+深股通合并口径
        df = fetch_with_retry(ak.stock_hsgt_fund_flow_summary_em, "北向资金")
        if df is not None and len(df) > 0:
            path = os.path.join(DATA_DIR, f"north_fund_{TODAY}.csv")
            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"【北向资金】✅ 保存成功：{path}")
        else:
            print("【北向资金】⚠️ 返回空数据，跳过")
    except Exception as e:
        print(f"【北向资金】拉取异常（不影响其他数据）：{e}")


def check_missing_data():
    """断档检查：最近N个交易日中，快照文件是否缺失"""
    print("\n【断档检查】正在检查最近交易日数据完整性...")
    trade_days = get_recent_trade_days()
    missing = []
    for day in trade_days:
        # 今天的不算缺失（可能马上就要拉了/还没到时间）
        if day == TODAY:
            continue
        spot_file = os.path.join(DATA_DIR, f"ashare_spot_{day}.csv")
        if not os.path.exists(spot_file):
            missing.append(day)

    if missing:
        alert_path = os.path.join(DATA_DIR, f"MISSING_DATA_ALERT_{TODAY}.txt")
        with open(alert_path, "w", encoding="utf-8") as f:
            f.write(f"⚠️ 断档报警！以下交易日的快照数据缺失：\n")
            for d in sorted(missing):
                f.write(f"  - {d}\n")
            f.write(f"\n生成时间：{now}\n")
            f.write(f"提示：历史K线数据不受影响（增量追加自带自愈补齐），\n")
            f.write(f"缺失的是每日快照文件。如需补齐请手动触发工作流。\n")
        print(f"【断档检查】⚠️ 发现 {len(missing)} 个交易日快照缺失！已写入报警文件：{alert_path}")
        print(f"【断档检查】缺失日期：{sorted(missing)}")
    else:
        print(f"【断档检查】✅ 最近 {len(trade_days)} 个交易日数据完整，无断档")


if __name__ == "__main__":
    print("=" * 60)
    print("===== A股每日数据拉取开始 =====")
    print(f"===== 当前时间：{now} =====")
    print(f"===== akshare版本：{ak.__version__} =====")
    print("=" * 60)

    # ---------- 防线1：时间闸门（15:30之前拦截） ----------
    if (now.hour, now.minute) < (GATE_HOUR, GATE_MINUTE):
        msg = (f"⛔ 时间闸门拦截：当前 {now.strftime('%H:%M')}，"
               f"早于 {GATE_HOUR}:{GATE_MINUTE:02d}（收盘数据未落定）。\n"
               f"   为防止盘中/盘前脏数据入库，本次不拉取任何数据。\n"
               f"   如需手动运行，请在交易日 15:30 之后触发。")
        print(msg)
        with open(os.path.join(DATA_DIR, f"blocked_{TODAY}.txt"), "w", encoding="utf-8") as f:
            f.write(msg + "\n")
        exit(0)  # 正常退出，不算失败

    # ---------- 防线2：交易日判断 ----------
    if not is_trade_day():
        msg = f"📅 今天（{TODAY}）不是A股交易日，无需拉取数据，正常退出。"
        print(msg)
        with open(os.path.join(DATA_DIR, f"trade_day_flag_{TODAY}.txt"), "w", encoding="utf-8") as f:
            f.write(msg + "\n")
        exit(0)

    # ---------- 主流程：快照 + 龙虎榜 + 北向 ----------
    save_snapshot()
    save_longhubang()
    save_north_fund()

    # ---------- 断档检查（放在最后，顺带检查今天是否成功落盘） ----------
    check_missing_data()

    print("\n" + "=" * 60)
    print("===== ✅ 每日数据拉取完成 =====")
    print("=" * 60)
