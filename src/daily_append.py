"""
A股每日增量追加脚本（整套系统的核心保险丝）
功能：
1. 自愈式增量追加：读取每只股票csv最后一行的日期，从那天之后补到今天
   → 即使定时任务漏跑3天，下次运行自动补齐3天，历史库永不缺数据
2. 时间闸门：北京时间15:30之前触发一律拦截（防盘中/盘前数据污染历史库）
3. 交易日判断：akshare官方日历（含节假日+调休补班）优先，本地备用表兜底
4. 新股自动建档：历史库不存在的股票，自动补齐上市以来全部数据
5. 初始化检测：历史库文件少于100个时，提示先运行历史初始化，跳过追加
6. 断点续传：中断后重跑，已追加的自动跳过

路径说明：本脚本位于 src/ 目录，通过自身位置定位仓库根目录下的 data/
复权口径：前复权（qfq），与历史初始化完全一致，无缝衔接
"""
import os
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

# ===================== 路径定位（基于脚本自身位置） =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
# ======================================================================

# ===================== 可配置参数区 =====================
RETRY_TIMES = 3        # 单只股票重试次数
SLEEP_SECONDS = 0.3    # 每只股票之间的停顿秒数（防风控）
PROGRESS_EVERY = 50    # 每处理多少只打印一次进度
MIN_HISTORY_FILES = 100  # 历史库最少文件数（低于则认为初始化没做过）
GATE_HOUR = 15         # 时间闸门：15:30之前拦截
GATE_MINUTE = 30
# =======================================================

os.makedirs(HISTORY_DIR, exist_ok=True)

now = datetime.now()
TODAY = now.strftime("%Y-%m-%d")

# ===================== 本地备用节假日表（兜底用） =====================
# 2026年法定节假日（休市日）。akshare日历拿不到时才用这份
LOCAL_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-02",                      # 元旦
    "2026-02-16", "2026-02-17", "2026-02-18",        # 春节（示例，以官方为准）
    "2026-02-19", "2026-02-20", "2026-02-23",
    "2026-04-06",                                    # 清明
    "2026-05-01",                                    # 劳动节
    "2026-06-19",                                    # 端午
    "2026-09-25",                                    # 中秋
    "2026-10-01", "2026-10-02", "2026-10-05",        # 国庆
    "2026-10-06", "2026-10-07", "2026-10-08",
}
# 补班开市白名单（调休周六上班且开市的日子，格式同上）
WORKDAY_LIST = set()


def is_trade_day(date_obj=None) -> bool:
    """
    判断某天是否为A股交易日
    优先用akshare官方日历（含节假日+调休），失败则用本地备用表
    """
    d = date_obj if date_obj else now
    date_str = d.strftime("%Y-%m-%d")

    # 第一道：akshare官方交易日历
    try:
        df = ak.tool_trade_date_hist_sina()
        trade_days = set(pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d"))
        return date_str in trade_days
    except Exception:
        pass

    # 第二道：本地备用判断（周末 + 节假日表 + 补班白名单）
    if date_str in WORKDAY_LIST:
        return True
    if d.weekday() >= 5:  # 周六周日
        return False
    if date_str in LOCAL_HOLIDAYS_2026:
        return False
    return True


def before_market_close() -> bool:
    """时间闸门：当前时间是否在15:30之前（盘前/盘中一律拦截）"""
    return (now.hour, now.minute) < (GATE_HOUR, GATE_MINUTE)


def fetch_history_range(code: str, start_date: str, end_date: str):
    """拉取单只股票指定日期范围的前复权日线，返回标准化的DataFrame"""
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="qfq",  # 前复权，与历史库口径一致
    )
    if df is None or len(df) == 0:
        return None
    keep_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]
    df = df[keep_cols]
    df.columns = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]
    return df


def get_all_stock_codes():
    """获取全市场A股股票代码清单"""
    print("【股票清单】正在获取全市场A股代码...")
    for i in range(RETRY_TIMES + 1):
        try:
            df = ak.stock_zh_a_spot_em()
            codes = df["代码"].astype(str).tolist()
            print(f"【股票清单】获取成功，共 {len(codes)} 只股票")
            return codes
        except Exception as e:
            print(f"【股票清单】第{i+1}次失败：{e}")
            time.sleep(3)
    print("【股票清单】❌ 获取失败，无法继续！")
    return None


if __name__ == "__main__":
    print("=" * 60)
    print("===== A股每日增量追加开始 =====")
    print(f"===== 当前时间：{now} =====")
    print(f"===== akshare版本：{ak.__version__} =====")
    print("=" * 60)

    # ---------- 防线1：时间闸门（15:30之前拦截） ----------
    if before_market_close():
        msg = (f"⛔ 时间闸门拦截：当前 {now.strftime('%H:%M')}，"
               f"早于 {GATE_HOUR}:{GATE_MINUTE:02d}（收盘数据未落定）。\n"
               f"   为防止盘中/盘前数据污染历史库，本次不追加任何数据。\n"
               f"   如需手动运行，请在交易日 15:30 之后触发。")
        print(msg)
        with open(os.path.join(DATA_DIR, f"append_blocked_{TODAY}.txt"), "w", encoding="utf-8") as f:
            f.write(msg + "\n")
        exit(0)  # 正常退出，不算失败

    # ---------- 防线2：交易日判断 ----------
    if not is_trade_day():
        msg = f"📅 今天（{TODAY}）不是A股交易日，无需追加数据，正常退出。"
        print(msg)
        with open(os.path.join(DATA_DIR, f"trade_day_flag_{TODAY}.txt"), "w", encoding="utf-8") as f:
            f.write(msg + "\n")
        exit(0)

    # ---------- 防线3：初始化完成检测 ----------
    history_files = [f for f in os.listdir(HISTORY_DIR)
                     if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv"]
    if len(history_files) < MIN_HISTORY_FILES:
        print("❌❌❌ 检测到历史库文件数不足（{} < {}），历史初始化可能还没完成！".format(
            len(history_files), MIN_HISTORY_FILES))
        print("❌ 请先运行历史初始化（history_init 工作流），完成后再让每日追加运行。")
        print("❌ 本次跳过追加，不做任何操作。")
        exit(1)

    # ---------- 获取股票清单 ----------
    all_codes = get_all_stock_codes()
    if all_codes is None:
        exit(1)

    # ---------- 逐只自愈式追加 ----------
    print(f"\n【开始追加】共 {len(all_codes)} 只股票待检查...")
    appended_count = 0      # 本次实际追加了数据的股票数
    new_stock_count = 0     # 本次新建档的新股数
    up_to_date_count = 0    # 已经是最新、无需操作的股票数
    fail_list = []
    start_time = time.time()

    for idx, code in enumerate(all_codes, 1):
        csv_path = os.path.join(HISTORY_DIR, f"{code}.csv")

        # ---- 情况A：新股（文件不存在）→ 自动建档，补齐上市以来全部数据 ----
        if not os.path.exists(csv_path):
            try:
                # 从2年前开始拉，接口只返回该股实际上市以来的数据
                start_dt = (now - timedelta(days=365 * 2)).strftime("%Y%m%d")
                df = fetch_history_range(code, start_dt, now.strftime("%Y%m%d"))
                if df is not None and len(df) > 0:
                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    new_stock_count += 1
                # 返回空 = 代码无效或长期停牌，跳过
            except Exception as e:
                fail_list.append(code)
                print(f"  [{idx}/{len(all_codes)}] {code} 新股建档失败：{e}")

        # ---- 情况B：已有历史文件 → 自愈式补齐 ----
        else:
            try:
                old = pd.read_csv(csv_path)
                if len(old) == 0:
                    fail_list.append(code)
                else:
                    last_date = str(old["date"].iloc[-1]).replace("-", "")  # 如 20261008
                    today_str = now.strftime("%Y%m%d")
                    if last_date >= today_str:
                        # 已是最新（今天已追加过/今天停牌），跳过
                        up_to_date_count += 1
                    else:
                        # ★ 核心：从最后日期的次日起，一路补到今天（漏几天补几天）
                        next_day = (
                            datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)
                        ).strftime("%Y%m%d")
                        new_df = fetch_history_range(code, next_day, today_str)
                        if new_df is not None and len(new_df) > 0:
                            # 只追加日期严格大于旧数据的部分（双保险防重复）
                            merged = pd.concat([old, new_df], ignore_index=True)
                            merged = merged.drop_duplicates(subset=["date"], keep="first")
                            merged.to_csv(csv_path, index=False, encoding="utf-8-sig")
                            appended_count += 1
                        else:
                            # 补充范围为空 = 期间一直停牌，正常
                            up_to_date_count += 1
            except Exception as e:
                fail_list.append(code)
                print(f"  [{idx}/{len(all_codes)}] {code} 追加失败：{e}")

        # 定期打印进度
        if idx % PROGRESS_EVERY == 0:
            elapsed = time.time() - start_time
            avg = elapsed / idx
            remain = avg * (len(all_codes) - idx)
            print(f"  ===== 进度：{idx}/{len(all_codes)} "
                  f"（{round(idx/len(all_codes)*100, 1)}%），"
                  f"已用 {round(elapsed/60, 1)} 分钟，"
                  f"预计剩余 {round(remain/60, 1)} 分钟 =====")

        time.sleep(SLEEP_SECONDS)  # 停顿防风控

    # ---------- 收尾报告 ----------
    total_time = round((time.time() - start_time) / 60, 1)
    print("\n" + "=" * 60)
    print("===== ✅ 每日增量追加完成 =====")
    print(f"  本次追加数据：{appended_count} 只")
    print(f"  新股建档：{new_stock_count} 只")
    print(f"  已是最新（跳过）：{up_to_date_count} 只")
    print(f"  失败：{len(fail_list)} 只")
    if fail_list:
        print(f"  失败清单：{fail_list[:30]}{'...' if len(fail_list) > 30 else ''}")
        print(f"  💡 失败的股票下次运行会自动重试（自愈机制）")
    print(f"  总耗时：{total_time} 分钟")
    print("=" * 60)
