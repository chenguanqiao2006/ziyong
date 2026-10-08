"""
A股定时拉取脚本，配合 GitHub Actions
更新：
1. 龙虎榜更换新浪源接口 stock_lhb_detail_daily_sina
2. 全市场行情、北向资金保持原有稳定逻辑
3. 单独捕获异常，接口不存在也不会中断整个脚本
数据源：akshare
适配量学建模：后续可扩展量柱、量比、位置判断、量化对倒识别
"""
import os
import time
import akshare as ak
import pandas as pd
from datetime import datetime

# ===================== 可配置参数区 =====================
DATA_DIR = "./data"  # 数据保存目录
# 2026年A股法定休市日期清单（交易所放假安排）
HOLIDAY_LIST = {
    "2026-01-01",
    "2026-01-02",
    "2026-01-03",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-21",
    "2026-04-04",
    "2026-04-05",
    "2026-04-06",
    "2026-05-01",
    "2026-05-02",
    "2026-05-03",
    "2026-05-04",
    "2026-05-05",
    "2026-06-19",
    "2026-10-01",
    "2026-10-02",
    "2026-10-03",
    "2026-10-04",
    "2026-10-05",
    "2026-10-06",
    "2026-10-07",
}
# 调休补班白名单（周末但是开市）
WORKDAY_LIST = set()
# 网络重试次数
RETRY_TIMES = 2
# =======================================================

# 创建数据文件夹，确保目录一定存在
os.makedirs(DATA_DIR, exist_ok=True)

def is_a_stock_trade_day(date_str: str) -> bool:
    """
    本地判断A股交易日
    规则：
    1. 在补班白名单内 → 开市（True）
    2. 周六、周日 → 休市（False）
    3. 在法定节假日列表 → 休市（False）
    其余视为交易日
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    week_num = dt.weekday() # 0周一，4周五，5周六，6周日

    # 补班日优先判定开市
    if date_str in WORKDAY_LIST:
        return True
    # 周末休市
    if week_num >= 5:
        return False
    # 法定节假日休市
    if date_str in HOLIDAY_LIST:
        return False
    return True

def fetch_with_retry(func, desc:str, *args, **kwargs):
    """带重试包装函数，网络失败自动重试"""
    for i in range(RETRY_TIMES+1):
        try:
            print(f"【{desc}】尝试第{i+1}次")
            df = func(*args, **kwargs)
            print(f"【{desc}】获取成功")
            time.sleep(2) # 每次请求之间停顿2秒，降低风控
            return df
        except Exception as e:
            print(f"【{desc}】第{i+1}次失败：{e}")
            time.sleep(3)
    print(f"【{desc}】全部重试失败！")
    return None

if __name__ == "__main__":
    now = datetime.now()
    print(f"===== 当前虚拟机本地时间：{now} =====")
    TODAY = now.strftime("%Y-%m-%d")
    print(f"脚本识别日期 TODAY = {TODAY}")

    trade_flag = is_a_stock_trade_day(TODAY)
    print(f"交易日历判断结果：{trade_flag}")

    if not trade_flag:
        print("今日不是A股交易日，程序退出，不生成行情csv")
        flag_file = os.path.join(DATA_DIR, f"trade_day_flag_{TODAY}.txt")
        with open(flag_file, "w", encoding="utf-8") as f:
            f.write(f"{TODAY} 非交易日\n")
        exit(0)

    # 1. 全市场A股当日行情
    print("正在拉取全市场A股日线行情...")
    df_spot = fetch_with_retry(ak.stock_zh_a_spot, "全市场行情")
    if df_spot is not None and len(df_spot) > 0:
        df_spot.to_csv(f"{DATA_DIR}/ashare_spot_{TODAY}.csv", index=False, encoding="utf-8-sig")
        print(f"日线行情保存成功，共 {len(df_spot)} 条")

    # 2. 北向资金
    print("正在拉取北向资金数据...")
    df_north = fetch_with_retry(ak.stock_hsgt_hist_em, "北向资金")
    if df_north is not None and len(df_north) > 0:
        df_north.to_csv(f"{DATA_DIR}/north_fund_{TODAY}.csv", index=False, encoding="utf-8-sig")
        print("北向资金保存成功")

    # 3. 龙虎榜：新浪源接口 stock_lhb_detail_daily_sina
    print("正在拉取龙虎榜数据...")
    try:
        df_lhb = fetch_with_retry(ak.stock_lhb_detail_daily_sina, "龙虎榜", date=TODAY)
        if df_lhb is not None and len(df_lhb) > 0:
            df_lhb.to_csv(f"{DATA_DIR}/longhubang_{TODAY}.csv", index=False, encoding="utf-8-sig")
            print(f"龙虎榜保存成功，共 {len(df_lhb)} 条")
    except AttributeError as e:
        print(f"【龙虎榜】akshare无stock_lhb_detail_daily_sina接口，跳过龙虎榜拉取，不中断任务：{e}")
    except Exception as e:
        print(f"【龙虎榜】拉取异常，跳过龙虎榜：{e}")

    print("===== 数据拉取任务完成 =====")
