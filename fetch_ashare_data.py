"""
A股定时拉取脚本，配合 GitHub Actions
修复重点：
1. workflow已经设置系统时区Asia/Shanghai，datetime.now()得到北京时间
2. 内置2026休市清单，不依赖akshare交易日接口，规避版本报错
3. 打印详细时间信息，方便日志排错
数据源：akshare
拉取内容：A股交易日判断、全市场日线、北向资金、龙虎榜
适配量学建模：后续可扩展量柱计算、量化对倒识别
"""
import os
import akshare as ak
import pandas as pd
from datetime import datetime

# ===================== 可配置参数区 =====================
DATA_DIR = "./data"  # 数据保存目录
TODAY = datetime.now().strftime("%Y-%m-%d")
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

    # 1. 拉取全市场A股当日行情日线
    print("正在拉取全市场A股日线行情...")
    try:
        stock_df = ak.stock_zh_a_spot_em()
        stock_df.to_csv(f"{DATA_DIR}/ashare_spot_{TODAY}.csv", index=False, encoding="utf-8-sig")
        print(f"日线行情保存成功，共 {len(stock_df)} 条")
    except Exception as e:
        print(f"拉取日线行情异常：{e}")

    # 2. 北向资金
    print("正在拉取北向资金数据...")
    try:
        north_df = ak.stock_hsgt_north_net_flow_in_em()
        north_df.to_csv(f"{DATA_DIR}/north_fund_{TODAY}.csv", index=False, encoding="utf-8-sig")
        print("北向资金保存成功")
    except Exception as e:
        print(f"拉取北向资金异常：{e}")

    # 3. 龙虎榜数据
    print("正在拉取龙虎榜数据...")
    try:
        lhb_df = ak.stock_lhb_em(date=TODAY)
        lhb_df.to_csv(f"{DATA_DIR}/longhubang_{TODAY}.csv", index=False, encoding="utf-8-sig")
        print(f"龙虎榜保存成功，共 {len(lhb_df)} 条")
    except Exception as e:
        print(f"拉取龙虎榜异常：{e}")

    print("===== 数据拉取任务完成 =====")
