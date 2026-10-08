"""
A股定时拉取脚本，配合 GitHub Actions
数据源：akshare
拉取内容：A股交易日判断、全市场日线、北向资金、龙虎榜
修复点：确保文件夹创建；异常捕获打印详细错误；非交易日不生成文件
适配量学建模：后续可扩展量柱计算、量化对倒识别
"""
import os
import akshare as ak
import pandas as pd
from datetime import datetime

# ===================== 可配置参数区 =====================
DATA_DIR = "./data"  # 数据保存目录
TODAY = datetime.now().strftime("%Y-%m-%d")
# =======================================================

# 创建数据文件夹，确保目录一定存在
os.makedirs(DATA_DIR, exist_ok=True)

def is_a_stock_trade_day(date_str: str) -> bool:
    """判断当天是否A股交易日，非交易日直接终止任务"""
    try:
        trade_cal_df = ak.tool_trade_date_hist_sina()
        return date_str in trade_cal_df["trade_date"].values
    except Exception as e:
        print(f"获取交易日历失败：{e}")
        return False

if __name__ == "__main__":
    print(f"===== 开始执行：{TODAY} =====")

    # 非交易日直接退出，不生成任何csv
    if not is_a_stock_trade_day(TODAY):
        print("今日不是A股交易日，程序退出，不生成数据文件")
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
