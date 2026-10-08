"""
A股历史数据初始化脚本
功能：
1. 拉取全部A股每只股票过去2年的日线数据（前复权，适合技术分析）
2. 每只股票保存为 data/history/股票代码.csv
3. 支持断点续传：已下载的股票自动跳过，中断后重新运行会接着下
4. 支持覆盖模式：环境变量 FORCE_OVERWRITE=1 时全量重拉（每月刷新用，修正前复权漂移）
5. 附赠：北向资金全部历史一次性存档
6. 收尾打印数据目录体积（监控仓库膨胀）

股票清单多数据源降级（2026-10-08修复）：
  GitHub Actions 服务器在美国，东财实时行情接口会拒绝海外数据中心IP，
  故清单获取改为三级降级：东财 → 沪深交易所官网 → 新浪
  （个股历史接口 stock_zh_a_hist 走东财另一个域名，通常不受此限制）

复权说明：
  adjust="qfq" 前复权——历史价格已按分红除权调整，K线连续无假跳空。
  注意：前复权数据会随未来除权事件漂移，故需要每月覆盖刷新一次。
"""
import os
import time
import akshare as ak
from datetime import datetime, timedelta

# ===================== 路径定位（关键：基于脚本自身位置） =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
# ============================================================================

# ===================== 可配置参数区 =====================
HISTORY_YEARS = 2      # 拉取几年的历史数据
RETRY_TIMES = 3        # 单只股票重试次数
SLEEP_SECONDS = 0.3    # 每只股票之间的停顿秒数（防风控）
PROGRESS_EVERY = 50    # 每下载多少只股票打印一次进度

# 覆盖模式开关（由workflow设置环境变量控制，本地手动跑默认关闭）
FORCE_OVERWRITE = os.environ.get("FORCE_OVERWRITE", "0") == "1"
# =======================================================

os.makedirs(HISTORY_DIR, exist_ok=True)

now = datetime.now()
TODAY = now.strftime("%Y-%m-%d")
START_DATE = (now - timedelta(days=365 * HISTORY_YEARS)).strftime("%Y%m%d")
END_DATE = now.strftime("%Y%m%d")


def fetch_stock_history(code: str):
    """拉取单只股票的历史日线（前复权），只保留核心字段"""
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=START_DATE,
        end_date=END_DATE,
        adjust="qfq",  # 前复权
    )
    if df is None or len(df) == 0:
        return None
    keep_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]
    df = df[keep_cols]
    df.columns = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]
    return df


def dedup_codes(codes) -> list:
    """代码去重并排序"""
    return sorted(set(str(c).zfill(6) for c in codes))


def get_all_stock_codes():
    """
    获取全市场A股股票代码清单 —— 三级数据源降级：
    ① 东财实时行情（最全，含北交所，但海外IP常被拦）
    ② 沪深交易所官网（对海外IP友好，不含北交所）
    ③ 新浪实时行情（慢但通用，含北交所）
    """
    print("【股票清单】正在获取全市场A股代码...")

    # ---- 数据源①：东财 ----
    for i in range(2):
        try:
            df = ak.stock_zh_a_spot_em()
            codes = dedup_codes(df["代码"].tolist())
            print(f"【股票清单】✅ 数据源①（东财）获取成功，共 {len(codes)} 只")
            return codes
        except Exception as e:
            print(f"【股票清单】数据源①（东财）第{i+1}次失败：{e}")
            time.sleep(5)

    # ---- 数据源②：沪深交易所官网 ----
    try:
        print("【股票清单】东财源不可用，切换数据源②（交易所官网）...")
        df = ak.stock_info_a_code_name()
        codes = dedup_codes(df["code"].tolist())
        print(f"【股票清单】✅ 数据源②（交易所官网）获取成功，共 {len(codes)} 只")
        return codes
    except Exception as e:
        print(f"【股票清单】数据源②（交易所官网）失败：{e}")
        time.sleep(5)

    # ---- 数据源③：新浪 ----
    try:
        print("【股票清单】交易所官网也不可用，切换数据源③（新浪）...")
        df = ak.stock_zh_a_spot()
        codes = dedup_codes(df["代码"].tolist())
        print(f"【股票清单】✅ 数据源③（新浪）获取成功，共 {len(codes)} 只")
        return codes
    except Exception as e:
        print(f"【股票清单】数据源③（新浪）失败：{e}")

    print("【股票清单】❌ 三个数据源全部失败，无法继续！")
    return None


def save_north_fund_history():
    """附赠：北向资金全部历史一次性存档"""
    print("\n【北向资金】正在拉取全部历史...")
    try:
        df = ak.stock_hsgt_hist_em(symbol="北向资金")
        if df is not None and len(df) > 0:
            save_path = os.path.join(HISTORY_DIR, "north_fund_ALL_HISTORY.csv")
            df.to_csv(save_path, index=False, encoding="utf-8-sig")
            print(f"【北向资金】✅ 历史存档保存成功：{save_path}，共 {len(df)} 条")
        else:
            print("【北向资金】⚠️ 接口返回空数据，跳过")
    except Exception as e:
        print(f"【北向资金】拉取失败（不影响股票历史下载）：{e}")


def get_dir_size_mb(path: str) -> float:
    """统计目录总体积（MB）"""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return round(total / 1024 / 1024, 1)


def clean_overwrite_dir():
    """覆盖模式：清空历史目录中的旧股票csv，准备全量重拉"""
    print("【覆盖模式】检测到 FORCE_OVERWRITE=1，将全量重拉覆盖旧数据...")
    confirm_marker = os.path.join(HISTORY_DIR, "_refreshing.tmp")
    with open(confirm_marker, "w") as f:
        f.write(f"refresh started at {now}\n")
    removed = 0
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
            os.remove(os.path.join(HISTORY_DIR, f))
            removed += 1
    print(f"【覆盖模式】已清空旧股票文件 {removed} 个，开始全量重拉")


if __name__ == "__main__":
    print("=" * 60)
    print("===== A股历史数据初始化开始 =====")
    print(f"===== 当前时间：{now} =====")
    print(f"===== 拉取范围：{HISTORY_YEARS} 年（{START_DATE} ~ {END_DATE}）=====")
    print(f"===== 复权方式：前复权（qfq）=====")
    print(f"===== 数据目录：{HISTORY_DIR} =====")
    print(f"===== 运行模式：{'🔁 覆盖模式（全量重拉）' if FORCE_OVERWRITE else '📦 续传模式（跳过已下载）'} =====")
    print(f"===== akshare版本：{ak.__version__} =====")
    print("=" * 60)

    # 覆盖模式下先清空目录
    if FORCE_OVERWRITE:
        clean_overwrite_dir()

    # 1. 获取全部股票代码
    all_codes = get_all_stock_codes()
    if all_codes is None:
        exit(1)

    # 2. 统计哪些已经下载过（断点续传的核心）
    already = set()
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
            already.add(f.replace(".csv", ""))
    todo_codes = [c for c in all_codes if c not in already]

    print(f"\n【进度统计】")
    print(f"  全部股票：{len(all_codes)} 只")
    print(f"  已下载：{len(already)} 只（自动跳过）")
    print(f"  本次待下载：{len(todo_codes)} 只")
    if todo_codes:
        print(f"  预计耗时：约 {round(len(todo_codes) * 1.8 / 3600, 1)} 小时\n")

    # 3. 逐只下载
    success_count = 0
    fail_list = []
    start_time = time.time()

    for idx, code in enumerate(todo_codes, 1):
        save_path = os.path.join(HISTORY_DIR, f"{code}.csv")

        # 双保险：文件已存在也跳过
        if os.path.exists(save_path):
            continue

        ok = False
        for attempt in range(RETRY_TIMES + 1):
            try:
                df = fetch_stock_history(code)
                if df is not None and len(df) > 0:
                    df.to_csv(save_path, index=False, encoding="utf-8-sig")
                    ok = True
                else:
                    # 2年内退市/长期停牌的股票返回空，属正常，不重试
                    ok = True
                    print(f"  [{idx}/{len(todo_codes)}] {code} 返回空数据（可能已退市/长期停牌），跳过")
                break
            except Exception as e:
                if attempt < RETRY_TIMES:
                    time.sleep(2)
                else:
                    print(f"  [{idx}/{len(todo_codes)}] {code} ❌ 失败：{e}")

        if ok:
            success_count += 1
        else:
            fail_list.append(code)

        # 定期打印进度
        if idx % PROGRESS_EVERY == 0:
            elapsed = time.time() - start_time
            avg = elapsed / idx
            remain = avg * (len(todo_codes) - idx)
            print(f"  ===== 进度：{idx}/{len(todo_codes)} "
                  f"（{round(idx/len(todo_codes)*100, 1)}%），"
                  f"已用 {round(elapsed/60, 1)} 分钟，"
                  f"预计剩余 {round(remain/60, 1)} 分钟 =====")

        time.sleep(SLEEP_SECONDS)  # 停顿防风控

    # 4. 附赠：北向资金全历史
    save_north_fund_history()

    # 5. 清理覆盖模式标记
    marker = os.path.join(HISTORY_DIR, "_refreshing.tmp")
    if os.path.exists(marker):
        os.remove(marker)

    # 6. 收尾报告 + 仓库体积监控
    total_time = round((time.time() - start_time) / 60, 1)
    dir_size = get_dir_size_mb(os.path.join(PROJECT_ROOT, "data"))
    print("\n" + "=" * 60)
    print("===== ✅ 历史数据初始化完成 =====")
    print(f"  本次新下载：{success_count} 只")
    print(f"  之前已下载（跳过）：{len(already)} 只")
    print(f"  失败：{len(fail_list)} 只")
    if fail_list:
        print(f"  失败清单（重新运行本脚本会自动重试这些）：")
        print(f"  {fail_list}")
    print(f"  总耗时：{total_time} 分钟")
    print(f"  数据目录：{HISTORY_DIR}")
    print(f"  📦 data目录总体积：{dir_size} MB")
    if dir_size > 800:
        print("  ⚠️⚠️⚠️ 警告：data目录体积已超过 800MB，接近GitHub仓库1GB软限制！")
        print("  ⚠️ 请考虑做仓库瘦身（历史压缩或分仓）！")
    print("=" * 60)
    print("💡 提示：如有失败，重新跑一次本脚本即可（会自动跳过成功的，只重试失败的）")
