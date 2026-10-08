"""
A股历史数据初始化脚本（腾讯主源·剔除北交所版 v4，2026-10-08）
策略：
1. 个股历史下载顺序：腾讯 → 新浪 → 东财（逐只自动降级，单源连续失败自动熔断）
   （GitHub Actions在美国，东财接口对海外IP拦截严重；腾讯/新浪对海外IP友好）
2. 剔除北交所：代码 43/83/87/88/92 开头一律不拉（用户不玩北交所）
3. 代码规范化：剥离 sh/sz/bj 前缀，统一存为纯6位数字文件名
4. 新浪源涨跌幅/换手率自行计算；腾讯源缺的列自动补空

其他功能不变：2年前复权日线、断点续传、覆盖模式、北向资金存档、体积监控
"""
import os
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

# ===================== 路径定位（基于脚本自身位置） =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HISTORY_DIR = os.path.join(PROJECT_ROOT, "data", "history")
# ======================================================================

# ===================== 可配置参数区 =====================
HISTORY_YEARS = 2       # 拉取几年历史
RETRY_TIMES = 2         # 单源单只重试次数
SLEEP_SECONDS = 0.15    # 每只股票之间停顿（防风控）
PROGRESS_EVERY = 50     # 进度打印间隔
CIRCUIT_LIMIT = 8       # 单源连续失败熔断阈值
EXCLUDE_BJ = True       # 剔除北交所（43/83/87/88/92开头）
BJ_PREFIXES = ("43", "83", "87", "88", "92")
# =======================================================

FORCE_OVERWRITE = os.environ.get("FORCE_OVERWRITE", "0") == "1"

os.makedirs(HISTORY_DIR, exist_ok=True)

now = datetime.now()
START_DATE = (now - timedelta(days=365 * HISTORY_YEARS)).strftime("%Y%m%d")
END_DATE = now.strftime("%Y%m%d")

STD_COLS = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]

# 熔断状态：每个源独立计数
STATE = {"consec_fail": {"腾讯": 0, "新浪": 0, "东财": 0},
         "dead": {"腾讯": False, "新浪": False, "东财": False}}
SRC_COUNT = {"东财": 0, "新浪": 0, "腾讯": 0}


# ===================== 代码规范化 =====================
def normalize_symbol(raw):
    """返回 (纯6位代码, 带前缀代码)。'sh600000'→('600000','sh600000')"""
    s = str(raw).strip().lower()
    for p in ("sh", "sz", "bj"):
        if s.startswith(p) and s[2:].isdigit():
            return s[2:], s
    code = "".join(ch for ch in s if ch.isdigit())
    if code.startswith(("60", "68", "90")):
        return code, "sh" + code
    return code, "sz" + code


def is_bj(code: str) -> bool:
    return code.startswith(BJ_PREFIXES)


# ===================== 数据整理 =====================
def finalize(df: pd.DataFrame) -> pd.DataFrame:
    """统一列名/排序/去重/数值化；缺失列补空；缺涨跌幅则用收盘价推算"""
    df = df.copy()
    df["date"] = df["date"].astype(str)
    for c in STD_COLS[1:]:
        if c not in df.columns:
            df[c] = None
    df = df[STD_COLS]
    for c in STD_COLS[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if df["pct_chg"].isna().all() and df["close"].notna().sum() > 1:
        df["pct_chg"] = (df["close"].pct_change() * 100).round(2)
    df = df.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return df


# ===================== 三个数据源 =====================
def fetch_tx(symbol: str):
    """数据源①：腾讯（主源，海外IP友好）"""
    df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=START_DATE,
                               end_date=END_DATE, adjust="qfq")
    if df is None or len(df) == 0:
        return None
    rename = {"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
              "成交量": "volume", "成交额": "amount", "涨跌幅": "pct_chg", "换手率": "turnover"}
    df = df.rename(columns=rename)
    return finalize(df)


def fetch_sina(symbol: str):
    """数据源②：新浪（海外IP友好，涨跌幅/换手率自行计算）"""
    df = ak.stock_zh_a_daily(symbol=symbol, start_date=START_DATE,
                             end_date=END_DATE, adjust="qfq")
    if df is None or len(df) == 0:
        return None
    out = pd.DataFrame()
    out["date"] = df["date"].astype(str)
    for c in ["open", "close", "high", "low"]:
        out[c] = df[c]
    out["volume"] = pd.to_numeric(df["volume"], errors="coerce") / 100  # 股→手
    out["amount"] = df.get("amount", None)
    out["pct_chg"] = (out["close"].astype(float).pct_change() * 100).round(2)
    if "outstanding_share" in df.columns:
        os_share = pd.to_numeric(df["outstanding_share"], errors="coerce")
        vol_shares = pd.to_numeric(df["volume"], errors="coerce")
        out["turnover"] = (vol_shares / os_share * 100).round(2)
    else:
        out["turnover"] = None
    return finalize(out)


def fetch_em(code: str):
    """数据源③：东财（国内IP最快，海外IP常被拦，排最后兜底）"""
    df = ak.stock_zh_a_hist(symbol=code, period="daily",
                            start_date=START_DATE, end_date=END_DATE, adjust="qfq")
    if df is None or len(df) == 0:
        return None
    keep = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]
    df = df[keep]
    df.columns = STD_COLS
    return finalize(df)


def fetch_stock_history(code: str, symbol: str):
    """逐源降级抓取。返回 (DataFrame, 源名) / (None, None)=空数据 / ('FAIL', 错误)"""
    sources = []
    for name, fn in (("腾讯", lambda: fetch_tx(symbol)),
                     ("新浪", lambda: fetch_sina(symbol)),
                     ("东财", lambda: fetch_em(code))):
        if not STATE["dead"][name]:
            sources.append((name, fn))

    last_err = None
    for name, fn in sources:
        for i in range(RETRY_TIMES + 1):
            try:
                df = fn()
                if df is None or len(df) == 0:
                    return None, None   # 空=退市/长期停牌，正常
                STATE["consec_fail"][name] = 0
                return df, name
            except Exception as e:
                last_err = e
                if i < RETRY_TIMES:
                    time.sleep(1)
        # 该源失败：累计并判断熔断
        STATE["consec_fail"][name] += 1
        if STATE["consec_fail"][name] >= CIRCUIT_LIMIT and not STATE["dead"][name]:
            STATE["dead"][name] = True
            print(f"\n【⚡熔断】{name}源连续失败{CIRCUIT_LIMIT}只，本次运行弃用该源\n")
    return "FAIL", str(last_err)


# ===================== 股票清单（三级降级） =====================
def get_all_stock_codes():
    """返回 [(纯代码, 带前缀代码), ...]。剔除北交所。降级：东财→交易所官网→新浪"""
    print("【股票清单】正在获取全市场A股代码...")

    for i in range(2):
        try:
            df = ak.stock_zh_a_spot_em()
            pairs = sorted(set(normalize_symbol(c) for c in df["代码"].tolist()))
            print(f"【股票清单】✅ 数据源①（东财）获取成功，共 {len(pairs)} 只")
            break
        except Exception as e:
            print(f"【股票清单】数据源①（东财）第{i+1}次失败：{e}")
            time.sleep(5)
    else:
        try:
            print("【股票清单】切换数据源②（交易所官网）...")
            df = ak.stock_info_a_code_name()
            pairs = sorted(set(normalize_symbol(c) for c in df["code"].tolist()))
            print(f"【股票清单】✅ 数据源②（交易所官网）获取成功，共 {len(pairs)} 只")
        except Exception as e:
            print(f"【股票清单】数据源②失败：{e}")
            try:
                print("【股票清单】切换数据源③（新浪）...")
                df = ak.stock_zh_a_spot()
                pairs = sorted(set(normalize_symbol(c) for c in df["代码"].tolist()))
                print(f"【股票清单】✅ 数据源③（新浪）获取成功，共 {len(pairs)} 只")
            except Exception as e:
                print(f"【股票清单】数据源③失败：{e}")
                print("【股票清单】❌ 三个数据源全部失败，无法继续！")
                return None

    # 剔除北交所
    if EXCLUDE_BJ:
        before = len(pairs)
        pairs = [(c, s) for c, s in pairs if not is_bj(c)]
        print(f"【股票清单】已剔除北交所 {before - len(pairs)} 只，剩余 {len(pairs)} 只")
    return pairs


# ===================== 北向资金 / 工具 =====================
def save_north_fund_history():
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
        print(f"【北向资金】拉取失败（不影响股票下载）：{e}")


def get_dir_size_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return round(total / 1024 / 1024, 1)


def clean_overwrite_dir():
    print("【覆盖模式】检测到 FORCE_OVERWRITE=1，将全量重拉覆盖旧数据...")
    removed = 0
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
            os.remove(os.path.join(HISTORY_DIR, f))
            removed += 1
    print(f"【覆盖模式】已清空旧股票文件 {removed} 个，开始全量重拉")


# ===================== 主流程 =====================
if __name__ == "__main__":
    print("=" * 60)
    print("===== A股历史数据初始化开始（腾讯主源·无北交所） =====")
    print(f"===== 当前时间：{now} =====")
    print(f"===== 拉取范围：{HISTORY_YEARS} 年（{START_DATE} ~ {END_DATE}）=====")
    print(f"===== 复权方式：前复权（qfq）=====")
    print(f"===== 数据目录：{HISTORY_DIR} =====")
    print(f"===== 运行模式：{'🔁 覆盖模式（全量重拉）' if FORCE_OVERWRITE else '📦 续传模式（跳过已下载）'} =====")
    print(f"===== akshare版本：{ak.__version__} =====")
    print("=" * 60)

    if FORCE_OVERWRITE:
        clean_overwrite_dir()

    all_pairs = get_all_stock_codes()
    if all_pairs is None:
        exit(1)

    already = set()
    for f in os.listdir(HISTORY_DIR):
        if f.endswith(".csv") and f != "north_fund_ALL_HISTORY.csv":
            already.add(f.replace(".csv", ""))
    todo_pairs = [(c, s) for c, s in all_pairs if c not in already]

    print(f"\n【进度统计】")
    print(f"  全部股票（不含北交所）：{len(all_pairs)} 只")
    print(f"  已下载：{len(already)} 只（自动跳过）")
    print(f"  本次待下载：{len(todo_pairs)} 只")
    if todo_pairs:
        print(f"  预计耗时：约 {round(len(todo_pairs) * 1.2 / 3600, 1)} 小时\n")

    success_count = 0
    empty_count = 0
    fail_list = []
    start_time = time.time()

    for idx, (code, symbol) in enumerate(todo_pairs, 1):
        save_path = os.path.join(HISTORY_DIR, f"{code}.csv")
        if os.path.exists(save_path):
            continue

        result, info = fetch_stock_history(code, symbol)

        if isinstance(result, pd.DataFrame):
            result.to_csv(save_path, index=False, encoding="utf-8-sig")
            success_count += 1
            SRC_COUNT[info] = SRC_COUNT.get(info, 0) + 1
        elif result is None:
            empty_count += 1
        else:
            fail_list.append(code)

        if idx % PROGRESS_EVERY == 0:
            elapsed = time.time() - start_time
            avg = elapsed / idx
            remain = avg * (len(todo_pairs) - idx)
            print(f"  ===== 进度：{idx}/{len(todo_pairs)} "
                  f"（{round(idx/len(todo_pairs)*100, 1)}%），"
                  f"成功{success_count} 空{empty_count} 败{len(fail_list)}，"
                  f"已用 {round(elapsed/60, 1)} 分钟，"
                  f"预计剩余 {round(remain/60, 1)} 分钟 =====")

        time.sleep(SLEEP_SECONDS)

    save_north_fund_history()

    total_time = round((time.time() - start_time) / 60, 1)
    dir_size = get_dir_size_mb(os.path.join(PROJECT_ROOT, "data"))
    print("\n" + "=" * 60)
    print("===== ✅ 历史数据初始化完成 =====")
    print(f"  本次新下载：{success_count} 只")
    print(f"  各数据源：腾讯{SRC_COUNT.get('腾讯',0)} 新浪{SRC_COUNT.get('新浪',0)} 东财{SRC_COUNT.get('东财',0)}")
    print(f"  空数据（退市/长期停牌，正常）：{empty_count} 只")
    print(f"  之前已下载（跳过）：{len(already)} 只")
    print(f"  失败：{len(fail_list)} 只")
    if fail_list:
        print(f"  失败清单（重跑本脚本会自动重试）：{fail_list[:50]}{'...' if len(fail_list) > 50 else ''}")
    print(f"  总耗时：{total_time} 分钟")
    print(f"  📦 data目录总体积：{dir_size} MB")
    if dir_size > 800:
        print("  ⚠️⚠️⚠️ 警告：data目录已超 800MB，接近GitHub仓库1GB软限制！")
    print("=" * 60)
    print("💡 提示：如有失败，重跑一次即可（自动跳过成功的，只重试失败的）")
