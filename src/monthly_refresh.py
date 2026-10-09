# -*- coding: utf-8 -*-
"""
A股月度全量刷新 · 三轮降级版
==========================================================
第一轮 baostock ：官方涨跌幅 + 官方换手率（能蹭就蹭）
第二轮 东财    ：官方涨跌幅 + 官方换手率（主力兜底，熔断自动冷却重启）
第三轮 新浪→腾讯：保底（日期+开高低收+成交量，涨跌幅本地算，换手率留空）
特性：永远能跑成 / 断点续跑 / 每月幂等 / 官方口径不足时下次自动升级
"""

import os
import re
import json
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ========================== 配置 ==========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "history")
PROGRESS_FILE = os.path.join(DATA_DIR, ".monthly_progress.json")

YEARS_BACK = 2
CSV_HEADER = ["date", "open", "high", "low", "close", "volume", "amount", "pctChg", "turn"]

BS_LOGIN_RETRY = 5      # baostock 登录重试次数
BS_LOGIN_WAIT = 60      # 登录重试间隔（秒）
BS_STOCK_RETRY = 3      # 单只查询失败重试（每次重试前重建会话）

EM_CONC = 2             # 东财并发（刻意压低，防风控）
EM_GAP = 0.4            # 东财全局最小请求间隔（秒）
EM_BREAK_N = 15         # 连续失败N只 → 熔断
EM_COOLDOWN = 180       # 熔断冷却（秒），之后自动重启
EM_MAX_TRIPS = 3        # 熔断超过N次 → 本轮弃用东财

R3_CONC = 4             # 第三轮并发
R3_GAP = 0.2            # 第三轮全局最小请求间隔（秒）

QUALITY_BAR = 0.5       # 官方口径占比达标线（低于则下次运行自动重刷升级）

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TMO = 15

# ========================== 工具 ==========================
def bj_now():
    return datetime.utcnow() + timedelta(hours=8)

RE_BS = re.compile(r'^(sh|sz)\.(\d{6})\.csv$')
RE_TS = re.compile(r'^(\d{6})\.(SH|SZ)\.csv$')
RE_PLAIN = re.compile(r'^(\d{6})\.csv$')

def scan_universe():
    """从现有数据文件推断命名风格，返回 (style, {文件名: (市场, 代码)})"""
    bs_map, ts_map, plain_map = {}, {}, {}
    for fn in os.listdir(DATA_DIR):
        if not fn.endswith(".csv"):
            continue
        m = RE_BS.match(fn)
        if m:
            bs_map[fn] = (m.group(1), m.group(2)); continue
        m = RE_TS.match(fn)
        if m:
            ts_map[fn] = (m.group(2).lower(), m.group(1)); continue
        m = RE_PLAIN.match(fn)
        if m:
            num = m.group(1)
            if num[0] == "6":
                plain_map[fn] = ("sh", num)
            elif num[0] in ("0", "3"):
                plain_map[fn] = ("sz", num)
    style, mp = max([("bs", bs_map), ("ts", ts_map), ("plain", plain_map)],
                    key=lambda x: len(x[1]))
    return style, mp

def fn_for(market, num, style):
    if style == "bs":
        return f"{market}.{num}.csv"
    if style == "ts":
        return f"{num}.{market.upper()}.csv"
    return f"{num}.csv"

def load_progress():
    try:
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            p = json.load(f)
    except Exception:
        p = {}
    p.setdefault("run_month", "")
    p.setdefault("completed", [])
    p.setdefault("abandoned", [])
    p.setdefault("attempts", {})
    p.setdefault("official", 0)
    return p

def save_progress(p):
    p["completed"] = sorted(set(p["completed"]))
    p["abandoned"] = sorted(set(p["abandoned"]))
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False)
    os.replace(tmp, PROGRESS_FILE)

def write_rows(market, num, rows, style):
    """整文件重写（原子写入），返回写入行数"""
    if not rows:
        return 0
    rows = sorted(rows, key=lambda r: r[0])
    path = os.path.join(DATA_DIR, fn_for(market, num, style))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(",".join(CSV_HEADER) + "\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")
    os.replace(tmp, path)
    return len(rows)

_thr_lock = threading.Lock()
_thr_last = 0.0
def throttle(gap):
    """全局节流：任意两次请求之间至少间隔 gap 秒"""
    global _thr_last
    with _thr_lock:
        wait = _thr_last + gap - time.time()
        if wait > 0:
            time.sleep(wait)
        _thr_last = time.time()

def fmt_vol(v):
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else str(f)
    except Exception:
        return str(v)

def fill_pct(rows):
    """为无官方涨跌幅的行本地计算 pctChg（基于前收盘）"""
    prev = None
    for r in rows:
        if prev:
            try:
                r[7] = f"{(float(r[4]) / float(prev) - 1) * 100:.4f}"
            except Exception:
                r[7] = ""
        prev = r[4]
    return rows

# ==================== 第一轮：baostock ====================
def round1_baostock(pending, start_d, end_d):
    ok, fail, empty = {}, [], []
    try:
        import baostock as bs
    except Exception:
        print("  baostock 未安装，本轮跳过")
        return ok, list(pending), empty

    print(f"\n【第一轮】baostock 批量拉取 {len(pending)} 只...")

    def try_login():
        try:
            lg = bs.login()
            return lg.error_code == "0"
        except Exception:
            return False

    logged = False
    for i in range(BS_LOGIN_RETRY):
        logged = try_login()
        if logged:
            print("  ✅ baostock 登录成功")
            break
        print(f"  【baostock】登录失败({i+1}/{BS_LOGIN_RETRY})，{BS_LOGIN_WAIT}秒后重试...")
        time.sleep(BS_LOGIN_WAIT)
    if not logged:
        print("  【baostock】本轮放弃，全部转入第二轮（正常降级，非故障）")
        return ok, list(pending), empty

    t0 = time.time()
    for idx, (market, num) in enumerate(pending, 1):
        code = f"{market}.{num}"
        rows = None
        for _ in range(BS_STOCK_RETRY):
            try:
                rs = bs.query_history_k_data_plus(
                    code, "date,open,high,low,close,volume,amount,pctChg,turn",
                    start_date=start_d, end_date=end_d,
                    frequency="d", adjustflag="2")
                if rs.error_code == "0":
                    rows = []
                    while rs.next():
                        rows.append(rs.get_row_data())
                    break
                raise RuntimeError(f"{rs.error_code} {rs.error_msg}")
            except Exception:
                rows = None
                try:
                    bs.logout()
                except Exception:
                    pass
                if not try_login():   # 重建会话失败就不死磕
                    break
        if rows:
            ok[(market, num)] = rows
        elif rows == []:
            empty.append((market, num))
        else:
            fail.append((market, num))
        if idx % 300 == 0:
            el = (time.time() - t0) / 60
            print(f"  【第一轮】进度 {idx}/{len(pending)}，成功{len(ok)} 空{len(empty)} 败{len(fail)}，耗时{el:.0f}分")
    try:
        bs.logout()
    except Exception:
        pass
    return ok, fail, empty

# ==================== 第二轮：东财（带官方字段） ====================
_em_lock = threading.Lock()
_em = {"streak": 0, "trips": 0, "paused_until": 0.0, "off": False}

def em_note(ok_req):
    with _em_lock:
        if ok_req:
            _em["streak"] = 0
            return
        _em["streak"] += 1
        if _em["streak"] >= EM_BREAK_N:
            _em["streak"] = 0
            _em["trips"] += 1
            if _em["trips"] > EM_MAX_TRIPS:
                _em["off"] = True
                print("  【⚡熔断】东财已达最大熔断次数，本轮弃用，转第三轮")
            else:
                _em["paused_until"] = time.time() + EM_COOLDOWN
                print(f"  【⚡熔断】东财连续失败{EM_BREAK_N}只，冷却{EM_COOLDOWN}秒后自动重启（第{_em['trips']}次）")

def em_alive():
    while True:
        with _em_lock:
            if _em["off"]:
                return False
            until = _em["paused_until"]
        if time.time() >= until:
            return True
        time.sleep(3)

def em_fetch(market, num, start_d, end_d):
    """东财日K前复权。字段顺序：0日期 1开 2收 3高 4低 5量(手) 6额 7振幅 8涨跌幅 9涨跌额 10换手率"""
    secid = ("1." if market == "sh" else "0.") + num
    try:
        r = requests.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get",
            params={
                "secid": secid,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101", "fqt": "1",
                "beg": start_d.replace("-", ""), "end": end_d.replace("-", ""),
            },
            headers={"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"},
            timeout=TMO)
        j = r.json()
        if not j or j.get("data") is None:
            return None
        rows = []
        for line in j["data"].get("klines") or []:
            p = line.split(",")
            if len(p) < 11:
                continue
            vol = str(int(round(float(p[5]) * 100)))   # 手→股（东财整手精度）
            rows.append([p[0], p[1], p[3], p[4], p[2], vol, p[6], p[8], p[10]])
        return rows
    except Exception:
        return None

def round2_eastmoney(pending, start_d, end_d, style, prog, stats):
    print(f"\n【第二轮】东财补拉 {len(pending)} 只（并发{EM_CONC}·官方涨跌幅+换手率）...")
    if not pending:
        return []
    fail = []
    lock = threading.Lock()
    cnt = [0]

    def work(mn):
        market, num = mn
        if not em_alive():
            return mn, False
        throttle(EM_GAP)
        rows = em_fetch(market, num, start_d, end_d)
        em_note(rows is not None)
        if rows is None:
            return mn, False
        with lock:
            if rows:
                stats["rows"] += write_rows(market, num, rows, style)
                stats["eastmoney"] += 1
                prog["official"] += 1
            else:
                stats["empty"] += 1
            prog["completed"].append(f"{market}.{num}")
            cnt[0] += 1
            if cnt[0] % 200 == 0:
                print(f"  【第二轮】进度 {cnt[0]}/{len(pending)}")
                save_progress(prog)
        return mn, True

    with ThreadPoolExecutor(max_workers=EM_CONC) as ex:
        futs = [ex.submit(work, mn) for mn in pending]
        for fu in as_completed(futs):
            mn, okk = fu.result()
            if not okk:
                fail.append(mn)
    save_progress(prog)
    return fail

# ==================== 第三轮：新浪→腾讯（保底） ====================
def sina_fetch(market, num, datalen):
    sym = market + num
    for host in ("https://money.finance.sina.com.cn", "https://quotes.sina.cn"):
        try:
            r = requests.get(
                f"{host}/quotes_service/api/json_v2.php/CN_MarketDataService.getKLineData",
                params={"symbol": sym, "scale": "240", "ma": "no", "datalen": str(datalen)},
                headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
                timeout=TMO)
            arr = r.json()
            if isinstance(arr, list) and arr:
                return [[it.get("day", ""), it.get("open", ""), it.get("high", ""),
                         it.get("low", ""), it.get("close", ""), fmt_vol(it.get("volume", "")),
                         "", "", ""] for it in arr]
        except Exception:
            continue
    return None

def tx_fetch(market, num, start_d, end_d):
    sym = market + num
    try:
        r = requests.get(
            "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
            params={"param": f"{sym},day,{start_d},{end_d},640,qfq"},
            headers={"User-Agent": UA}, timeout=TMO)
        j = r.json()
        node = (j.get("data") or {}).get(sym) or {}
        arr = node.get("qfqday") or node.get("day") or []
        rows = []
        for it in arr:
            if len(it) < 6:
                continue
            vol = str(int(round(float(it[5]) * 100)))   # 手→股
            rows.append([it[0], it[1], it[3], it[4], it[2], vol, "", "", ""])
        return rows if rows else None
    except Exception:
        return None

def round3_fallback(pending, start_d, end_d, style, prog, stats):
    print(f"\n【第三轮】新浪→腾讯 保底补拉 {len(pending)} 只...")
    if not pending:
        return []
    fail = []
    lock = threading.Lock()
    cnt = [0]

    def work(mn):
        market, num = mn
        throttle(R3_GAP)
        rows = sina_fetch(market, num, 550)
        src = "sina"
        if rows is None:
            throttle(R3_GAP)
            rows = tx_fetch(market, num, start_d, end_d)
            src = "tencent"
        return mn, rows, src

    with ThreadPoolExecutor(max_workers=R3_CONC) as ex:
        futs = [ex.submit(work, mn) for mn in pending]
        for fu in as_completed(futs):
            mn, rows, src = fu.result()
            if rows is None:
                fail.append(mn)
                continue
            market, num = mn
            if rows:
                rows = fill_pct(sorted(rows, key=lambda r: r[0]))
                with lock:
                    stats["rows"] += write_rows(market, num, rows, style)
                    stats[src] += 1
            else:
                with lock:
                    stats["empty"] += 1
            with lock:
                prog["completed"].append(f"{market}.{num}")
                cnt[0] += 1
                if cnt[0] % 200 == 0:
                    print(f"  【第三轮】进度 {cnt[0]}/{len(pending)}")
                    save_progress(prog)
    save_progress(prog)
    return fail

# ========================== 主流程 ==========================
def main():
    bj = bj_now()
    print("=" * 60)
    print("===== A股月度全量刷新开始（三轮降级版） =====")
    print(f"===== 北京时间 {bj:%Y-%m-%d %H:%M}，数据深度：近{YEARS_BACK}年 =====")
    print("=" * 60)

    if not os.path.isdir(DATA_DIR):
        print(f"❌ 数据目录不存在：{DATA_DIR}，请先运行 init")
        raise SystemExit(1)

    style, mapped = scan_universe()
    universe = sorted(set(mapped.values()))
    total = len(universe)
    if total == 0:
        print("❌ 数据目录里没有股票CSV，请先运行 init")
        raise SystemExit(1)
    print(f"【股票清单】从现有数据目录读取：{total} 只（命名风格：{style}）")

    prog = load_progress()
    this_month = f"{bj:%Y-%m}"
    if prog["run_month"] != this_month:
        prog = {"run_month": this_month, "completed": [], "abandoned": [],
                "attempts": {}, "official": 0}

    all_keys = {f"{m}.{n}" for m, n in universe}
    covered = set(prog["completed"]) | set(prog["abandoned"])
    if all_keys <= covered:
        if prog["official"] >= QUALITY_BAR * total:
            print("✅ 本月已全部刷新完成且官方口径达标，无需重跑（幂等退出）")
            return
        print("ℹ️ 上月已全覆盖，但官方口径占比偏低 → 重置进度再战一轮（自动升级）")
        prog["completed"] = []
        prog["official"] = 0

    skip = set(prog["completed"]) | set(prog["abandoned"])
    pending = [(m, n) for m, n in universe if f"{m}.{n}" not in skip]
    print(f"【运行模式】续传：已完成 {total - len(pending)} 只，本次待刷新 {len(pending)} 只")

    stats = {"baostock": 0, "eastmoney": 0, "sina": 0, "tencent": 0,
             "empty": 0, "abandoned": 0, "rows": 0}
    start_d = (bj - timedelta(days=365 * YEARS_BACK + 5)).strftime("%Y-%m-%d")
    end_d = bj.strftime("%Y-%m-%d")
    t0 = time.time()

    # ---- 第一轮 ----
    ok1, fail1, empty1 = round1_baostock(pending, start_d, end_d)
    for (m, n), rows in ok1.items():
        stats["rows"] += write_rows(m, n, rows, style)
        stats["baostock"] += 1
        prog["official"] += 1
        prog["completed"].append(f"{m}.{n}")
    for m, n in empty1:
        stats["empty"] += 1
        prog["completed"].append(f"{m}.{n}")
    save_progress(prog)
    print(f"【第一轮】完成：baostock {len(ok1)}，空 {len(empty1)}，转第二轮 {len(fail1)}")

    # ---- 第二轮 ----
    fail2 = round2_eastmoney(fail1, start_d, end_d, style, prog, stats)

    # ---- 第三轮 ----
    fail3 = round3_fallback(fail2, start_d, end_d, style, prog, stats)

    # ---- 收尾：连续失败两次的标记放弃（疑似退市）----
    for m, n in fail3:
        key = f"{m}.{n}"
        prog["attempts"][key] = prog["attempts"].get(key, 0) + 1
        if prog["attempts"][key] >= 2:
            stats["abandoned"] += 1
            prog["abandoned"].append(key)
    save_progress(prog)

    print("=" * 60)
    print("===== ✅ 月度刷新完成 =====")
    print(f"  刷新股票：{total} 只（本次处理 {len(pending)} 只）")
    print(f"  各数据源：baostock {stats['baostock']}  东财 {stats['eastmoney']}  "
          f"新浪 {stats['sina']}  腾讯 {stats['tencent']}")
    print(f"  官方口径占比：{prog['official'] / max(total, 1) * 100:.0f}%（baostock+东财/全部）")
    print(f"  空数据（退市/长期停牌）：{stats['empty']} 只")
    if stats["abandoned"]:
        print(f"  连续两轮失败已放弃（疑似退市）：{stats['abandoned']} 只")
    still = len(fail3) - stats["abandoned"]
    print(f"  本次失败：{still} 只" + ("（下次运行自动重试）" if still else " 🎉"))
    print(f"  本次写入K线：{stats['rows']:,} 根")
    print(f"  总耗时：{(time.time() - t0) / 60:.0f} 分钟")
    print("=" * 60)
    print("💡 官方口径占比不足50%时，下次运行会自动重刷升级；达标后再运行秒退（幂等）")

if __name__ == "__main__":
    main()
