# -*- coding: utf-8 -*-
"""
数据源体检（只读，不写任何数据）
==========================================================
目的：摸清当前 runner 出口IP对4个数据源的真实可用性
测法：每源3只代表股（沪主板/深主板/创业板各1），拉近30天日线
判据：能否返回数据 + 关键字段（涨跌幅/换手率）是否非空
耗时：约1~2分钟
"""
import io
import csv
import time
from datetime import datetime, timedelta

import requests

TEST_STOCKS = [("sh", "600519"), ("sz", "000001"), ("sz", "300750")]
BJ = datetime.utcnow() + timedelta(hours=8)
START = (BJ - timedelta(days=30)).strftime("%Y-%m-%d")
END = BJ.strftime("%Y-%m-%d")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TMO = 20

TDX_SERVERS = [
    ("119.147.212.81", 7709), ("180.153.18.170", 7709), ("180.153.18.171", 7709),
    ("202.108.253.130", 7709), ("202.108.253.131", 7709), ("60.12.136.250", 7709),
    ("115.238.56.198", 7709), ("218.108.98.244", 7709),
]

_tdx_api = None


def tdx_connect():
    """连接通达信服务器（只连一次），返回 api 或 None"""
    global _tdx_api
    if _tdx_api is not None:
        return _tdx_api
    try:
        from pytdx.hq import TdxHq_API
    except ImportError:
        return None
    for ip, port in TDX_SERVERS:
        try:
            api = TdxHq_API()
            if api.connect(ip, port, time_out=8):
                _tdx_api = api
                return api
        except Exception:
            continue
    return None


def test_netease(market, num):
    """网易CSV：官方涨跌幅[9] + 官方换手率[10]"""
    code = ("0" if market == "sh" else "1") + num
    try:
        r = requests.get("http://quotes.money.163.com/service/chddata.html",
                         params={"code": code, "start": START.replace("-", ""),
                                 "end": END.replace("-", "")},
                         headers={"User-Agent": UA}, timeout=TMO)
        if r.status_code != 200 or len(r.content) < 60:
            return False, f"HTTP {r.status_code}"
        r.encoding = "gbk"
        reader = csv.reader(io.StringIO(r.text))
        next(reader, None)
        rows = [l for l in reader if len(l) >= 13 and l[3] and l[3] != "None"]
        if not rows:
            return False, "无数据行"
        last = rows[-1]
        pct_ok = "有" if last[9].strip() and last[9] != "None" else "无"
        turn_ok = "有" if last[10].strip() and last[10] != "None" else "无"
        return True, f"{len(rows)}行 涨跌幅{pct_ok} 换手率{turn_ok}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:40]}"


def test_tdx(market, num):
    api = tdx_connect()
    if api is None:
        return False, "服务器连接失败"
    try:
        mkt = 1 if market == "sh" else 0
        bars = api.get_security_bars(9, mkt, num, 0, 40)
        if not bars:
            return False, "无K线"
        return True, f"{len(bars)}根K线"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:40]}"


def test_tencent(market, num):
    sym = market + num
    try:
        r = requests.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                         params={"param": f"{sym},day,{START},{END},40,qfq"},
                         headers={"User-Agent": UA}, timeout=TMO)
        j = r.json()
        node = (j.get("data") or {}).get(sym) or {}
        arr = node.get("qfqday") or node.get("day") or []
        if arr:
            return True, f"{len(arr)}行(qfq)"
        return False, "无数据"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:40]}"


def test_sina(market, num):
    sym = market + num
    try:
        r = requests.get(
            "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketDataService.getKLineData",
            params={"symbol": sym, "scale": "240", "ma": "no", "datalen": "40"},
            headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/"},
            timeout=TMO)
        arr = r.json()
        if isinstance(arr, list) and arr:
            return True, f"{len(arr)}行"
        return False, "无数据"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:40]}"


def main():
    print("=" * 60)
    print("===== 数据源体检（只读，不写任何数据） =====")
    print(f"===== 北京时间 {BJ:%Y-%m-%d %H:%M}，测试区间 {START} ~ {END} =====")
    print("===== 样本：600519茅台 / 000001平安 / 300750宁德 =====")
    print("=" * 60)

    probes = [("网易(官方字段)", test_netease), ("通达信(TCP直连)", test_tdx),
              ("腾讯(qfq)", test_tencent), ("新浪(保底)", test_sina)]
    summary = {}
    for name, fn in probes:
        print(f"\n【{name}】")
        ok_n = 0
        for market, num in TEST_STOCKS:
            t0 = time.time()
            try:
                ok, note = fn(market, num)
            except Exception as e:
                ok, note = False, f"异常 {type(e).__name__}"
            print(f"  {market}{num}: {'✅' if ok else '❌'} {note}（{time.time() - t0:.1f}s）")
            ok_n += 1 if ok else 0
            time.sleep(0.3)
        summary[name] = ok_n
        print(f"  → 小结：{ok_n}/3")

    print("\n" + "=" * 60)
    print("===== 体检结论 =====")
    good = [n for n, k in summary.items() if k >= 2]
    half = [n for n, k in summary.items() if k == 1]
    bad = [n for n, k in summary.items() if k == 0]
    if good:
        print(f"  可用(≥2/3)：{'、'.join(good)}")
    if half:
        print(f"  半死(1/3)：{'、'.join(half)}")
    if bad:
        print(f"  全灭(0/3)：{'、'.join(bad)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
