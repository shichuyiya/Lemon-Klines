#!/usr/bin/env python3
"""新浪期货主连品种 K 线采集（快照仓库期货数据源）。

数据源: InnerFuturesNewService（stock2.finance.sina.com.cn）
- 日K:   getDailyKLine?symbol={VAR}0            上市首日起全历史，单请求全量返回（无翻页）
- 分钟级: getFewMinLine?symbol={VAR}0&type={N}  单请求上限 1023 根，覆盖夜盘：
    type=1  约 3 个交易日（m1）
    type=5  约 3 周（m5）
    type=15 约 2.5 个月（m15）
    type=30 约 4 个月（m30）
    type=60 约 9 个月（m60）
- 实时: hq.sinajs.cn/list=nf_{VAR}0（App 端实时行情前缀与快照符号一致）

主连符号约定: {品种缩写}0 → 快照符号 nf_{VAR}0（如 nf_RB0 螺纹钢连续），
与腾讯/新浪实时行情的 nf_ 前缀对齐，App 侧无缝衔接。

口径: 主连为未复权真实拼接价（换月跳空保留）。期货单合约无除权概念，
连续合约的"前复权"是换月价差抹平——如需平滑序列由消费端自算差价平移。
bar 字段: date/open/close/high/low/volume/amount
- o/h/l/c 元；v 手；p(持仓) 丢弃；s(结算价) 丢弃；新浪未给成交额 → amount=0
"""
from __future__ import annotations

import re
import time
import urllib.error
import urllib.request

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Lemon-Light-kline-backfill/1.1"
DAY_URL = "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t=/InnerFuturesNewService.getDailyKLine?symbol={sym}"
MIN_URL = "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t=/InnerFuturesNewService.getFewMinLine?symbol={sym}&type={n}"
# 快照周期 → 新浪 getFewMinLine type；day 走 getDailyKLine 单列
PERIOD_TYPE = {"m1": 1, "m5": 5, "m15": 15, "m30": 30, "m60": 60}
# 品种缩写+0 主连；防注入白名单
SYM_RE = re.compile(r"^[A-Z]{1,3}0$")
BAR_RE = re.compile(r'\{"d":"([\d\- :]+)","o":"([\d.]+)","h":"([\d.]+)","l":"([\d.]+)","c":"([\d.]+)","v":"([\d.]+)"')


def fetch_kline(sym: str, kind: str, retries: int = 3) -> list[dict]:
    """拉取主连日K/分钟级K线。kind: day | m1 | m5 | m15 | m30 | m60。失败重试，耗尽返回 []。"""
    if not SYM_RE.fullmatch(sym):
        raise ValueError(f"bad futures symbol: {sym!r}")
    if kind == "day":
        url = DAY_URL.format(sym=sym)
    else:
        n = PERIOD_TYPE.get(kind)
        if n is None:
            raise ValueError(f"bad futures kind: {kind!r}")
        url = MIN_URL.format(sym=sym, n=n)
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn"})
            with urllib.request.urlopen(req, timeout=90) as res:
                text = res.read().decode("utf-8", errors="replace")
            if '"__ERROR"' in text or text.rstrip().endswith("(null);"):
                raise RuntimeError(f"sina returned error/null for {sym} {kind}")
            bars = []
            for m in BAR_RE.finditer(text):
                d, o, h, l, c, v = m.groups()
                if kind != "day":
                    # '2026-06-30 22:00:00' → '2026-06-30 22:00'（与股票 m30 时刻格式对齐）
                    d = d[:16]
                bars.append({"date": d, "open": float(o), "close": float(c),
                             "high": float(h), "low": float(l),
                             "volume": float(v), "amount": 0.0})
            return bars
        except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
            last = exc
            print(f"sina {sym} {kind} attempt {attempt}/{retries} failed: {exc}", flush=True)
            time.sleep(5 * attempt)
    print(f"sina {sym} {kind} gave up: {last}", flush=True)
    return []
