#!/usr/bin/env python3
"""通达信官网日线包底座 + 前复权计算（全历史、零限流、一次到位）。

数据源: https://www.tdx.com.cn/products/data/data/vipdoc/{sh,sz,bj}lday.zip
- .day 二进制 32 字节/条: date(YYYYMMDD) open*100 high*100 low*100 close*100
  amount(元,float) volume(股) 保留字段
- 不复权原始价。前复权 = 原始价 × 累计复权因子，因子链由分红事件推导。

复权因子来源（方案A）: 腾讯 fqkline qfq 接口的除权日 K 线自带分红 dict:
  {"nd":"2023","fh_sh":"1.4"(每股派息,元), "sg_sh":"0"(送股比例), "peigu":"0",
   "peigu_price":"0", "cqr":"2024-06-19"(除权日), ...}
除权日r的因子 = (收盘r-1 - 派息 + 配股价×配股比例) / ((1+送股比例+配股比例) × 收盘r-1)
对每个除权日，r 之前的所有历史价格 × 该因子（标准前复权链式累积）。

-volume 口径: 官网"股"、腾讯 m30 "手"×100——本模块输出保持"股"，m30 合并层已有换算。
"""
from __future__ import annotations

import json
import re
import struct
import time
import urllib.request
import zipfile
from pathlib import Path

TDX_BASE = "https://www.tdx.com.cn/products/data/data/vipdoc"
DAY_STRUCT = struct.Struct("<IIIIIfII")
DAY_SIZE = DAY_STRUCT.size  # 32
# 保留沪深 A 股/B 股 + ETF/LOF；过滤债券回购、可转债、4-5 位残余代码
_KEEP_PREFIX = (
    "600", "601", "603", "605", "688", "689", "900",          # 沪 A/B
    "000", "001", "002", "003", "300", "301", "302", "200",   # 深 A/B
    "510", "511", "512", "513", "515", "516", "517", "518", "560", "561",
    "562", "563", "588", "589", "501", "502", "506", "159",   # 沪深 ETF/LOF
)


def keep(code: str) -> bool:
    return len(code) == 6 and code.isdigit() and code.startswith(_KEEP_PREFIX)


def download_zip(market: str, dest: Path, timeout: int = 600, retries: int = 3) -> Path:
    """下载 {market}lday.zip（sh/sz/bj）。已存在且校验通过则跳过。

    下载中断/CDN 异常会产生截断或 HTML 伪装文件（>1MB 但不是 zip，
    实测 Actions runner 上出现过），必须用 is_zipfile 校验——
    残缺 zip 缺 central directory，is_zipfile 返回 False。
    校验失败自动重下，重试耗尽抛 RuntimeError 由调用方回退。"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{TDX_BASE}/{market}lday.zip"
    last: Exception | None = None
    for attempt in range(1, retries + 1):
        if dest.exists() and dest.stat().st_size > (1 << 20) and zipfile.is_zipfile(dest):
            return dest
        if dest.exists():  # 残留的截断/损坏文件，删掉重下
            try: dest.unlink()
            except OSError: pass
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as res, dest.open("wb") as f:
                while True:
                    chunk = res.read(1 << 20)
                    if not chunk: break
                    f.write(chunk)
            if zipfile.is_zipfile(dest):
                return dest
            last = zipfile.BadZipFile(
                f"{market}lday.zip downloaded but invalid (size={dest.stat().st_size})")
        except (urllib.error.URLError, TimeoutError, OSError, zipfile.BadZipFile) as exc:
            last = exc
        print(f"tdx {market}lday.zip attempt {attempt}/{retries} failed: {last}", flush=True)
        time.sleep(10 * attempt)
    raise RuntimeError(f"{market}lday.zip download failed after {retries} attempts: {last}")


def parse_lday_zip(zip_path: Path, market: str) -> dict[str, list[dict]]:
    """解包 zip 内全部 .day → {symbol: [bar,...]}，bar 按日期升序，不复权。
    symbol 市场前缀取 .day 文件名自带前缀（官网包内文件名即 shXXXXXX.day）。"""
    out: dict[str, list[dict]] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            m = re.fullmatch(r"(sh|sz|bj)(\d{6})\.day", Path(name).name)
            if not m: continue
            market_real, code = m.group(1), m.group(2)
            if not keep(code): continue
            raw = zf.read(name)
            n = len(raw) // DAY_SIZE
            if n <= 0: continue
            bars = []
            for i in range(n):
                d, o, h, l, c, amt, vol, _rsv = DAY_STRUCT.unpack_from(raw, i * DAY_SIZE)
                if not 19900101 <= d <= 29991231: continue
                bars.append({
                    "date": f"{d // 10000}-{d // 100 % 100:02d}-{d % 100:02d}",
                    "open": o / 100,
                    "high": h / 100,
                    "low": l / 100,
                    "close": c / 100,
                    "amount": amt,
                    "volume": vol,
                })
            if bars:
                bars.sort(key=lambda b: b["date"])
                out[market_real + code] = bars
    return out


# ---------- 复权 ----------

def qfq_events_from_rows(rows: list) -> dict[str, dict]:
    """腾讯 fqkline 行里的分红 dict → {除权日: 事件}。rows 为原始 p 列表（备用口径）。"""
    events: dict[str, dict] = {}
    for p in rows or []:
        if not isinstance(p, list) or len(p) < 7 or not isinstance(p[6], dict):
            continue
        ev = p[6]
        cqr = ev.get("cqr") or ""
        if not cqr: continue
        try:
            fh = float(ev.get("fh_sh") or 0)      # 每股派息（元，税前）
            sg = float(ev.get("sg_sh") or 0)      # 送股比例
            pg = float(ev.get("peigu") or 0)      # 配股比例
            pgp = float(ev.get("peigu_price") or 0)
        except (TypeError, ValueError):
            continue
        if sg >= 1: sg = sg / 10.0  # 部分数据按"每10股"填
        if pg >= 1: pg = pg / 10.0
        events[cqr] = {"fh": fh, "sg": sg, "pg": pg, "pgp": pgp}
    return events


def anchor_ratio(raw_bars: list[dict], qfq_bars: list[dict]) -> tuple[float, str]:
    """用腾讯 qfq 与官网原始价的重叠区算锚定比值：
    ratio = qfq昨收 / 原始昨收（取重叠区最早一日，即最近一次除权前的基准）。
    返回 (ratio, 该日日期)。无除权时 ratio=1（qfq==原始）。"""
    qfq_map = {b["date"]: b["close"] for b in qfq_bars}
    for b in raw_bars:
        q = qfq_map.get(b["date"])
        if q and b["close"] > 0:
            return q / b["close"], b["date"]
    return 1.0, ""


def apply_anchor_qfq(raw_bars: list[dict], qfq_bars: list[dict]) -> list[dict]:
    """前复权（锚定比值法，不依赖分红字段口径）：
    - 重叠区（腾讯 qfq 覆盖的最近 ~640 根）直接用腾讯 qfq 价（天然前复权、含除权日当天）
    - 重叠区之前的历史：原始价 × anchor_ratio（重叠区首日 qfq/原始 比值，
      即"最近一次除权"的整体复权系数——更早的除权在原始价里本来就已反映，
      前复权只需修正"基准日之后"的除权）
    成交量/成交额不动（仅价格口径复权，量能用原始值）。
    """
    if not raw_bars: return []
    if not qfq_bars: return raw_bars
    qfq_map = {b["date"]: b for b in qfq_bars}
    overlap_first = min(d for d in qfq_map if any(b["date"] == d for b in raw_bars)) \
        if any(b["date"] in qfq_map for b in raw_bars) else ""
    ratio = 1.0
    if overlap_first:
        raw0 = next(b for b in raw_bars if b["date"] == overlap_first)
        q0 = qfq_map[overlap_first]
        if raw0["close"] > 0: ratio = q0["close"] / raw0["close"]
    out = []
    for b in raw_bars:
        q = qfq_map.get(b["date"])
        if q is not None:
            out.append({**b, "open": q["open"], "high": q["high"], "low": q["low"], "close": q["close"]})
        elif ratio != 1.0:
            out.append({**b, "open": round(b["open"] * ratio, 4), "high": round(b["high"] * ratio, 4),
                        "low": round(b["low"] * ratio, 4), "close": round(b["close"] * ratio, 4)})
        else:
            out.append(b)
    return out
