#!/usr/bin/env python3
"""Build and publish Lemon Light K-line snapshots (Tencent data source).

History: the original script pulled daily/weekly klines from Eastmoney.  Eastmoney
progressively blocked GitHub Actions runner IP ranges (clist pages first, then
stock/kline endpoints -- curl exit 52 empty replies), so three consecutive runs
failed or hit the 6-hour job timeout.  This version switches to Tencent's
web.ifzq.gtimg.cn kline API (same family as the app's own quote service, tolerant
of datacenter IPs) and reads the symbol universe from scripts/universe.json
generated locally, so Actions never requests a listing endpoint at all.

Weekly bars are derived locally from daily bars (identical semantics, halves the
request count).  A 5.5h deadline guard guarantees the snapshot is always packed
and published even under extreme latency.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import random
import re
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".kline-cache"
UNIVERSE_FILE = ROOT / "scripts" / "universe.json"
SYMBOLS_FILE = ROOT / "scripts" / "symbols.txt"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Lemon-Light-kline-backfill/1.1"
PERIODS = {"day": (101, 1023), "week": (102, 260), "m30": (30, 320)}
REQUEST_DELAY = 0.35
REQUEST_TIMEOUT = 60
REQUEST_RETRIES = 5
QQ_HOSTS = ("web.ifzq.gtimg.cn", "ifzq.gtimg.cn")
SNAPSHOT_MAGIC = b"LKSNAP1"
SAFE_SNAPSHOT_NAME = re.compile(r"^(?:sh|sz|bj)\d{6}\.(?:day|week|m30|m60|m15|m5)\.json\.gz$|^_universe\.json$")


def http_json(url: str, retries: int = REQUEST_RETRIES) -> dict:
    """Fetch a Tencent market endpoint with host rotation + backoff."""
    last: Exception | None = None
    parsed = urllib.parse.urlsplit(url)
    for attempt in range(retries):
        host = QQ_HOSTS[attempt % len(QQ_HOSTS)]
        request_url = urllib.parse.urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
        try:
            curl = subprocess.run([
                "curl", "--ipv4", "--fail-with-body", "--silent", "--show-error", "--location",
                "--connect-timeout", "15", "--max-time", str(REQUEST_TIMEOUT),
                "-A", UA, "-e", "https://gu.qq.com/", request_url,
            ], capture_output=True, text=True, check=True)
            text = curl.stdout
            # ifzq.gtimg.cn 裸域会带 JSONP 前缀（var xxx=...;），web. 域返回纯 JSON。
            text = re.sub(r"^[^=]+=", "", text, count=1).rstrip().rstrip(";")
            return json.loads(text)
        except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            last = exc
            if isinstance(exc, FileNotFoundError):
                try:
                    req = urllib.request.Request(request_url, headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"})
                    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as res:
                        return json.loads(res.read().decode("utf-8"))
                except (TimeoutError, urllib.error.URLError, json.JSONDecodeError) as fallback_exc:
                    last = fallback_exc
        if attempt + 1 < retries:
            time.sleep(min(30, 2 ** attempt) + random.random())
    raise RuntimeError(f"tencent request failed after {retries} retries: {last}")


def symbols_from_file() -> list[str]:
    if not SYMBOLS_FILE.exists():
        return []
    out = []
    for line in SYMBOLS_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip().lower()
        if re.fullmatch(r"(?:sh|sz|bj)\d{6}", s):
            out.append(s)
    return sorted(set(out))


def universe_from_file() -> list[str]:
    """读取本地生成的 scripts/universe.json（家宽生成后提交，Actions 不再请求列表接口）。"""
    try:
        data = json.loads(UNIVERSE_FILE.read_text(encoding="utf-8"))
        values = data.get("symbols") if isinstance(data, dict) else data
        return sorted({s for s in values if isinstance(s, str) and re.fullmatch(r"(?:sh|sz|bj)\d{6}", s)})
    except (OSError, ValueError, TypeError):
        return []


def save_universe(symbols: list[str]) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / "_universe.json").write_text(json.dumps(symbols, separators=(",", ":")), encoding="utf-8")


def fetch(symbol: str, period: str, limit: int) -> list[dict]:
    time.sleep(max(0.0, REQUEST_DELAY) + random.random() * 0.2)
    market, code = symbol[:2], symbol[2:]
    if period == "m30":
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/kline/mkline?"
               f"param={symbol},m30,,{limit}")
    else:
        kind = "day" if period == "day" else "week"
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
               f"param={symbol},{kind},,,{limit},qfq")
    data = http_json(url).get("data") or {}
    node = data.get(symbol) or {}
    if period == "m30":
        rows = node.get("m30") or []
        out = []
        for p in rows:
            if len(p) < 6: continue
            try:
                out.append({"date": f"{p[0][:4]}-{p[0][4:6]}-{p[0][6:8]} {p[0][8:10]}:{p[0][10:12]}",
                            "open": float(p[1]), "close": float(p[2]), "high": float(p[3]),
                            "low": float(p[4]), "volume": float(p[5]), "amount": 0.0})
            except (ValueError, IndexError):
                continue
        return out
    rows = node.get(f"qfq{period if period != 'day' else 'day'}") or node.get(period) or []
    out = []
    for p in rows:
        if len(p) < 6: continue
        try:
            out.append({"date": p[0], "open": float(p[1]), "close": float(p[2]), "high": float(p[3]),
                        "low": float(p[4]), "volume": float(p[5]), "amount": float(p[6]) if len(p) > 6 else 0})
        except (ValueError, IndexError):
            continue
    return out


def save_cache(symbol: str, period: str, bars: list[dict]) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    payload = {"symbol": symbol, "tf": period, "bars": bars, "updatedAt": int(time.time() * 1000)}
    (CACHE / f"{symbol}.{period}.json.gz").write_bytes(gzip.compress(json.dumps(payload, separators=(",", ":")).encode()))


def derive_week_bars(day_bars: list[dict]) -> list[dict]:
    """由日线聚合周线（同东财/腾讯口径：周内最后交易日为日期、首日开、末日收、极值高低、量额求和）。"""
    grouped: dict[tuple[int, int], list[dict]] = {}
    for b in day_bars:
        y, m, d = b["date"].split("-")
        iso = date(int(y), int(m), int(d)).isocalendar()
        grouped.setdefault((iso[0], iso[1]), []).append(b)
    out: list[dict] = []
    for key in sorted(grouped):
        days = grouped[key]
        out.append({
            "date": days[-1]["date"],
            "open": days[0]["open"],
            "close": days[-1]["close"],
            "high": max(x["high"] for x in days),
            "low": min(x["low"] for x in days),
            "volume": sum(x["volume"] for x in days),
            "amount": sum(x.get("amount", 0.0) for x in days),
        })
    return out


def pack_snapshot(out: Path) -> str:
    names = sorted(p.name for p in CACHE.iterdir() if p.name.endswith(".json.gz") or p.name == "_universe.json")
    if not names: raise RuntimeError("no cache files produced")
    files = [{"name": n, "size": (CACHE / n).stat().st_size} for n in names]
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    header = json.dumps({"date": date, "files": files}, separators=(",", ":")).encode()
    with out.open("wb") as f:
        f.write(SNAPSHOT_MAGIC + struct.pack("<I", len(header)) + header)
        for item in files: f.write((CACHE / item["name"]).read_bytes())
    return hashlib.sha256(out.read_bytes()).hexdigest()


def unpack_snapshot(raw: bytes) -> int:
    """Merge a snapshot into CACHE, validating names and lengths."""
    if len(raw) < len(SNAPSHOT_MAGIC) + 4 or raw[:len(SNAPSHOT_MAGIC)] != SNAPSHOT_MAGIC:
        raise RuntimeError("invalid snapshot magic")
    pos = len(SNAPSHOT_MAGIC)
    header_len = struct.unpack_from("<I", raw, pos)[0]
    pos += 4
    if header_len > 32 * 1024 * 1024 or pos + header_len > len(raw):
        raise RuntimeError("invalid snapshot header length")
    header = json.loads(raw[pos:pos + header_len].decode("utf-8"))
    pos += header_len
    files = header.get("files")
    if not isinstance(files, list): raise RuntimeError("invalid snapshot file list")
    CACHE.mkdir(parents=True, exist_ok=True)
    count = 0
    for item in files:
        name, size = item.get("name"), item.get("size")
        if not isinstance(name, str) or not SAFE_SNAPSHOT_NAME.fullmatch(name):
            raise RuntimeError(f"unsafe snapshot filename: {name!r}")
        if not isinstance(size, int) or size < 0 or pos + size > len(raw):
            raise RuntimeError(f"invalid snapshot file size: {name}")
        (CACHE / name).write_bytes(raw[pos:pos + size])
        pos += size
        count += 1
    if pos != len(raw): raise RuntimeError("snapshot has trailing bytes")
    return count


def gh(method: str, url: str, token: str, body: bytes | None = None, content_type: str = "application/json", retries: int = 5):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "User-Agent": UA}
    if body is not None: headers["Content-Type"] = content_type
    last_body = b""
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=90) as res: return res.status, res.read()
        except urllib.error.HTTPError as e:
            last_body = e.read()
            if e.code not in (408, 425, 429, 500, 502, 503, 504): return e.code, last_body
        except (TimeoutError, urllib.error.URLError) as e:
            last_body = str(e).encode()
        if attempt + 1 < retries:
            time.sleep(min(30, 2 ** attempt) + (attempt * 0.17))
    return 599, last_body


def restore_latest_snapshot(repo: str, token: str) -> int:
    """Restore the newest release asset before a clean Actions run."""
    base = f"https://api.github.com/repos/{repo}"
    status, raw = gh("GET", f"{base}/releases/tags/kline-snapshot", token)
    if status == 404: return 0
    if status != 200: raise RuntimeError(f"read release for restore failed: HTTP {status} {raw[:300]!r}")
    release = json.loads(raw)
    assets = [a for a in release.get("assets", []) if a.get("name", "").endswith(".ksnap")]
    if not assets: return 0
    asset = max(assets, key=lambda a: a.get("created_at", a.get("name", "")))
    req = urllib.request.Request(asset["url"], headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/octet-stream",
        "User-Agent": UA,
    })
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            count = unpack_snapshot(response.read())
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"download snapshot failed: {exc}") from exc
    print(f"restored {asset.get('name')} ({count} files)", flush=True)
    return count


def publish(snapshot: Path, repo: str, token: str, keep: int) -> None:
    base = f"https://api.github.com/repos/{repo}"
    status, raw = gh("GET", f"{base}/releases/tags/kline-snapshot", token)
    if status == 200: release = json.loads(raw)
    elif status == 404:
        status, raw = gh("POST", f"{base}/releases", token, json.dumps({"tag_name": "kline-snapshot", "name": "K 线快照", "prerelease": True}).encode())
        if status != 201: raise RuntimeError(f"create release failed: HTTP {status} {raw[:300]!r}")
        release = json.loads(raw)
    else: raise RuntimeError(f"read release failed: HTTP {status} {raw[:300]!r}")
    name = f"kline-snapshot-{datetime.now(timezone.utc).strftime('%Y%m%d')}.ksnap"
    for asset in release.get("assets", []):
        if asset.get("name") == name: gh("DELETE", f"{base}/releases/assets/{asset['id']}", token)
    upload = f"https://uploads.github.com/repos/{repo}/releases/{release['id']}/assets?name={urllib.parse.quote(name)}"
    status, raw = gh("POST", upload, token, snapshot.read_bytes(), "application/octet-stream")
    if status != 201: raise RuntimeError(f"upload failed: HTTP {status} {raw[:300]!r}")
    status, raw = gh("GET", f"{base}/releases/tags/kline-snapshot", token)
    if status == 200:
        assets = sorted(json.loads(raw).get("assets", []), key=lambda a: a.get("name", ""), reverse=True)
        for old in assets[keep:]: gh("DELETE", f"{base}/releases/assets/{old['id']}", token)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all-a-shares", action="store_true", help="使用 scripts/universe.json 全量（默认）")
    ap.add_argument("--all-m30", action="store_true", help="将全体 A 股 30 分钟数据按批次拉取")
    ap.add_argument("--batch-index", type=int, default=0, help="30 分钟批次编号，从 0 开始")
    ap.add_argument("--batch-size", type=int, default=100, help="每个 30 分钟批次的股票数")
    ap.add_argument("--skip-daily-weekly", action="store_true", help="批量补分钟线时跳过日线/周线")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.35, help="每个行情请求前的秒数间隔")
    ap.add_argument("--keep", type=int, default=7)
    args = ap.parse_args()
    global REQUEST_DELAY
    REQUEST_DELAY = max(0.0, args.delay)
    if args.batch_index < 0 or args.batch_size <= 0:
        raise RuntimeError("batch-index 必须 >= 0，batch-size 必须 > 0")
    token, repo = os.environ.get("GITHUB_TOKEN", ""), os.environ.get("SNAPSHOT_REPO", "")
    if token and repo: restore_latest_snapshot(repo, token)
    watched = symbols_from_file()
    if args.all_a_shares or args.all_m30:
        universe = universe_from_file()
        if not universe:
            raise RuntimeError("scripts/universe.json 缺失或为空——本地运行 .verify/pull-universe.cjs 生成后提交")
        save_universe(universe)
    else:
        universe = watched
    if not universe: raise RuntimeError("no symbols; add symbols to scripts/symbols.txt or use --all-a-shares")
    # 周线不单独请求：由日线按自然周聚合（derive_week_bars），请求量减半。
    jobs = [] if args.skip_daily_weekly else [(s, "day", PERIODS["day"][1]) for s in universe]
    m30_symbols = universe if args.all_m30 else watched
    if args.all_m30:
        start = args.batch_index * args.batch_size
        m30_symbols = m30_symbols[start:start + args.batch_size]
        if not m30_symbols:
            raise RuntimeError(f"batch-index {args.batch_index} 超出范围（共 {len(universe)} 只股票）")
        print(f"m30 batch {args.batch_index}: {start}-{start + len(m30_symbols) - 1} / {len(universe)}", flush=True)
    jobs += [(s, "m30", PERIODS["m30"][1]) for s in m30_symbols]
    if not jobs: raise RuntimeError("没有待执行任务")
    workers = max(1, min(args.workers, 4))
    # 死线守卫：Actions 单 job 6 小时硬上限，5.5 小时强制收尾——
    # 剩余任务直接放弃（缺口靠快照合并 + 下轮运行自然补齐），
    # 保证无论如何都能走到打包发布那一步。
    started = time.monotonic()
    deadline = started + 5.5 * 3600
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch, s, p, lim): (s, p) for s, p, lim in jobs}
        failures = []
        aborted = 0
        for i, fut in enumerate(as_completed(futures), 1):
            s, p = futures[fut]
            if time.monotonic() > deadline:
                aborted = len(futures) - i + 1
                break
            try:
                bars = fut.result()
            except Exception as exc:
                failures.append(f"{s} {p}: {exc}")
                print(f"[{i}/{len(futures)}] FAILED {s} {p}: {exc}", flush=True)
                continue
            if bars:
                save_cache(s, p, bars)
                if p == "day":
                    save_cache(s, "week", derive_week_bars(bars))
            print(f"[{i}/{len(futures)}] {s} {p} {len(bars)}", flush=True)
        if aborted:
            for f in futures:
                f.cancel()
            print(f"deadline reached; aborted {aborted}/{len(futures)} remaining requests (will catch up next run)", flush=True)
    if failures:
        print(f"completed with {len(failures)}/{len(jobs)} failed requests", file=sys.stderr)
        if len(failures) > max(20, len(jobs) // 20):
            raise RuntimeError("too many failed market requests; snapshot was not published")
    out = ROOT / "kline-snapshot.ksnap"
    digest = pack_snapshot(out)
    print(f"snapshot={out} sha256={digest} bytes={out.stat().st_size}")
    if token and repo: publish(out, repo, token, args.keep)
    else: print("GITHUB_TOKEN/SNAPSHOT_REPO not set; snapshot built locally only")
    return 0


if __name__ == "__main__":
    try: raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
