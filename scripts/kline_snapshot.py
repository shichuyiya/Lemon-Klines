#!/usr/bin/env python3
"""Build and publish Lemon Light K-line snapshots from Eastmoney.

The script intentionally uses only Python's standard library so it can run in
GitHub Actions without installing a data SDK.  Daily/weekly history is pulled
for the A-share universe; 30-minute history is pulled only for symbols listed
in scripts/symbols.txt by default. ``--all-m30`` enables resumable batches for
the whole A-share universe.
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
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".kline-cache"
SYMBOLS_FILE = ROOT / "scripts" / "symbols.txt"
UA = "Lemon-Light-kline-backfill/1.0"
PERIODS = {"day": (101, 1023), "week": (102, 260), "m30": (30, 320)}
REQUEST_DELAY = 0.5
REQUEST_TIMEOUT = 90
REQUEST_RETRIES = 8
MARKET_HOSTS = {
    "push2.eastmoney.com": ("push2.eastmoney.com", "80.push2.eastmoney.com", "82.push2.eastmoney.com", "81.push2.eastmoney.com"),
    "push2his.eastmoney.com": ("push2his.eastmoney.com", "80.push2his.eastmoney.com", "82.push2his.eastmoney.com", "81.push2his.eastmoney.com"),
}
SNAPSHOT_MAGIC = b"LKSNAP1"
SAFE_SNAPSHOT_NAME = re.compile(r"^(?:sh|sz|bj)\d{6}\.(?:day|week|m30|m60|m15)\.json\.gz$|^_universe\.json$")


def http_json(url: str, retries: int = REQUEST_RETRIES) -> dict:
    """Fetch a market endpoint with backoff for transient gateway/rate-limit errors."""
    last: Exception | None = None
    parsed = urllib.parse.urlsplit(url)
    hosts = MARKET_HOSTS.get(parsed.hostname or "", (parsed.hostname or "",))
    for attempt in range(retries):
        host = hosts[attempt % len(hosts)]
        request_url = urllib.parse.urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
        try:
            # GitHub's Ubuntu runners occasionally stall during Python/OpenSSL's
            # handshake with Eastmoney, while the system curl path is reliable.
            curl = subprocess.run([
                "curl", "--ipv4", "--fail-with-body", "--silent", "--show-error", "--location",
                "--retry", "2", "--retry-all-errors", "--retry-delay", "2",
                "--connect-timeout", "20", "--max-time", str(REQUEST_TIMEOUT),
                "-A", UA, "-e", "https://quote.eastmoney.com/", request_url,
            ], capture_output=True, text=True, check=True)
            return json.loads(curl.stdout)
        except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            last = exc
            if isinstance(exc, FileNotFoundError):
                # Keep local/non-Linux environments working if curl is absent.
                try:
                    req = urllib.request.Request(request_url, headers={"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"})
                    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as res:
                        return json.loads(res.read().decode("utf-8"))
                except (TimeoutError, urllib.error.URLError, json.JSONDecodeError) as fallback_exc:
                    last = fallback_exc
        if attempt + 1 < retries:
            time.sleep(min(60, 2 ** attempt) + random.random())
    raise RuntimeError(f"market request failed after {retries} retries: {last}")


def symbols_from_file() -> list[str]:
    if not SYMBOLS_FILE.exists():
        return []
    out = []
    for line in SYMBOLS_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip().lower()
        if re.fullmatch(r"(?:sh|sz|bj)\d{6}", s):
            out.append(s)
    return sorted(set(out))


def all_a_shares() -> list[str]:
    """拉全市场 A 股代码列表。Actions 的出口 IP 对东财 clist 网关单页偶发被拒（curl exit 52 空响应），
    且同一备用 host 上重试往往继续被拒——单页失败改为跳过继续（缺口下一轮运行自然补上，
    铺底数据不需要一次拿全），仅当总收获过少（<3000，说明网关大面积拒绝）才报错。"""
    out: list[str] = []
    failures = 0
    # pz=100 is materially more stable than 200 on Eastmoney's gateway.
    for page in range(1, 90):
        q = urllib.parse.urlencode({"pn": page, "pz": 100, "po": 0, "np": 1, "fltt": 2, "invt": 2,
                                    "fid": "f12", "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
                                    "fields": "f12,f13"})
        try:
            data = http_json(f"https://push2.eastmoney.com/api/qt/clist/get?{q}").get("data") or {}
        except Exception as exc:
            failures += 1
            if failures > 8:
                raise
            print(f"clist page {page} failed ({exc}); skip", flush=True)
            continue
        rows = data.get("diff") or {}
        if isinstance(rows, dict): rows = list(rows.values())
        if not rows: break
        for row in rows:
            code, market = str(row.get("f12", "")), int(row.get("f13", -1))
            if re.fullmatch(r"\d{6}", code):
                out.append(("sh" if market == 1 else "sz" if market == 0 else "bj") + code)
        if data.get("total") and len(out) + failures * 100 >= int(data["total"]): break
    if len(out) < 3000:
        raise RuntimeError(f"universe too small ({len(out)}); eastmoney gateway likely blocking")
    return sorted(set(out))


def save_universe(symbols: list[str]) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / "_universe.json").write_text(json.dumps(symbols, separators=(",", ":")), encoding="utf-8")


def cached_universe() -> list[str]:
    path = CACHE / "_universe.json"
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
        return sorted({s for s in values if isinstance(s, str) and re.fullmatch(r"(?:sh|sz|bj)\d{6}", s)})
    except (OSError, ValueError, TypeError):
        return []


def fetch(symbol: str, period: str, limit: int) -> list[dict]:
    # Eastmoney 对大量并发请求较敏感；即使线程池只有少量 worker，
    # 也在每个请求前留出间隔，避免首批任务形成瞬时洪峰。
    time.sleep(max(0.0, REQUEST_DELAY) + random.random() * 0.25)
    klt = PERIODS[period][0]
    market, code = symbol[:2], symbol[2:]
    secid = ("1." if market == "sh" else "0.") + code
    q = urllib.parse.urlencode({"secid": secid, "klt": klt, "fqt": 1, "lmt": limit,
                                "end": "20500101", "fields1": "f1,f2,f3,f4,f5,f6",
                                "fields2": "f51,f52,f53,f54,f55,f56,f57"})
    rows = (http_json(f"https://push2his.eastmoney.com/api/qt/stock/kline/get?{q}").get("data") or {}).get("klines") or []
    out = []
    for line in rows:
        p = line.split(",")
        if len(p) < 6: continue
        try:
            out.append({"date": p[0], "open": float(p[1]), "close": float(p[2]), "high": float(p[3]),
                        "low": float(p[4]), "volume": float(p[5]), "amount": float(p[6]) if len(p) > 6 else 0})
        except ValueError:
            continue
    return out


def save_cache(symbol: str, period: str, bars: list[dict]) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    payload = {"symbol": symbol, "tf": period, "bars": bars, "updatedAt": int(time.time() * 1000)}
    (CACHE / f"{symbol}.{period}.json.gz").write_bytes(gzip.compress(json.dumps(payload, separators=(",", ":")).encode()))


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
    ap.add_argument("--all-a-shares", action="store_true")
    ap.add_argument("--all-m30", action="store_true", help="将全体 A 股 30 分钟数据按批次拉取")
    ap.add_argument("--batch-index", type=int, default=0, help="30 分钟批次编号，从 0 开始")
    ap.add_argument("--batch-size", type=int, default=100, help="每个 30 分钟批次的股票数")
    ap.add_argument("--skip-daily-weekly", action="store_true", help="批量补分钟线时跳过日线/周线")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--delay", type=float, default=0.5, help="每个行情请求前的秒数间隔")
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
        try:
            universe = all_a_shares()
            save_universe(universe)
        except Exception as exc:
            universe = cached_universe()
            if universe:
                print(f"universe discovery failed; using cached universe ({len(universe)}): {exc}", flush=True)
            else:
                raise
    else:
        universe = watched
    if not universe: raise RuntimeError("no symbols; add symbols to scripts/symbols.txt or use --all-a-shares")
    jobs = [] if args.skip_daily_weekly else [(s, p, PERIODS[p][1]) for s in universe for p in ("day", "week")]
    m30_symbols = universe if args.all_m30 else watched
    if args.all_m30:
        start = args.batch_index * args.batch_size
        m30_symbols = m30_symbols[start:start + args.batch_size]
        if not m30_symbols:
            raise RuntimeError(f"batch-index {args.batch_index} 超出范围（共 {len(universe)} 只股票）")
        print(f"m30 batch {args.batch_index}: {start}-{start + len(m30_symbols) - 1} / {len(universe)}", flush=True)
    jobs += [(s, "m30", PERIODS["m30"][1]) for s in m30_symbols]
    if not jobs: raise RuntimeError("没有待执行任务")
    workers = max(1, min(args.workers, 2))
    if args.workers != workers:
        print(f"workers={args.workers} too high for Eastmoney; capped at {workers}", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch, s, p, lim): (s, p) for s, p, lim in jobs}
        failures = []
        for i, fut in enumerate(as_completed(futures), 1):
            s, p = futures[fut]
            try:
                bars = fut.result()
            except Exception as exc:
                failures.append(f"{s} {p}: {exc}")
                print(f"[{i}/{len(futures)}] FAILED {s} {p}: {exc}", flush=True)
                continue
            if bars: save_cache(s, p, bars)
            print(f"[{i}/{len(futures)}] {s} {p} {len(bars)}", flush=True)
    if failures:
        print(f"completed with {len(failures)}/{len(jobs)} failed requests", file=sys.stderr)
        # A few symbols can be unavailable/suspended; fail only when the data
        # loss is material enough that publishing a snapshot would be unsafe.
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
