#!/usr/bin/env python3
"""
Coinbase INTX candle puller — the ARCHIVE grab.
Daedalus 2026-08-10. Stdlib only. NO API KEY.

WHY THE FINEST GRANULARITY: coarser bars are derivable from finer ones, never
the reverse. If the international view is IP-bound, this window closes when
James lands. Spend it on 15-minute data. Hourly falls out of it for free.

WRITTEN WITHOUT NETWORK ACCESS TO TEST IT. Run --discover FIRST. It tries the
plausible endpoint shapes and granularity spellings and reports which works.
Then run --pull.

  python3 scripts/intx_candles_pull.py --discover BTC-PERP
  python3 scripts/intx_candles_pull.py --pull --granularity 900
  python3 scripts/intx_candles_pull.py --pull --granularity 900 --symbols BTC-PERP,NVDA-PERP
"""
import argparse, csv, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://api.international.coinbase.com/api/v1"
OUTDIR = os.path.join("data", "coinbase", "candles_intx")
SLEEP = 0.15
START_FLOOR = "2023-01-01T00:00:00Z"   # funding proved 2023-03-22; start earlier


def get(url, timeout=30, retries=3):
    last = None
    for a in range(retries):
        req = urllib.request.Request(url, headers={
            "User-Agent": "daedalus-collector/1.0", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            if e.code == 429:
                time.sleep(2 ** a); last = f"429 {body}"; continue
            return e.code, body
        except Exception as e:
            last = f"{type(e).__name__}: {e}"; time.sleep(1 + a)
    return None, last


def rows_of(p):
    if isinstance(p, list):
        return p
    if isinstance(p, dict):
        for k in ("aggregations", "candles", "results", "data", "items"):
            v = p.get(k)
            if isinstance(v, list):
                return v
    return []


def discover(symbol):
    """The docs we read covered funding, not candles. Probe the shapes."""
    paths = [f"{BASE}/instruments/{symbol}/candles",
             f"{BASE}/instruments/{symbol}/quote/candles",
             f"{BASE}/instruments/{symbol}/aggregations"]
    grans = ["900", "FIFTEEN_MINUTE", "FIFTEEN_MINUTES", "15m", "ONE_MINUTE", "3600", "ONE_HOUR"]
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(hours=6)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    print(f"probing candle endpoints for {symbol}\n")
    hits = []
    for path in paths:
        for g in grans:
            q = urllib.parse.urlencode({"granularity": g,
                                        "start": start.strftime(fmt),
                                        "end": end.strftime(fmt)})
            s, d = get(f"{path}?{q}")
            n = len(rows_of(d)) if s == 200 else 0
            mark = "OK " if (s == 200 and n) else "   "
            print(f"  {mark}{s}  n={n:<4} {path.split('/api/v1')[1]}?granularity={g}")
            if s == 200 and n:
                hits.append((path, g, n, rows_of(d)[0]))
            time.sleep(0.1)
    print()
    if not hits:
        print("NOTHING WORKED. Send me one raw response body and I will fix this in one pass.")
        return
    path, g, n, sample = hits[0]
    print(f"WORKING: {path}\n  granularity={g}\n  sample row: {json.dumps(sample)[:300]}")
    print("\nUse --granularity with the spelling above.")


def instruments():
    s, p = get(f"{BASE}/instruments")
    items = rows_of(p) or (p if isinstance(p, list) else [])
    out = []
    for i in items:
        if isinstance(i, dict):
            sym = i.get("symbol") or i.get("instrument_id") or i.get("name")
            if sym and str(sym).upper().endswith("-PERP"):
                out.append(str(sym))
    return sorted(set(out))


def pull_symbol(symbol, gran, path_tpl, window_hours):
    """Walk backwards in windows until the venue stops returning rows."""
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    floor = datetime.strptime(START_FLOOR, fmt).replace(tzinfo=timezone.utc)
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    seen, out, empty_streak = set(), [], 0
    while end > floor and empty_streak < 3:
        start = max(end - timedelta(hours=window_hours), floor)
        q = urllib.parse.urlencode({"granularity": gran,
                                    "start": start.strftime(fmt),
                                    "end": end.strftime(fmt)})
        s, d = get(f"{path_tpl.format(sym=symbol)}?{q}")
        rows = rows_of(d) if s == 200 else []
        if not rows:
            empty_streak += 1
        else:
            empty_streak = 0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                ts = r.get("start") or r.get("time") or r.get("timestamp") or r.get("t")
                if ts is None or ts in seen:
                    continue
                seen.add(ts)
                out.append({"product_id": symbol, "ts": ts,
                            "open": r.get("open") or r.get("o"),
                            "high": r.get("high") or r.get("h"),
                            "low": r.get("low") or r.get("l"),
                            "close": r.get("close") or r.get("c"),
                            "volume": r.get("volume") or r.get("v"),
                            "granularity": gran, "venue": "coinbase_intx"})
        end = start
        time.sleep(SLEEP)
    out.sort(key=lambda x: str(x["ts"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--discover", metavar="SYMBOL")
    ap.add_argument("--pull", action="store_true")
    ap.add_argument("--granularity", default="900")
    ap.add_argument("--symbols")
    ap.add_argument("--path", default=BASE + "/instruments/{sym}/candles",
                    help="override if --discover found a different shape")
    ap.add_argument("--window-hours", type=int, default=72,
                    help="hours per request; lower it if pages truncate")
    a = ap.parse_args()

    if a.discover:
        discover(a.discover); return

    if a.pull:
        syms = ([s.strip() for s in a.symbols.split(",")] if a.symbols else instruments())
        os.makedirs(OUTDIR, exist_ok=True)
        manifest = {}
        for i, sym in enumerate(syms, 1):
            fp = os.path.join(OUTDIR, f"{sym}_{a.granularity}.csv")
            if os.path.exists(fp) and os.path.getsize(fp) > 100:
                print(f"[{i}/{len(syms)}] {sym} — already present, skipping")
                continue
            print(f"[{i}/{len(syms)}] {sym}", flush=True)
            try:
                rows = pull_symbol(sym, a.granularity, a.path, a.window_hours)
            except Exception as e:
                print(f"    FAILED: {e}"); manifest[sym] = {"error": str(e)}; continue
            if not rows:
                print("    no rows"); manifest[sym] = {"rows": 0}; continue
            with open(fp, "w", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                w.writeheader(); w.writerows(rows)
            manifest[sym] = {"rows": len(rows), "earliest": rows[0]["ts"],
                             "latest": rows[-1]["ts"], "file": fp}
            print(f"    {len(rows)} rows  {rows[0]['ts']} -> {rows[-1]['ts']}")
            with open(os.path.join(OUTDIR, "_manifest.json"), "w") as fh:
                json.dump(manifest, fh, indent=2)   # write as we go; survives a kill
        print(f"\nDONE. manifest -> {OUTDIR}/_manifest.json")
        print("RESUMABLE: re-running skips symbols already written.")
        return

    ap.print_help()


if __name__ == "__main__":
    main()
