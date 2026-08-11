#!/usr/bin/env python3
"""
DIP-02 · Coinbase International Exchange funding history puller.
Written by Daedalus 2026-08-10. Stdlib only. NO API KEY REQUIRED.

The spec lists security:[] on this endpoint, so it is public. This script
was written WITHOUT being able to test it — neither the cloud sandbox nor
the device VM has network. Response-shape handling is therefore defensive.
Run it, then report what it actually returns.

USAGE
  python3 intx_funding_pull.py --list                 # what instruments exist
  python3 intx_funding_pull.py --probe BTC-PERP       # how deep does history go
  python3 intx_funding_pull.py --pull                 # full pull, all perps
  python3 intx_funding_pull.py --pull --symbols BTC-PERP,ETH-PERP

OUTPUT
  data/coinbase/funding_intx.csv   product_id,ts,funding_rate,mark_price,venue
  data/coinbase/funding_intx_horizon.json   earliest ts per instrument
"""
import argparse, csv, json, os, sys, time, urllib.error, urllib.request

BASE = "https://api.international.coinbase.com/api/v1"
OUTDIR = os.path.join("data", "coinbase")
PAGE = 100          # endpoint maximum
SLEEP = 0.15        # be polite; raise if you see 429
MAX_PAGES = 5000    # runaway guard: 500k rows per instrument


def get(url, timeout=30, retries=3):
    last = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url, headers={"User-Agent": "daedalus-collector/1.0",
                          "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            if e.code == 429:
                time.sleep(2 ** attempt)
                last = f"429 {body}"
                continue
            raise RuntimeError(f"HTTP {e.code} on {url} :: {body}")
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(1 + attempt)
    raise RuntimeError(f"failed after {retries} tries: {url} :: {last}")


def rows_of(payload):
    """The response shape is undocumented in the part we read. Accept the
    three plausible forms rather than guessing one."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "data", "funding", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                return v
        # a single object rather than a page
        if "funding_rate" in payload:
            return [payload]
    return []


def norm(r, symbol=None):
    """DEFECT FIXED 2026-08-10: the endpoint returns a numeric instrument_id,
    not the ticker. Carry the requested symbol through so the CSV is keyed on
    something a human and the engine both recognise. Keep the numeric id in
    its own column so the join back to /instruments is never lost."""
    return {
        "product_id":    symbol or r.get("symbol") or r.get("instrument"),
        "instrument_id": r.get("instrument_id"),
        "ts":            r.get("event_time") or r.get("timestamp") or r.get("time"),
        "funding_rate":  r.get("funding_rate"),
        "mark_price":    r.get("mark_price"),
        "venue":         "coinbase_intx",
    }


def list_instruments():
    payload = get(f"{BASE}/instruments")
    items = rows_of(payload) or (payload if isinstance(payload, list) else [])
    out = []
    for i in items:
        if not isinstance(i, dict):
            continue
        sym = i.get("symbol") or i.get("instrument_id") or i.get("name")
        if sym and str(sym).upper().endswith("-PERP"):
            out.append(str(sym))
    return sorted(set(out))


def pull(symbol, cap_pages=MAX_PAGES, verbose=True):
    """Walk result_offset until the endpoint stops giving new rows."""
    seen, out, offset = set(), [], 0
    for page in range(cap_pages):
        url = f"{BASE}/instruments/{symbol}/funding?result_limit={PAGE}&result_offset={offset}"
        rows = rows_of(get(url))
        if not rows:
            break
        fresh = 0
        for r in rows:
            n = norm(r, symbol)
            key = (n["product_id"], n["ts"])
            if n["ts"] and key not in seen:
                seen.add(key)
                out.append(n)
                fresh += 1
        if fresh == 0:                 # pagination stopped advancing
            break
        offset += PAGE
        if verbose and page % 20 == 0 and page:
            print(f"    {symbol}: {len(out)} rows, offset {offset}", flush=True)
        time.sleep(SLEEP)
    out.sort(key=lambda x: (x["ts"] or ""))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--probe", metavar="SYMBOL")
    ap.add_argument("--pull", action="store_true")
    ap.add_argument("--symbols", help="comma separated; default = all -PERP")
    a = ap.parse_args()

    if a.list:
        syms = list_instruments()
        print(f"{len(syms)} perpetual instruments:")
        for s in syms:
            print(" ", s)
        return

    if a.probe:
        print(f"probing {a.probe} ...")
        rows = pull(a.probe, cap_pages=MAX_PAGES)
        if not rows:
            print("NO ROWS RETURNED. Report the raw response before going further.")
            return
        print(f"  rows      : {len(rows)}")
        print(f"  earliest  : {rows[0]['ts']}")
        print(f"  latest    : {rows[-1]['ts']}")
        print(f"  sample    : {json.dumps(rows[0])}")
        print("\n  ^ 'earliest' is the number DIP-02 has waited 14 days for.")
        return

    if a.pull:
        syms = ([s.strip() for s in a.symbols.split(",")] if a.symbols
                else list_instruments())
        os.makedirs(OUTDIR, exist_ok=True)
        csv_path = os.path.join(OUTDIR, "funding_intx.csv")
        hz_path = os.path.join(OUTDIR, "funding_intx_horizon.json")
        FIELDS = ["product_id", "instrument_id", "ts",
                  "funding_rate", "mark_price", "venue"]

        # RESUMABLE (added 2026-08-10 — defect: the original truncated the CSV
        # on every run, so a kill mid-pull lost the horizon file and forced a
        # full restart. On a time-boxed collection window that is unacceptable.)
        horizon = {}
        if os.path.exists(hz_path):
            try:
                horizon = json.load(open(hz_path))
            except Exception:
                horizon = {}
        done = {k for k, v in horizon.items()
                if isinstance(v, dict) and v.get("rows", 0) > 0}
        fresh = not os.path.exists(csv_path) or os.path.getsize(csv_path) < 10
        total = sum(v.get("rows", 0) for v in horizon.values() if isinstance(v, dict))
        if done:
            print(f"RESUMING: {len(done)} instruments already complete, "
                  f"{total} rows on disk. Skipping them.")

        with open(csv_path, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            if fresh:
                w.writeheader()
            for i, sym in enumerate(syms, 1):
                if sym in done:
                    continue
                print(f"[{i}/{len(syms)}] {sym}", flush=True)
                try:
                    rows = pull(sym)
                except Exception as e:
                    print(f"    FAILED: {e}")
                    horizon[sym] = {"error": str(e)}
                    continue
                if not rows:
                    horizon[sym] = {"rows": 0}
                    print("    no rows")
                    continue
                w.writerows(rows)
                total += len(rows)
                horizon[sym] = {"rows": len(rows),
                                "earliest": rows[0]["ts"],
                                "latest": rows[-1]["ts"]}
                print(f"    {len(rows)} rows  {rows[0]['ts']} -> {rows[-1]['ts']}")
                fh.flush()
                with open(hz_path, "w") as hf:      # checkpoint after EVERY symbol
                    json.dump(horizon, hf, indent=2)
        with open(hz_path, "w") as fh:
            json.dump(horizon, fh, indent=2)
        print(f"\nDONE. {total} rows -> {csv_path}")
        print("Horizon per instrument -> funding_intx_horizon.json")
        print("\nHONEST GAPS: do not interpolate missing intervals. Report them.")
        return

    ap.print_help()


if __name__ == "__main__":
    main()
