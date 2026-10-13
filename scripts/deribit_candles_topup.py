#!/usr/bin/env python3
"""
Deribit candle top-up — continues candles_intx/<PID>_FIFTEEN_MINUTE.csv past
the INTX->Deribit migration cutover (2026-10-01 09:00Z, ORDER 097).

The INTX endpoint these files were built from stopped moving at the cutover
(public/get_tradingview_chart_data confirmed: the archive now carries OPEN=
HIGH=LOW=CLOSE at 09:00Z with volume 0.0002 -- a dead print, not a quiet
market). INTX rows are left exactly as they are: this script only APPENDS,
never rewrites, so the outage stays visible in the archive's own history.
New rows get venue="deribit"; everything before the cutover keeps
venue="coinbase_intx".

Scope: the 28 products in universe.json (COLLECTOR's live lane), not the
full 264-product historical archive -- most of those aren't live on Deribit
at all (delisted) and nobody asked to top them up.

Instrument names are discovered at runtime (public/get_instruments), never
hardcoded, and mapped back to the manifest's own <BASE>-PERP key.

  python3 scripts/deribit_candles_topup.py                  # all 28
  python3 scripts/deribit_candles_topup.py --symbols BTC,ETH # subset, testing
  python3 scripts/deribit_candles_topup.py --dry-run         # fetch + print, no write
"""
import argparse, csv, json, os, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone

DERIBIT = "https://www.deribit.com/api/v2"
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
UNIVERSE = os.path.join(REPO, "universe.json")
CANDLES_DIR = os.path.join(REPO, "archive", "coinbase_intx", "candles_intx")
MANIFEST = os.path.join(CANDLES_DIR, "_manifest.json")
FIELDS = ["product_id", "ts", "open", "high", "low", "close", "volume",
          "granularity", "venue"]
SLEEP = 0.2
CHUNK_S = 1000 * 15 * 60  # window WIDTH in seconds for 1000 fifteen-minute
                          # bars per call; Deribit did not truncate a 121-bar
                          # (30h) request in testing -- this is a defensive
                          # cap, not an observed venue limit.

ALIAS = {"PEPE": "1000PEPE", "SHIB": "1000SHIB"}


def get(url, retries=4):
    last = None
    for a in range(retries):
        req = urllib.request.Request(url, headers={"User-Agent": "daedalus-collector/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200]
            if e.code == 429:
                time.sleep(2 ** a); last = f"429 {body}"; continue
            raise RuntimeError(f"HTTP {e.code} on {url} :: {body}")
        except Exception as e:
            last = f"{type(e).__name__}: {e}"; time.sleep(1 + a)
    raise RuntimeError(f"failed after {retries} tries: {url} :: {last}")


def deribit_result(path):
    return (get(f"{DERIBIT}{path}") or {}).get("result")


def discover_instruments():
    insts = deribit_result("/public/get_instruments?currency=USDC&kind=future&expired=false") or []
    return {i["instrument_name"].split("_USDC-PERPETUAL")[0]: i["instrument_name"]
            for i in insts if i.get("settlement_period") == "perpetual"}


def fetch_chart(name, start_s, end_s):
    """Yield {ts_iso, open, high, low, close, volume} forward from start_s, chunked."""
    cur = start_s
    guard = 0
    while cur < end_s and guard < 200:
        guard += 1
        nxt = min(cur + CHUNK_S, end_s)
        d = get(f"{DERIBIT}/public/get_tradingview_chart_data?instrument_name={name}"
                f"&start_timestamp={cur*1000}&end_timestamp={nxt*1000}&resolution=15")
        res = (d or {}).get("result") or {}
        ticks = res.get("ticks") or []
        for i, t_ms in enumerate(ticks):
            t_s = t_ms // 1000
            if t_s <= start_s:
                continue  # floor is exclusive, like intx_topup's convention
            yield {
                "ts_iso": datetime.fromtimestamp(t_s, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "open": res["open"][i], "high": res["high"][i],
                "low": res["low"][i], "close": res["close"][i],
                "volume": res["volume"][i],
            }
        cur = nxt
        time.sleep(SLEEP)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", help="comma-separated universe bases, for testing a subset")
    ap.add_argument("--dry-run", action="store_true", help="fetch and print, write nothing")
    a = ap.parse_args()

    uni = json.load(open(UNIVERSE))["perps"]
    if a.symbols:
        wanted = set(s.strip() for s in a.symbols.split(","))
        uni = [s for s in uni if s in wanted]
    manifest = json.load(open(MANIFEST))
    by_base = discover_instruments()
    time.sleep(1.0)  # get_instruments is 1 req/s; let it clear before the chart calls

    now = int(time.time())
    written, skipped, failed, not_on_deribit = [], [], [], []

    for sym in uni:
        base = ALIAS.get(sym, sym)
        pid = f"{base}-PERP"
        name = by_base.get(base)
        if not name:
            not_on_deribit.append(sym)
            continue
        meta = manifest.get(pid)
        if not meta or not meta.get("latest"):
            failed.append((sym, "no manifest floor -- not this script's job to backfill from scratch"))
            continue
        floor_s = int(datetime.fromisoformat(meta["latest"].replace("Z", "+00:00")).timestamp())
        if floor_s >= now - 900:
            skipped.append(sym)
            continue

        fp = os.path.join(CANDLES_DIR, f"{pid}_FIFTEEN_MINUTE.csv")
        try:
            new_rows = list(fetch_chart(name, floor_s, now))
        except Exception as e:
            failed.append((sym, str(e)[:160]))
            continue

        if not new_rows:
            skipped.append(sym)
            continue
        if a.dry_run:
            print(f"[DRY] {sym:<8}{pid:<14}{name:<24} +{len(new_rows)} bars "
                  f"{new_rows[0]['ts_iso']} -> {new_rows[-1]['ts_iso']}")
            written.append(sym)
            continue

        with open(fp, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            for r in new_rows:
                w.writerow({"product_id": pid, "ts": r["ts_iso"], "open": r["open"],
                            "high": r["high"], "low": r["low"], "close": r["close"],
                            "volume": r["volume"], "granularity": "FIFTEEN_MINUTE",
                            "venue": "deribit"})
        manifest[pid]["rows"] = manifest[pid].get("rows", 0) + len(new_rows)
        manifest[pid]["latest"] = new_rows[-1]["ts_iso"]
        written.append(sym)
        print(f"  {sym:<8}{pid:<14}{name:<24} +{len(new_rows)} bars "
              f"{new_rows[0]['ts_iso']} -> {new_rows[-1]['ts_iso']}")

    if not a.dry_run and written:
        with open(MANIFEST, "w") as fh:
            json.dump(manifest, fh, indent=2)

    print(f"\ndone: {len(written)} updated, {len(skipped)} already current, "
          f"{len(not_on_deribit)} not on Deribit roster ({not_on_deribit}), "
          f"{len(failed)} failed = {len(uni)} attempted")
    for sym, err in failed:
        print(f"  FAILED {sym}: {err}")


if __name__ == "__main__":
    sys.exit(main() or 0)
