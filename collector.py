#!/usr/bin/env python3
"""
Market data collector — runs on GitHub Actions (hourly cron).

Collects, per asset in universe.json:
  - spot hourly candle (last complete hour) from Coinbase Exchange public API
  - perp snapshot (funding_rate, open_interest, mark, index -> basis) from
    Coinbase Advanced public market endpoint, where a -PERP-INTX product exists

Appends to daily CSVs (data/candles/YYYY-MM-DD.csv, data/perp/YYYY-MM-DD.csv)
and rewrites data/latest.json. Idempotent per (ts, product): safe to re-run.

Why CSVs, not SQLite: git diffs stay small and human-readable; the repo's
history IS the audit trail. import_to_sqlite.py folds them into market.db
locally whenever the engine wants the archive.

Stdlib only. No keys — public endpoints exclusively.
"""

import csv
import json
import os
import time
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
EXCHANGE = "https://api.exchange.coinbase.com"
BROKERAGE = "https://api.coinbase.com/api/v3/brokerage/market/products"

CANDLE_FIELDS = ["ts", "product_id", "open", "high", "low", "close", "volume"]
PERP_FIELDS = ["ts", "product_id", "funding_rate", "open_interest",
               "mark_price", "index_price", "basis"]


def get(url, timeout=15, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "market-collector/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception:
            if attempt == retries:
                return None
            time.sleep(1 + attempt)


def load_universe():
    with open(os.path.join(BASE, "universe.json")) as f:
        return json.load(f)


def append_rows(kind, fields, rows, day):
    """Append rows to data/<kind>/<day>.csv, skipping (ts, product_id) dupes."""
    d = os.path.join(BASE, "data", kind)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{day}.csv")
    seen = set()
    exists = os.path.exists(path)
    if exists:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                seen.add((row["ts"], row["product_id"]))
    wrote = 0
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        for row in rows:
            if (str(row["ts"]), row["product_id"]) in seen:
                continue
            w.writerow(row)
            wrote += 1
    return wrote


def main():
    uni = load_universe()
    now = int(time.time())
    day = time.strftime("%Y-%m-%d", time.gmtime(now))
    candle_rows, perp_rows, latest = [], [], {"generated_at": now, "assets": {}}

    for sym in uni["spot"]:
        pid = f"{sym}-USD"
        # last complete hourly candle (request 2, take the older = closed one)
        data = get(f"{EXCHANGE}/products/{pid}/candles?granularity=3600")
        if data and len(data) >= 2:
            c = sorted(data, key=lambda x: x[0])[-2]  # newest-first API; -2 = closed hour
            candle_rows.append(dict(zip(CANDLE_FIELDS,
                                        [c[0], pid, c[3], c[2], c[1], c[4], c[5]])))
            latest["assets"].setdefault(sym, {})["close"] = c[4]
        time.sleep(0.15)  # polite pacing

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    for sym in uni["perps"]:
        pid = f"{sym}-PERP-INTX"
        p = get(f"{BROKERAGE}/{pid}")
        if not p or p.get("product_id") != pid:
            continue
        # funding/OI live nested under future_product_details.perpetual_details
        perp = (p.get("future_product_details") or {}).get("perpetual_details") or {}
        funding, oi = num(perp.get("funding_rate")), num(perp.get("open_interest"))
        mark = num(p.get("mid_market_price")) or num(p.get("price"))
        # endpoint exposes no index price; fresh spot ticker is the index proxy
        t = get(f"{EXCHANGE}/products/{sym}-USD/ticker")
        index = num((t or {}).get("price"))
        basis = ((mark - index) / index) if mark and index else None
        perp_rows.append({"ts": now, "product_id": pid,
                          "funding_rate": funding, "open_interest": oi,
                          "mark_price": mark, "index_price": index,
                          "basis": round(basis, 8) if basis is not None else None})
        latest["assets"].setdefault(sym, {}).update(
            funding_rate=funding, open_interest=oi, basis=basis)
        time.sleep(0.15)

    n_c = append_rows("candles", CANDLE_FIELDS, candle_rows, day)
    n_p = append_rows("perp", PERP_FIELDS, perp_rows, day)
    with open(os.path.join(BASE, "data", "latest.json"), "w") as f:
        json.dump(latest, f, indent=1)

    print(f"collected: {n_c} candle rows, {n_p} perp rows "
          f"({len(candle_rows)} fetched / {len(perp_rows)} perp products live)")
    # Non-zero exit if we truly got nothing — surfaces red X in Actions UI
    if not candle_rows and not perp_rows:
        raise SystemExit("all fetches failed")


if __name__ == "__main__":
    main()
