#!/usr/bin/env python3
"""
Market data collector — runs on GitHub Actions (hourly cron).

Collects, per asset in universe.json:
  - spot hourly candle (last complete hour) from Coinbase Exchange public API
  - perp snapshot (funding_rate, open_interest, mark, index -> basis) from
    Deribit's public API, for the <BASE>-PERP-INTX ids our ledger already uses

VENUE MIGRATION, 2026-10-02 (ORDER 097). Coinbase migrated International
Exchange (INTX) onto Deribit's matching engine on 2026-10-01 09:00Z. The old
INTX-backed brokerage endpoint kept answering with its LAST value forever
after that (a dead feed that returns a number looks like a quiet market —
market.db.perp_snapshots carried the identical row for BTC-PERP-INTX etc.
every hour from 09:00Z on). Deribit is the new venue; its public ticker +
funding-history methods replace the old Coinbase Advanced perpetuals call.
Instrument names are discovered at runtime via public/get_instruments (never
hardcoded — Deribit's own roster changes), then mapped back to our internal
<BASE>-PERP-INTX ids so the ledger stays continuous across the cutover. See
00_command/INTX_TO_DERIBIT_MIGRATION_2026-10-01.md for the full writeup.

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
DERIBIT = "https://www.deribit.com/api/v2"

CANDLE_FIELDS = ["ts", "product_id", "open", "high", "low", "close", "volume"]
# source + mark_age_s added 2026-10-02 (ORDER 097): the venue's own quote age,
# so a stale mark is visible in the data itself, not just inferred from outside.
PERP_FIELDS = ["ts", "product_id", "funding_rate", "open_interest",
               "mark_price", "index_price", "basis", "source", "mark_age_s"]


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
            header = next(csv.reader(f), [])
        # A field list that grows mid-day (ORDER 097 added source/mark_age_s)
        # leaves the OLD, shorter header on an already-existing file, since a
        # header is only written once. csv.DictReader then keys the new
        # columns on every later row under the restkey, not their field name
        # -- a reader asking for row["source"] silently gets None forever
        # (found 2026-10-02: new deribit rows landed in market.db tagged
        # coinbase_intx). If the old header is a prefix of the new fields,
        # the data is positionally fine -- only the header line is stale.
        if header and header != fields and fields[:len(header)] == header:
            with open(path, newline="") as f:
                lines = f.readlines()
            lines[0] = ",".join(fields) + "\r\n"
            with open(path, "w", newline="") as f:
                f.writelines(lines)
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
    # Bucket perp snapshots to the top of the hour. The raw run time was stamped
    # before, so the "hourly" series was not hour-aligned (runs land ~2.3h apart,
    # never on :00). Downstream d5a treats perp_snapshots rows AS hourly (funding_z
    # / oi_z windows, funding_streak_h). Bucketing + the (ts, product_id) PK make
    # every run within an hour collapse to ONE aligned row => a genuinely hourly,
    # gap-free series once the cron fires several times per hour (see */15).
    perp_ts = now - (now % 3600)
    day = time.strftime("%Y-%m-%d", time.gmtime(now))
    candle_rows, perp_rows, latest = [], [], {"generated_at": now, "assets": {}}

    for sym in uni["spot"]:
        pid = f"{sym}-USD"
        # Keep the last 48 CLOSED hourly candles each run, not just one. The API
        # returns ~350 bars (verified); dropping only the in-progress newest bar
        # makes the hourly job SELF-HEALING across dropped/late cron runs — GitHub
        # schedule is best-effort (observed gaps median 2.3h, max 4.5h), and a
        # 1-bar window lost every hour between runs. 48h covers the worst outage
        # 10x. CSV dedup + INSERT OR IGNORE absorb the overlap for free; deep
        # holes are replay.py backfill's job, not this hourly job's.
        data = get(f"{EXCHANGE}/products/{pid}/candles?granularity=3600")
        if data and len(data) >= 2:
            closed = sorted(data, key=lambda x: x[0])[-49:-1]  # drop newest = in-progress
            for c in closed:
                candle_rows.append(dict(zip(CANDLE_FIELDS,
                                            [c[0], pid, c[3], c[2], c[1], c[4], c[5]])))
            latest["assets"].setdefault(sym, {})["close"] = closed[-1][4]  # newest closed
        time.sleep(0.15)  # polite pacing

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def deribit_get(path):
        d = get(f"{DERIBIT}{path}")
        return (d or {}).get("result")

    # Deribit keeps the same 1000x-denominated contracts INTX used, under the
    # same alias convention (its own price_index is "1000pepe_usdc" etc., so
    # the ticker's index/mark are already in contract units — no manual ×1000
    # rescale needed here, unlike the old INTX spot-ticker fallback above).
    perp_alias = {"PEPE": "1000PEPE", "SHIB": "1000SHIB"}

    # Discover the live perpetual roster ONCE per run (never hardcoded: Deribit
    # adds/removes instruments on its own schedule). One call, not one per
    # product, to respect the 1 req/s get_instruments limit.
    instruments = deribit_get("/public/get_instruments?currency=USDC&kind=future&expired=false") or []
    deribit_by_base = {
        i["instrument_name"].split("_USDC-PERPETUAL")[0]: i["instrument_name"]
        for i in instruments if i.get("settlement_period") == "perpetual"
    }
    time.sleep(1.0)  # get_instruments is rate-limited to 1 req/s; let it clear

    perp_fetch_ages = []
    for sym in uni["perps"]:
        base = perp_alias.get(sym, sym)
        pid = f"{base}-PERP-INTX"  # internal id unchanged -- ledger stays continuous
        name = deribit_by_base.get(base)
        if not name:
            continue  # not live on Deribit under this base -- UNMEASURED, not zero
        t = deribit_get(f"/public/ticker?instrument_name={name}")
        if not t:
            continue
        mark, index, oi = num(t.get("mark_price")), num(t.get("index_price")), num(t.get("open_interest"))
        quote_ts_ms = t.get("timestamp")
        mark_age_s = round(time.time() - quote_ts_ms / 1000.0, 3) if quote_ts_ms else None
        time.sleep(0.2)

        # Hourly-equivalent funding, matching INTX's hourly print: Deribit's
        # funding is continuous, but get_funding_rate_history's interest_1h is
        # the venue's own realized rate for the hour ENDING at its timestamp.
        # Ask for the hour ending at perp_ts (the most recently closed one as
        # of this run) and read that row directly -- no re-derivation.
        hist = deribit_get(f"/public/get_funding_rate_history?instrument_name={name}"
                            f"&start_timestamp={(perp_ts - 3600) * 1000}&end_timestamp={perp_ts * 1000}")
        funding = None
        if hist:
            row = min(hist, key=lambda r: abs(r.get("timestamp", 0) - perp_ts * 1000))
            funding = num(row.get("interest_1h"))
        basis = ((mark - index) / index) if mark and index else None
        if mark_age_s is not None:
            perp_fetch_ages.append(mark_age_s)
        perp_rows.append({"ts": perp_ts, "product_id": pid,
                          "funding_rate": funding, "open_interest": oi,
                          "mark_price": mark, "index_price": index,
                          "basis": round(basis, 8) if basis is not None else None,
                          "source": "deribit", "mark_age_s": mark_age_s})
        latest["assets"].setdefault(sym, {}).update(
            funding_rate=funding, open_interest=oi, basis=basis)
        time.sleep(0.2)

    # Health lines: this feed's venue and the staleness of its worst quote,
    # visible in the published status itself (ORDER 097 item 4), not just
    # inferable from silence the way the INTX freeze was.
    latest["perp_feed_source"] = "deribit" if perp_rows else "unmeasured_no_rows_this_run"
    latest["perp_mark_age_s"] = round(max(perp_fetch_ages), 3) if perp_fetch_ages else None

    # The 48-candle window can straddle a UTC midnight, so route each candle to
    # its own day-file — otherwise yesterday's bars land in today's file and dodge
    # that file's (ts, product_id) dedup. Perp rows are all in the current hour.
    n_c = 0
    by_day = {}
    for row in candle_rows:
        by_day.setdefault(time.strftime("%Y-%m-%d", time.gmtime(int(row["ts"]))), []).append(row)
    for d, rows in by_day.items():
        n_c += append_rows("candles", CANDLE_FIELDS, rows, d)
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
