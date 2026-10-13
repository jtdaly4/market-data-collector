#!/usr/bin/env python3
"""funding_intx.db top-up -- continues the 264-instrument Coinbase INTX
funding archive (~/RegimeData/funding/funding_intx.db) past the INTX->Deribit
migration cutover (2026-10-01 09:00:00Z, ORDER 097), per ORDER 103 part 2's
addendum (DAEDALUS 2026-10-02T20:55:49Z): "continue it from Deribit's
get_funding_rate_history (interest_1h) for every base Deribit lists, with a
source column and a cutover marker row" -- COLLECTOR's own call, stated early
in the 2026-10-02T21:09:41Z receipt: continuing costs one column; closing a
3.5-year hourly series with a README costs the org its only deep funding
history across a venue change.

WHAT "CONTINUE" MEANS HERE. The INTX endpoint did not go silent uniformly --
of the 264 archived instruments, 133 had ALREADY stopped getting new rows
before the cutover (delisted on INTX earlier, over months); only 131 were
still being written hourly right up to the cutover hour itself (measured:
MAX(ts_iso) == '2026-10-01T09:00:00Z' for exactly 131 product_ids). This
script continues ONLY those 131 -- a "continuation" needs a series that was
actually live at the seam. Of the 131, 130 match a Deribit perpetual base
1:1 by literal id (strip "-PERP"); ANTHROPIC-PERP is the one exception,
listed on Deribit as "ANTH" (checked: ANTHROPIC-PERP's last INTX mark was
2042-2162, Deribit's live ANTH mark is ~2050 -- same underlying, confirmed
before aliasing, not guessed). Everything else Deribit lists (134 bases
total) that is NOT one of these 131 has no INTX history to continue -- adding
it would be a NEW series, not a continuation, and nobody asked for that
(zero scope creep, same judgment as the universe.json PAXG/BNB/HYPE call).

SCHEMA: the existing table (product_id, instrument_id, ts, ts_iso,
funding_rate, funding_annualized, mark_price, venue) is unchanged. New rows
get venue='deribit'; funding_rate=interest_1h (the SAME realized-hourly
definition the table already carries for its coinbase_intx rows -- this is a
venue change, not a definition change, so funding_annualized stays
funding_rate*8760, keeping every existing reader's math valid across the
seam). instrument_id carries Deribit's instrument_name (a string) where the
original rows carry Coinbase's numeric instrument id -- documented here, not
silently coerced. mark_price is Deribit's index_price (the history endpoint
has no per-hour mark_price; index_price is the nearest honest substitute --
labeled via the cutover marker row, not pretended to be the same quantity).

CUTOVER MARKER: one single row, product_id='__CUTOVER__', ts=1790845229
(2026-10-01T09:00:29Z, the venue's last real INTX quote per the 2026-10-02
migration receipts), venue stamped with the transition note. Not one row per
instrument -- the seam is already visible per-instrument via the venue column
changing from coinbase_intx to deribit at the hour boundary; one table-level
marker documents WHY, which a bare column flip does not.

IDEMPOTENT: a UNIQUE index on (product_id, ts) is created if absent, and all
inserts use INSERT OR IGNORE -- a re-run (crash, retry, hourly re-schedule)
backfills only what is still missing.

    python3 scripts/funding_intx_topup.py --dry-run     # fetch + print, no write
    python3 scripts/funding_intx_topup.py --commit      # fetch + write

Stdlib only. Public Deribit endpoints, keyless. Backs up the db first and
checks the backup's SIZE before writing (standing law).
"""
import argparse, glob, json, os, shutil, sqlite3, sys, time, urllib.error, urllib.request
from datetime import datetime, timezone

DERIBIT = "https://www.deribit.com/api/v2"
DB = os.path.expanduser("~/RegimeData/funding/funding_intx.db")
CUTOVER_TS = 1790845229        # 2026-10-01T09:00:29Z -- last real INTX quote
ALIAS = {"ANTHROPIC": "ANTH"}  # archive base -> Deribit base, verified by hand (see docstring)
SLEEP = 0.2


def _get(url, retries=4):
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


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def discover_instruments():
    """Pure-ish (one network call). Deribit's live perpetual roster ->
    {RAW_BASE: instrument_name}. RAW base, not folded -- this table keeps
    Deribit's own 1000x-prefixed ids (e.g. "1000PEPE-PERP"), matching how
    the ORIGINAL 264-instrument archive already names those products, so a
    continued series keeps its own product_id unchanged."""
    insts = (_get(f"{DERIBIT}/public/get_instruments?currency=USDC&kind=future&expired=false")
             or {}).get("result") or []
    return {i["instrument_name"].split("_USDC-PERPETUAL")[0]: i["instrument_name"]
            for i in insts if i.get("settlement_period") == "perpetual"}


def continuable_products(conn):
    """Query, not guess: product_ids whose last archived row sits AT OR
    AFTER the cutover hour -- i.e. were genuinely still live on INTX right
    up to the moment it died (first run: last row == exactly the cutover
    hour), or have already been continued by a prior run of this same
    script (later runs: last row is some deribit-venue hour past cutover).
    Excludes the 133 that had already gone quiet on INTX earlier -- nothing
    to 'continue' there (see module docstring) -- and the __CUTOVER__
    marker row itself (ts < cutover is impossible for it to satisfy this,
    but excluded by name defensively since it is not a real product).
    A >= comparison, not an exact-string match on the first run's cutover
    timestamp, is what makes this script idempotent and safe to schedule
    hourly: each run picks up exactly where the previous one left off."""
    rows = conn.execute(
        "SELECT product_id, MAX(ts) as last_ts FROM funding_intx "
        "WHERE product_id != '__CUTOVER__' "
        "GROUP BY product_id HAVING last_ts >= ?", (CUTOVER_TS - 29,)).fetchall()
    return {r[0]: r[1] for r in rows}


def rows_for_product(name, floor_ts, closed_hour_end):
    """One network call: Deribit get_funding_rate_history over the whole
    gap. Returns rows strictly after floor_ts and at/before closed_hour_end
    -- never the in-progress hour, never re-fetching what's already archived."""
    hist = (_get(f"{DERIBIT}/public/get_funding_rate_history?instrument_name={name}"
                 f"&start_timestamp={floor_ts * 1000}&end_timestamp={closed_hour_end * 1000}")
            or {}).get("result") or []
    out = []
    for r in hist:
        ts_ms = r.get("timestamp")
        if ts_ms is None:
            continue
        ts = ts_ms // 1000
        if ts <= floor_ts or ts > closed_hour_end:
            continue
        rate = _num(r.get("interest_1h"))
        if rate is None:
            continue
        out.append({
            "ts": ts,
            "ts_iso": datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "funding_rate": rate,
            "funding_annualized": rate * 8760,
            "mark_price": _num(r.get("index_price")),
        })
    return out


def backup_db(force=False):
    """ORDER 115: one backup per calendar UTC day, not one per hourly run.
    The hourly cadence was writing+deleting a full 505 MB copy every hour
    (janitor's keep-newest-2 class already prunes to 2 -- that rotation is
    untouched); the fix is cadence, not retention. If a backup already
    exists for today's UTC date, reuse it and skip the copy. force=True
    (schema change / migration) always takes a fresh one regardless."""
    if not os.path.exists(DB):
        raise SystemExit(f"FATAL: {DB} does not exist")
    today = time.strftime("%Y%m%d", time.gmtime())
    if not force:
        existing = sorted(glob.glob(f"{DB}.bak-fundingtopup-{today}-*"))
        if existing:
            dst = existing[-1]
            return dst, os.path.getsize(dst), False
    stamp = time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())
    dst = f"{DB}.bak-fundingtopup-{stamp}"
    shutil.copy2(DB, dst)
    src_size, dst_size = os.path.getsize(DB), os.path.getsize(dst)
    if src_size != dst_size:
        raise SystemExit(f"FATAL: backup size mismatch {src_size} != {dst_size}, refusing to write")
    return dst, dst_size, True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--commit", action="store_true")
    a = ap.parse_args()
    if not a.dry_run and not a.commit:
        sys.exit("specify --dry-run or --commit")

    conn = sqlite3.connect(DB)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_product_ts ON funding_intx(product_id, ts)")
    conn.commit()

    continuable = continuable_products(conn)
    by_base = discover_instruments()
    time.sleep(1.0)  # get_instruments is rate-limited to 1 req/s (collector.py precedent)

    now = int(time.time())
    closed_hour_end = (now // 3600) * 3600

    written, no_deribit_match, failed = [], [], []
    all_new_rows = []
    for pid, last_ts in continuable.items():
        base = pid[:-5]  # strip "-PERP"
        deribit_base = ALIAS.get(base, base)
        name = by_base.get(deribit_base)
        if not name:
            no_deribit_match.append(pid)
            continue
        try:
            new_rows = rows_for_product(name, last_ts, closed_hour_end)
        except Exception as e:
            failed.append((pid, str(e)[:160]))
            continue
        time.sleep(SLEEP)
        if not new_rows:
            continue
        for r in new_rows:
            all_new_rows.append((pid, name, r["ts"], r["ts_iso"], r["funding_rate"],
                                  r["funding_annualized"], r["mark_price"], "deribit"))
        written.append((pid, len(new_rows), new_rows[0]["ts_iso"], new_rows[-1]["ts_iso"]))

    marker = ("__CUTOVER__", None, CUTOVER_TS,
              datetime.fromtimestamp(CUTOVER_TS, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              None, None, None,
              "cutover_marker:coinbase_intx->deribit:interest_1h @ 2026-10-01T09:00:29Z "
              "(INTX funding feed died; continuation per ORDER 103 part 2 addendum)")

    print(f"continuable (cutover-active) products: {len(continuable)}")
    print(f"matched a live Deribit base: {len(written)}  (+{sum(n for _, n, _, _ in written)} rows total)")
    print(f"no Deribit match (archived but not live there): {len(no_deribit_match)} {no_deribit_match}")
    if failed:
        print(f"FAILED: {len(failed)}")
        for pid, err in failed:
            print(f"  {pid}: {err}")
    for pid, n, first_iso, last_iso in written[:10]:
        print(f"  {pid:<16} +{n:<4} {first_iso} -> {last_iso}")
    if len(written) > 10:
        print(f"  ... and {len(written) - 10} more")

    if a.dry_run:
        print("\n[DRY RUN] nothing written. marker row would be:", marker)
        return

    before_n, before_max = conn.execute(
        "SELECT COUNT(*), MAX(ts_iso) FROM funding_intx").fetchone()
    backup_path, backup_size, fresh = backup_db()
    tag = "new copy" if fresh else "reused -- already have one for today (ORDER 115 cadence)"
    print(f"\nbackup: {backup_path} ({backup_size:,} B) [{tag}]")

    conn.executemany(
        "INSERT OR IGNORE INTO funding_intx "
        "(product_id, instrument_id, ts, ts_iso, funding_rate, funding_annualized, "
        " mark_price, venue) VALUES (?,?,?,?,?,?,?,?)", all_new_rows)
    conn.execute(
        "INSERT OR IGNORE INTO funding_intx "
        "(product_id, instrument_id, ts, ts_iso, funding_rate, funding_annualized, "
        " mark_price, venue) VALUES (?,?,?,?,?,?,?,?)", marker)
    conn.commit()

    after_n, after_max = conn.execute(
        "SELECT COUNT(*), MAX(ts_iso) FROM funding_intx").fetchone()
    deribit_n = conn.execute(
        "SELECT COUNT(*) FROM funding_intx WHERE venue='deribit'").fetchone()[0]
    print(f"rows BEFORE: {before_n:,} (max {before_max})")
    print(f"rows AFTER:  {after_n:,} (max {after_max})  venue=deribit rows: {deribit_n:,}")
    conn.close()


if __name__ == "__main__":
    main()
