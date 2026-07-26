# MANIFEST — market-data-collector

What this archive is, what's trustworthy in it, and the fixes that made it so.
Read this before backtesting off `market.db` or the CSVs.

## The archive

- `data/candles/YYYY-MM-DD.csv` — hourly spot OHLCV, `ts` = Unix epoch seconds UTC,
  hour-aligned (candle open time). Re-fetchable from Coinbase, so gaps are repairable.
- `data/perp/YYYY-MM-DD.csv` — per-perp funding_rate, open_interest, mark, index, basis.
  **NOT re-fetchable** — Coinbase serves no historical funding/OI anywhere, so a missed
  capture is gone forever. This is the irreplaceable series.
- `data/latest.json` — most recent snapshot.
- Consumers fold the CSVs into SQLite via `import_to_sqlite.py`
  (candles PK `(ts, product_id, granularity)`, perp PK `(ts, product_id)`, both
  `INSERT OR IGNORE` — overlapping re-writes are free and safe).

## 2026-07-26 — archive repair (two compounding bugs fixed)

**Bug 1 — candles: only 1 bar kept per run.** `collector.py` kept a single closed
candle (`sorted(...)[-2]`) while the API returns ~350. Combined with Bug 2's timing,
this left `market.db` missing ~17% of hourly candles overall (BTC measured 75/168 in
the last week).

**Bug 2 — cron doesn't fire hourly.** GitHub `schedule` is best-effort: observed run
gaps median 2.3h, max 4.5h, never on :07. A 1-hour fetch window under a 2.3h run
interval loses every hour between runs. This hit perp hardest (~58% of hourly perp
observations missing since 2026-07-18) — and those are unrecoverable.

### Fixes
1. **Candles self-heal.** Keep the last **48 closed** bars per run (drop only the
   in-progress newest). 48h covers the worst observed outage (4.5h) ~10x. CSV dedup +
   `INSERT OR IGNORE` absorb the overlap; typical growth is only the genuinely-new
   hours. Deep holes are `regime_engine/replay.py backfill`'s job, not this job's.
   Candles spanning a UTC midnight are routed to their correct day-file.
2. **Cadence → `*/15`.** 4 attempts/hour turns GitHub's dropped runs into redundancy
   instead of loss. Public repo = free Actions minutes.
3. **Perp timestamp bucketed to the hour** (`ts = now - now % 3600`). Previously the
   raw run time was stamped, so the "hourly" perp series was never hour-aligned —
   which matters because downstream `d5a` treats these rows AS hourly (funding_z /
   oi_z windows, funding_streak_h). Bucketing + the `(ts, product_id)` PK collapse all
   runs within an hour into ONE aligned row ⇒ a genuinely hourly, gap-free perp series.

### The two explicit decisions
- **(a) De-dup policy: `INSERT OR IGNORE` — first reading in the hour wins.** Rationale:
  it matches the existing CSV-level dedup and the importer's DB-level dedup end-to-end
  (one consistent rule), is deterministic/reproducible, and the first hit of the hour
  is the reading closest to top-of-hour. (Last-wins would require `INSERT OR REPLACE`
  and non-deterministic late-run overwrites — rejected.)
- **(b) Old rows: seam left in place, documented here — NOT normalised.** Pre-fix perp
  rows carry raw-`now` timestamps (irregularly spaced ~2.3h); post-fix rows are
  hour-aligned. The CSVs are an append-only audit trail (git history *is* the record),
  and the pre-fix data is ~58% incomplete regardless — rewriting it would fabricate an
  hourly regularity it never had. Consumers should treat the changeover as a boundary.

### Changeover timestamp
- Perp timestamping changes from raw-`now` to hour-bucketed starting with the first
  `*/15` production run on/after this deploy.
- **Exact first hour-aligned perp `ts`: _<recorded after first post-deploy run — see below>_**
- Rows with `ts % 3600 == 0` are post-fix (aligned). Pre-fix rows will (almost always)
  have `ts % 3600 != 0`.

### Honest limitation (unrecoverable)
Pre-fix funding/OI history (2026-07-18 → this deploy) is **~58% incomplete** — 8–11 of
24 hours missing on most days — and **cannot be backfilled** (no historical funding/OI
endpoint exists). Backtests using funding/OI before the changeover must account for
this sparsity. Candle history before the changeover is being repaired via
`replay.py backfill` and is not subject to this limitation.

## Coordination
`regime_engine/sync_market_db()` git-pulls this repo hourly, so these changes reach the
engine automatically. **The perp-timestamp change was flagged to the engine thread** —
`d5a`'s funding_z / oi_z windows and funding_streak_h counters count rows as hourly and
must be re-checked against the new (now genuinely hourly) spacing and the changeover seam.
