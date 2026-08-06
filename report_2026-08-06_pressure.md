# Report — pressure-family ingredients now collecting (2026-08-06)

Order: START THE CLOCK FIRST. Order-book depth cannot be backfilled — every
hour before the job ran is history that never existed. **The clock is running.**

## What now runs, hourly
`collect_pressure.py` (committed) → `pressure_snapshots.db` (collector-owned,
gitignored, does **not** touch market.db). launchd
`com.jamesdaly.pressure-collector` at :09 every hour (clear of the collector's
:07); RunAtLoad verified; idempotent (`INSERT OR IGNORE`, re-runs within an hour
are no-ops). Stdlib urllib only; depth/OI logic **copied, not imported** from
`A cards/A Card 1/thermostat_card_3.py`.

Two tables, **ingredients only — never the computed satio ratio/score** (that
stays a read-time formula):

- `satio_snapshots(ts, venue, depth_1pct_btc)` — one row per venue that answered.
- `oi_snapshots(ts, oi_total_btc, short_share, source)` — one row per hour.

**Honest gaps:** a venue/metric that fails an hour is a **MISSING row (or NULL),
never a zero** — an outage must not be forgeable as "zero depth." No interpolation.

## First captured hour — 2026-08-06 16:00 UTC (the sample ingredients)
| ingredient | value | source |
|---|---|---|
| depth ±1% (BTC) — Coinbase | 469.70 | coinbase L2 book |
| depth ±1% (BTC) — Kraken | 574.29 | kraken Depth |
| depth ±1% (BTC) — Bitstamp | 146.44 | bitstamp order_book |
| depth ±1% (BTC) — Binance | **MISSING** | 451 geo-blocked |
| open interest (BTC) | 31,610.69 | **okx** (Binance blocked) |
| short share | 0.429 | **okx** long/short 1.33 |

## The material finding — Binance is geo-blocked from this machine
From this US IP, **all three Binance endpoints return 451 "restricted location"**:
`api.binance.com` depth, `fapi.binance.com` openInterest, and the
globalLongShortAccountRatio. So on this host:

- **Depth: 3 of the 4 named venues** capture (Coinbase, Kraken, Bitstamp).
  Binance depth is a missing row every hour until a non-US path exists. (James's
  call: **"use the three."**)
- **OI + short-share fall back to OKX**, source-stamped `okx`. This is the
  source's own OI fallback; OKX also serves a keyless long/short account ratio,
  so positioning still accrues from hour one — but it is **OKX's book, not
  Binance's**, and the numbers won't line up with a Binance-based ThermoSat card.

The Binance fetchers are still in the code (they'll light up unchanged if ever
run from a non-US IP). Nothing here demanded a key/login/payment, so the
hard-stop did not trigger; subscribed to nothing.

**Decision for the architect:** keep the OKX-stamped OI/positioning as the
standing source, or route a non-US runner (VPS/Actions) to get the exact
Binance ingredients the card was built on? Depth is unaffected either way — the
three spot venues are the real prize and they're capturing now.

## First-week report (as ordered) will carry
rows per venue, gap count, and one sample hour's ingredients — after ~168 hours
accrue. Nothing computes the ratio yet; that's read-time, downstream.
