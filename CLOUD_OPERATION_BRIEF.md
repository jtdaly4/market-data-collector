# Brief for the architect — moving CoinLand collection to the cloud

**From:** the collector thread · **To:** Daedalus / whoever builds the cloud system
**Date:** 2026-08-07 · **Status:** problem statement + decision surface, not a design

---

## The mission, in one line
**Capture our un-backfillable market-microstructure series 24/7, with no laptop
in the loop.** Everything below serves that sentence.

## Why this is urgent — the failure we're designing out
Most of what we collect **cannot be backfilled**. Order-book depth is a live-only
snapshot; Coinbase serves **no** funding or open-interest history; a missed hour
is gone forever. Today these run on James's Mac under `launchd`, and a laptop
sleeps. Concrete, just observed on the pressure collector:

> captured 08-06 16:00→18:00 UTC, then a **21-hour hole** (08-06 18:00 →
> 08-07 15:00) while the Mac was asleep overnight, then resumed. Three clean
> hours, twenty-one hours of depth that will never exist, three more clean hours.

`launchd` cannot fire on a sleeping machine, and `caffeinate` only blocks *idle*
sleep while a process runs — a closed lid or a shutdown defeats it. **The laptop
is the single point of data loss.** That is the whole problem.

## What runs today (the substrate to move or generalize)
| collector | captures | cadence | backfillable? | host today | storage today |
|---|---|---|---|---|---|
| **market-data-collector** | Coinbase spot candles + per-perp funding/OI/mark/basis | hourly | candles yes, **funding/OI no** | **GitHub Actions cron** | CSVs committed to this repo; engine git-pulls + `import_to_sqlite.py` hourly → `market.db` |
| **pressure_snapshots** (new) | ±1% BTC book depth (3 venues) + OI/short-share | hourly | **no** | laptop `launchd` | local `pressure_snapshots.db` (not yet synced anywhere) |
| **liquidation-collector** | Hyperliquid liquidation proxy + HLP-backstop truth | continuous (websocket) | **no** | laptop `launchd` KeepAlive | local `liquidations.db` |
| **1-minute BTC archive** | 1-min spot candles back to 2015 | one-shot backfill | yes (re-fetchable) | laptop | local `candles_1m.db` |

Note the split personality: **one collector already lives in the cloud** (GitHub
Actions), the rest are laptop-bound. So this is *generalizing a pattern that
half-exists*, not greenfield — and the cloud one already taught us two of the
landmines below.

## The design envelope — hard constraints (non-negotiable)
1. **Keyless public endpoints only.** No API keys, logins, or paid plans. If a
   route demands one, we stop and report — we subscribe to nothing.
2. **Store ingredients, never the computed answer.** We persist raw inputs
   (depth per venue, OI, short-share, funding); the scores/ratios stay a
   read-time formula. An outage must never be able to forge a "signal."
3. **Honest gaps: missing ≠ zero.** A failed venue/hour is an absent row, never
   a zero and never interpolated.
4. **Idempotent, append-only, source-stamped.** `INSERT OR IGNORE`; every row
   carries where it came from (labels never lie — OKX data is stamped OKX, not
   Binance).
5. **Never corrupt the engine's `market.db`.** Collectors own their own stores.
6. **Stdlib-only today** (zero pip deps is a deliberate feature of the current
   collectors). A cloud container *could* relax this — that's a call for you,
   not an assumption.

## Landmines we've already stepped on (handing them forward)
1. **Geo-blocking is real and it bites the obvious solution.** Binance — its
   depth, its open interest, and its global long/short ratio — returns
   **`451 restricted location` from every US IP we've tried, including
   GitHub's US-hosted runners.** Coinbase, Kraken, Bitstamp, and OKX are all
   reachable. So "just run it on a cloud box" gets you 3 spot venues + OKX
   around the clock but **never Binance** unless the egress is non-US.
2. **GitHub Actions cron is not a clock.** Its scheduled `cron` fires *late and
   irregularly* under load — we measured **~2.3h between "hourly" runs**, not
   60 min. We worked around it with a `*/15` self-throttle, but for
   hour-critical capture this scheduler is a hazard, not a foundation.
3. **No history to lean on.** Re-stated because it drives everything: funding,
   OI, and depth have no historical endpoint. Continuity *is* the product.
4. **Storage doesn't scale as CSV-in-git.** Committing CSVs to the repo works at
   one-row-per-perp-per-hour. It will not hold up to depth snapshots or any
   finer cadence.
5. **The laptop VM quirk is host-specific** (an `EDEADLK` on file flush from the
   Claude Desktop VM). It simply disappears in a clean cloud host — a *reason to
   move*, not a thing to port.

## The decisions you own (the real fork points)
- **A. Runner location & egress.** US-free (GH Actions / most cloud regions) =
  3 venues + OKX, 24/7, \$0, **no Binance**. Non-US egress (a VPS in an allowed
  region, or a proxy) = adds Binance depth/OI/long-short, at some cost. This is
  the single biggest lever and it's a cost-vs-completeness call. *(It's also the
  open question already on the wire from the pressure report.)*
- **B. Scheduler.** Something that fires on time every time — an always-on
  container loop, a real cloud cron/timer — versus GH Actions' unreliable cron.
- **C. Storage & sync-back.** Where cloud-captured data lands (a hosted DB?
  object storage? keep committing?) **and** how it reconciles into the engine's
  `market.db`, which today pulls this repo hourly. The pressure/liquidation
  stores have *no* sync path yet — that's part of the design, not an afterthought.
- **D. One service or a fleet.** A single orchestrated always-on collector for
  all families, versus independent per-family jobs. Failure isolation vs.
  operational simplicity.
- **E. Observability.** The entire point is *not missing hours* — so we need to
  know within minutes when a collector silently stops. Heartbeat / coverage
  readout / alert. Without it, "24/7" is a hope.

## What "done" looks like
- **Zero sleep gaps** — continuous capture independent of any personal machine.
- Every reachable venue captured every hour; the unreachable ones honestly
  marked missing (never zeroed).
- The engine consumes cloud-collected ingredients exactly as it reads local
  ones today.
- We can **prove** continuity — a coverage/heartbeat view that shows the last
  captured hour per series at a glance.

## What I am deliberately *not* deciding for you
The architecture. Above is the mission, the envelope, the landmines, and the
forks. **Pick the shape** — and if a constraint here is in your way, name it and
we'll weigh relaxing it. The one line at the top is the acceptance test.
