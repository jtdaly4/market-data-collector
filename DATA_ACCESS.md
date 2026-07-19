# Accessing the market dataset (for other projects / agents)

This repo is the **portable source of truth** for CoinLand market data. Any
project that wants the data should pull from here and build its **own** local
copy. Do NOT read another project's `market.db` directly — see "Rules" below.

Repo (public): https://github.com/jtdaly4/market-data-collector

## What's in it

A GitHub Actions job commits Coinbase public data **hourly** (cron `:07 UTC`):

| Path | Contents | Cadence |
|------|----------|---------|
| `data/candles/YYYY-MM-DD.csv` | spot **hourly** OHLCV, one row per asset per hour | hourly |
| `data/perp/YYYY-MM-DD.csv` | per-perp funding rate, open interest, mark, index, basis — **point-in-time snapshot**, not OHLC | hourly |
| `data/latest.json` | most recent snapshot only, for "now" without parsing CSVs | hourly |

The git history is the audit trail: every collection is one commit.

**Resolution today is hourly.** There is no minute data yet. (A minute-cadence
layer is planned but not built; if it lands, the schema below is unchanged —
only row density increases.)

## Two ways to consume

### A. Read the CSVs directly (language-agnostic, zero deps)
```bash
git clone https://github.com/jtdaly4/market-data-collector
# then parse data/candles/*.csv and data/perp/*.csv
```
CSV columns:
- candles: `ts, product_id, open, high, low, close, volume`
- perp:    `ts, product_id, funding_rate, open_interest, mark_price, index_price, basis`

### B. Fold into your own SQLite archive (recommended for backtests)
```bash
git clone https://github.com/jtdaly4/market-data-collector
cd market-data-collector
python3 import_to_sqlite.py /path/to/YOUR_PROJECT/market.db   # builds/updates YOUR copy
```
`import_to_sqlite.py` is idempotent (`INSERT OR IGNORE` on the primary keys), so
your refresh loop is just:
```bash
git pull && python3 import_to_sqlite.py /path/to/YOUR_PROJECT/market.db
```
Run it on whatever cadence you like — hourly, or "every few hours" — it only
inserts new rows. **Pass your own DB path**; the default target is the regime
engine's private DB and you should not write there.

## Schema (the SQLite tables `import_to_sqlite.py` creates)
```sql
CREATE TABLE candles (
  ts INTEGER, product_id TEXT, granularity INTEGER DEFAULT 3600,
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  PRIMARY KEY (ts, product_id, granularity));
CREATE TABLE perp_snapshots (
  ts INTEGER, product_id TEXT,
  funding_rate REAL, open_interest REAL, mark_price REAL, index_price REAL, basis REAL,
  PRIMARY KEY (ts, product_id));
```

Example query — last 24h of funding for one perp:
```sql
SELECT ts, funding_rate, open_interest, basis
FROM perp_snapshots
WHERE product_id = 'BTC-PERP-INTX' AND ts >= strftime('%s','now') - 86400
ORDER BY ts;
```

## Gotchas (read before you trust a row)

1. **`ts` is Unix epoch SECONDS, UTC.** Not milliseconds, not local time.
2. **Spot vs perp product ids differ.** Spot candles are `{SYM}-USD`
   (e.g. `BTC-USD`); perp snapshots are `{SYM}-PERP-INTX` (e.g. `BTC-PERP-INTX`).
   To align a perp with its spot, map on the base symbol.
3. **PEPE and SHIB perps are 1000x contracts.** Their ids are
   `1000PEPE-PERP-INTX` / `1000SHIB-PERP-INTX`; `mark_price` is priced per 1000
   tokens. The collector already scales the spot index ×1000 so `basis` is
   apples-to-apples, but if you compare `mark_price` to a `{SYM}-USD` spot
   candle, divide by 1000 first.
4. **NULL funding/OI in the earliest perp rows.** The first production run
   (2026-07-18 17:19 UTC) captured only `mark_price` before a parser bug was
   fixed. Those rows have `funding_rate`/`open_interest` = NULL. Filter
   `WHERE funding_rate IS NOT NULL` for funding work.
5. **Candles are the last CLOSED hour.** The collector drops the in-progress
   bar, so the newest candle is always a finalized hour — no repaint.
6. **Funding/OI history is irreplaceable.** Coinbase serves no historical
   funding or open interest; this repo's hourly capture is the only record of
   that series. Candles, by contrast, are always re-fetchable from Coinbase, so
   a gap in candles self-heals but a gap in perp snapshots is permanent.
7. **Cron jitter.** GitHub cron can run minutes late or (rarely) skip an hour.
   Expect occasional hour gaps; don't assume exactly-hourly spacing.

## Universe (27 perps + 28 spot)

Products tracked are listed in `universe.json`. Spot adds PAXG (no perp).
Perp coverage: BTC ETH SOL XRP DOGE ADA AVAX LINK DOT LTC BCH NEAR ATOM UNI APT
ARB OP SUI INJ AAVE ENA HBAR ONDO XLM ZEC + 1000PEPE + 1000SHIB.

## Rules for multi-project use

- **Each project builds its own `market.db` from the repo.** Never point two
  projects at the same DB file — the regime engine writes to its copy hourly and
  concurrent writers will contend. Reads of the repo are free and conflict-free.
- **Treat the repo as read-only.** Only the Actions bot commits here. Don't push
  data or add secrets — it's a public repo of public data by design.
- **Keep `universe.json` as the coverage contract.** If a project needs a symbol
  that isn't tracked, add it here (and to the engine's `config.json`) so the
  collector starts capturing it — you can't backfill perp history after the fact.
