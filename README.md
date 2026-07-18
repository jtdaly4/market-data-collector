# market-data-collector

Always-on market data collection for the CoinLand trading system — runs on
GitHub's servers so no computer at home needs to stay awake. Every hour a
GitHub Actions job fetches Coinbase public data and commits it to this repo:

- `data/candles/YYYY-MM-DD.csv` — hourly spot OHLCV per asset
- `data/perp/YYYY-MM-DD.csv` — funding rate, open interest, mark/index, basis
  per perp product (**the series that cannot be backfilled later — this is the
  irreplaceable part**)
- `data/latest.json` — most recent snapshot, one file, for anything that wants
  "now" without parsing CSVs

The git history is the audit trail: every collection is a commit.

## One-time setup (5 minutes, via Claude Code or by hand)

1. Create a GitHub repo (public = unlimited free Actions minutes):
   `gh repo create market-data-collector --public --source . --push`
2. That's it. The workflow (`.github/workflows/collect.yml`) starts on the
   next hour tick (:07 UTC). Test immediately: repo → Actions →
   collect-market-data → "Run workflow".

## Pulling data into the engine

```bash
git pull && python3 import_to_sqlite.py
```

folds all CSVs into the engine's `market.db` (idempotent — only new rows
insert). Run manually, or let the engine's cycle shell out to it.

## Universe

Edit `universe.json`. Keep it in sync with `regime_engine/config.json`.
Missing perp products are skipped harmlessly (not every asset has -PERP-INTX).

## Known limits (accepted)

- GitHub cron has jitter (runs can be minutes late; rarely, skipped). Fine at
  hourly cadence; NOT suitable for minute-level work — that's the Cloudflare
  Worker upgrade documented in the project roadmap.
- Public repo = public data. This is all public market data anyway; no keys,
  no positions, nothing private lives here. Never add secrets to this repo.
- ~50 requests/run, well inside Coinbase public rate limits.

## Upgrade path

When minute-cadence or a query API is wanted: Cloudflare Worker (cron every
minute) + D1, exposing an HTTPS/MCP endpoint. The CSVs here import into D1
with the same schema — nothing collected now is wasted.
