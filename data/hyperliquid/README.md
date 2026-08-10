# data/hyperliquid — Moon Dev / Hyperliquid history

Populated by **`grab_moondev_history.py`** (run on a machine with network + your
`MOONDEV_API_KEY`; **not** part of the hourly Actions job). Files here carry a
`#`-commented metadata block (venue, source endpoint, caveat, tier, fetched_at)
above the CSV header — `import_to_sqlite.py` skips those comment lines.

| File | Table | Columns |
|------|-------|---------|
| `funding_history.csv` | `hl_funding` | `ts, coin, venue, funding_rate, funding_annualized, mark_price, open_interest, source` |
| `liquidations_history.csv` | `liquidations` | `ts, venue, coin, side, window, notional_usd, price, count, source` |
| `_grab_report.json` | — | machine-readable probe report (per-endpoint reach/rows/date-range) |

Caveats:
- **`ts` is Unix epoch SECONDS, UTC.**
- **Liquidations span multiple venues** (Hyperliquid, Binance, Bybit, OKX) — always
  filter/group by `venue`.
- **Coverage depends on your API tier.** A standard key may return only live
  snapshots / rolling windows; deep history (bulk Binance liqs) needs a Quant
  Elite (`_qe`) key. The grab script probes and reports exactly what your key
  returned — read `_grab_report.json` for the verdict.
- **Not backfillable.** For Coinbase-INTX-relevant funding/OI/liquidations there
  is no historical source to re-fetch from later; a gap is permanent.
