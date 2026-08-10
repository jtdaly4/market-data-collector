#!/usr/bin/env python3
"""
Fold the repo's daily CSVs into a local SQLite archive (market.db).

Usage:  python3 import_to_sqlite.py [path/to/market.db]
Default DB path: ../Radar/Hourly Radar/regime_engine/data/market.db
Idempotent: INSERT OR IGNORE on (ts, product_id) primary keys — re-run anytime
(e.g., after `git pull` in this repo) and only new rows land.
"""

import csv
import glob
import os
import sqlite3
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE, "..", "Radar", "Hourly Radar",
                          "regime_engine", "data", "market.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
  ts INTEGER NOT NULL, product_id TEXT NOT NULL, granularity INTEGER NOT NULL DEFAULT 3600,
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  PRIMARY KEY (ts, product_id, granularity));
CREATE TABLE IF NOT EXISTS perp_snapshots (
  ts INTEGER NOT NULL, product_id TEXT NOT NULL,
  funding_rate REAL, open_interest REAL, mark_price REAL, index_price REAL, basis REAL,
  PRIMARY KEY (ts, product_id));
-- Hyperliquid / Moon Dev history (from grab_moondev_history.py, data/hyperliquid/*.csv).
-- Point-in-time; venue kept explicit since liquidations span multiple exchanges.
CREATE TABLE IF NOT EXISTS hl_funding (
  ts INTEGER NOT NULL, coin TEXT NOT NULL, venue TEXT NOT NULL DEFAULT 'hyperliquid',
  funding_rate REAL, funding_annualized REAL, mark_price REAL, open_interest REAL,
  source TEXT,
  PRIMARY KEY (ts, coin, venue));
CREATE TABLE IF NOT EXISTS liquidations (
  ts INTEGER NOT NULL, venue TEXT NOT NULL DEFAULT '', coin TEXT NOT NULL DEFAULT '',
  side TEXT NOT NULL DEFAULT '', window TEXT NOT NULL DEFAULT '',
  notional_usd REAL, price REAL, count REAL, source TEXT,
  PRIMARY KEY (ts, venue, coin, side, window));
"""


def rows(pattern):
    """Yield CSV rows across matching files, skipping '#' metadata header lines
    (the data/hyperliquid/*.csv files carry a venue+caveat comment block)."""
    for path in sorted(glob.glob(os.path.join(BASE, "data", pattern))):
        with open(path, newline="") as f:
            uncommented = (ln for ln in f if not ln.lstrip().startswith("#"))
            yield from csv.DictReader(uncommented)


def f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def ti(v):
    """Timestamp -> int seconds (tolerates '1.7e9' / float strings)."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def main():
    db_path = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA)

    n = con.executemany(
        "INSERT OR IGNORE INTO candles (ts, product_id, granularity, open, high, low, close, volume) "
        "VALUES (?, ?, 3600, ?, ?, ?, ?, ?)",
        [(int(r["ts"]), r["product_id"], f(r["open"]), f(r["high"]),
          f(r["low"]), f(r["close"]), f(r["volume"])) for r in rows("candles/*.csv")]
    ).rowcount
    m = con.executemany(
        "INSERT OR IGNORE INTO perp_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(int(r["ts"]), r["product_id"], f(r["funding_rate"]), f(r["open_interest"]),
          f(r["mark_price"]), f(r["index_price"]), f(r["basis"])) for r in rows("perp/*.csv")]
    ).rowcount

    # Hyperliquid / Moon Dev history (present only after grab_moondev_history.py runs).
    hf = con.executemany(
        "INSERT OR IGNORE INTO hl_funding "
        "(ts, coin, venue, funding_rate, funding_annualized, mark_price, open_interest, source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(ti(r.get("ts")), (r.get("coin") or "").upper(), r.get("venue") or "hyperliquid",
          f(r.get("funding_rate")), f(r.get("funding_annualized")), f(r.get("mark_price")),
          f(r.get("open_interest")), r.get("source"))
         for r in rows("hyperliquid/funding*.csv") if ti(r.get("ts"))]
    ).rowcount
    lq = con.executemany(
        "INSERT OR IGNORE INTO liquidations "
        "(ts, venue, coin, side, window, notional_usd, price, count, source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(ti(r.get("ts")), (r.get("venue") or "").lower(), (r.get("coin") or "").upper(),
          (r.get("side") or "").lower(), r.get("window") or "",
          f(r.get("notional_usd")), f(r.get("price")), f(r.get("count")), r.get("source"))
         for r in rows("hyperliquid/liquidations*.csv") if ti(r.get("ts"))]
    ).rowcount
    con.commit()

    c = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM candles").fetchone()
    p = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM perp_snapshots").fetchone()
    print(f"imported {n} new candle rows, {m} new perp rows -> {db_path}")
    print(f"totals: candles {c[0]} rows / {c[1]} products; perp {p[0]} rows / {p[1]} products")
    if hf or lq or con.execute("SELECT 1 FROM hl_funding LIMIT 1").fetchone() \
            or con.execute("SELECT 1 FROM liquidations LIMIT 1").fetchone():
        hfc = con.execute("SELECT COUNT(*), COUNT(DISTINCT coin) FROM hl_funding").fetchone()
        lqc = con.execute("SELECT COUNT(*), COUNT(DISTINCT venue) FROM liquidations").fetchone()
        print(f"imported {hf} new hl_funding rows, {lq} new liquidation rows")
        print(f"totals: hl_funding {hfc[0]} rows / {hfc[1]} coins; "
              f"liquidations {lqc[0]} rows / {lqc[1]} venues")
    con.close()


if __name__ == "__main__":
    main()
