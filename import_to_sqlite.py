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
"""


def rows(pattern):
    for path in sorted(glob.glob(os.path.join(BASE, "data", pattern))):
        with open(path, newline="") as f:
            yield from csv.DictReader(f)


def f(v):
    try:
        return float(v)
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
    con.commit()

    c = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM candles").fetchone()
    p = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id) FROM perp_snapshots").fetchone()
    print(f"imported {n} new candle rows, {m} new perp rows -> {db_path}")
    print(f"totals: candles {c[0]} rows / {c[1]} products; perp {p[0]} rows / {p[1]} products")
    con.close()


if __name__ == "__main__":
    main()
