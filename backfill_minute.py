#!/usr/bin/env python3
"""
backfill_minute.py — slow, resumable 1-minute candle archive from Coinbase.

Builds a real historical price archive (SQLite) for backtesting. Walks the
public Coinbase Exchange candles API backward from *now* to each product's
listing date, ~290 minutes per request, at a polite pace. Products are
processed ROUND-ROBIN: every asset gets its most-recent window before any
asset gets its second, so the archive is broadly useful for short-term
backtests long before the full multi-year history finishes.

Interrupt anytime and re-run — each product resumes from the oldest bar it
already has and keeps reaching further back. Idempotent (INSERT OR IGNORE),
so overlaps self-heal and re-runs never duplicate.

    python3 backfill_minute.py                        # all universe spot products, full history
    python3 backfill_minute.py --since 2024-01-01     # floor: don't go older than this
    python3 backfill_minute.py --products BTC-USD,ETH-USD --sleep 0.5
    python3 backfill_minute.py --db /path/to/archive.db

Stdlib only. Public endpoints, no keys. DB defaults to archive/candles_1m.db
(gitignored — too big for git; the repo keeps THIS SCRIPT as the recipe).

Schema matches the engine's market.db `candles` table (granularity column),
so a backtest can ATTACH or copy rows across without translation.
"""
import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
API = "https://api.exchange.coinbase.com"
GRAN = 60
WINDOW = 290 * GRAN   # seconds per request; 291 candles < Coinbase ~300 cap (no truncation)
MAX_EMPTY_WINDOWS = 50  # consecutive empty/barren windows (~10 days) before we call it
                        # end-of-history. Coinbase candles have gaps (exchange outages)
                        # with real data on the far side — e.g. a ~6.5h hole on
                        # 2026-05-08 that false-stopped the first run at 73 days.
                        # Skip gaps; only a run this long is a genuine listing edge.

SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
  ts INTEGER NOT NULL, product_id TEXT NOT NULL, granularity INTEGER NOT NULL DEFAULT 3600,
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  PRIMARY KEY (ts, product_id, granularity));
CREATE INDEX IF NOT EXISTS idx_candles_pid ON candles (product_id, granularity, ts);
"""


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def day(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def get(url, retries=4):
    """GET with exponential backoff — a rate-limit blip or hiccup must never
    kill an hours-long backfill."""
    for a in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "coinland-backfill/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r)
        except Exception:
            if a == retries:
                return None
            time.sleep(2 + 3 * a)


def fetch_window(pid, start_ts, end_ts):
    """Newest-first [time, low, high, open, close, volume], or None on error."""
    url = (f"{API}/products/{pid}/candles?granularity={GRAN}"
           f"&start={iso(start_ts)}&end={iso(end_ts)}")
    d = get(url)
    return d if isinstance(d, list) else None


def store(con, pid, rows):
    return con.executemany(
        "INSERT OR IGNORE INTO candles "
        "(ts, product_id, granularity, open, high, low, close, volume) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(c[0], pid, GRAN, c[3], c[2], c[1], c[4], c[5]) for c in rows]
    ).rowcount


def load_products(cfg_products):
    if cfg_products:
        return [p.strip() for p in cfg_products.split(",") if p.strip()]
    with open(os.path.join(BASE, "universe.json")) as f:
        uni = json.load(f)
    return [f"{s}-USD" for s in uni["spot"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.path.join(BASE, "archive", "candles_1m.db"))
    ap.add_argument("--sleep", type=float, default=0.35, help="seconds between requests")
    ap.add_argument("--since", default=None, help="floor date YYYY-MM-DD (don't go older)")
    ap.add_argument("--products", default=None, help="comma list, else universe.json spot")
    ap.add_argument("--max-requests", type=int, default=0, help="global cap (0 = unlimited)")
    args = ap.parse_args()

    floor_ts = 0
    if args.since:
        floor_ts = int(datetime.strptime(args.since, "%Y-%m-%d")
                       .replace(tzinfo=timezone.utc).timestamp())

    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    con = sqlite3.connect(args.db, timeout=60)
    # WAL: this archive is written by THIS backfill while the engine server
    # serves /api/archive/* reads off it. In the default rollback-journal mode
    # a reader blocks the writer and commit() dies with "database is locked"
    # (which killed an earlier multi-hour run). WAL lets readers and the single
    # writer coexist. busy_timeout makes any residual contention wait, not crash.
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=60000")
    con.executescript(SCHEMA)

    products = load_products(args.products)
    now = int(time.time()) // GRAN * GRAN

    # resume cursor per product = oldest bar we have, minus one step (or now if empty)
    cursor, done, errors, empties = {}, set(), {}, {}
    for pid in products:
        mn = con.execute("SELECT MIN(ts) FROM candles WHERE product_id=? AND granularity=?",
                         (pid, GRAN)).fetchone()[0]
        cursor[pid] = (mn - GRAN) if mn is not None else now
        errors[pid] = empties[pid] = 0

    print(f"archive: {os.path.abspath(args.db)}")
    print(f"{len(products)} products, granularity {GRAN}s, "
          f"floor {args.since or 'listing start'}, sleep {args.sleep}s")
    print("round-robin backward fill — recent data lands across all assets first\n")

    total_reqs = total_new = 0
    round_no = 0
    while len(done) < len(products):
        round_no += 1
        round_new = 0
        for pid in products:
            if pid in done:
                continue
            cur = cursor[pid]
            if cur <= floor_ts:
                done.add(pid)
                continue
            start = max(cur - WINDOW, floor_ts)
            rows = fetch_window(pid, start, cur)
            total_reqs += 1
            if rows is None:                       # transient error — retry same window next round
                errors[pid] += 1
                if errors[pid] >= 6:
                    print(f"  {pid}: too many errors, giving up this product")
                    done.add(pid)
                time.sleep(args.sleep)
                continue
            errors[pid] = 0
            if not rows:
                # Empty window = a GAP in Coinbase's candles (exchange outage),
                # NOT necessarily end-of-history — data resumes on the far side
                # (verified: ~6.5h hole on 2026-05-08 with data both sides). Skip
                # the window and keep walking back; only quit after a long run of
                # consecutive empties (bigger than any real outage) or the floor.
                empties[pid] += 1
                if empties[pid] >= MAX_EMPTY_WINDOWS:
                    done.add(pid)
                    print(f"  {pid}: reached start of history (~{day(cur)}, "
                          f"after {empties[pid]} empty windows)", flush=True)
                    continue
                cursor[pid] = start - GRAN         # step past the gap
                time.sleep(args.sleep)
                continue
            n = store(con, pid, rows)
            con.commit()
            total_new += n
            round_new += n
            oldest = min(c[0] for c in rows)
            if oldest >= cur:
                # Data came back but none of it is older than the cursor — this
                # is a GAP BOUNDARY (the window returned only its top edge), not
                # the end of history. Force-step past it exactly like an empty
                # window; quitting here stopped 27/28 products after one request.
                empties[pid] += 1
                if empties[pid] >= MAX_EMPTY_WINDOWS:
                    done.add(pid)
                    print(f"  {pid}: reached start of history (~{day(cur)}, "
                          f"after {empties[pid]} barren windows)", flush=True)
                else:
                    cursor[pid] = start - GRAN     # step past the gap
            else:
                empties[pid] = 0
                cursor[pid] = oldest - GRAN
            time.sleep(args.sleep)
            if args.max_requests and total_reqs >= args.max_requests:
                print(f"\nhit --max-requests {args.max_requests}, stopping (resumable).")
                done = set(products)
                break
        # progress line per full round
        frontier = min((cursor[p] for p in products if p not in done), default=None)
        edge = day(frontier) if frontier else "done"
        print(f"round {round_no}: +{round_new} rows | {len(done)}/{len(products)} products "
              f"complete | reqs {total_reqs} | frontier ~{edge}", flush=True)

    c = con.execute("SELECT COUNT(*), COUNT(DISTINCT product_id), MIN(ts), MAX(ts) "
                    "FROM candles WHERE granularity=?", (GRAN,)).fetchone()
    size_mb = os.path.getsize(args.db) / 1e6
    print(f"\nDONE. {total_new} new rows this run. archive now: {c[0]} 1-min rows, "
          f"{c[1]} products, {day(c[2])}..{day(c[3])}, {size_mb:.1f} MB")
    con.close()


if __name__ == "__main__":
    main()
