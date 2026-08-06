#!/usr/bin/env python3
"""COIN50 price series for the collector — into index_prices.db (collector-owned;
does NOT touch market.db). Route: Coinbase brokerage market candles for the
COIN50-PERP-INTX perpetual, the only clean keyless route (the raw INTX index
endpoint is IP-blocked here; the consumer index history is behind Coinbase's
private GraphQL). The perp TRACKS the COIN50 index — stored as the perp, with
source stamped on every row (labels never lie). Keyless public, read-only, stdlib.

Backfills ONE_HOUR and ONE_DAY back to listing (~2024-11); INSERT OR IGNORE so
re-running is resumable and safe.

    python3 fetch_index_candles.py                 # COIN50-PERP-INTX
    python3 fetch_index_candles.py ETH-PERP-INTX
"""
import json, os, sqlite3, sys, time, urllib.request

BROKERAGE = "https://api.coinbase.com/api/v3/brokerage/market/products"
PID = sys.argv[1] if len(sys.argv) > 1 else "COIN50-PERP-INTX"
SOURCE = "coinbase brokerage market candles (perp; tracks COIN50 index)"
BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "index_prices.db")
GRANS = {"ONE_HOUR": 3600, "ONE_DAY": 86400}       # take both

SCHEMA = """CREATE TABLE IF NOT EXISTS index_candles(
  ts INTEGER NOT NULL, index_id TEXT NOT NULL, granularity INTEGER NOT NULL,
  open REAL, high REAL, low REAL, close REAL, volume REAL, source TEXT,
  PRIMARY KEY (ts, index_id, granularity));"""


def get(url, retries=4):
    for a in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "coinland-idx/1.0",
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r)
        except Exception:
            if a == retries:
                return None
            time.sleep(2 + 2 * a)


def backfill(con, gran_name, gran_s):
    now = int(time.time()) // gran_s * gran_s
    win = 300 * gran_s
    cur = now
    empties = 0
    total = 0
    while empties < 4:
        start = cur - win
        d = get(f"{BROKERAGE}/{PID}/candles?granularity={gran_name}&start={start}&end={cur}")
        candles = (d or {}).get("candles") if isinstance(d, dict) else d
        if candles is None:
            empties += 1; cur = start - gran_s; time.sleep(0.4); continue
        if not candles:
            empties += 1; cur = start - gran_s; time.sleep(0.3); continue
        empties = 0
        rows = [(int(c["start"]), PID, gran_s, float(c["open"]), float(c["high"]),
                 float(c["low"]), float(c["close"]), float(c["volume"]), SOURCE) for c in candles]
        con.executemany("INSERT OR IGNORE INTO index_candles "
                        "(ts,index_id,granularity,open,high,low,close,volume,source) "
                        "VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.commit()
        total += con.total_changes  # cumulative; recomputed below anyway
        oldest = min(int(c["start"]) for c in candles)
        cur = (oldest - gran_s) if oldest < cur else (start - gran_s)
        time.sleep(0.3)
    return total


def main():
    con = sqlite3.connect(DB)
    con.executescript(SCHEMA)
    for name, secs in GRANS.items():
        backfill(con, name, secs)
    import datetime
    f = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    print(f"DB: {DB}")
    for secs, label in [(3600, "hourly"), (86400, "daily")]:
        r = con.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM index_candles "
                        "WHERE index_id=? AND granularity=?", (PID, secs)).fetchone()
        if r and r[0]:
            print(f"  {label:6} ({secs}s): {r[0]} rows, {f(r[1])} .. {f(r[2])} UTC")
        else:
            print(f"  {label:6} ({secs}s): none")
    con.close()


if __name__ == "__main__":
    main()
