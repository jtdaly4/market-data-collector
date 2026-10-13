#!/usr/bin/env python3
"""Coinbase Exchange hourly-candle sweep (5 min cadence) -> ~/RegimeData/
market.db `candles` table + ~/RegimeData/feeds/spot_health.json.

ORDER 103 part 3 (DAEDALUS 2026-10-02T20:40:26Z). "Spot loop, every 5 min:
hourly candles for the 28 -USD names from the Coinbase Exchange public API
(the engine's own fetch_candles source today), written into market.db
candles (granularity 3600, current partial hour updated in place) and a
feeds/spot_health.json with per-name last-candle age. This is what lets
GitHub Actions and the collect-pinger be retired (step 4) -- do not retire
anything yourself." Nothing retired here. Public endpoint, no keys, no
engine files, no sudo.

UNIVERSE: universe.json's `perps` list minus COIN50 (no -USD spot product;
it is an index) -- 28 names today, matching the order's own count. Not
universe.json's separate `spot` list (30 names, includes BNB/HYPE), because
the engine's OWN load_universe() excludes those two as "trading-only, no
spot candles" (regime_engine/engine.py, confirmed live 2026-10-07) -- this
loop feeds the engine, so it uses the engine's actual research universe,
not the superset.

CURRENT-HOUR SEMANTICS, and why this writer uses INSERT OR REPLACE where
import_to_sqlite.py uses INSERT OR IGNORE. The Coinbase candles endpoint's
newest bar is the IN-PROGRESS hour -- its OHLCV legitimately changes every
call until the hour closes. IGNORE would silently freeze that bar at
whatever it read on the FIRST 5-min tick of the hour; REPLACE keeps it live,
exactly the order's "current partial hour updated in place". For an already
CLOSED hour, REPLACE is a no-op in practice (the venue does not revise
settled candles) but is not a correctness regression if it ever did --
unlike IGNORE, which would keep a stale value forever past a venue
correction. Nothing already in market.db from the hourly GitHub/sync path
is touched differently; this writer is additive to the SAME table, same PK
(ts, product_id, granularity).

FIXTURES FIRST: `_rows_for_insert` / `_health_entry` are pure (no network, no
wall-clock, no db) -- `python3 spot_loop.py --selftest` exercises them
against canned Coinbase payloads, network unreachable.

Stdlib only. Public Coinbase Exchange endpoint. No keys. No engine files.
No sudo.
"""

import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.request

EXCHANGE = "https://api.exchange.coinbase.com"
UNIVERSE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe.json")
FEEDS_DIR = os.path.expanduser("~/RegimeData/feeds")
DB = os.path.expanduser("~/RegimeData/market.db")
GRANULARITY = 3600
BACKUP_DONE_FLAG = os.path.expanduser("~/RegimeData/market.db.spotloop-first-backup-done")


def _get(url, timeout=15, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "spot-loop/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception:
            if attempt == retries:
                return None
            time.sleep(1 + attempt)


def _write_json_atomic(path, obj):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _rows_for_insert(raw, pid, max_bars=50):
    """Pure. Coinbase's raw candle rows ([time, low, high, open, close, vol],
    newest-first, API order) -> ascending (ts, product_id, granularity,
    open, high, low, close, volume) tuples ready for executemany. Keeps the
    newest (in-progress) bar -- unlike collector.py's GitHub-Actions path,
    which drops it on purpose for a clean archive. Caps at max_bars most
    recent (self-heals gaps up to that many hours, matching collector.py's
    existing 48-bar precedent) -- never the whole multi-hundred-bar response,
    which would just re-write settled history every 5 minutes for nothing."""
    if not raw:
        return []
    rows = sorted(raw, key=lambda c: c[0])[-max_bars:]
    return [(r[0], pid, GRANULARITY, r[3], r[2], r[1], r[4], r[5]) for r in rows]


def _health_entry(raw, now):
    """Pure. Coinbase's raw candle rows -> {last_ts, last_iso, age_s} for
    spot_health.json, or {last_ts: None, age_s: None} (UNMEASURED, never a
    fabricated 0) if the fetch returned nothing usable."""
    if not raw:
        return {"last_ts": None, "last_iso": None, "age_s": None}
    last_ts = max(c[0] for c in raw)
    return {"last_ts": int(last_ts),
            "last_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts)),
            "age_s": round(now - last_ts, 1)}


def _selftest():
    """Canned payloads, zero network."""
    now = 1791374400.0  # 2026-10-07T12:00:00Z
    raw = [
        [1791367200, 83395.72, 83914.54, 83784.28, 83719.72, 287.2],   # 10:00Z, closed
        [1791370800, 83600.0, 83800.0, 83719.72, 83750.0, 150.0],      # 11:00Z, closed
        [1791374400, 83750.0, 83780.0, 83750.0, 83760.0, 12.5],        # 12:00Z, IN PROGRESS
    ]
    rows = _rows_for_insert(raw, "BTC-USD")
    assert len(rows) == 3, rows
    assert rows[-1] == (1791374400, "BTC-USD", 3600, 83750.0, 83780.0, 83750.0, 83760.0, 12.5), rows[-1]
    assert rows[0][0] < rows[-1][0], "must be ascending, oldest first"
    print("PASS: _rows_for_insert keeps the in-progress bar, ascending order ->", rows[-1])

    capped = _rows_for_insert([[1000 + i * 3600] + [1.0] * 5 for i in range(80)], "ETH-USD", max_bars=50)
    assert len(capped) == 50, len(capped)
    print("PASS: _rows_for_insert caps to max_bars (50), never writes the full history back")

    h = _health_entry(raw, now)
    assert h["last_ts"] == 1791374400 and h["age_s"] == 0.0, h
    print("PASS: _health_entry, fresh in-progress bar -> age_s 0.0 ->", h)

    stale = _health_entry(raw[:1], now)
    assert stale["age_s"] == now - 1791367200, stale
    print("PASS: _health_entry, only an old closed bar -> honest nonzero age ->", stale)

    empty = _health_entry([], now)
    assert empty["last_ts"] is None and empty["age_s"] is None, empty
    print("PASS: _health_entry, no candles at all -> None/None (UNMEASURED, not a fabricated 0)")

    assert _rows_for_insert(None, "BTC-USD") == []
    assert _rows_for_insert([], "BTC-USD") == []
    print("PASS: _rows_for_insert, no data -> [] (not a crash, not a fabricated row)")

    print("ALL SELFTESTS PASS")


def main():
    with open(UNIVERSE_PATH) as f:
        perps = json.load(f)["perps"]
    bases = [s for s in perps if s != "COIN50"]  # COIN50 has no -USD spot product

    if not os.path.exists(BACKUP_DONE_FLAG):
        stamp = time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())
        dst = f"{DB}.bak-spotloop-first-write-{stamp}"
        shutil.copy2(DB, dst)
        if os.path.getsize(DB) != os.path.getsize(dst):
            raise SystemExit(f"FATAL: backup size mismatch, refusing first write: {dst}")
        with open(BACKUP_DONE_FLAG, "w") as f:
            f.write(f"{dst} {os.path.getsize(dst)}\n")
        print(f"first-write backup: {dst} ({os.path.getsize(dst):,} B)")

    now = time.time()
    conn = sqlite3.connect(DB, timeout=10)
    health, misses = {}, []
    n_rows = 0
    for sym in bases:
        pid = f"{sym}-USD"
        raw = _get(f"{EXCHANGE}/products/{pid}/candles?granularity={GRANULARITY}")
        if not raw:
            misses.append(sym)
            health[pid] = {"last_ts": None, "last_iso": None, "age_s": None}
            time.sleep(0.15)
            continue
        rows = _rows_for_insert(raw, pid)
        conn.executemany(
            "INSERT OR REPLACE INTO candles "
            "(ts, product_id, granularity, open, high, low, close, volume) "
            "VALUES (?,?,?,?,?,?,?,?)", rows)
        n_rows += len(rows)
        health[pid] = _health_entry(raw, time.time())
        time.sleep(0.15)
    conn.commit()
    conn.close()

    os.makedirs(FEEDS_DIR, exist_ok=True)
    worst_age = max((h["age_s"] for h in health.values() if h["age_s"] is not None), default=None)
    _write_json_atomic(os.path.join(FEEDS_DIR, "spot_health.json"), {
        "generated_at": int(now), "n": len(bases) - len(misses), "expect": len(bases),
        "per_asset": health, "worst_age_s": worst_age, "source": "coinbase_exchange",
    })

    print(f"spot_loop: {len(bases) - len(misses)}/{len(bases)} assets live, "
          f"{n_rows} candle rows written (incl. in-progress hour)"
          + (f"; UNMEASURED (no fetch): {','.join(misses)}" if misses else ""))
    if len(misses) == len(bases):
        raise SystemExit("all candle fetches failed")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        main()
