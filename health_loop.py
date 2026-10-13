#!/usr/bin/env python3
"""Feed health aggregator (60s cadence) -> ~/RegimeData/feeds/health.json.

ORDER 103 part 4 (DAEDALUS 2026-10-02T20:40:26Z): "feeds/health.json -- per
feed (marks, funding, snapshots, spot, archive, liquidations): last venue
timestamp, age_s, last write, error count. The daily report reads it; the
engine's stale guard reads it."

READ-ONLY aggregator -- no network calls, no venue of its own. It reads what
parts 1-3 and the existing collector/liquidation pipelines already wrote
(feeds/live_marks.json, feeds/live_funding.json, feeds/spot_health.json,
market.db's perp_snapshots/liquidations tables, the 15-min archive's own
manifest) and reports each one's own honest staleness, keyed to THAT feed's
own expected cadence -- a 60s ticker and an hourly snapshot are not graded on
the same clock (the standing frontier-vs-interior law: a feed with no
scheduler of its own reads as exactly that, not folded into one composite
verdict that would manufacture a false alarm against a feed with a wider
natural lag, or hide a genuinely missing scheduler inside an average).

FIXTURES FIRST: `_freshness` is pure (no network, no wall-clock, no io) --
`python3 health_loop.py --selftest` exercises it, including the
demonstrated failure mode (no data at all -> UNMEASURED, not a fabricated
age or a false "ok").

Stdlib only. Reads ~/RegimeData/feeds/*.json and market.db (read-only). No
keys, no engine files, no sudo, no network.
"""

import calendar
import json
import os
import sqlite3
import sys
import time

FEEDS_DIR = os.path.expanduser("~/RegimeData/feeds")
DB = os.path.expanduser("~/RegimeData/market.db")
ARCHIVE_MANIFEST = os.path.expanduser(
    "~/CoinLandRepos/market-data-collector/archive/coinbase_intx/candles_intx/_manifest.json")
HEALTH_PATH = os.path.join(FEEDS_DIR, "health.json")


def _write_json_atomic(path, obj):
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _freshness(last_ts, now, n_present=None, n_expected=None, last_write_ts=None):
    """Pure. One feed's raw numbers -> its health.json entry. `last_ts` is
    the VENUE/DATA timestamp (what the data describes); `last_write_ts`
    (defaults to last_ts when a feed has no separate write-time concept) is
    when this process last touched the file/table. error_count is simply
    "how many expected items are missing right now" -- the only error
    signal every one of these feeds can report without a NEW persisted
    counter this part didn't ask for. UNMEASURED (None), never a fabricated
    0 or a false 'ok', when there is no data at all."""
    if last_ts is None:
        return {"last_venue_ts": None, "last_venue_iso": None, "age_s": None,
                "last_write_ts": last_write_ts, "n": n_present, "expect": n_expected,
                "error_count": n_expected if n_expected is not None else None,
                "status": "UNMEASURED"}
    age = round(now - last_ts, 1)
    err = max(0, (n_expected - n_present)) if (n_expected is not None and n_present is not None) else None
    return {"last_venue_ts": int(last_ts),
            "last_venue_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts)),
            "age_s": age, "last_write_ts": int(last_write_ts) if last_write_ts else int(last_ts),
            "n": n_present, "expect": n_expected, "error_count": err, "status": "measured"}


def _marks_health(now):
    d = _read_json(os.path.join(FEEDS_DIR, "live_marks.json"))
    if not d:
        return _freshness(None, now)
    marks = d.get("marks") or {}
    ages = [e["venue_ts"] for e in marks.values() if e.get("venue_ts") is not None]
    last_ts = max(ages) if ages else None
    return _freshness(last_ts, now, n_present=len(marks), n_expected=29,
                       last_write_ts=d.get("generated_at"))


def _funding_health(now):
    d = _read_json(os.path.join(FEEDS_DIR, "live_funding.json"))
    if not d:
        return _freshness(None, now)
    gen = d.get("generated_at")
    worst = d.get("worst_age_s")
    last_ts = (now - worst) if (gen and worst is not None) else gen
    return _freshness(last_ts, now, n_present=d.get("n"), n_expected=29, last_write_ts=gen)


def _spot_health(now):
    d = _read_json(os.path.join(FEEDS_DIR, "spot_health.json"))
    if not d:
        return _freshness(None, now)
    per = d.get("per_asset") or {}
    tss = [h["last_ts"] for h in per.values() if h.get("last_ts") is not None]
    last_ts = max(tss) if tss else None
    return _freshness(last_ts, now, n_present=d.get("n"), n_expected=d.get("expect", 28),
                       last_write_ts=d.get("generated_at"))


def _db_health(now, table, expect, where=""):
    """Shared shape for the two market.db-backed feeds (snapshots,
    liquidations) -- MAX(ts) + distinct-product count at that ts."""
    try:
        conn = sqlite3.connect(DB, timeout=5)
        row = conn.execute(f"SELECT MAX(ts) FROM {table} {where}").fetchone()
        last_ts = row[0] if row else None
        n = None
        if last_ts is not None:
            n_row = conn.execute(
                f"SELECT COUNT(DISTINCT product_id) FROM {table} WHERE ts = ?"
                if table == "perp_snapshots" else
                f"SELECT COUNT(*) FROM {table} WHERE ts = ?", (last_ts,)).fetchone()
            n = n_row[0] if n_row else None
        conn.close()
    except Exception:
        return _freshness(None, now)
    return _freshness(last_ts, now, n_present=n, n_expected=expect)


def _archive_health(now):
    """The 15-min candle archive (deribit_candles_topup.py) has NO scheduler
    of its own as of this build (ORDER 103 part 4) -- confirmed by `launchctl
    list` carrying nothing matching it. That is reported honestly as a large
    age, not suppressed, and not treated as this script's job to fix.

    n_expected is the count of manifest entries that HAVE EVER carried a
    'latest' (151) -- not the manifest's full 264 keys. The other 113 are
    legacy INTX-only instruments with no Deribit equivalent at all (same
    roster mismatch already measured for funding_intx.db's continuation);
    counting them as 'missing' would manufacture exactly the false-outage
    composite the frontier/interior law warns against, over a venue gap
    that was never going to close."""
    m = _read_json(ARCHIVE_MANIFEST)
    if not m:
        return _freshness(None, now)
    latests = [v.get("latest") for v in m.values() if v.get("latest")]
    if not latests:
        return _freshness(None, now)
    last_iso = max(latests)
    last_ts = calendar.timegm(time.strptime(last_iso, "%Y-%m-%dT%H:%M:%SZ"))
    return _freshness(last_ts, now, n_present=len(latests), n_expected=len(latests))


def main():
    now = time.time()
    health = {
        "generated_at": int(now),
        "marks": _marks_health(now),
        "funding": _funding_health(now),
        "snapshots": _db_health(now, "perp_snapshots", 28),
        "spot": _spot_health(now),
        "archive": _archive_health(now),
        "liquidations": _db_health(now, "liquidations", None),
    }
    os.makedirs(FEEDS_DIR, exist_ok=True)
    _write_json_atomic(HEALTH_PATH, health)
    degraded = [k for k, v in health.items() if isinstance(v, dict) and v.get("status") == "UNMEASURED"]
    print("health: " + ", ".join(f"{k} age={v.get('age_s')}" for k, v in health.items() if isinstance(v, dict))
          + (f"; UNMEASURED: {','.join(degraded)}" if degraded else ""))


def _selftest():
    now = 1791374400.0
    f = _freshness(1791370800, now, n_present=28, n_expected=29)
    assert f["age_s"] == 3600.0 and f["error_count"] == 1 and f["status"] == "measured", f
    print("PASS: _freshness, normal case ->", f)

    none_f = _freshness(None, now)
    assert none_f["status"] == "UNMEASURED" and none_f["age_s"] is None, none_f
    assert none_f["error_count"] is None, "no n_expected given -> error_count stays None, not a guessed 0"
    print("PASS: _freshness, no data at all -> UNMEASURED, age_s None (not a fabricated 0)")

    full = _freshness(1791374400, now, n_present=29, n_expected=29)
    assert full["error_count"] == 0 and full["age_s"] == 0.0, full
    print("PASS: _freshness, fully fresh + fully present -> error_count 0 ->", full)

    print("ALL SELFTESTS PASS")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        main()
