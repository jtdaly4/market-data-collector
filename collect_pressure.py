#!/usr/bin/env python3
"""Pressure-family INGREDIENTS collector (Satio depth + OI/positioning).

Stores the raw inputs of the squeezeometer, NEVER the computed satio ratio/score
(that stays a read-time formula). Hourly, keyless public, read-only, stdlib only.

  satio_snapshots(ts, venue, depth_1pct_btc)     -- one row per venue that answered
  oi_snapshots(ts, oi_total_btc, short_share, source)

Depth/OI logic COPIED (not imported) from A cards/A Card 1/thermostat_card_3.py
(_depth_within_1pct_*, _binance_open_interest_btc, _binance_short_share), with
the HTTP reimplemented in urllib (stdlib) and the fallbacks kept.

HONEST GAPS: a venue/metric that fails is a MISSING row (or NULL), never a zero —
an outage must not be forgeable as "zero depth". No interpolation. INSERT OR
IGNORE, append-only. Does NOT touch market.db.

NOTE: run from a US IP, Binance (api.binance.com / fapi.binance.com) returns 451
"restricted location", so Binance depth/OI/short-share come back missing here and
OI/positioning fall back to OKX (source-stamped). Run from a non-US IP to capture
the Binance ingredients the original ThermoSat card uses.
"""
import json, os, sqlite3, sys, time, urllib.request, urllib.error

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "pressure_snapshots.db")
SCHEMA = """
CREATE TABLE IF NOT EXISTS satio_snapshots(
  ts INTEGER NOT NULL, venue TEXT NOT NULL, depth_1pct_btc REAL,
  PRIMARY KEY (ts, venue));
CREATE TABLE IF NOT EXISTS oi_snapshots(
  ts INTEGER NOT NULL, oi_total_btc REAL, short_share REAL, source TEXT,
  PRIMARY KEY (ts));
"""


def _http_json(url, retries=3, timeout=12):
    """Stdlib GET with a couple of in-hour retries. Returns parsed JSON or raises."""
    last = None
    for a in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ThermoSat/1.0",
                                                       "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception as e:
            last = e
            time.sleep(1.5 * (a + 1))
    raise last


def _sum_1pct(bids, asks):
    """±1% two-sided depth in BTC from [(price,qty),...] books (copied windowing)."""
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = 0.5 * (best_bid + best_ask)
    low, high = mid * 0.99, mid * 1.01
    btc = 0.0
    for p, q in bids:
        if p >= low: btc += q
        else: break
    for p, q in asks:
        if p <= high: btc += q
        else: break
    return btc


# --- ±1% depth per venue (None on any failure = missing, never zero) ---------
def depth_binance():
    j = _http_json("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000")
    return _sum_1pct([(float(p), float(q)) for p, q in j["bids"]],
                     [(float(p), float(q)) for p, q in j["asks"]])

def depth_coinbase():
    j = _http_json("https://api.exchange.coinbase.com/products/BTC-USD/book?level=2")
    return _sum_1pct([(float(p), float(q)) for p, q, _ in j["bids"]],
                     [(float(p), float(q)) for p, q, _ in j["asks"]])

def depth_kraken():
    j = _http_json("https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=500")
    k = next(iter(j["result"]))
    return _sum_1pct([(float(p), float(q)) for p, q, _ in j["result"][k]["bids"]],
                     [(float(p), float(q)) for p, q, _ in j["result"][k]["asks"]])

def depth_bitstamp():
    j = _http_json("https://www.bitstamp.net/api/v2/order_book/btcusd/")
    return _sum_1pct([(float(p), float(q)) for p, q in j["bids"]],
                     [(float(p), float(q)) for p, q in j["asks"]])

DEPTH = {"binance": depth_binance, "coinbase": depth_coinbase,
         "kraken": depth_kraken, "bitstamp": depth_bitstamp}


# --- OI (BTC) + short share, Binance first then OKX fallback, source-stamped --
def oi_and_short():
    oi = short = None
    src = []
    # Binance OI (geo-blocked from US -> None)
    try:
        j = _http_json("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT")
        v = float(j.get("openInterest", 0)); oi = v if v > 0 else None
        if oi: src.append("binance")
    except Exception:
        pass
    # Binance global long/short -> short share
    try:
        d = _http_json("https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1")
        ratio = float(d[-1]["longShortRatio"]); short = max(0.0, min(1.0, 1.0 / (1.0 + ratio)))
        if "binance" not in src: src.append("binance")
    except Exception:
        pass
    # OKX fallback (BTC-USDT-SWAP): oiCcy is BTC units; long/short account ratio
    if oi is None:
        try:
            k = _http_json("https://www.okx.com/api/v5/public/open-interest?instId=BTC-USDT-SWAP")
            oc = (k.get("data") or [{}])[0].get("oiCcy")
            if oc is not None: oi = float(oc); src.append("okx")
        except Exception:
            pass
    if short is None:
        try:
            k = _http_json("https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=BTC&period=5m")
            row = (k.get("data") or [])[-1]
            ratio = float(row[1]); short = max(0.0, min(1.0, 1.0 / (1.0 + ratio)))
            if "okx" not in src: src.append("okx")
        except Exception:
            pass
    return oi, short, ("+".join(dict.fromkeys(src)) or None)


def main():
    ts = int(time.time()) // 3600 * 3600          # bucket to the hour
    con = sqlite3.connect(DB)
    con.executescript(SCHEMA)

    got = {}
    for venue, fn in DEPTH.items():
        try:
            d = fn()
            if d and d > 0:                       # a real book
                con.execute("INSERT OR IGNORE INTO satio_snapshots(ts,venue,depth_1pct_btc) "
                            "VALUES (?,?,?)", (ts, venue, round(d, 6)))
                got[venue] = round(d, 4)
        except Exception:
            pass                                  # missing row, never a zero

    oi, short, src = oi_and_short()
    if oi is not None or short is not None:        # at least one ingredient present
        con.execute("INSERT OR IGNORE INTO oi_snapshots(ts,oi_total_btc,short_share,source) "
                    "VALUES (?,?,?,?)", (ts, oi, short, src))
    con.commit(); con.close()

    # Diagnostic line only — the DB write above is the payload and has already
    # landed. Guard the flush: the Claude Desktop VM intermittently throws
    # EDEADLK ("Resource deadlock avoided") on stdout flush, which must never
    # dirty the exit code or imply the capture failed.
    import datetime
    stamp = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
    line = f"[{stamp:%Y-%m-%d %H:00} UTC] depth: {got}  | oi={oi} short={short} src={src}"
    for _ in range(3):
        try:
            sys.stdout.write(line + "\n"); sys.stdout.flush(); break
        except OSError:
            time.sleep(0.3)


if __name__ == "__main__":
    try:
        main()
    except OSError:
        pass  # VM-induced EDEADLK on a flush must not fail the hour's run
    # Swallow the interpreter's final implicit stdout flush too (same EDEADLK).
    try:
        sys.stdout.flush()
    except OSError:
        try: os.close(sys.stdout.fileno())
        except Exception: pass
