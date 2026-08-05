#!/usr/bin/env python3
"""Deep HOURLY history for a Coinbase product (default BTC-USD) → history/<SYM>_hourly.csv.
Paginates the public Exchange candles API back to listing start (~2016 for BTC-USD).
Skips outage gaps; stops after a long run of empties = real start. Stdlib, keyless,
read-only. ~4.6 MB for 10y BTC — small enough to commit to git (unlike the 1-min archive).

    python3 fetch_hourly_history.py            # BTC-USD
    python3 fetch_hourly_history.py ETH-USD
"""
import csv, datetime, json, os, sys, time, urllib.request

API = "https://api.exchange.coinbase.com"
SYM = sys.argv[1] if len(sys.argv) > 1 else "BTC-USD"
GRAN = 3600
WIN = 300 * GRAN
BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "history", f"{SYM}_hourly.csv")
os.makedirs(os.path.dirname(OUT), exist_ok=True)


def get(url, retries=4):
    for a in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "coinland-hist/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r)
        except Exception:
            if a == retries:
                return None
            time.sleep(2 + 2 * a)


def iso(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


now = int(time.time()) // GRAN * GRAN
data = {}
cur = now
empties = 0
miss = 0
while empties < 8:                     # 8 empty 12.5d windows (~100d) = past the start
    start = cur - WIN
    rows = get(f"{API}/products/{SYM}/candles?granularity={GRAN}&start={iso(start)}&end={iso(cur)}")
    if rows is None:
        miss += 1
        if miss >= 4:
            miss, empties = 0, empties + 1; cur = start - GRAN   # give up this window
        else:
            time.sleep(3)
        continue
    miss = 0
    if not rows:
        empties += 1; cur = start - GRAN; time.sleep(0.25); continue
    empties = 0
    for c in rows:                     # [time, low, high, open, close, volume]
        data[c[0]] = (c[3], c[2], c[1], c[4], c[5])
    oldest = min(c[0] for c in rows)
    cur = (oldest - GRAN) if oldest < cur else (start - GRAN)
    time.sleep(0.25)
    if len(data) % 6000 < 300:
        print(f"  {len(data):6} bars, at {iso(oldest)[:10]}", flush=True)

ts = sorted(data)
fmt = lambda t: datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
with open(OUT, "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["datetime_utc", "open", "high", "low", "close", "volume"])
    for t in ts:
        o, h, l, c, v = data[t]; w.writerow([fmt(t), o, h, l, c, v])
size = os.path.getsize(OUT) / 1e6
print(f"\nwrote {OUT}\n  {len(ts)} rows, {fmt(ts[0])} .. {fmt(ts[-1])}, {size:.1f} MB")
