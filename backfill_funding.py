#!/usr/bin/env python3
"""
backfill_funding.py — historical funding-rate backlog for the universe, pulled
from FREE public venue APIs (no key). RUN ON YOUR MAC (needs outbound network).

Why this exists: Moon Dev's standard tier serves funding as a LIVE SNAPSHOT
only — no history (confirmed by grab_moondev_history.py). But the same assets'
funding history is public and free at the source venues, and cross-venue funding
is highly correlated, so it's a sound backtest backfill:

  - Binance USDⓈ-M  /fapi/v1/fundingRate   — years of history, ~8h cadence
  - Hyperliquid     POST /info fundingHistory — hourly, since ~2023

Both land in the SAME schema as the rest of data/hyperliquid/, so
import_to_sqlite.py folds them straight into the hl_funding table (keyed by
ts+coin+venue, so venues coexist and your engine can pick).

    python3 backfill_funding.py                          # both venues, all universe
    python3 backfill_funding.py --venues hyperliquid     # one venue
    python3 backfill_funding.py --since 2021-01-01        # floor
    python3 backfill_funding.py --coins BTC,ETH,SOL

Then: python3 import_to_sqlite.py /path/to/your/market.db

Stdlib only. No API key. Public endpoints.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "hyperliquid")
BINANCE = "https://fapi.binance.com/fapi/v1/fundingRate"
HL_INFO = "https://api.hyperliquid.xyz/info"
YEAR = 365 * 24 * 3600

FUNDING_COLS = ["ts", "coin", "venue", "funding_rate", "funding_annualized",
                "mark_price", "open_interest", "source"]

# base ticker -> venue-specific symbol. PEPE/SHIB are 1000x on Binance, k-prefixed on HL.
BINANCE_SYM = {"PEPE": "1000PEPEUSDT", "SHIB": "1000SHIBUSDT"}
HL_SYM = {"PEPE": "kPEPE", "SHIB": "kSHIB"}


def universe(cli_coins: str | None) -> list[str]:
    if cli_coins:
        return [c.strip().upper() for c in cli_coins.split(",") if c.strip()]
    try:
        with open(os.path.join(HERE, "universe.json")) as f:
            u = json.load(f)
        return list(dict.fromkeys(u.get("perps", []) + u.get("spot", [])))
    except Exception:
        return ["BTC", "ETH", "SOL"]


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def http_json(url: str, *, data: bytes | None = None, retries: int = 4):
    """GET (or POST if data) returning parsed JSON, or None. Backs off on error."""
    for a in range(retries + 1):
        try:
            headers = {"User-Agent": "coinland-funding-backfill/1.0"}
            if data is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            # 400 = symbol not listed on this venue: don't retry, just skip.
            if e.code in (400, 404, 451):
                return None
            if a == retries:
                return None
            time.sleep(2 + 3 * a)
        except Exception:
            if a == retries:
                return None
            time.sleep(2 + 3 * a)


# ---------------------------------------------------------------------------
# venue fetchers -> list of dicts {ts, funding_rate, mark_price}
# ---------------------------------------------------------------------------
def fetch_binance(base: str, since_ms: int, sleep: float, max_pages: int):
    sym = BINANCE_SYM.get(base, f"{base}USDT")
    out, start, now_ms = [], since_ms, int(time.time() * 1000)
    for _ in range(max_pages):
        url = f"{BINANCE}?symbol={sym}&startTime={start}&endTime={now_ms}&limit=1000"
        rows = http_json(url)
        if not rows:                      # None (unlisted) or [] (caught up)
            break
        for r in rows:
            out.append({"ts": int(r["fundingTime"]) // 1000,
                        "funding_rate": _num(r.get("fundingRate")),
                        "mark_price": _num(r.get("markPrice"))})
        last = int(rows[-1]["fundingTime"])
        if len(rows) < 1000 or last <= start:
            break
        start = last + 1
        time.sleep(sleep)
    return out


def fetch_hyperliquid(base: str, since_ms: int, sleep: float, max_pages: int):
    coin = HL_SYM.get(base, base)
    out, start, now_ms = [], since_ms, int(time.time() * 1000)
    seen_last = None
    for _ in range(max_pages):
        body = json.dumps({"type": "fundingHistory", "coin": coin,
                           "startTime": start, "endTime": now_ms}).encode()
        rows = http_json(HL_INFO, data=body)
        if not rows:
            break
        for r in rows:
            out.append({"ts": int(r["time"]) // 1000,
                        "funding_rate": _num(r.get("fundingRate")),
                        "mark_price": None})
        last = int(rows[-1]["time"])
        if last == seen_last or len(rows) < 500:
            break
        seen_last = last
        start = last + 1
        time.sleep(sleep)
    return out


def annualize(rows: list[dict]) -> None:
    """Fill funding_annualized from the median settlement interval (in place)."""
    ts = sorted(r["ts"] for r in rows if r["ts"])
    if len(ts) < 2:
        periods = 3 * 365            # assume 8h cadence when we can't measure
    else:
        deltas = sorted(ts[i + 1] - ts[i] for i in range(len(ts) - 1) if ts[i + 1] > ts[i])
        interval = deltas[len(deltas) // 2] if deltas else 8 * 3600
        periods = YEAR / interval if interval else 3 * 365
    for r in rows:
        r["funding_annualized"] = (r["funding_rate"] * periods
                                   if r["funding_rate"] is not None else None)


def write_csv(fname: str, rows: list[dict], meta: dict) -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, fname)
    seen, uniq = set(), []
    for r in rows:
        k = (r["ts"], r["coin"], r["venue"])
        if k in seen or not r["ts"]:
            continue
        seen.add(k)
        uniq.append(r)
    uniq.sort(key=lambda r: (r["coin"], r["ts"]))
    with open(path, "w", newline="") as f:
        for k, v in meta.items():
            f.write(f"# {k}: {v}\n")
        w = csv.DictWriter(f, fieldnames=FUNDING_COLS)
        w.writeheader()
        for r in uniq:
            w.writerow({c: r.get(c) for c in FUNDING_COLS})
    return path


def day(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d") if ts else "-"


def run(args):
    coins = universe(args.coins)
    since_ms = int(datetime.strptime(args.since, "%Y-%m-%d")
                   .replace(tzinfo=timezone.utc).timestamp() * 1000)
    venues = [v.strip().lower() for v in args.venues.split(",") if v.strip()]
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for venue in venues:
        fetch = {"binance": fetch_binance, "hyperliquid": fetch_hyperliquid}.get(venue)
        if not fetch:
            print(f"skip unknown venue: {venue}"); continue
        src = BINANCE if venue == "binance" else HL_INFO
        all_rows, report = [], []
        print(f"\n=== {venue} funding backfill ({len(coins)} tickers, since {args.since}) ===")
        for base in coins:
            got = fetch(base, since_ms, args.sleep, args.max_pages)
            for r in got:
                r.update(coin=base, venue=venue, open_interest=None, source=src)
            annualize(got)
            all_rows += got
            mn = min((r["ts"] for r in got if r["ts"]), default=None)
            mx = max((r["ts"] for r in got if r["ts"]), default=None)
            report.append((base, len(got), day(mn), day(mx)))
            print(f"  {base:6s} {len(got):>6} rows  {day(mn)} .. {day(mx)}")
            time.sleep(args.sleep)

        rows_with_data = [r for r in all_rows if r["funding_rate"] is not None]
        if not rows_with_data:
            print(f"  {venue}: no data returned (network? venue listing?)."); continue
        meta = {"venue": venue, "source": src, "fetched_at": fetched_at,
                "caveat": "Historical funding from the source venue (public, free). "
                          "Cross-venue proxy for Coinbase-INTX funding; rates correlate "
                          "but are not identical. ts=epoch seconds UTC. OI not available historically."}
        path = write_csv(f"funding_{venue}.csv", all_rows, meta)
        mn = min(r["ts"] for r in rows_with_data)
        mx = max(r["ts"] for r in rows_with_data)
        covered = sum(1 for _, n, *_ in report if n)
        print(f"  -> saved {len(rows_with_data)} rows -> {os.path.relpath(path, HERE)}")
        print(f"     coverage: {covered}/{len(coins)} tickers, {day(mn)} .. {day(mx)}")

    print("\nNext: python3 import_to_sqlite.py /path/to/your/market.db  "
          "(folds into hl_funding, keyed by ts+coin+venue)")


def main():
    ap = argparse.ArgumentParser(description="Backfill historical funding from public venue APIs.")
    ap.add_argument("--venues", default="binance,hyperliquid", help="comma list")
    ap.add_argument("--since", default="2020-01-01", help="floor date YYYY-MM-DD")
    ap.add_argument("--coins", default=None, help="comma list, else universe.json")
    ap.add_argument("--sleep", type=float, default=0.25, help="seconds between requests")
    ap.add_argument("--max-pages", type=int, default=400, help="page cap per symbol")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
