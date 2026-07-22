#!/usr/bin/env python3
"""
grab_moondev_history.py — one-shot historical pull + probe of the Moon Dev
Hyperliquid Data Layer API. RUN THIS ON YOUR MAC (needs outbound network to
api.moondev.com and your API key). It is NOT part of the hourly Actions job.

What it does
------------
1. PROBES every liquidation- and funding-related endpoint we know of, and
   empirically tests HOW FAR BACK each one actually serves data — trying
   pagination (limit/offset, page, cursor), date-range params (several naming
   conventions), and per-coin iteration. It does NOT trust the "no bulk
   history" tier note; it measures what your key actually returns.
2. SAVES whatever history it successfully pulls to data/hyperliquid/*.csv,
   each file carrying a commented header block (venue, source endpoint,
   caveat, tier, fetched_at) above the column row.
3. REPORTS, at the end, per endpoint: reachable? gated? row count, and the
   actual min/max date range obtained — plus a plain-English verdict. If the
   honest answer is "live only, no history," it says so.

Usage
-----
    export MOONDEV_API_KEY=moonstream_xxxx     # or put it in a .env file
    python3 grab_moondev_history.py                      # sensible defaults
    python3 grab_moondev_history.py --since 2024-01-01   # floor for backward walk
    python3 grab_moondev_history.py --max-requests 500   # politeness cap
    python3 grab_moondev_history.py --commit             # git add+commit results

Stdlib only — no pip install needed. Auth is a query param (?api_key=...),
per Moon Dev's own examples. The key is never written to any output file.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE_URL = "https://api.moondev.com"
HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "data", "hyperliquid")

# Coins to try for per-coin endpoints (aligned to the repo universe when present)
def _universe_coins() -> list[str]:
    try:
        with open(os.path.join(HERE, "universe.json")) as f:
            u = json.load(f)
        return list(dict.fromkeys(u.get("perps", []) + u.get("spot", [])))
    except Exception:
        return ["BTC", "ETH", "SOL", "XRP", "DOGE"]


# ---------------------------------------------------------------------------
# key loading + HTTP
# ---------------------------------------------------------------------------
def load_key() -> str:
    key = os.environ.get("MOONDEV_API_KEY")
    if not key:
        env = os.path.join(HERE, ".env")
        if os.path.exists(env):
            with open(env) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("MOONDEV_API_KEY="):
                        key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break
    if not key:
        sys.exit("ERROR: set MOONDEV_API_KEY in the environment or a .env file.")
    return key


class Http:
    """Tiny GET client. Returns (status, parsed_or_text). Never raises on HTTP."""

    def __init__(self, key: str, timeout: int = 40):
        self.key = key
        self.timeout = timeout
        self.calls = 0

    def get(self, path: str, params: dict | None = None):
        p = {"api_key": self.key}
        if params:
            p.update(params)
        url = f"{BASE_URL}{path}?{urllib.parse.urlencode(p)}"
        self.calls += 1
        req = urllib.request.Request(url, headers={"User-Agent": "moondev-grab/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                body = r.read()
                try:
                    return r.status, json.loads(body)
                except Exception:
                    return r.status, body.decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            try:
                txt = e.read().decode("utf-8", "replace")
            except Exception:
                txt = str(e)
            return e.code, txt
        except Exception as e:
            return 0, f"{type(e).__name__}: {e}"


def is_gated(status: int, body) -> bool:
    if status in (401, 402, 403):
        return True
    if isinstance(body, str) and any(w in body.lower()
                                     for w in ("quant elite", "_qe", "upgrade", "forbidden")):
        return True
    return False


# ---------------------------------------------------------------------------
# record normalization — the API's exact shapes are unknown, so probe defensively
# ---------------------------------------------------------------------------
TS_KEYS = ("ts", "time", "timestamp", "t", "created_at", "createdAt",
           "datetime", "date", "start", "startTime", "start_time", "T")
COIN_KEYS = ("coin", "symbol", "name", "ticker", "asset", "product", "s")
FUND_KEYS = ("funding_rate", "fundingRate", "funding", "rate", "rate_pct", "predicted_funding")
ANN_KEYS = ("annualized", "annual", "apr", "funding_annualized")
MARK_KEYS = ("mark_price", "markPrice", "mark", "price", "mid", "mid_market_price")
OI_KEYS = ("open_interest", "openInterest", "oi", "oi_value", "oiValue")
SIDE_KEYS = ("side", "direction", "type", "is_long", "long_short")
NOTIONAL_KEYS = ("notional_usd", "notional", "usd", "value", "amount", "sz", "size", "qty")
COUNT_KEYS = ("count", "n", "num", "trades")


def _first(d: dict, keys) -> object:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _epoch_seconds(v) -> int | None:
    """Coerce a timestamp-ish value to Unix SECONDS (UTC)."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        n = float(v)
        if n > 1e17:   # nanoseconds
            n /= 1e9
        elif n > 1e14:  # microseconds
            n /= 1e6
        elif n > 1e11:  # milliseconds
            n /= 1e3
        return int(n)
    s = str(v).strip()
    if s.isdigit():
        return _epoch_seconds(int(s))
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None


def find_records(obj) -> list[dict]:
    """Pull the most plausible list-of-dicts out of an arbitrary JSON payload."""
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        # prefer known container keys, then any list-of-dicts value
        for k in ("data", "liquidations", "rates", "all_rates", "history",
                  "history_24h", "rows", "results", "items", "records", "coins"):
            v = obj.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        best = []
        for v in obj.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and len(v) > len(best):
                best = v
        if best:
            return best
        # dict keyed by symbol -> row
        rows = []
        for sym, v in obj.items():
            if isinstance(v, dict):
                v = dict(v)
                v.setdefault("coin", sym)
                rows.append(v)
        return rows
    return []


def ts_range(records: list[dict]) -> tuple[int | None, int | None]:
    tss = [t for t in (_epoch_seconds(_first(r, TS_KEYS)) for r in records) if t]
    return (min(tss), max(tss)) if tss else (None, None)


def norm_funding(r: dict, source: str, fallback_ts: int | None = None) -> dict:
    return {
        "ts": _epoch_seconds(_first(r, TS_KEYS)) or fallback_ts,
        "coin": (str(_first(r, COIN_KEYS)).upper() if _first(r, COIN_KEYS) else None),
        "venue": "hyperliquid",
        "funding_rate": _num(_first(r, FUND_KEYS)),
        "funding_annualized": _num(_first(r, ANN_KEYS)),
        "mark_price": _num(_first(r, MARK_KEYS)),
        "open_interest": _num(_first(r, OI_KEYS)),
        "source": source,
    }


def norm_liq(r: dict, source: str, venue_default: str = "", fallback_ts: int | None = None) -> dict:
    return {
        "ts": _epoch_seconds(_first(r, TS_KEYS)) or fallback_ts,
        "venue": (str(_first(r, ("venue", "exchange", "ex")) or venue_default)).lower(),
        "coin": (str(_first(r, COIN_KEYS)).upper() if _first(r, COIN_KEYS) else None),
        "side": (str(_first(r, SIDE_KEYS)).lower() if _first(r, SIDE_KEYS) else None),
        "window": str(r.get("window") or r.get("interval") or ""),
        "notional_usd": _num(_first(r, NOTIONAL_KEYS)),
        "price": _num(_first(r, ("price", "px", "avg_price", "mark_price"))),
        "count": _num(_first(r, COUNT_KEYS)),
        "source": source,
    }


FUNDING_COLS = ["ts", "coin", "venue", "funding_rate", "funding_annualized",
                "mark_price", "open_interest", "source"]
LIQ_COLS = ["ts", "venue", "coin", "side", "window", "notional_usd",
            "price", "count", "source"]


def write_csv(fname: str, cols: list[str], rows: list[dict], meta: dict) -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, fname)
    # dedupe on the natural key (all non-source cols) to keep re-runs clean
    seen, uniq = set(), []
    for r in rows:
        key = tuple(r.get(c) for c in cols if c != "source")
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    with open(path, "w", newline="") as f:
        for k, v in meta.items():
            f.write(f"# {k}: {v}\n")
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in uniq:
            w.writerow({c: r.get(c) for c in cols})
    return path


# ---------------------------------------------------------------------------
# probing: pagination + date-range walk
# ---------------------------------------------------------------------------
def paginate(http: Http, path: str, base_params: dict, max_pages: int) -> tuple[list[dict], str]:
    """Follow limit/offset style pagination if the payload exposes it."""
    out, note, offset = [], "", 0
    for page in range(max_pages):
        params = dict(base_params)
        params.update({"limit": base_params.get("limit", 1000), "offset": offset})
        status, body = http.get(path, params)
        if status != 200:
            note = f"stopped: HTTP {status}"
            break
        recs = find_records(body)
        if not recs:
            break
        out += recs
        has_more = isinstance(body, dict) and body.get("has_more")
        if not has_more:
            note = "paginated" if page else "single page"
            break
        offset = (body.get("next_offset") if isinstance(body, dict) else None) or (offset + len(recs))
        note = "paginated"
    return out, note


# Date-param naming conventions to try, each as (older_kwargs_builder)
def _date_variants(start_ts: int, end_ts: int) -> list[dict]:
    s_iso = datetime.fromtimestamp(start_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    e_iso = datetime.fromtimestamp(end_ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return [
        {"start": s_iso, "end": e_iso},
        {"start_time": start_ts * 1000, "end_time": end_ts * 1000},
        {"startTime": start_ts * 1000, "endTime": end_ts * 1000},
        {"start": start_ts, "end": end_ts},
        {"since": s_iso},
        {"from": s_iso, "to": e_iso},
    ]


def date_param_works(http: Http, path: str, base_min: int | None) -> dict | None:
    """Return the date-param dict that reaches OLDER than the base call, or None."""
    if base_min is None:
        base_min = int(time.time())
    probe_start = base_min - 30 * 86400  # ask for 30 days before earliest seen
    for variant in _date_variants(probe_start, base_min):
        status, body = http.get(path, variant)
        if status != 200:
            continue
        mn, _ = ts_range(find_records(body))
        if mn and mn < base_min - 3600:   # got meaningfully older data
            return variant
    return None


# ---------------------------------------------------------------------------
# endpoint catalog
# ---------------------------------------------------------------------------
FUNDING_ENDPOINTS = [
    {"name": "prices (live funding+OI)", "path": "/api/prices", "kind": "funding"},
    {"name": "hip3 funding", "path": "/api/hip3_funding", "kind": "funding"},
    # candidate history paths — probed blindly; 404s are reported, not fatal
    {"name": "funding_history?", "path": "/api/funding_history", "kind": "funding"},
    {"name": "funding/history?", "path": "/api/funding/history", "kind": "funding"},
    {"name": "hl funding history?", "path": "/api/hl/funding_history", "kind": "funding"},
]

LIQ_ENDPOINTS = [
    {"name": "liq totals", "path": "/api/all_liquidations/totals.json", "kind": "liq"},
    {"name": "liq 24h", "path": "/api/all_liquidations/24h.json", "kind": "liq"},
    {"name": "liq 1h", "path": "/api/all_liquidations/1h.json", "kind": "liq"},
    {"name": "liq 10m", "path": "/api/all_liquidations/10m.json", "kind": "liq"},
    {"name": "liquidations (heatmap/top)", "path": "/api/liquidations", "kind": "liq"},
    {"name": "hip3 liquidations", "path": "/api/hip3_liquidations", "kind": "liq"},
    {"name": "binance liq coverage", "path": "/api/binance_liquidations/coverage.json", "kind": "coverage"},
    {"name": "bulk binance liqs [_qe?]", "path": "/api/bulk/binance_liquidations", "kind": "liq_bulk"},
]


def fmt_day(ts: int | None) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d") if ts else "-"


def run(http: Http, args) -> list[dict]:
    report = []
    funding_rows: list[dict] = []
    liq_rows: list[dict] = []
    now = int(time.time())
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tier = "quant_elite" if http.key.endswith("_qe") else "standard"
    floor_ts = 0
    if args.since:
        floor_ts = int(datetime.strptime(args.since, "%Y-%m-%d")
                       .replace(tzinfo=timezone.utc).timestamp())

    for ep in FUNDING_ENDPOINTS + LIQ_ENDPOINTS:
        if args.max_requests and http.calls >= args.max_requests:
            report.append({**ep, "status": "-", "rows": 0, "range": "-",
                           "history": "skipped", "note": "hit --max-requests"})
            continue
        status, body = http.get(ep["path"])
        entry = {"name": ep["name"], "path": ep["path"], "status": status,
                 "rows": 0, "range": "-", "history": "?", "note": ""}

        if is_gated(status, body):
            entry.update(history="no (gated)", note="tier-gated (likely needs _qe)")
            report.append(entry); continue
        if status == 404:
            entry.update(history="n/a", note="endpoint not found"); report.append(entry); continue
        if status != 200:
            entry.update(history="error", note=str(body)[:70]); report.append(entry); continue

        recs = find_records(body)
        # coverage endpoint: just surface the advertised range
        if ep["kind"] == "coverage":
            entry.update(rows=len(recs), history="manifest",
                         note=json.dumps(body)[:120] if not recs else f"{len(recs)} entries")
            report.append(entry); continue

        # try to reach further back: pagination, then date params
        all_recs = list(recs)
        base_min, base_max = ts_range(recs)
        pag, pnote = ([], "")
        if isinstance(body, dict) and (body.get("has_more") or "data" in body):
            pag, pnote = paginate(http, ep["path"], {}, args.max_pages)
            all_recs += pag
        dparam = None
        if not args.no_date_probe and ep["kind"] in ("funding", "liq", "liq_bulk"):
            dparam = date_param_works(http, ep["path"], base_min)
            if dparam:
                # walk backward in ~7-day windows until empty / floor / cap
                walked = _walk_back(http, ep["path"], dparam, base_min or int(time.time()),
                                    floor_ts, args)
                all_recs += walked

        mn, mx = ts_range(all_recs)
        entry["rows"] = len(all_recs)
        entry["range"] = f"{fmt_day(mn)} .. {fmt_day(mx)}"
        span_days = ((mx - mn) / 86400) if (mn and mx) else 0
        if dparam:
            entry["history"] = f"YES (~{span_days:.0f}d, date param)"
            entry["note"] = f"date param: {list(dparam)[0]}…"
        elif pnote == "paginated":
            entry["history"] = f"YES (~{span_days:.0f}d, paginated)"
        elif span_days > 1.5:
            entry["history"] = f"window (~{span_days:.1f}d)"
        else:
            entry["history"] = "live/snapshot only"
        report.append(entry)

        # snapshot endpoints carry no per-row ts -> stamp fetch time (point-in-time)
        snap_ts = now if entry["history"] in ("live/snapshot only",) else None
        if ep["kind"] == "funding":
            funding_rows += [norm_funding(r, ep["path"], snap_ts) for r in all_recs]
        elif ep["kind"] in ("liq", "liq_bulk"):
            vd = "binance" if "binance" in ep["path"] else ""
            liq_rows += [norm_liq(r, ep["path"], vd, snap_ts) for r in all_recs]

    # ---- write outputs ----
    meta_common = {"venue": "hyperliquid (Moon Dev Data Layer)", "fetched_at": fetched_at,
                   "tier": tier, "api": BASE_URL,
                   "caveat": "Coverage limited by API tier. Funding/OI/liquidation history "
                             "is NOT backfillable elsewhere for Coinbase-INTX; treat gaps as permanent."}
    written = []
    funding_rows = [r for r in funding_rows if r["ts"] and r["coin"]]
    liq_rows = [r for r in liq_rows if r["ts"]]
    if funding_rows:
        p = write_csv("funding_history.csv", FUNDING_COLS, funding_rows,
                      {**meta_common, "source": "funding endpoints (see 'source' column)"})
        written.append((p, len(funding_rows)))
    if liq_rows:
        p = write_csv("liquidations_history.csv", LIQ_COLS, liq_rows,
                      {**meta_common, "source": "liquidation endpoints (see 'source' column)"})
        written.append((p, len(liq_rows)))

    _print_report(report, written, tier, fetched_at, http.calls)
    _save_report_json(report, written, tier, fetched_at)
    return report


def _walk_back(http, path, dparam_template, end_ts, floor_ts, args) -> list[dict]:
    """Given a working date-param style, page backward in 7-day windows."""
    out, cur = [], end_ts
    win = 7 * 86400
    key_start = next(iter(dparam_template))  # the 'start'-like key
    for _ in range(args.max_windows):
        if args.max_requests and http.calls >= args.max_requests:
            break
        start = cur - win
        if floor_ts and start < floor_ts:
            start = floor_ts
        variant = _date_variants(start, cur)
        # reuse the same NAMING that worked (match by its first key)
        chosen = next((v for v in variant if next(iter(v)) == key_start), variant[0])
        status, body = http.get(path, chosen)
        if status != 200:
            break
        recs = find_records(body)
        if not recs:
            break
        out += recs
        mn, _ = ts_range(recs)
        if not mn or mn >= cur:      # no backward progress
            break
        cur = mn - 1
        if floor_ts and cur <= floor_ts:
            break
        time.sleep(args.sleep)
    return out


def _print_report(report, written, tier, fetched_at, calls):
    print("\n" + "=" * 78)
    print("MOON DEV HISTORY GRAB — REPORT")
    print("=" * 78)
    print(f"tier (by key suffix): {tier}    fetched_at: {fetched_at}    api calls: {calls}\n")
    print(f"{'endpoint':30s} {'HTTP':>4} {'rows':>7}  {'range (UTC)':21s} {'history?':16s} note")
    print("-" * 110)
    for e in report:
        print(f"{e['name'][:30]:30s} {str(e['status']):>4} {e['rows']:>7}  "
              f"{e['range']:21s} {e['history'][:16]:16s} {e['note'][:34]}")
    print("-" * 110)

    fund_hist = any("YES" in e["history"] for e in report
                    if e["path"] in ("/api/prices", "/api/hip3_funding") or "funding" in e["path"])
    liq_hist = any("YES" in e["history"] for e in report if "liquidation" in e["path"])
    print("\nVERDICT")
    if written:
        for p, n in written:
            print(f"  saved {n:>6} rows -> {os.path.relpath(p, HERE)}")
    else:
        print("  no rows saved.")
    if not fund_hist:
        print("  FUNDING: no deep history at this tier — LIVE SNAPSHOT ONLY. "
              "Forward-collection (the Worker) is the only way to build it.")
    else:
        print("  FUNDING: some history obtainable — see rows above.")
    if not liq_hist:
        print("  LIQUIDATIONS: only rolling windows / live — no deep history at this tier.")
    else:
        print("  LIQUIDATIONS: some history obtainable — see rows above.")
    print("\nNext: `python3 import_to_sqlite.py /path/to/market.db` folds these into "
          "hl_funding / liquidations tables.\n")


def _save_report_json(report, written, tier, fetched_at):
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "_grab_report.json"), "w") as f:
        json.dump({"fetched_at": fetched_at, "tier": tier,
                   "written": [{"file": os.path.relpath(p, HERE), "rows": n} for p, n in written],
                   "endpoints": report}, f, indent=2)


def main():
    ap = argparse.ArgumentParser(description="One-shot Moon Dev history grab + probe.")
    ap.add_argument("--since", default=None, help="floor date YYYY-MM-DD for backward walk")
    ap.add_argument("--sleep", type=float, default=0.4, help="seconds between paged requests")
    ap.add_argument("--max-requests", type=int, default=800, help="global API call cap (0=unlimited)")
    ap.add_argument("--max-pages", type=int, default=200, help="pagination page cap per endpoint")
    ap.add_argument("--max-windows", type=int, default=200, help="date-walk window cap per endpoint")
    ap.add_argument("--no-date-probe", action="store_true", help="skip date-range probing")
    ap.add_argument("--commit", action="store_true", help="git add+commit the results")
    args = ap.parse_args()

    http = Http(load_key())
    print(f"probing {BASE_URL} …  (key tier: {'quant_elite' if http.key.endswith('_qe') else 'standard'})")
    run(http, args)

    if args.commit:
        os.system(f'cd "{HERE}" && git add data/hyperliquid && '
                  'git commit -m "grab: moondev historical liquidations + funding" || true')


if __name__ == "__main__":
    main()
