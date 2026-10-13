#!/usr/bin/env python3
"""Deribit ticker sweep (60s cadence) -> ~/RegimeData/feeds/live_marks.json
and ~/RegimeData/feeds/live_funding.json.

ORDER 103 part 1 (DAEDALUS 2026-10-02T20:40:26Z). "You become the only
process in the estate that calls a venue": the engine's own fetch_perp_mark /
fetch_funding (regime_engine/engine.py, ORDER 100) already call Deribit's
ticker directly, once per cycle, per caller -- a second, independent caller
of the same venue. This script is a NEW path (feeds/, not the engine's
existing live_marks.json/live_funding.json at ~/RegimeData/) so there is
never a second writer on PURPLE's files; PURPLE switches readers in ORDER 102
once this receipts. Nothing here imports or edits an engine file.

SHAPE: exactly PURPLE's documented live_marks.json / live_funding.json shape
(ORDER 100 receipt) --
    marks[pid]   = {price, ts, funding_rate_hr, funding_time}
    live_funding = {rates_hr, median_ann, n, generated_at}
-- plus, per this order's instruction, three extra keys per entry: source,
venue_ts, age_s. Extra keys are additive; a reader doing marks[pid]["price"]
sees no difference. funding_rate_hr here is funding_8h/8, matching the
engine's OWN current definition (engine.py:_deribit_funding_from_ticker).

ORDER 103 part 2 (DAEDALUS 2026-10-02T20:40:26Z): carry BOTH funding
definitions the estate actually has -- the realized hourly print
(`interest_1h`, D5a's semantics, collector.py's perp_snapshots writer) and
the current live estimate (`funding_8h/8`, the engine's own definition) --
named `funding_rate_hr` (realized) / `funding_rate_hr_current` (current
estimate) per the order's own naming.

**WHY THAT EXACT RENAME IS NOT DONE IN PLACE, AND WHAT IS DONE INSTEAD.**
`marks[pid]["funding_rate_hr"]` is not a green field: engine.py:579
(`_funding_from_feeds_entry`, the DEFAULT `perp.mark_source: "feeds"` path,
live since ORDER 102) already reads that exact key, today, expecting the
funding_8h/8 value, to compute every open position's `funding_est_usd` and
the crowd-heat median. This script is re-executed as a fresh subprocess every
60s by run_feed_ticker.sh's loop -- editing this file changes what the LIVE
engine reads within one cycle, with no PURPLE-side code change and no
restart to gate it (unlike ORDER 103 part 1's NEW-file swap, which is why
that one was safe to ship immediately). Renaming the key in place would
silently flip the definition under a live trading computation.

So: `funding_rate_hr` keeps its CURRENT meaning and value (funding_8h/8),
UNCHANGED -- zero behavior change for engine.py:579 today. The order's two
names are both shipped, but as ADDITIVE keys: `funding_rate_hr_current`
duplicates the existing value under the order's own name for the current
estimate, and `funding_rate_hr_realized` (not bare `funding_rate_hr`) carries
the new realized-hourly value. `funding_source` (new, file-level) states all
three definitions verbatim. The literal rename to `funding_rate_hr` happens
only after a PURPLE-side receipt points `_funding_from_feeds_entry` at
`funding_rate_hr_current` -- the same part-1-receipt-gates-part-102 sequencing
this order already uses elsewhere. Flagged for the desk in this part's
receipt; this is my call, named so it can be overruled.

PID AND PRICE SCALE -- deliberately NOT collector.py's convention. Deribit
quotes PEPE/SHIB as "1000PEPE"/"1000SHIB" contracts (base_currency field),
priced per 1000 tokens. collector.py (ORDER 097, perp_snapshots/D5a) kept the
RAW Deribit units under id "1000PEPE-PERP-INTX" -- correct for THAT table,
verified continuous across the cutover (0.0045 before, 0.0042 after, same
order of magnitude, same id). The ENGINE's own convention, already live in
engine.py:_parse_deribit_instruments (ORDER 100), is different on purpose:
it folds "1000PEPE" back to base "PEPE" and divides the price by 1000, so
read_perp_ids()'s "PEPE-PERP-INTX" carries a true per-token price -- because
the engine's positions and stops are denominated in actual PEPE tokens, not
1000-token lots. This file feeds the ENGINE, so it must match the ENGINE's
convention (plain base id, divided price), not collector.py's. Measured
2026-10-02, not assumed: both conventions are internally consistent for
their own table; they disagree with each other by design, and a reader
must never silently assume one perp_snapshots row's units equal one
live_marks.json entry's units for PEPE/SHIB.

FIXTURES FIRST: _mark_entry()/_funding_summary() are pure (no network, no
wall-clock) -- run `python3 feed_ticker.py --selftest` to execute them against
canned Deribit payloads with no network reachable. The live path (main())
feeds them real ticker() results.

Stdlib only. Public Deribit endpoints. No keys. No engine files. No sudo.
"""

import json
import os
import statistics
import sys
import time
import urllib.request

DERIBIT = "https://www.deribit.com/api/v2"
UNIVERSE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe.json")
FEEDS_DIR = os.path.expanduser("~/RegimeData/feeds")
REALIZED_CACHE_PATH = os.path.join(FEEDS_DIR, "_funding_realized_cache.json")

FUNDING_SOURCE_NOTE = (
    "funding_rate_hr=deribit:ticker.funding_8h/8 (current estimate, UNCHANGED "
    "definition -- engine.py:579 reads this key live, ORDER 102 default "
    "mark_source:feeds); funding_rate_hr_current=same value, new name per "
    "ORDER 103 part 2; funding_rate_hr_realized=deribit:"
    "get_funding_rate_history.interest_1h (realized hourly, D5a's semantics, "
    "collector.py's perp_snapshots writer) -- the rename of funding_rate_hr "
    "itself to this realized definition is held for a PURPLE-side receipt, "
    "see module docstring."
)


def _get(url, timeout=10, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "feed-ticker/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception:
            if attempt == retries:
                return None
            time.sleep(0.5 * (attempt + 1))


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _write_json_atomic(path, obj):
    """temp-file-then-os.replace -- the armor every other durable writer in
    this estate already uses (engine.py:2988 write_json_atomic)."""
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _parse_instruments(rows):
    """Pure. public/get_instruments rows -> {BASE: {"name", "divisor"}}.
    Byte-for-byte the same folding rule as engine.py:_parse_deribit_instruments
    (base_currency "1000PEPE" -> base "PEPE", divisor 1000.0) -- this file
    must agree with the engine it feeds, not reinvent the rule."""
    m = {}
    for row in rows:
        if row.get("settlement_period") != "perpetual":
            continue
        bc, name = row.get("base_currency") or "", row.get("instrument_name")
        if not bc or not name:
            continue
        if bc.startswith("1000") and bc[4:].isalpha():
            base, divisor = bc[4:], 1000.0
        else:
            base, divisor = bc, 1.0
        m[base] = {"name": name, "divisor": divisor}
    return m


def _mark_entry(ticker_result, now, divisor=1.0):
    """Pure. A Deribit /public/ticker `result` dict + wall-clock `now` (epoch
    seconds) -> one marks[pid] entry, or None if the print is unusable.
    `divisor` undoes Deribit's 1000x contract quoting (PEPE/SHIB) so price is
    always true per-token, matching the engine's own convention -- see the
    module docstring's PID AND PRICE SCALE note. Mirrors engine.py's
    _deribit_funding_from_ticker (funding_8h/8) for the funding_rate_hr
    value, by design."""
    mark = _num(ticker_result.get("mark_price"))
    vts_ms = ticker_result.get("timestamp")
    f8h = _num(ticker_result.get("funding_8h"))
    if mark is None or vts_ms is None:
        return None
    venue_ts = round(vts_ms / 1000.0, 3)
    entry = {"price": mark / divisor, "ts": int(now), "source": "deribit",
              "venue_ts": venue_ts, "age_s": round(now - venue_ts, 3)}
    if f8h is not None:
        entry["funding_rate_hr"] = f8h / 8.0
        entry["funding_time"] = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(venue_ts))
    return entry


def _funding_summary(marks, now):
    """Pure. marks (pid -> entry, as built by _mark_entry + the realized
    enrichment in main()) -> the live_funding.json body. Base-symbol keyed
    (pid.split('-')[0]), matching server.py:funding_watch_loop exactly.
    median_ann = median(rate*24*365), rounded 5 -- same formula, same
    rounding. Carries BOTH funding definitions (ORDER 103 part 2): the
    existing current-estimate aggregate is UNCHANGED (rates_hr/median_ann/n/
    worst_age_s -- no live consumer of this aggregate file exists yet, per
    ORDER 102 part 3 still being gated, but kept stable anyway for symmetry
    with live_marks.json's own unchanged key); a new realized aggregate is
    additive (rates_hr_realized/median_ann_realized/n_realized/
    worst_age_s_realized -- a DIFFERENT kind of staleness, hourly-print age,
    never compared against the 60s ticker's worst_age_s)."""
    rates, ages, rates_r, ages_r = {}, [], {}, []
    for pid, m in marks.items():
        base = pid.split("-")[0]
        if m.get("funding_rate_hr") is not None:
            rates[base] = m["funding_rate_hr"]
        if m.get("age_s") is not None:
            ages.append(m["age_s"])
        if m.get("funding_rate_hr_realized") is not None:
            rates_r[base] = m["funding_rate_hr_realized"]
        if m.get("funding_rate_hr_realized_age_s") is not None:
            ages_r.append(m["funding_rate_hr_realized_age_s"])
    out = {"generated_at": int(now),
           "rates_hr": rates, "median_ann": None, "n": len(rates),
           "source": "deribit", "worst_age_s": round(max(ages), 3) if ages else None,
           "rates_hr_realized": rates_r, "median_ann_realized": None,
           "n_realized": len(rates_r), "source_realized": "deribit:get_funding_rate_history",
           "worst_age_s_realized": round(max(ages_r), 1) if ages_r else None,
           "funding_source": FUNDING_SOURCE_NOTE}
    if rates:
        out["median_ann"] = round(statistics.median(r * 24 * 365 for r in rates.values()), 5)
    if rates_r:
        out["median_ann_realized"] = round(
            statistics.median(r * 24 * 365 for r in rates_r.values()), 5)
    return out


def _closed_hour_end(now):
    """Pure. The most recent Deribit hourly funding-settlement boundary at or
    before `now` (epoch seconds) -- Deribit's get_funding_rate_history rows
    are timestamped at the END of the hour they realize (collector.py's own
    convention, ORDER 097)."""
    return int(now // 3600) * 3600


def _needs_realized_fetch(cache_entry, closed_hour_end):
    """Pure. True if the cached realized print for this pid does not yet
    cover the current closed hour (or there is no cache) -- bounds Deribit
    calls to ~once/hour/product instead of every 60s tick."""
    return not cache_entry or cache_entry.get("hour_end") != closed_hour_end


def _parse_realized_row(hist_rows, closed_hour_end):
    """Pure. Deribit get_funding_rate_history `result` rows -> interest_1h
    for the row matching closed_hour_end, or None if no row lands within 30
    minutes of it -- demonstrates the failure mode: a history gap (the venue
    returns rows for a DIFFERENT hour, or an empty list) must read as
    UNMEASURED, never as a stale number silently reused or a fabricated 0."""
    if not hist_rows:
        return None
    target_ms = closed_hour_end * 1000
    row = min(hist_rows, key=lambda r: abs(r.get("timestamp", 0) - target_ms))
    if abs(row.get("timestamp", 0) - target_ms) > 1800 * 1000:
        return None
    return _num(row.get("interest_1h"))


def _selftest():
    """Canned payloads, zero network. Demonstrates the failure mode too:
    a ticker missing `timestamp` (seen live from Deribit under load) must
    drop that product, not crash the sweep or fabricate a 0."""
    now = 1790974179.0
    good = {"mark_price": 84368.24, "timestamp": 1790974179995, "funding_8h": 0.00012394}
    m = _mark_entry(good, now)
    assert m["price"] == 84368.24, m
    assert abs(m["funding_rate_hr"] - 0.00012394 / 8) < 1e-12, m
    assert m["source"] == "deribit" and m["age_s"] < 1, m
    print("PASS: _mark_entry, well-formed ticker ->", m)

    broken = {"mark_price": 84368.24, "funding_8h": 0.00012394}  # no timestamp
    assert _mark_entry(broken, now) is None, "a quote with no venue timestamp must be dropped, not faked"
    print("PASS: _mark_entry, missing timestamp -> None (not a fabricated 0)")

    thousandx = _parse_instruments([
        {"settlement_period": "perpetual", "base_currency": "1000PEPE",
         "instrument_name": "1000PEPE_USDC-PERPETUAL"},
        {"settlement_period": "perpetual", "base_currency": "BTC",
         "instrument_name": "BTC_USDC-PERPETUAL"}])
    assert thousandx["PEPE"] == {"name": "1000PEPE_USDC-PERPETUAL", "divisor": 1000.0}, thousandx
    assert thousandx["BTC"]["divisor"] == 1.0, thousandx
    pepe = _mark_entry({"mark_price": 4.18, "timestamp": 1790974179000, "funding_8h": 0.0},
                        now, divisor=thousandx["PEPE"]["divisor"])
    assert abs(pepe["price"] - 0.00418) < 1e-12, pepe  # per-token, not per-1000-lot
    print("PASS: _parse_instruments folds 1000PEPE -> base PEPE, divisor 1000 "
          "-> true per-token price", pepe["price"])

    marks = {"BTC-PERP-INTX": m,
              "ETH-PERP-INTX": _mark_entry(
                  {"mark_price": 2749.0, "timestamp": 1790974180000, "funding_8h": -0.00008},
                  now)}
    fs = _funding_summary(marks, now)
    assert fs["n"] == 2 and set(fs["rates_hr"]) == {"BTC", "ETH"}, fs
    assert fs["source"] == "deribit"
    print("PASS: _funding_summary, 2 products ->", fs)

    empty = _funding_summary({}, now)
    assert empty["n"] == 0 and empty["rates_hr"] == {} and empty["median_ann"] is None, empty
    print("PASS: _funding_summary, no usable rates -> n=0, median_ann=None (UNMEASURED, not 0)")

    # ORDER 103 part 2 -- realized hourly funding (interest_1h), separate
    # from the current-estimate (funding_8h/8) carried above.
    assert _closed_hour_end(1791373335) == 1791370800, _closed_hour_end(1791373335)
    print("PASS: _closed_hour_end floors to the hour boundary")

    assert _needs_realized_fetch(None, 1791372000) is True
    assert _needs_realized_fetch({"hour_end": 1791368400}, 1791372000) is True
    assert _needs_realized_fetch({"hour_end": 1791372000}, 1791372000) is False
    print("PASS: _needs_realized_fetch -- only fetches once per closed hour")

    hist = [{"timestamp": 1791372000000, "interest_1h": 5.7e-06},
            {"timestamp": 1791368400000, "interest_1h": 2.3e-06}]
    assert abs(_parse_realized_row(hist, 1791372000) - 5.7e-06) < 1e-12
    print("PASS: _parse_realized_row picks the row matching the closed hour")

    # Demonstrated failure mode: a history response with NO row anywhere near
    # the closed hour (a real gap, seen when a product briefly delists/
    # relists) must read as UNMEASURED, never as the wrong hour's value
    # silently reused or a fabricated 0.
    gap_hist = [{"timestamp": 1791200000000, "interest_1h": 9.9e-05}]  # >30min away
    assert _parse_realized_row(gap_hist, 1791372000) is None, \
        "a history gap must be None, not the nearest-but-wrong-hour value"
    assert _parse_realized_row([], 1791372000) is None
    assert _parse_realized_row(None, 1791372000) is None
    print("PASS: _parse_realized_row, no row within 30min of the closed hour "
          "-> None (UNMEASURED, not a fabricated or stale value)")

    # _enrich_realized_funding itself is impure (real HTTP) -- not run here,
    # per this file's own fixtures-first convention; its pure pieces
    # (_closed_hour_end, _needs_realized_fetch, _parse_realized_row) are
    # exercised above, and the wrapper is verified live in the receipt.

    print("ALL SELFTESTS PASS")


def _load_realized_cache():
    try:
        with open(REALIZED_CACHE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _enrich_realized_funding(marks, by_base, cache, now):
    """Impure (network + the cache dict passed in is mutated/returned). For
    each pid with a live mark, duplicates the current-estimate value under
    the order's new name (funding_rate_hr_current) and, at most once per
    closed hour per product, fetches Deribit's realized interest_1h and
    caches it -- so a 60s loop does not hammer get_funding_rate_history for
    a value that only changes once an hour. Returns (marks, cache, n_fetched)."""
    closed = _closed_hour_end(now)
    n_fetched = 0
    for pid, entry in marks.items():
        if entry.get("funding_rate_hr") is not None:
            entry["funding_rate_hr_current"] = entry["funding_rate_hr"]
        base = pid.split("-")[0]
        inst = by_base.get(base)
        c = cache.get(pid)
        if inst and _needs_realized_fetch(c, closed):
            hist = (_get(f"{DERIBIT}/public/get_funding_rate_history"
                         f"?instrument_name={inst['name']}"
                         f"&start_timestamp={(closed - 3600) * 1000}"
                         f"&end_timestamp={closed * 1000}") or {}).get("result")
            v = _parse_realized_row(hist, closed)
            n_fetched += 1
            time.sleep(0.2)
            if v is not None:
                c = {"hour_end": closed, "interest_1h": v}
                cache[pid] = c
        if c and c.get("hour_end") == closed and c.get("interest_1h") is not None:
            entry["funding_rate_hr_realized"] = c["interest_1h"]
            entry["funding_rate_hr_realized_time"] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(closed))
            entry["funding_rate_hr_realized_age_s"] = round(now - closed, 1)
        # else: UNMEASURED this cycle -- no key written, never a stale reuse
        # or a fabricated 0 (the cache miss is visible as an absent key).
    return marks, cache, n_fetched


def main():
    with open(UNIVERSE_PATH) as f:
        perps = json.load(f)["perps"]

    instruments = (_get(f"{DERIBIT}/public/get_instruments"
                         "?currency=USDC&kind=future&expired=false") or {}).get("result") or []
    by_base = _parse_instruments(instruments)
    time.sleep(1.0)  # get_instruments is rate-limited to 1 req/s (collector.py precedent)

    now = time.time()
    marks, misses = {}, []
    for sym in perps:  # universe.json perps are already plain bases (PEPE, not 1000PEPE)
        pid = f"{sym}-PERP-INTX"
        inst = by_base.get(sym)
        if not inst:
            misses.append(sym)
            continue
        t = (_get(f"{DERIBIT}/public/ticker?instrument_name={inst['name']}") or {}).get("result")
        if not t:
            misses.append(sym)
            continue
        entry = _mark_entry(t, time.time(), divisor=inst["divisor"])
        if entry:
            marks[pid] = entry
        else:
            misses.append(sym)
        time.sleep(0.15)

    cache = _load_realized_cache()
    marks, cache, n_fetched = _enrich_realized_funding(marks, by_base, cache, time.time())
    os.makedirs(FEEDS_DIR, exist_ok=True)
    _write_json_atomic(REALIZED_CACHE_PATH, cache)

    _write_json_atomic(os.path.join(FEEDS_DIR, "live_marks.json"),
                        {"generated_at": int(now), "marks": marks,
                         "funding_source": FUNDING_SOURCE_NOTE})
    _write_json_atomic(os.path.join(FEEDS_DIR, "live_funding.json"),
                        _funding_summary(marks, time.time()))

    n_realized = sum(1 for m in marks.values() if m.get("funding_rate_hr_realized") is not None)
    print(f"feed_ticker: {len(marks)}/{len(perps)} perps live"
          f"; realized funding {n_realized}/{len(marks)} ({n_fetched} fetched this cycle)"
          + (f"; UNMEASURED (no live print): {','.join(misses)}" if misses else ""))
    if not marks:
        raise SystemExit("all ticker fetches failed")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        main()
