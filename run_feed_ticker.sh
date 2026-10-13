#!/bin/bash
# FEED TICKER RUNNER — ORDER 103 part 1. Deribit ticker sweep -> a NEW path,
# ~/RegimeData/feeds/live_marks.json + live_funding.json, so there is never a
# second writer on PURPLE's existing ~/RegimeData/live_marks.json.
#
# CADENCE, and why this is a LOOP and not a one-shot.
# The order says every 60 s. The first build was launchd StartInterval=60 on a
# one-shot script, and it did NOT deliver 60 s. Measured 2026-10-02 21:00-21:12Z
# from this log's own start/end stamps: starts at 21:00:01, 21:01:23, 21:02:45,
# 21:04:06, 21:05:28 — every 82 s. StartInterval does not mean "start every 60 s";
# it restarts its countdown when the job EXITS. The sweep takes 21-23 s (28
# sequential ticker calls at 0.15 s apart plus a rate-limited instruments call),
# so the real period was 60 + 22 = 82 s, 37% slower than specified. Nothing
# errored; the job exited 0 every time. A one-shot schedule cannot hold a period
# shorter than its own runtime plus the interval, so the schedule was the bug.
#
# Fix: launchd keeps ONE long-lived process alive (KeepAlive) and this loop
# paces itself against the WALL CLOCK — it sleeps until the next 60 s boundary,
# so the period stays 60 s whatever the sweep costs. If a sweep ever overruns
# 60 s the loop skips to the next boundary rather than queueing up behind itself,
# and says so in the log. Cadence is then independent of venue latency.
set -u
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$TOOLS/logs_feed_ticker.log"
PERIOD=60
PY=/opt/homebrew/bin/python3
[ -x "$PY" ] || PY=$(command -v python3)

cd "$TOOLS" || { echo "FATAL: cannot cd $TOOLS" >> "$LOG"; exit 1; }

# ---- SINGLE-RUN LOCK ------------------------------------------------------
# feed_ticker.py writes atomically (tmp+rename), so a concurrent run cannot
# tear a file — but it CAN lose an update: two sweeps in flight, the slower one
# renames last and overwrites a fresher snapshot with a staler one. The file
# would look valid and read ~1 sweep old forever. Same lost-update shape as the
# intx_topup race (2026-10-01). mkdir is atomic on macOS.
LOCK="$TOOLS/.feed_ticker.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  OWNER=$(cat "$LOCK/pid" 2>/dev/null || echo "?")
  if [ "$OWNER" != "?" ] && ! kill -0 "$OWNER" 2>/dev/null; then
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) stale lock from dead pid $OWNER — reclaiming ===" >> "$LOG"
    rm -rf "$LOCK"; mkdir "$LOCK" 2>/dev/null || exit 0
  else
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) SKIPPED: ticker already running (pid $OWNER) ===" >> "$LOG"
    exit 0            # a skip is not a failure
  fi
fi
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT INT TERM

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) ticker loop up, period ${PERIOD}s, pid $$ ===" >> "$LOG"

while true; do
  T0=$(date +%s)
  "$PY" feed_ticker.py >> "$LOG" 2>&1
  RC=$?
  T1=$(date +%s)

  # FRESHNESS ASSERTION — the WORST cell, not "ran without error". A sweep that
  # exits 0 having written a stale or short file must not read as healthy.
  "$PY" - >> "$LOG" 2>&1 <<'EOF'
import json, os, time
p = os.path.expanduser("~/RegimeData/feeds")
EXPECT = 29   # ORDER 110 (2026-10-05): PAXG added to universe.json's perps list, 28 -> 29
try:
    m = json.load(open(os.path.join(p, "live_marks.json")))
    f = json.load(open(os.path.join(p, "live_funding.json")))
    now = time.time()
    marks = m.get("marks", {})
    ages = [e["age_s"] for e in marks.values() if e.get("age_s") is not None]
    gen_age = now - m.get("generated_at", 0)
    bad = []
    if len(marks) < EXPECT:
        bad.append(f"{EXPECT - len(marks)} products MISSING")
    if gen_age > 180:
        bad.append(f"file {gen_age:.0f}s stale")
    if ages and max(ages) > 180:
        bad.append(f"venue quote {max(ages):.0f}s stale")
    if not ages:
        bad.append("no product carries an age — UNMEASURED")
    # A negative age is the local clock trailing the venue's, not freshness.
    # Report it as skew; never let it read as "very fresh".
    skew = -min(ages) if ages and min(ages) < 0 else 0.0
    print(f"  marks {len(marks)}/{EXPECT}  file_age {gen_age:.0f}s  "
          f"worst_venue_age {max(ages):.1f}s" if ages else f"  marks {len(marks)}/{EXPECT}")
    print(f"  funding n={f.get('n')} median_ann={f.get('median_ann')} "
          f"worst_age_s={f.get('worst_age_s')}"
          + (f"  clock_skew_behind_venue {skew:.2f}s" if skew > 0.05 else ""))
    print("  FEEDS DEGRADED: " + "; ".join(bad) if bad else "  feeds ok")
except Exception as e:
    print(f"  FEEDS CHECK FAILED: {e}")
EOF

  # keep the log bounded; this process is long-lived
  if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 20000 ]; then
    tail -n 8000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
  fi

  # Pace to the next wall-clock boundary. If the sweep overran the period, say
  # so and skip to the next boundary instead of falling permanently behind.
  NOW=$(date +%s)
  SLEEP=$(( PERIOD - (NOW % PERIOD) ))
  if [ $(( T1 - T0 )) -ge $PERIOD ]; then
    echo "  OVERRUN: sweep took $(( T1 - T0 ))s >= ${PERIOD}s period — skipping to next boundary" >> "$LOG"
  fi
  sleep "$SLEEP"
done
