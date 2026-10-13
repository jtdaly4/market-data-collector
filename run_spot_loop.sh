#!/bin/bash
# SPOT LOOP RUNNER — ORDER 103 part 3. Coinbase Exchange hourly-candle sweep
# -> ~/RegimeData/market.db candles (granularity 3600) + feeds/spot_health.json.
#
# SAME wall-clock-paced KeepAlive-loop shape as run_feed_ticker.sh (ORDER 103
# part 1) -- that script's own postmortem is why this is not a bare launchd
# StartInterval on a one-shot script: a one-shot's period is its OWN runtime
# PLUS the interval, not the interval alone (measured 82s against a
# specified 60s there). Pacing against the wall clock inside a long-lived
# loop keeps the period correct (300s) regardless of sweep duration.
set -u
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$TOOLS/logs_spot_loop.log"
PERIOD=300
PY=/opt/homebrew/bin/python3
[ -x "$PY" ] || PY=$(command -v python3)

cd "$TOOLS" || { echo "FATAL: cannot cd $TOOLS" >> "$LOG"; exit 1; }

# ---- SINGLE-RUN LOCK -------------------------------------------------------
# spot_loop.py's writes are per-row INSERT OR REPLACE inside one transaction,
# not a whole-file atomic rename -- two overlapping sweeps could interleave
# writes. mkdir is atomic on macOS (same armor as run_feed_ticker.sh).
LOCK="$TOOLS/.spot_loop.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  OWNER=$(cat "$LOCK/pid" 2>/dev/null || echo "?")
  if [ "$OWNER" != "?" ] && ! kill -0 "$OWNER" 2>/dev/null; then
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) stale lock from dead pid $OWNER — reclaiming ===" >> "$LOG"
    rm -rf "$LOCK"; mkdir "$LOCK" 2>/dev/null || exit 0
  else
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) SKIPPED: spot loop already running (pid $OWNER) ===" >> "$LOG"
    exit 0
  fi
fi
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK"; exit 0' EXIT INT TERM

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) spot loop up, period ${PERIOD}s, pid $$ ===" >> "$LOG"

while true; do
  T0=$(date +%s)
  "$PY" spot_loop.py >> "$LOG" 2>&1
  T1=$(date +%s)

  # FRESHNESS ASSERTION, same shape as run_feed_ticker.sh's -- the worst
  # cell, not "ran without error".
  "$PY" - >> "$LOG" 2>&1 <<'EOF'
import json, os, time
p = os.path.expanduser("~/RegimeData/feeds/spot_health.json")
EXPECT = 28
try:
    h = json.load(open(p))
    now = time.time()
    file_age = now - h.get("generated_at", 0)
    worst = h.get("worst_age_s")
    bad = []
    if h.get("n", 0) < EXPECT:
        bad.append(f"{EXPECT - h.get('n', 0)} assets MISSING")
    if file_age > 900:
        bad.append(f"file {file_age:.0f}s stale")
    if worst is not None and worst > 3900:  # >65min: a closed hour should land within ~5-10min
        bad.append(f"worst candle {worst:.0f}s stale")
    if worst is None:
        bad.append("no asset carries an age — UNMEASURED")
    print(f"  spot {h.get('n')}/{EXPECT}  file_age {file_age:.0f}s  worst_candle_age {worst}")
    print("  SPOT DEGRADED: " + "; ".join(bad) if bad else "  spot ok")
except Exception as e:
    print(f"  SPOT CHECK FAILED: {e}")
EOF

  if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 20000 ]; then
    tail -n 8000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
  fi

  NOW=$(date +%s)
  SLEEP=$(( PERIOD - (NOW % PERIOD) ))
  if [ $(( T1 - T0 )) -ge $PERIOD ]; then
    echo "  OVERRUN: sweep took $(( T1 - T0 ))s >= ${PERIOD}s period — skipping to next boundary" >> "$LOG"
  fi
  sleep "$SLEEP"
done
