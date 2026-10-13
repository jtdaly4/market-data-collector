#!/bin/bash
# HEALTH LOOP RUNNER — ORDER 103 part 4. Read-only aggregator, no venue of
# its own, so this one is cheap -- but it still gets the SAME wall-clock-
# paced KeepAlive shape as run_feed_ticker.sh / run_spot_loop.sh for one
# consistent story across the feed layer, and because a bare StartInterval
# on a one-shot script already proved itself wrong once (ORDER 103 part 1's
# measured 82s-vs-60s).
set -u
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$TOOLS/logs_health_loop.log"
PERIOD=60
PY=/opt/homebrew/bin/python3
[ -x "$PY" ] || PY=$(command -v python3)

cd "$TOOLS" || { echo "FATAL: cannot cd $TOOLS" >> "$LOG"; exit 1; }

LOCK="$TOOLS/.health_loop.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  OWNER=$(cat "$LOCK/pid" 2>/dev/null || echo "?")
  if [ "$OWNER" != "?" ] && ! kill -0 "$OWNER" 2>/dev/null; then
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) stale lock from dead pid $OWNER — reclaiming ===" >> "$LOG"
    rm -rf "$LOCK"; mkdir "$LOCK" 2>/dev/null || exit 0
  else
    echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) SKIPPED: health loop already running (pid $OWNER) ===" >> "$LOG"
    exit 0
  fi
fi
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT INT TERM

echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) health loop up, period ${PERIOD}s, pid $$ ===" >> "$LOG"

while true; do
  T0=$(date +%s)
  "$PY" health_loop.py >> "$LOG" 2>&1
  T1=$(date +%s)

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
