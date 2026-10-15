#!/bin/bash
# LONDON PUSH — ORDER 104 part 2 ("pull local, then push out"), as written
# 2026-10-02T20:42:36Z, with the desk's 2026-10-13 amendment: NO new full-file
# copies on the Mac. market.db and funding_intx.db travel once per UTC day,
# using the daily backup files ORDER 115's own cadence fix already makes
# (market.db.bak-liqtransport-*, funding_intx.db.bak-fundingtopup-*) as the
# rsync source — never a fresh `.backup` temp file per push, and never the
# live sqlite file itself.
#
# Three cadences in one wall-clock-paced loop (same pattern as
# run_feed_ticker.sh — KeepAlive + sleep-to-boundary, not StartInterval, for
# the reason recorded there: a one-shot schedule cannot hold a period shorter
# than its own runtime plus the interval):
#   - feeds/          every PERIOD (60s)  — small JSON, "as they are written"
#   - candles_intx/   once per UTC hour   — matches the archive's own topup cadence
#   - market.db + funding_intx.db  once per UTC day — matches 115's backup cadence
#
# Host/path live in scripts/london_push.conf, gitignored — this script is
# committed, the config naming real infra is not.
set -u
TOOLS="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="$TOOLS/logs_london_push.log"
STATE_DIR="$TOOLS/.london_push_state"
PERIOD=60
CONF="$TOOLS/scripts/london_push.conf"
# ORDER 104 part 2 repair, 2026-10-15: ORDER 115's own backups land at 00:10Z
# (market.db) and ~00:21Z (funding) each day. push_dbs used to fire at the
# first loop iteration after 00:00Z, before either existed, so `ls -t | head
# -1` picked up YESTERDAY's backup (still the newest file on disk at that
# moment) and marked the day done — the mirror silently lagged by up to two
# days. Gated below so the leg can't fire before both are certain to exist.
DB_PUSH_AFTER_HHMM="0045"

mkdir -p "$STATE_DIR"
# shellcheck disable=SC1090
source "$CONF"
SSH_OPTS="-o BatchMode=yes -o ConnectTimeout=10"
RSYNC_SSH="ssh $SSH_OPTS"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >> "$LOG"; }

# Runs one rsync, logs "label bytes=<n> seconds=<n.n> rc=<n>". Never throws —
# a push that fails is a log line, not a crashed loop (ConnectTimeout above
# bounds how long a dead link can block the next push).
do_rsync() {
  local label="$1" src="$2" dst="$3"
  local t0 t1 out rc bytes
  t0=$(date +%s)
  out=$(rsync -a --stats -e "$RSYNC_SSH" "$src" "$LONDON_HOST:$dst" 2>&1)
  rc=$?
  t1=$(date +%s)
  # rsync 3.2.7's --stats line is "Total sent: N B" (verified against this
  # host's actual output 2026-10-14 — NOT "Total bytes sent:", the GNU/older
  # wording this originally matched on, which silently parsed to empty and
  # printed a false bytes=0 on every real push. Absence rendered as a value —
  # exactly the failure mode this org has been bitten by before.
  bytes=$(printf '%s\n' "$out" | awk -F'[: ]+' '/^Total sent/{print $3}')
  [ -n "${bytes:-}" ] || bytes="UNMEASURED(stats_line_not_found)"
  log "$label bytes=$bytes seconds=$((t1 - t0)) rc=$rc"
  if [ "$rc" -ne 0 ]; then
    log "$label STDERR: $(printf '%s' "$out" | tail -3 | tr '\n' ' ')"
  fi
  return "$rc"
}

push_feeds() {
  do_rsync "feeds" "$HOME/RegimeData/feeds/" "$LONDON_MIRROR/feeds/"
}

push_archive() {
  local cur_hour marker
  cur_hour=$(date -u +%Y%m%d%H)
  marker="$STATE_DIR/last_archive_hour"
  if [ -f "$marker" ] && [ "$(cat "$marker")" = "$cur_hour" ]; then
    return 0
  fi
  if do_rsync "archive_candles_intx" \
      "$TOOLS/archive/coinbase_intx/candles_intx/" \
      "$LONDON_MIRROR/archive/coinbase_intx/candles_intx/"; then
    echo "$cur_hour" > "$marker"
  fi
}

push_db_file() {
  local label="$1" src="$2" dst="$3"
  local src_name src_size rc remote_size
  src_name=$(basename "$src")
  src_size=$(stat -f%z "$src" 2>/dev/null)
  do_rsync "$label" "$src" "$dst"
  rc=$?
  log "$label source=$src_name source_bytes=${src_size:-UNMEASURED}"
  if [ "$rc" -eq 0 ]; then
    remote_size=$($RSYNC_SSH "$LONDON_HOST" "stat -c%s '$dst'" 2>/dev/null)
    if [ -n "$remote_size" ] && [ "$remote_size" = "$src_size" ]; then
      log "$label VERIFY ok bytes=$remote_size"
    else
      log "$label VERIFY MISMATCH local=${src_size:-UNMEASURED} remote=${remote_size:-UNMEASURED}"
      rc=1
    fi
  fi
  return "$rc"
}

push_dbs() {
  local cur_day now_hm marker
  cur_day=$(date -u +%Y%m%d)
  marker="$STATE_DIR/last_db_day"
  if [ -f "$marker" ] && [ "$(cat "$marker")" = "$cur_day" ]; then
    return 0
  fi
  # Both of today's backups must exist before this leg trusts "newest" to mean
  # "today's" — see DB_PUSH_AFTER_HHMM above. now_hm and the threshold are
  # fixed-width zero-padded 4-digit strings, compared lexicographically with
  # [[ x < y ]], NOT -lt/-le. Verified live (bash 3.2.57, this Mac): `[[
  # "0815" -lt "0900" ]]` and `$(( 0815 ))` both abort with "value too great
  # for base" (0815 isn't valid octal — real HHMM values hit this routinely,
  # any HH or MM of 08/09/18/19/28/.../89); plain `[ -lt ]` happened not to on
  # this build, but nothing here depends on that holding across bash
  # versions. String comparison is correct for same-width zero-padded digits
  # in any base and sidesteps the question entirely.
  now_hm=$(date -u +%H%M)
  if [[ "$now_hm" < "$DB_PUSH_AFTER_HHMM" ]]; then
    return 0
  fi
  local market_bak funding_bak ok=1
  # Sorted by filename (the embedded YYYYMMDD-HHMMSS), not mtime — mtime
  # reflects whatever last touched the file (e.g. this rsync's own earlier
  # attempt, or a restore); the timestamp the backup script itself wrote into
  # the name is the only thing guaranteed to mean "when this backup is of."
  market_bak=$(ls -1 "$HOME/RegimeData"/market.db.bak-liqtransport-* 2>/dev/null | sort | tail -1)
  funding_bak=$(ls -1 "$HOME/RegimeData/funding"/funding_intx.db.bak-fundingtopup-* 2>/dev/null | sort | tail -1)
  if [ -n "$market_bak" ]; then
    push_db_file "market.db" "$market_bak" "$LONDON_MIRROR/market.db" || ok=0
  else
    log "market.db SKIPPED no_backup_file_found"
    ok=0
  fi
  if [ -n "$funding_bak" ]; then
    push_db_file "funding_intx.db" "$funding_bak" "$LONDON_MIRROR/funding/funding_intx.db" || ok=0
  else
    log "funding_intx.db SKIPPED no_backup_file_found"
    ok=0
  fi
  [ "$ok" -eq 1 ] && echo "$cur_day" > "$marker"
}

main_loop() {
  log "START pid=$$"
  while true; do
    push_feeds
    push_archive
    push_dbs
    local now next sleep_s
    now=$(date +%s)
    next=$(( (now / PERIOD + 1) * PERIOD ))
    sleep_s=$(( next - now ))
    [ "$sleep_s" -gt 0 ] && sleep "$sleep_s"
  done
}

if [ "${1:-}" = "--once" ]; then
  push_feeds
  push_archive
  push_dbs
elif [ "${1:-}" = "--selftest" ]; then
  # see scripts/london_push_selftest.sh — demonstrated failure modes live there
  echo "use scripts/london_push_selftest.sh"
  exit 1
else
  main_loop
fi
