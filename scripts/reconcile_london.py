#!/usr/bin/env python3
"""
ORDER 104 part 3 — hourly two-way reconciliation on London, between London's
own locally-continued capture (seeded 2026-10-14 from the Mac's archive, then
extended by London's own watchman-funding-intx-topup / watchman-deribit-
candles-topup timers) and the Mac-origin mirror (pushed hourly/daily by
london_push.sh). Both sides are independent continuations from the same seed
point, so a gap after that point means a missed run on one side, not
conflicting data -- filling is always dedup-safe (funding: INSERT OR IGNORE
against the table's own (product_id, ts) unique index; candles: append rows
keyed on ts, same convention collector.py / deribit_candles_topup.py use)
and never overwrites an existing row on either side.

Full ts-SET diff per product/file, not a MAX(ts) comparison. A first draft
compared only the latest timestamp on each side; a synthetic 3-row repro
(live={100,200}, mirror={100,150}) showed that design silently skips an
INTERIOR hole once one side's own max passes the other's -- mirror's max
(150) never exceeds live's max (200), so live's hole at the mirror-only
150 would be found, but a mirror-only row that falls BEHIND the already-
reconciled max is invisible to a max-only check. This is the same failure
shape the standing laws warn about generally (absence judged by a summary
statistic instead of measured directly) -- rewritten to diff actual ts sets
before this ever ran unattended.

Run with --dry-run first. Writes reconcile.json to BOTH ends -- LIVE_ROOT
and MIRROR_ROOT -- same content, so either side's own monitoring can read
what happened without needing the other path.
"""
import argparse
import csv
import json
import os
import sqlite3
import time

LIVE_FUNDING_DB = os.environ.get(
    "LIVE_FUNDING_DB", os.path.expanduser("~/RegimeData/funding/funding_intx.db"))
MIRROR_FUNDING_DB = os.environ.get(
    "MIRROR_FUNDING_DB", "/opt/watchman/mirror/funding/funding_intx.db")
LIVE_CANDLES_DIR = os.environ.get(
    "LIVE_CANDLES_DIR",
    "/opt/watchman/market-data-collector/archive/coinbase_intx/candles_intx")
MIRROR_CANDLES_DIR = os.environ.get(
    "MIRROR_CANDLES_DIR", "/opt/watchman/mirror/archive/coinbase_intx/candles_intx")
LIVE_ROOT = os.environ.get("LIVE_ROOT", "/opt/watchman")
MIRROR_ROOT = os.environ.get("MIRROR_ROOT", "/opt/watchman/mirror")

FUNDING_COLS = ["product_id", "instrument_id", "ts", "ts_iso",
                "funding_rate", "funding_annualized", "mark_price", "venue"]


def reconcile_funding(dry_run):
    report = {}
    if not (os.path.exists(LIVE_FUNDING_DB) and os.path.exists(MIRROR_FUNDING_DB)):
        return {"_error": "UNMEASURED(one_or_both_db_missing)"}
    con_live = sqlite3.connect(LIVE_FUNDING_DB)
    con_mir = sqlite3.connect(MIRROR_FUNDING_DB)
    try:
        products = sorted({r[0] for r in con_live.execute(
            "SELECT DISTINCT product_id FROM funding_intx")} |
            {r[0] for r in con_mir.execute("SELECT DISTINCT product_id FROM funding_intx")})
        cols_sql = ",".join(FUNDING_COLS)
        placeholders = ",".join("?" for _ in FUNDING_COLS)
        for p in products:
            live_ts = {r[0] for r in con_live.execute(
                "SELECT ts FROM funding_intx WHERE product_id=?", (p,))}
            mir_ts = {r[0] for r in con_mir.execute(
                "SELECT ts FROM funding_intx WHERE product_id=?", (p,))}
            missing_on_live = mir_ts - live_ts
            missing_on_mirror = live_ts - mir_ts
            filled_to_live = filled_to_mirror = 0
            if missing_on_live:
                rows = con_mir.execute(
                    f"SELECT {cols_sql} FROM funding_intx WHERE product_id=? "
                    f"AND ts IN ({','.join('?' for _ in missing_on_live)})",
                    (p, *missing_on_live)).fetchall()
                if rows and not dry_run:
                    con_live.executemany(
                        f"INSERT OR IGNORE INTO funding_intx ({cols_sql}) VALUES ({placeholders})",
                        rows)
                filled_to_live = len(rows)
            if missing_on_mirror:
                rows = con_live.execute(
                    f"SELECT {cols_sql} FROM funding_intx WHERE product_id=? "
                    f"AND ts IN ({','.join('?' for _ in missing_on_mirror)})",
                    (p, *missing_on_mirror)).fetchall()
                if rows and not dry_run:
                    con_mir.executemany(
                        f"INSERT OR IGNORE INTO funding_intx ({cols_sql}) VALUES ({placeholders})",
                        rows)
                filled_to_mirror = len(rows)
            if filled_to_live or filled_to_mirror:
                report[p] = {
                    "live_rows_before": len(live_ts), "mirror_rows_before": len(mir_ts),
                    "filled_to_live": filled_to_live, "filled_to_mirror": filled_to_mirror,
                }
        if not dry_run:
            con_live.commit()
            con_mir.commit()
    finally:
        con_live.close()
        con_mir.close()
    return report


def _read_csv_rows(path):
    """{ts: row} for every row in the file, plus the fieldnames list."""
    with open(path, newline="") as f:
        r = csv.reader(f)
        header = next(r, [])
        by_ts = {}
        for row in r:
            if row:
                by_ts[row[1]] = row
    return header, by_ts


def reconcile_candles(dry_run):
    report = {}
    if not (os.path.isdir(LIVE_CANDLES_DIR) and os.path.isdir(MIRROR_CANDLES_DIR)):
        return {"_error": "UNMEASURED(one_or_both_dir_missing)"}
    live_files = {f for f in os.listdir(LIVE_CANDLES_DIR) if f.endswith(".csv")}
    mir_files = {f for f in os.listdir(MIRROR_CANDLES_DIR) if f.endswith(".csv")}
    for fname in sorted(live_files & mir_files):
        live_path = os.path.join(LIVE_CANDLES_DIR, fname)
        mir_path = os.path.join(MIRROR_CANDLES_DIR, fname)
        live_header, live_by_ts = _read_csv_rows(live_path)
        mir_header, mir_by_ts = _read_csv_rows(mir_path)
        missing_on_live = set(mir_by_ts) - set(live_by_ts)
        missing_on_mirror = set(live_by_ts) - set(mir_by_ts)
        filled_to_live = filled_to_mirror = 0
        if missing_on_live:
            filled_to_live = len(missing_on_live)
            if not dry_run:
                with open(live_path, "a", newline="") as f:
                    w = csv.writer(f)
                    for ts in sorted(missing_on_live):
                        w.writerow(mir_by_ts[ts])
        if missing_on_mirror:
            filled_to_mirror = len(missing_on_mirror)
            if not dry_run:
                with open(mir_path, "a", newline="") as f:
                    w = csv.writer(f)
                    for ts in sorted(missing_on_mirror):
                        w.writerow(live_by_ts[ts])
        if filled_to_live or filled_to_mirror:
            report[fname] = {
                "live_rows_before": len(live_by_ts), "mirror_rows_before": len(mir_by_ts),
                "filled_to_live": filled_to_live, "filled_to_mirror": filled_to_mirror,
            }
    only_live = live_files - mir_files
    only_mirror = mir_files - live_files
    if only_live:
        report["_files_only_on_live"] = sorted(only_live)
    if only_mirror:
        report["_files_only_on_mirror"] = sorted(only_mirror)
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    funding_report = reconcile_funding(args.dry_run)
    candles_report = reconcile_candles(args.dry_run)
    finished = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    out = {
        "run_started": started,
        "run_finished": finished,
        "dry_run": args.dry_run,
        "funding": funding_report,
        "candles": candles_report,
    }
    for root in (LIVE_ROOT, MIRROR_ROOT):
        try:
            os.makedirs(root, exist_ok=True)
            with open(os.path.join(root, "reconcile.json"), "w") as f:
                json.dump(out, f, indent=1)
        except OSError as e:
            print(f"WARN could not write reconcile.json under {root}: {e}")

    n_funding_filled = sum(
        v.get("filled_to_live", 0) + v.get("filled_to_mirror", 0)
        for k, v in funding_report.items() if not k.startswith("_"))
    n_candles_filled = sum(
        v.get("filled_to_live", 0) + v.get("filled_to_mirror", 0)
        for k, v in candles_report.items() if not k.startswith("_"))
    print(f"reconcile: funding rows filled={n_funding_filled} "
          f"({len([k for k in funding_report if not k.startswith('_')])} products touched), "
          f"candle rows filled={n_candles_filled} "
          f"({len([k for k in candles_report if not k.startswith('_')])} files touched)"
          f"{' [DRY RUN]' if args.dry_run else ''}")


if __name__ == "__main__":
    main()
