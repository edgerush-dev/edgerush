#!/usr/bin/env python3
"""The table of a soak (bench/run.sh soak): what the proxy held in its first minutes against
its last, and what the load and the uploads came to.

Flat is the pass: a process that serves the same load for half an hour, reloading all the
while, holds no more at the end than at the beginning. Each of what was sampled is compared
as the mean of its first five minutes, after a minute of settling in, against the mean of
its last five, and is called out where it grew by more than a twentieth.

    bench/soak.py RESULTS_DIR
"""

import csv
import json
import pathlib
import sys

WINDOW = 300  # seconds at each end
SETTLING = 60  # seconds before the first window: connections opening, pools filling
GROWTH = 0.05


def mean(values):
    return sum(values) / len(values) if values else 0.0


def load(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def outcome(result):
    codes = result.get("statusCodeDistribution", {})
    total = sum(codes.values())
    bad = sum(count for code, count in codes.items() if not str(code).startswith("2"))
    errors = sum(result.get("errorDistribution", {}).values())
    return total, bad, errors


def main() -> int:
    out = pathlib.Path(sys.argv[1])
    with (out / "soak.csv").open() as samples:
        rows = list(csv.DictReader(samples))
    if not rows:
        print("no samples")
        return 1
    seconds = [int(row["seconds"]) for row in rows]
    last = max(seconds)
    first_rows = [row for row in rows if SETTLING <= int(row["seconds"]) <= SETTLING + WINDOW]
    last_rows = [row for row in rows if int(row["seconds"]) >= max(last - WINDOW, SETTLING)]
    print(f"{len(rows)} samples over {last} s; {WINDOW} s from {SETTLING} s against the last {WINDOW} s")
    print("| held | first | last | growth |")
    print("|---|---:|---:|---:|")
    grew = []
    for column in ("rss_kb", "fds", "client_sockets", "storage_bytes", "exchanges", "idle_upstream"):
        def values(chosen):
            return [float(row[column]) for row in chosen if row.get(column, "").strip() not in ("", None)]
        before, after = mean(values(first_rows)), mean(values(last_rows))
        growth = (after - before) / before if before else 0.0
        mark = ""
        if column in ("rss_kb", "fds", "client_sockets", "storage_bytes") and growth > GROWTH:
            mark = " GROWS"
            grew.append(column)
        print(f"| {column} | {before:,.0f} | {after:,.0f} | {growth:+.1%}{mark} |")
    for name in ("load", "uploads"):
        total, bad, errors = outcome(load(out / f"soak.{name}.json"))
        print(f"{name}: {total:,} answered, {bad:,} not 2xx, {errors:,} errors")
    print("FLAT" if not grew else f"GROWS: {', '.join(grew)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
