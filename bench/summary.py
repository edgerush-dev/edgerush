#!/usr/bin/env python3
"""The table of a run of bench/run.sh: a line for every scenario and variant, the median of
the repetitions with their range, what a request cost the proxy in CPU time and how its
threads shared the work."""

import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

TICKS_PER_SECOND = 100  # of /proc/<pid>/task/<tid>/stat, on every Linux we run on


def h2load(text):
    """Closed loop: what came back, and how many of them were not a 2xx."""
    rate = re.search(r"finished in [\d.]+\w+, ([\d.]+) req/s", text)
    codes = re.search(r"status codes: (\d+) 2xx, (\d+) 3xx, (\d+) 4xx, (\d+) 5xx", text)
    done = re.search(r"requests: (\d+) total.* (\d+) failed, (\d+) errored", text)
    if not (rate and codes and done):
        return None
    good = int(codes.group(1))
    bad = sum(int(codes.group(n)) for n in (2, 3, 4)) + int(done.group(2))
    return {"requests": good + bad, "rate": float(rate.group(1)), "bad": bad}


def oha(text):
    """Open loop: the rate that was reached and the latency, in milliseconds."""
    result = json.loads(text)
    codes = result.get("statusCodeDistribution", {})
    good = sum(count for code, count in codes.items() if code.startswith("2"))
    errors = dict(result.get("errorDistribution", {}))
    # Requests that were under way when the time was up: the end of the run, not a failure.
    errors.pop("aborted due to deadline", None)
    bad = sum(codes.values()) - good + sum(errors.values())
    percentiles = result["latencyPercentiles"]
    return {
        "requests": good + bad,
        "rate": result["summary"]["requestsPerSec"],
        "bad": bad,
        "p50": percentiles["p50"] * 1000,
        "p99": percentiles["p99"] * 1000,
        "p99.9": percentiles["p99.9"] * 1000,
    }


def cpu(before, after):
    """How many CPUs the proxy kept busy between the two snapshots, and each thread's part
    of that in percent, the busiest first. The snapshots are around the warm-up too, which
    is load like the rest."""
    def snapshot(path):
        threads = {}
        for line in path.read_text().splitlines():
            thread, user, kernel = line.rsplit(" ", 2)
            threads[thread] = float(user) + float(kernel)
        return threads.pop("time"), threads

    if not (before.exists() and after.exists()):
        return None
    (started, start), (ended, end) = snapshot(before), snapshot(after)
    spent = sorted((end[thread] - start.get(thread, 0) for thread in end), reverse=True)
    busy = sum(spent) / TICKS_PER_SECOND / (ended - started)
    if not busy:
        return None
    shares = [round(100 * ticks / sum(spent)) for ticks in spent]
    return {"cpus": busy, "threads": "/".join(str(share) for share in shares if share)}


def middle(values):
    return statistics.median(values), min(values), max(values)


def main(directory):
    runs = defaultdict(list)
    for out in sorted(directory.glob("*.out")):
        subject, _, scenario = out.name[: -len(".out")].rpartition(".")
        model = subject.rsplit(".", 1)[0] if subject.rsplit(".", 1)[-1].isdigit() else subject
        text = out.read_text()
        try:
            result = oha(text) if text.lstrip().startswith("{") else h2load(text)
        except (ValueError, KeyError):
            result = None
        if result is None:
            print(f"cannot read {out.name}", file=sys.stderr)
            continue
        stem = out.name[: -len(".out")]
        used = cpu(directory / f"{stem}.cpu-before", directory / f"{stem}.cpu-after")
        if used:
            result.update(used)
            result["cpu_per_request"] = used["cpus"] / result["rate"] * 1e6
        clock = directory / f"{stem}.freq"
        samples = [int(line) for line in clock.read_text().split()] if clock.exists() else []
        if samples:
            result["mhz"] = statistics.fmean(samples) / 1000
        runs[(scenario, model)].append(result)

    print((directory / "environment.txt").read_text())
    columns = ["rate", "bad", "p50", "p99", "p99.9", "cpus", "cpu_per_request", "mhz"]
    titles = ["req/s", "not 2xx", "p50 ms", "p99 ms", "p99.9 ms", "CPUs busy", "CPU µs/req", "MHz"]
    print("| scenario | variant | runs | " + " | ".join(titles) + " | threads' share % |")
    print("|---|---|---|" + "---|" * (len(columns) + 1))
    for (scenario, model), results in sorted(runs.items()):
        cells = []
        for column in columns:
            values = [result[column] for result in results if column in result]
            if not values:
                cells.append("")
                continue
            median, low, high = middle(values)
            digits = 0 if column in ("rate", "bad", "mhz") else 2
            cell = f"{median:,.{digits}f}"
            if len(values) > 1 and low != high:
                cell += f" ({low:,.{digits}f}–{high:,.{digits}f})"
            cells.append(cell)
        threads = "; ".join(result.get("threads", "") for result in results)
        print(f"| {scenario} | {model} | {len(results)} | " + " | ".join(cells) + f" | {threads} |")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
