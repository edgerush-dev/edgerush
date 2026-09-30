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

import residency

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
    # A run where nothing finished has no latency to report, and one is meant to: every
    # request of the cancelled scenario is given up on.
    percentiles = result["latencyPercentiles"]

    def took(name):
        return percentiles[name] * 1000 if percentiles.get(name) is not None else None

    return {
        "requests": good + bad,
        "rate": result["summary"]["requestsPerSec"],
        "bad": bad,
        "p50": took("p50"),
        "p99": took("p99"),
        "p99.9": took("p99.9"),
    }


def wsbench(result):
    """bench/wsbench's line: messages echoed, and their latency in milliseconds."""
    return {
        "requests": result["messages"] + result["failed"],
        "rate": result["rate"],
        "bad": result["failed"],
        "p50": result["p50"],
        "p99": result["p99"],
        "p99.9": result["p99.9"],
    }


def generator(text):
    """Whichever generator's output `text` is: oha's JSON, wsbench's, or h2load's."""
    if not text.lstrip().startswith("{"):
        return h2load(text)
    result = json.loads(text)
    return wsbench(result) if "wsbench" in result else oha(text)


def logged(path):
    """Latency from h2load's --log-file, one request a line (start µs, status, µs taken), in
    milliseconds: what a fixed rate by h2load reports, as oha's JSON does its own. A failed
    stream, status -1, has no latency; it is counted among those not 2xx."""
    took = sorted(
        int(fields[2]) / 1000
        for fields in (line.split("\t") for line in path.read_text().splitlines())
        if len(fields) >= 3 and fields[1] != "-1" and fields[2].strip().isdigit()
    )
    if not took:
        return {}

    def at(share):
        return took[min(len(took) - 1, int(share * len(took)))]

    return {"p50": at(0.5), "p99": at(0.99), "p99.9": at(0.999)}


def memory(text):
    """What idle connections cost: the lines bench/run.sh's idle_memory writes."""
    read = dict(
        line.split(maxsplit=1) for line in text.splitlines() if " " in line
    )
    if "per_connection_bytes" not in read:
        return None
    return {key: value if key == "kind" else int(value) for key, value in read.items()}


def probed(path):
    """What curl's one request every 10 ms beside the load took (run.sh's probe_h3), in
    milliseconds, and how many connections it made to take them: one, unless it lost it."""
    took, connects = [], 0
    for line in path.read_text().splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        connects += int(fields[0])
        took.append(float(fields[2]) * 1000)
    if not took:
        return {}
    took.sort()
    return {
        "probe_p50": took[len(took) // 2],
        "probe_p99": took[min(len(took) - 1, int(0.99 * len(took)))],
        "probe_connects": connects,
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


def counts(path, rate):
    """What a request cost the proxy in instructions and cycles, user and kernel apart, in
    thousands: COUNT=1's perf stat over a window inside the load, whose length run.sh adds as
    its last line, by the rate the load kept."""
    counted = {}
    window = None
    for line in path.read_text().splitlines():
        if line.startswith("# window"):
            window = float(line.split()[2])
            continue
        fields = line.split(",")
        if line.startswith("#") or len(fields) < 3:
            continue
        try:
            counted[fields[2]] = float(fields[0])
        except ValueError:
            continue
    if not (window and rate):
        return {}

    def per_request(event):
        return counted[event] / window / rate / 1000 if event in counted else None

    cycles = [per_request(event) for event in ("cycles:u", "cycles:k")]
    return {
        "instructions_user": per_request("instructions:u"),
        "instructions_kernel": per_request("instructions:k"),
        "cycles": sum(cycles) if all(value is not None for value in cycles) else None,
    }


def resident(path):
    """The most the proxy held while the load was on, in MiB: its resident memory, all its
    processes summed, sampled every half second."""
    samples = [int(line) for line in path.read_text().split() if line.isdigit()]
    return {"rss_peak": max(samples) / 1024} if samples else {}


def spent(directory, stem, result):
    """What the measurement `stem` cost the proxy and held, beside what the client saw."""
    used = cpu(directory / f"{stem}.cpu-before", directory / f"{stem}.cpu-after")
    if used and result["rate"]:
        result.update(used)
        result["cpu_per_request"] = used["cpus"] / result["rate"] * 1e6
    stat = directory / f"{stem}.stat"
    if stat.exists():
        result.update(counts(stat, result["rate"]))
    rss = directory / f"{stem}.rss"
    if rss.exists():
        result.update(resident(rss))
    idle = directory / f"{stem}.idle"
    if idle.exists():
        try:
            result.update(residency.proxy_view(json.loads(idle.read_text())))
        except (ValueError, KeyError):
            print(f"cannot read {idle.name}", file=sys.stderr)
    clock = directory / f"{stem}.freq"
    samples = [int(line) for line in clock.read_text().split()] if clock.exists() else []
    if samples:
        result["mhz"] = statistics.fmean(samples) / 1000
    return result


def runs_of(directory, stem):
    """One generator's result, read again for the whole it was part of."""
    out = directory / f"{stem}.out"
    if not out.exists():
        return None
    text = out.read_text()
    try:
        return generator(text)
    except (ValueError, KeyError):
        return None


def middle(values):
    return statistics.median(values), min(values), max(values)


def main(directory):
    runs = defaultdict(list)
    held = {}
    for out in sorted(directory.glob("*.out")):
        subject, _, scenario = out.name[: -len(".out")].rpartition(".")
        model = subject.rsplit(".", 1)[0] if subject.rsplit(".", 1)[-1].isdigit() else subject
        text = out.read_text()
        if "per_connection_bytes" in text:
            held[(scenario, model)] = memory(text)
            continue
        try:
            result = generator(text)
        except (ValueError, KeyError):
            result = None
        if result is None:
            print(f"cannot read {out.name}", file=sys.stderr)
            continue
        stem = out.name[: -len(".out")]
        requests = directory / f"{stem}.requests"
        if requests.exists():
            result.update(logged(requests))
        probe = directory / f"{stem}.probe"
        if probe.exists():
            result.update(probed(probe))
        runs[(scenario, model)].append(spent(directory, stem, result))

    # A mixed load's three generators are a row each; what the proxy spent on all of them is
    # `<name>-all`, a row of its own at the rate of the three together.
    for before in sorted(directory.glob("*-all.cpu-before")):
        stem = before.name[: -len(".cpu-before")]
        parts = [runs_of(directory, stem[: -len("all")] + protocol) for protocol in ("h1", "h2", "h3")]
        if not all(parts):
            continue
        subject, _, scenario = stem.rpartition(".")
        model = subject.rsplit(".", 1)[0] if subject.rsplit(".", 1)[-1].isdigit() else subject
        result = {
            "requests": sum(part["requests"] for part in parts),
            "rate": sum(part["rate"] for part in parts),
            "bad": sum(part["bad"] for part in parts),
        }
        runs[(scenario, model)].append(spent(directory, stem, result))

    print((directory / "environment.txt").read_text())
    columns = [
        "rate", "bad", "p50", "p99", "p99.9", "probe_p50", "probe_p99", "cpus",
        "cpu_per_request", "instructions_user", "instructions_kernel", "cycles", "rss_peak",
        "mhz",
    ]
    titles = [
        "req/s", "not 2xx", "p50 ms", "p99 ms", "p99.9 ms", "curl p50 ms", "curl p99 ms",
        "CPUs busy", "CPU µs/req", "k instr/req user", "k instr/req kernel", "k cycles/req",
        "RSS peak MiB", "MHz",
    ]
    print("| scenario | variant | runs | " + " | ".join(titles) + " | threads' share % |")
    print("|---|---|---|" + "---|" * (len(columns) + 1))
    for (scenario, model), results in sorted(runs.items()):
        cells = []
        for column in columns:
            values = [
                result[column]
                for result in results
                if result.get(column) is not None
            ]
            if not values:
                cells.append("")
                continue
            median, low, high = middle(values)
            digits = 0 if column in ("rate", "bad", "mhz", "rss_peak") else 2
            if column.startswith("instructions") or column == "cycles":
                digits = 1
            cell = f"{median:,.{digits}f}"
            if len(values) > 1 and low != high:
                cell += f" ({low:,.{digits}f}–{high:,.{digits}f})"
            cells.append(cell)
        threads = "; ".join(result.get("threads", "") for result in results)
        print(f"| {scenario} | {model} | {len(results)} | " + " | ".join(cells) + f" | {threads} |")

    if any("window" in result for results in runs.values() for result in results):
        # Where the CPUs idled over each measurement's window: the hardware's residency as a
        # share of TSC (C0 is the proxy's CPUs', CC6 and CC7 its cores' and the others' on
        # average, PC the package's), the idle entries a second of the proxy's CPUs, and the
        # deepest state the OS asked for there with its share of the window (per run).
        print()
        idle_columns = [
            "proxy_c0", "proxy_cc6", "proxy_cc7", "other_cc6", "other_cc7", "pc2", "pc3",
            "pc6_plus", "proxy_entries",
        ]
        idle_titles = [
            "proxy C0 %", "proxy CC6 %", "proxy CC7 %", "others CC6 %", "others CC7 %",
            "PC2 %", "PC3 %", "PC6+ %", "proxy idle entries/s",
        ]
        print("| scenario | variant | runs | " + " | ".join(idle_titles) + " | OS asked, deepest |")
        print("|---|---|---|" + "---|" * (len(idle_columns) + 1))
        for (scenario, model), results in sorted(runs.items()):
            if not any("window" in result for result in results):
                continue
            cells = []
            for column in idle_columns:
                values = [result[column] for result in results if result.get(column) is not None]
                cells.append(f"{statistics.median(values):,.0f}" if values else "")
            asked = "; ".join(result.get("proxy_deepest_asked", "") for result in results)
            print(f"| {scenario} | {model} | {len(results)} | " + " | ".join(cells) + f" | {asked} |")

    if held:
        # Not a row of the table: what a connection costs while nothing happens on it is
        # not a rate and has no latency.
        print()
        print("| scenario | variant | connections | RSS quiet | RSS held | bytes each |")
        print("|---|---|---|---|---|---|")
        for (scenario, model), read in sorted(held.items()):
            if not read:
                continue
            print(
                f"| {scenario} | {model} | {read['connections']:,} |"
                f" {read['rss_quiet_kb']:,} KiB | {read['rss_held_kb']:,} KiB |"
                f" {read['per_connection_bytes']:,} |"
            )


if __name__ == "__main__":
    main(Path(sys.argv[1]))
