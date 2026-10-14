"""Tables from a sweep's JSON lines.

    python3 report.py RESULTS.jsonl [more.jsonl ...]

Each cell, for traffic over the contract: admitted against one instance of the same system
on the same traffic (what sharing costs) / against one bucket holding our contract (100 a
second, burst 100, what the simulator measures) / the most over that contract in any span,
worst run. For conforming traffic, which one bucket admits whole: the share refused by the
fleet / by one instance of the same system on the same traffic.
"""

import collections
import json
import sys

import workloads

CONDITIONS = [
    ("Three times the limit, spread", "over-spread", "none"),
    ("A whole bucket on every instance at once, every second", "bursts", "none"),
    ("One instance at the limit, then such bursts", "one-then-bursts", "none"),
    ("On the edge, spread: refused", "edge-spread", "none"),
    ("On the edge, one instance: refused", "edge-one", "none"),
    ("On the edge, uneven: refused", "edge-uneven", "none"),
    ("Within, spread: refused", "within-spread", "none"),
    ("Bursts, 10% of packets lost to the store or peers", "bursts", "loss"),
    ("Owner lost: three times the limit", "over-spread", "owner-lost"),
    ("Owner lost: bursts", "bursts", "owner-lost"),
    ("Owner restarted: three times the limit", "over-spread", "owner-restart"),
    ("Owner restarted: bursts", "bursts", "owner-restart"),
    ("An instance restarted: bursts", "bursts", "restart"),
    ("Rolling restart: three times the limit", "over-spread", "rolling"),
    ("Rolling restart: bursts", "bursts", "rolling"),
    ("Partition 3–7 s: three times the limit", "over-spread", "partition"),
    ("Partition 3–7 s: within, spread: refused", "within-spread", "partition"),
    ("Partition 3–7 s: bursts", "bursts", "partition"),
]


def load(paths):
    rows = []
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    rows.append(json.loads(line))
    return rows


class Cell:
    """The runs of one system, size, workload and fault, added up over seeds."""

    def __init__(self):
        self.runs = 0
        self.sent = self.admitted = self.refused = self.failed = self.one_bucket = 0
        self.worst = None
        self.latency_mean = []
        self.latency_p99 = []
        self.packets = []

    def add(self, row):
        self.runs += 1
        self.sent += row["sent"]
        self.admitted += row["admitted"]
        self.refused += row["refused"]
        self.failed += row["failed"]
        self.one_bucket += row["one_bucket"]
        if row["worst_excess"] is not None:
            self.worst = row["worst_excess"] if self.worst is None else max(self.worst, row["worst_excess"])
        if row["latency_ms"]["mean"] is not None:
            self.latency_mean.append(row["latency_ms"]["mean"])
            self.latency_p99.append(row["latency_ms"]["p99"])
        if row.get("packets_per_s") is not None:
            self.packets.append(row["packets_per_s"])


def cells(rows):
    """{(system, instances, traffic pods, workload, fault): Cell}."""
    out = collections.defaultdict(Cell)
    for row in rows:
        key = (row["system"], row["instances"], row["traffic_pods"], row["workload"], row["fault"])
        out[key].add(row)
    return out


def describe(table, system, n, workload, fault):
    """One cell's text, or '—' when it was not run."""
    cell = table.get((system, n, n, workload, fault))
    if cell is None or cell.runs == 0:
        return "—"
    yardstick = table.get((system, 1, n, workload, "none"))
    conforming = workloads.WORKLOADS[workload][2]
    failed = f", {cell.failed / cell.sent:.1%} failed" if cell.failed else ""
    if conforming:
        own = f" / {yardstick.refused / yardstick.sent:.1%}" if yardstick and yardstick.sent else ""
        return f"{cell.refused / cell.sent:.1%}{own}{failed}"
    own = f"{cell.admitted / yardstick.admitted:.2f}×" if yardstick and yardstick.admitted else "?"
    contract = cell.admitted / cell.one_bucket if cell.one_bucket else 0
    return f"{own} / {contract:.2f}× ({cell.worst}){failed}"


def systems_in(rows):
    seen = []
    for row in rows:
        if row["system"] not in seen:
            seen.append(row["system"])
    return seen


def headline(table, names, n):
    lines = [f"**{n} instances**", "", "| | " + " | ".join(names) + " |",
             "|---|" + "---|" * len(names)]
    for title, workload, fault in CONDITIONS:
        texts = [describe(table, s, n, workload, fault) for s in names]
        if all(text == "—" for text in texts):
            continue
        lines.append(f"| {title} | " + " | ".join(texts) + " |")
    for title, pick in (("Latency, mean / p99 ms, spread", "latency"),
                        ("Packets a second on each instance's back network, spread", "packets")):
        texts = []
        for s in names:
            cell = table.get((s, n, n, "within-spread", "none"))
            if cell is None or cell.runs == 0:
                texts.append("—")
            elif pick == "latency":
                mean = sum(cell.latency_mean) / len(cell.latency_mean)
                p99 = max(cell.latency_p99)
                texts.append(f"{mean:.1f} / {p99:.1f}")
            else:
                texts.append(f"{sum(cell.packets) / len(cell.packets):.0f}" if cell.packets else "—")
        lines.append(f"| {title} | " + " | ".join(texts) + " |")
    return "\n".join(lines)


def by_size(table, names, sizes, workload):
    lines = [f"**{workload}**", "", "| System | " + " | ".join(f"{n}" for n in sizes) + " |",
             "|---|" + "---|" * len(sizes)]
    for s in names:
        lines.append(f"| {s} | " + " | ".join(describe(table, s, n, workload, "none") for n in sizes) + " |")
    return "\n".join(lines)


def main(paths):
    rows = load(paths)
    table = cells(rows)
    names = systems_in(rows)
    sizes = sorted({row["instances"] for row in rows if row["instances"] > 1})
    print(__doc__.split("\n\n", 2)[2].strip(), "\n")
    for n in reversed(sizes):
        print(headline(table, names, n), "\n")
    for workload in workloads.WORKLOADS:
        print(by_size(table, names, sizes, workload), "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
