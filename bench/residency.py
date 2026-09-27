#!/usr/bin/env python3
"""Idle states for bench/run.sh: which states a policy leaves the CPUs, and how long they
spent in each while a measurement ran.

A policy is `normal` (every state the idle driver has) or the name of the deepest state
allowed, such as `C1E`, on every CPU, on the proxy's cores alone (`C1E-proxy`) or on every
other core (`C1E-others`). States are compared by exit latency, and a core's idle state is
the shallower of its threads', so a scope takes whole cores.

What a measurement saw is written down raw and read later: the OS's view from sysfs (per
CPU and state: entries, time, and whether the state was disabled), timed on the monotonic
clock, and, where perf has them, the hardware's residency counters (per core C3/C6/C7, per
package C2..C10, per CPU TSC and MPERF). The hardware may not enter the state the OS asks
for; only the counters say what it did.

    residency.py plan POLICY PROXY_CPUS     the sysfs writes a policy needs, `path value`
    residency.py save FILE                  every state's `disable` now, as writes to undo
    residency.py window SECONDS FILE PROXY_CPUS
                                            a measurement's residency, as JSON
"""

import json
import pathlib
import subprocess
import sys
import time

CPUS = pathlib.Path("/sys/devices/system/cpu")
HARDWARE = [
    "cstate_core/c3-residency/", "cstate_core/c6-residency/", "cstate_core/c7-residency/",
    "cstate_pkg/c2-residency/", "cstate_pkg/c3-residency/", "cstate_pkg/c6-residency/",
    "cstate_pkg/c7-residency/", "cstate_pkg/c8-residency/", "cstate_pkg/c9-residency/",
    "cstate_pkg/c10-residency/", "msr/tsc/", "msr/mperf/",
]
FIELDS = ("name", "latency", "residency", "disable", "usage", "time", "above", "below")


def cpu_list(text):
    """`0-2,5` as [0, 1, 2, 5]."""
    cpus = []
    for part in text.strip().split(","):
        if "-" in part:
            low, high = part.split("-")
            cpus.extend(range(int(low), int(high) + 1))
        elif part:
            cpus.append(int(part))
    return cpus


def cpus_of(root=CPUS):
    return sorted(int(path.name[3:]) for path in root.glob("cpu[0-9]*") if path.name[3:].isdigit())


def core_of(cpu, root=CPUS):
    """The CPUs of `cpu`'s core, itself among them."""
    siblings = root / f"cpu{cpu}" / "topology" / "thread_siblings_list"
    return cpu_list(siblings.read_text()) if siblings.exists() else [cpu]


def states(cpu, root=CPUS):
    """A CPU's idle states, as the driver lists them: index, name, exit latency (µs)."""
    found = []
    for path in sorted((root / f"cpu{cpu}" / "cpuidle").glob("state[0-9]*"),
                       key=lambda path: int(path.name[5:])):
        found.append((int(path.name[5:]), (path / "name").read_text().strip(),
                      int((path / "latency").read_text())))
    return found


def scope(policy, proxy_cpus, root=CPUS):
    """The CPUs a policy constrains: whole cores, those of the proxy or all the others."""
    where = policy.split("-", 1)[1] if "-" in policy else "all"
    every = cpus_of(root)
    proxy = sorted({sibling for cpu in proxy_cpus for sibling in core_of(cpu, root)})
    if where == "all":
        return every
    if where == "proxy":
        return proxy
    if where == "others":
        return [cpu for cpu in every if cpu not in proxy]
    raise ValueError(f"no such scope in {policy!r}: all, proxy or others")


def plan(policy, proxy_cpus, root=CPUS):
    """The `disable` value every state is to have under `policy`: each state enabled, but
    on the policy's CPUs those with a longer exit latency than the one it names."""
    deepest = policy.split("-", 1)[0]
    constrained = set() if policy == "normal" else set(scope(policy, proxy_cpus, root))
    writes = []
    for cpu in cpus_of(root):
        listed = states(cpu, root)
        limit = None
        if cpu in constrained:
            limit = next((latency for _, name, latency in listed if name == deepest), None)
            if limit is None:
                raise ValueError(f"cpu{cpu} has no idle state {deepest!r}: "
                                 + " ".join(name for _, name, _ in listed))
        for index, _, latency in listed:
            value = 1 if limit is not None and latency > limit else 0
            writes.append((root / f"cpu{cpu}" / "cpuidle" / f"state{index}" / "disable", value))
    return writes


def save(root=CPUS):
    """Every state's `disable` as it is, as the writes that would put it back."""
    return [(path, int(path.read_text())) for path in
            sorted(root.glob("cpu[0-9]*/cpuidle/state[0-9]*/disable"))]


def snapshot(root=CPUS):
    """Every CPU's idle states as sysfs has them, timed on the monotonic clock: the middle
    of the time it took to read them."""
    started = time.monotonic_ns()
    cpus = {}
    for cpu in cpus_of(root):
        read = []
        for index, _, _ in states(cpu, root):
            directory = root / f"cpu{cpu}" / "cpuidle" / f"state{index}"
            entry = {}
            for field in FIELDS:
                path = directory / field
                if path.exists():
                    text = path.read_text().strip()
                    entry[field] = text if field == "name" else int(text)
            read.append(entry)
        cpus[str(cpu)] = read
    ended = time.monotonic_ns()
    return {"at_ns": (started + ended) // 2, "read_ns": ended - started, "cpus": cpus}


def hardware(seconds):
    """perf's residency counters over `seconds`, per CPU as it reports them (a core's on its
    first CPU, the package's on CPU 0): the raw CSV, or None where perf cannot count them."""
    try:
        done = subprocess.run(
            ["sudo", "-n", "perf", "stat", "-a", "-A", "-x,", "-e", ",".join(HARDWARE),
             "--", "sleep", str(seconds)],
            capture_output=True, text=True, timeout=seconds + 30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if done.returncode != 0:
        return None
    return done.stderr


def window(seconds, proxy_cpus, root=CPUS):
    """A measurement's residency: sysfs before and after, the hardware's counts between."""
    topology = {str(cpu): core_of(cpu, root) for cpu in cpus_of(root)}
    before = snapshot(root)
    counted = hardware(seconds) if (pathlib.Path("/sys/bus/event_source/devices/cstate_core")
                                    .exists()) else None
    if counted is None:
        time.sleep(seconds)
    after = snapshot(root)
    return {"proxy_cpus": proxy_cpus, "cores": topology, "before": before, "after": after,
            "perf": counted}


def perf_counts(text):
    """perf stat -A -x, output as {(cpu, event): count}; what it could not count is left out."""
    counts = {}
    for line in (text or "").splitlines():
        fields = line.split(",")
        if len(fields) < 4 or not fields[0].startswith("CPU"):
            continue
        try:
            counts[(int(fields[0][3:]), fields[3])] = float(fields[1])
        except ValueError:
            continue
    return counts


def shares(record):
    """What a window saw, as shares of it in percent and entries a second.

    OS view, by the monotonic time between the snapshots: `os_time[cpu][state]`,
    `os_entries[cpu][state]`. Hardware, each as a share of the TSC of the CPU it was counted
    on: `c0[cpu]` (MPERF), `core[cpu][c3|c6|c7]` for the core whose first CPU that is,
    `package[c2..c10]`."""
    elapsed = (record["after"]["at_ns"] - record["before"]["at_ns"]) / 1e9
    os_time, os_entries = {}, {}
    for cpu, after in record["after"]["cpus"].items():
        before = {entry["name"]: entry for entry in record["before"]["cpus"].get(cpu, [])}
        for entry in after:
            then = before.get(entry["name"])
            if not then or elapsed <= 0:
                continue
            os_time.setdefault(int(cpu), {})[entry["name"]] = \
                (entry["time"] - then["time"]) / 1e6 / elapsed * 100
            os_entries.setdefault(int(cpu), {})[entry["name"]] = \
                (entry["usage"] - then["usage"]) / elapsed
    counts = perf_counts(record.get("perf"))
    c0, core, package = {}, {}, {}
    for (cpu, event), count in counts.items():
        tsc = counts.get((cpu, "msr/tsc/"))
        if not tsc:
            continue
        share = count / tsc * 100
        kind, _, name = event.strip("/").partition("/")
        state = name.split("-")[0]
        if kind == "msr" and name == "mperf":
            c0[cpu] = share
        elif kind == "cstate_core":
            core.setdefault(cpu, {})[state] = share
        elif kind == "cstate_pkg":
            package[state] = share
    return {"elapsed": elapsed, "os_time": os_time, "os_entries": os_entries, "c0": c0,
            "core": core, "package": package}


def proxy_view(record):
    """The figures the run table shows: the proxy's cores against the rest, and the package.
    Hardware shares are of TSC, the OS's of the monotonic window."""
    seen = shares(record)
    proxy = set(record["proxy_cpus"])
    cores = {int(cpu): siblings for cpu, siblings in record["cores"].items()}
    proxy_cores = {min(cores[cpu]) for cpu in proxy if cpu in cores}
    other_cores = {min(siblings) for siblings in cores.values()} - proxy_cores

    def mean(values):
        values = list(values)
        return sum(values) / len(values) if values else None

    view = {"window": seen["elapsed"]}
    for state in ("c3", "c6", "c7"):
        # A count perf could not take is left out, not taken for zero.
        view[f"proxy_cc{state[1:]}"] = mean(seen["core"][cpu][state] for cpu in proxy_cores
                                             if state in seen["core"].get(cpu, {}))
        view[f"other_cc{state[1:]}"] = mean(seen["core"][cpu][state] for cpu in other_cores
                                             if state in seen["core"].get(cpu, {}))
    view["proxy_c0"] = mean(seen["c0"][cpu] for cpu in proxy if cpu in seen["c0"])
    for state in ("c2", "c3"):
        view[f"pc{state[1:]}"] = seen["package"].get(state)
    deeper = [seen["package"][state] for state in ("c6", "c7", "c8", "c9", "c10")
              if state in seen["package"]]
    view["pc6_plus"] = sum(deeper) if deeper else None
    view["proxy_entries"] = mean(sum(by_state.values()) for cpu, by_state
                                 in seen["os_entries"].items() if cpu in proxy)
    # The deepest state the OS asked for on the proxy's CPUs, and for how much of the time:
    # what a policy allowed, against what the counters show the hardware did.
    asked = {}
    for cpu in proxy:
        for name, share in seen["os_time"].get(cpu, {}).items():
            asked[name] = asked.get(name, 0) + share / len(proxy)
    order = [entry["name"] for entry in
             record["after"]["cpus"].get(str(min(proxy)), [])] if proxy else []
    for name in reversed(order):
        if asked.get(name, 0) >= 0.5:
            view["proxy_deepest_asked"] = f"{name} {asked[name]:.0f}%"
            break
    return view


def main(argv):
    command = argv[1] if len(argv) > 1 else ""
    if command == "plan":
        for path, value in plan(argv[2], cpu_list(argv[3])):
            print(path, value)
    elif command == "save":
        for path, value in save():
            print(path, value)
    elif command == "window":
        record = window(float(argv[2]), cpu_list(argv[4]))
        pathlib.Path(argv[3]).write_text(json.dumps(record))
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except ValueError as error:
        print(f"residency.py: {error}", file=sys.stderr)
        sys.exit(1)
