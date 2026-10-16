"""Runs the simulator's workloads and faults against real rate limiters in Docker Compose.

    python3 harness.py sweep OUT.jsonl [--systems a,b] [--sizes 2,4,8] [--workloads a,b]
                                      [--faults none,loss,...] [--seeds 3]
                                      [--fault-workloads over-spread,bursts,within-spread]
    python3 harness.py up SYSTEM N        # bring one up and leave it, to look at
    python3 harness.py down

A sweep writes one JSON line per run and skips runs already in OUT, so it can be stopped
and started again. For each size N it also plays the same traffic against one instance of
the same system (`"yardstick": true`): the measure of what sharing costs. Needs Linux,
Docker and passwordless sudo (faults enter the containers' network namespaces).
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import pathlib
import random
import statistics
import subprocess
import sys
import time
import urllib.request

import load
import systems
import workloads
from systems import SYSTEMS, PROJECT, back_ip, front_ip, name

WORK = pathlib.Path(os.environ.get("RLH_WORK", "/tmp/rlh"))
FAULTS = ["none", "loss", "owner-lost", "owner-restart", "restart", "rolling", "partition"]
# Packet loss on the back network, as the simulator's one message in ten.
LOSS = "10%"
GAP = 2.0  # seconds between runs: every window and bucket is whole again
# Traffic is steered away this long before an instance is stopped, as a rollout drains it,
# so that the stop lands at the fault's time with nothing in flight.
DRAIN = 0.25


def sh(*args, check=True):
    out = subprocess.run(args, capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(args)}: {out.stderr.strip()[-400:]}")
    return out.stdout


def compose(*args):
    return sh("docker", "compose", "-p", PROJECT, "-f", str(WORK / "compose.yaml"), *args)


# --- bringing a system up -----------------------------------------------------------


def write_project(system, n):
    compose_doc, files = system.project(n)
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "compose.yaml").write_text(json.dumps(compose_doc, indent=2) + "\n")
    for path, content in files.items():
        target = WORK / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def down():
    if (WORK / "compose.yaml").exists():
        compose("down", "-t", "0", "--remove-orphans")


def gubernator_ask(i, key, timeout=1.0):
    """Instance `i`'s answer for `key`, asking with no hits and without GLOBAL, so that an
    answer forwarded to the owner names it."""
    body = json.dumps({"requests": [{"name": "rl", "unique_key": key, "hits": 0, "limit": 100,
                                     "duration": 1000, "algorithm": "LEAKY_BUCKET",
                                     "behavior": "BATCHING", "burst": 100}]}).encode()
    request = urllib.request.Request(f"http://{front_ip(i)}:1050/v1/GetRateLimits", data=body,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as r:
        return json.loads(r.read())["responses"][0]


def probe(system, i, timeout=1.0):
    """Whether instance `i` answers a decision (any decision). Not Gubernator's health
    check: that reports a peer's error for five minutes after it."""
    host, port = front_ip(i), system.port
    try:
        if system.ask == "gubernator":
            return not gubernator_ask(i, "probe", timeout).get("error")
        request = urllib.request.Request(f"http://{host}:{port}/", headers={"X-Key": "probe"})
        with urllib.request.urlopen(request, timeout=timeout) as r:
            return r.status == 200
    except urllib.error.HTTPError as error:
        return error.code == 429
    except Exception:  # noqa: BLE001 - not ready yet
        return False


def gubernator_ring(i, n):
    """The owners instance `i` forwards to, over enough keys to meet every instance."""
    owners = set()
    for _ in range(20 * n):
        try:
            answer = gubernator_ask(i, f"ring-{random.randrange(1 << 40)}")
        except Exception:  # noqa: BLE001
            return set()
        if answer.get("error"):
            return set()
        owners.add(answer["metadata"].get("owner", f"{back_ip(i)}:1051"))
    return owners


def wait_ready(system, instances, deadline=120.0):
    start = time.monotonic()
    for i in instances:
        while not probe(system, i):
            if time.monotonic() - start > deadline:
                raise RuntimeError(f"{system.key}: instance {i + 1} not ready")
            time.sleep(0.25)


def wait_cluster(system, n):
    """Ready, and for Gubernator every instance seeing every other."""
    wait_ready(system, range(n))
    if system.ask == "gubernator":
        start = time.monotonic()
        while any(len(gubernator_ring(i, n)) != n for i in range(n)):
            if time.monotonic() - start > 60:
                raise RuntimeError("gubernator peers never agreed")
            time.sleep(0.5)
    # Membership by heartbeat (Tyk's rate split, Gubernator's ring) settles after a few.
    time.sleep(5 if system.key in ("tyk", "gubernator") else 1)


def up(system, n):
    down()
    write_project(system, n)
    compose("up", "-d", "--remove-orphans")
    wait_cluster(system, n)


def restore(system, n):
    """Everything a fault touched back as it was: containers started, rules gone."""
    for container in [name(i) for i in range(n)] + [f"{PROJECT}-{s}" for s in system.stores]:
        clear_network(container)
    compose("up", "-d")
    wait_cluster(system, n)


# --- the network inside a container -------------------------------------------------


def pid(container):
    return sh("docker", "inspect", "-f", "{{.State.Pid}}", container).strip()


def netns(container, *args):
    return sh("sudo", "-n", "nsenter", "-t", pid(container), "-n", *args)


def back_interface(container):
    for link in json.loads(netns(container, "ip", "-j", "addr")):
        for address in link.get("addr_info", []):
            if address.get("local", "").startswith(systems.BACK + "."):
                return link["ifname"]
    raise RuntimeError(f"{container}: no back interface")


def packets(container):
    """Packets in and out on the back network, so far."""
    try:
        interface = back_interface(container)
        link = json.loads(netns(container, "ip", "-s", "-j", "link", "show", "dev", interface))[0]
        stats = link["stats64"]
        return stats["rx"]["packets"] + stats["tx"]["packets"]
    except Exception:  # noqa: BLE001 - a container gone has none
        return None


def clear_network(container):
    try:
        interface = back_interface(container)
    except Exception:  # noqa: BLE001 - not running
        return
    sh("sudo", "-n", "nsenter", "-t", pid(container), "-n", "tc", "qdisc", "del", "dev",
       interface, "root", check=False)
    for chain in ("INPUT", "OUTPUT"):
        sh("sudo", "-n", "nsenter", "-t", pid(container), "-n", "iptables", "-F", chain,
           check=False)


def lose(container):
    interface = back_interface(container)
    netns(container, "tc", "qdisc", "add", "dev", interface, "root", "netem", "loss", LOSS)


def cut(container, addresses):
    for address in addresses:
        netns(container, "iptables", "-I", "INPUT", "-s", address, "-j", "DROP")
        netns(container, "iptables", "-I", "OUTPUT", "-d", address, "-j", "DROP")


# --- faults -------------------------------------------------------------------------


def owner_of(system, n, key):
    """The instance a fault on the owner strikes, or None when the owner is the store."""
    if system.owner in ("store", "service"):
        return None
    if system.owner == "first":
        return 0
    for i in range(n):
        owner = gubernator_ask(i, key, timeout=2)["metadata"].get("owner")
        if owner:
            host = owner.split(":")[0]
            return next(j for j in range(n) if back_ip(j) == host)
    return 0


async def run_command(*args):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await process.wait()


async def at(t0, seconds):
    delay = (t0 + int(seconds * 1e9) - time.monotonic_ns()) / 1e9
    if delay > 0:
        await asyncio.sleep(delay)


async def ready_again(system, i):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, wait_ready, system, [i])


async def restart_instance(system, n, i, route, stay_down=False):
    """Steers instance i's traffic to the next while it restarts (or goes for good)."""
    route[i] = (i + 1) % n
    await asyncio.sleep(DRAIN)
    if stay_down:
        await run_command("docker", "kill", name(i))
        return
    await run_command("docker", "restart", "-t", "0", name(i))
    await ready_again(system, i)
    route[i] = i


async def strike(fault, system, n, owner, t0, route, events, key):
    span = workloads.SPAN / 1e9
    store = f"{PROJECT}-{system.stores[0]}" if system.stores else None
    # EdgeRush's service: the pod that owns the load's key.
    if system.owner == "service":
        store = f"{PROJECT}-rl{systems.service_owner()}"
    if fault in ("owner-lost", "owner-restart"):
        await at(t0, span / 2 - (0 if owner is None else DRAIN))
        events.append(("strike", time.monotonic_ns() - t0))
        if owner is None:
            verb = ["docker", "kill"] if fault == "owner-lost" else ["docker", "restart", "-t", "0"]
            await run_command(*verb, store)
        else:
            await restart_instance(system, n, owner, route, stay_down=fault == "owner-lost")
    elif fault == "restart":
        await at(t0, span / 2 - DRAIN)
        events.append(("strike", time.monotonic_ns() - t0))
        await restart_instance(system, n, n - 1, route)
    elif fault == "rolling":
        await at(t0, span * 3 / 10 - DRAIN)
        for i in range(n):
            events.append((f"restart {i + 1}", time.monotonic_ns() - t0))
            await restart_instance(system, n, i, route)
    elif fault == "partition":
        split = n // 2
        side = [back_ip(i) for i in range(split)] + [systems.STORE_IPS[s] for s in system.stores]
        await at(t0, span * 3 / 10)
        events.append(("cut", time.monotonic_ns() - t0))
        for i in range(split, n):
            cut(name(i), side)
        await at(t0, span * 7 / 10)
        events.append(("healed", time.monotonic_ns() - t0))
        for i in range(split, n):
            clear_network(name(i))
    events.append(("done", time.monotonic_ns() - t0))


# --- a run --------------------------------------------------------------------------


def play(system, n, traffic, fault, seed, key):
    """Plays `traffic` (arrivals over `traffic` pods; all on the one instance when n is 1)
    with `fault`, and returns every request's fate."""
    arrivals = workloads.arrivals(traffic["workload"], traffic["pods"], seed)
    if n == 1:
        arrivals = [(t, 0) for t, _ in arrivals]
    per_pod = [[t for t, p in arrivals if p == pod] for pod in range(n)]
    targets = [(front_ip(i), system.port) for i in range(n)]
    owner = owner_of(system, n, key) if fault != "none" else None
    route = multiprocessing.Array("i", list(range(n)), lock=False)
    if fault == "loss":
        for container in [name(i) for i in range(n)] + [f"{PROJECT}-{s}" for s in system.stores]:
            lose(container)
    before = {name(i): packets(name(i)) for i in range(n)}
    out = multiprocessing.Queue()
    t0 = time.monotonic_ns() + int(1.5e9)
    workers = []
    for pod in range(n):
        if not per_pod[pod]:
            continue
        burst = max(per_pod[pod].count(t) for t in set(per_pod[pod]))
        warm = min(max(burst, 4), 200)
        process = multiprocessing.Process(
            target=load.play,
            args=(pod, per_pod[pod], targets, system.ask, key, t0, route, warm, out))
        process.start()
        workers.append(process)
    events = []
    if fault not in ("none", "loss"):
        asyncio.run(strike(fault, system, n, owner, t0, route, events, key))
    results = []
    for _ in workers:
        results.extend(out.get())
    for process in workers:
        process.join()
    after = {name(i): packets(name(i)) for i in range(n)}
    counted = [after[c] - before[c] for c in before if before[c] is not None and after[c] is not None
               and after[c] >= before[c]]
    return results, events, counted


def summarise(results, counted, n):
    """The simulator's measures of a run, from every request's fate."""
    results.sort(key=lambda r: r[3])
    sent_times = [r[3] for r in results]
    admitted_times = sorted(r[3] for r in results if r[5] == "admitted")
    refused = sum(1 for r in results if r[5] == "refused")
    failed = [r[5] for r in results if r[5].startswith("failed")]
    latencies = sorted((r[4] - r[3]) / 1e6 for r in results)
    span = workloads.SPAN / 1e9
    return {
        "sent": len(results),
        "admitted": len(admitted_times),
        "refused": refused,
        "failed": len(failed),
        "admitted_by_instance": [sum(1 for r in results if r[2] == i and r[5] == "admitted")
                                 for i in range(n)],
        # By the second it was due in: which burst, or which part of a run, went over.
        "admitted_by_second": [
            sum(1 for r in results if r[5] == "admitted" and r[0] // workloads.SECOND == second)
            for second in range(workloads.SPAN // workloads.SECOND)
        ],
        "failures": sorted(set(failed))[:5],
        "one_bucket": workloads.one_bucket(sent_times),
        "worst_excess": workloads.worst_excess(admitted_times),
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 3) if latencies else None,
            "p50": round(latencies[len(latencies) // 2], 3) if latencies else None,
            "p99": round(latencies[int(len(latencies) * 0.99)], 3) if latencies else None,
        },
        # The schedule kept: how late requests went out against when they were due.
        "late_ms_p99": round(sorted((r[3] - r[0]) / 1e6 for r in results)[int(len(results) * 0.99)], 3)
        if results else None,
        "packets_per_s": round(sum(counted) / len(counted) / span, 1) if counted else None,
    }


def done_runs(path):
    seen = set()
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            seen.add((r["system"], r["instances"], r["traffic_pods"], r["workload"], r["fault"], r["seed"]))
    return seen


def sweep(args):
    out = pathlib.Path(args.out)
    seen = done_runs(out)
    chosen = args.systems.split(",") if args.systems else list(SYSTEMS)
    sizes = [int(s) for s in args.sizes.split(",")]
    loads = args.workloads.split(",") if args.workloads else list(workloads.WORKLOADS)
    faults = args.faults.split(",")
    fault_loads = [w for w in args.fault_workloads.split(",") if w in loads]
    for key in chosen:
        system = SYSTEMS[key]
        # The yardstick first: one instance, each size's traffic, no fault.
        plan = [(1, size, "none") for size in sizes]
        plan += [(size, size, fault) for size in sizes for fault in faults
                 if fault == "none" or size == max(sizes)]
        current = None
        for n, traffic_pods, fault in plan:
            for workload in loads if fault == "none" else fault_loads:
                for seed in range(args.seeds):
                    if (key, n, traffic_pods, workload, fault, seed) in seen:
                        continue
                    if current != n:
                        print(f"{key}: up with {n}", file=sys.stderr, flush=True)
                        up(system, n)
                        current = n
                    run_key = f"{workload}-{seed}-{random.randrange(1 << 30)}"
                    traffic = {"workload": workload, "pods": traffic_pods}
                    results, events, counted = play(system, n, traffic, fault, seed, run_key)
                    row = {"system": key, "instances": n, "traffic_pods": traffic_pods,
                           "yardstick": n == 1 and traffic_pods > 1, "workload": workload,
                           "fault": fault, "seed": seed, "events": events}
                    row.update(summarise(results, counted, n))
                    with out.open("a") as f:
                        f.write(json.dumps(row) + "\n")
                    print(f"  {n}/{traffic_pods} {workload} {fault} {seed}: "
                          f"{row['admitted']}/{row['sent']} (bucket {row['one_bucket']}), "
                          f"over {row['worst_excess']}, failed {row['failed']}",
                          file=sys.stderr, flush=True)
                    if fault != "none":
                        restore(system, n)
                    time.sleep(GAP)
        down()


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("sweep")
    s.add_argument("out")
    s.add_argument("--systems", default="")
    s.add_argument("--sizes", default="2,4,8")
    s.add_argument("--workloads", default="")
    s.add_argument("--faults", default="none")
    s.add_argument("--seeds", type=int, default=3)
    s.add_argument("--fault-workloads", default="over-spread,bursts,within-spread",
                   help="the workloads a fault is played with")
    u = sub.add_parser("up")
    u.add_argument("system")
    u.add_argument("n", type=int)
    sub.add_parser("down")
    k = sub.add_parser("smoke", help="bring up, then 150 at once on each instance, twice")
    k.add_argument("system")
    k.add_argument("n", type=int)
    args = parser.parse_args()
    if args.command == "sweep":
        sweep(args)
    elif args.command == "smoke":
        system = SYSTEMS[args.system]
        up(system, args.n)
        for round_ in range(2):
            key = f"smoke-{random.randrange(1 << 30)}"
            per = [[0] * 150 for _ in range(args.n)]
            targets = [(front_ip(i), system.port) for i in range(args.n)]
            route = list(range(args.n))
            t0 = time.monotonic_ns() + int(0.5e9)

            async def all_pods():
                return await asyncio.gather(*[
                    load.play_async(p, per[p], targets, system.ask, key, t0, route, 150)
                    for p in range(args.n)])

            results = [r for pod in asyncio.run(all_pods()) for r in pod]
            print(json.dumps(summarise(results, [], args.n)))
            time.sleep(GAP)
    elif args.command == "up":
        up(SYSTEMS[args.system], args.n)
    else:
        down()


if __name__ == "__main__":
    main()
