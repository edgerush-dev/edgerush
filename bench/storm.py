#!/usr/bin/env python3
"""Opens connections to a listener as fast as it can and holds them, so that the overload
ladder's first rung — the per-worker connection cap and the per-listener share ([03 §9]) —
is seen to engage, and to engage cleanly.

This is not an attack tool: it points at an EdgeRush instance you run, over loopback, and
drives the gateway's *own* accept path. A SYN flood would not — half-open connections pile
up in the kernel's SYN backlog and `accept` never returns them, so none of EdgeRush's code
runs. These are completed connections, which is what the cap counts.

What to watch, read from `--metrics` while the storm grows:

- `edgerush_listener_connections_active` plateaus at the worker's cap rather than climbing
  with the client's count. The gap between the two is the kernel's accept backlog: once a
  worker stops calling `accept`, the handshake still completes, so the client's `connect`
  keeps returning — the server simply never takes the socket. That divergence is the
  clearest sign the cap has engaged.
- `edgerush_listener_accept_paused_total{reason="worker_cap"}` climbs while a worker sits at
  its cap, and `{reason="share"}` climbs while a listener holds its 7/8 share and others
  carry on. Pass `--probe` at a second listener to confirm that one still accepts: that is
  the share rung, the flood on `:80` that must not stop `:443`.
- `edgerush_listener_accept_errors_total` should *not* climb: a worker stops at its cap,
  where the pause costs nothing, not at the descriptors running out, where accepting fails.

Connections are silent by default (nothing sent), which needs no backend and is what a
connection costs before its first byte. `--request` sends one GET first, which needs the
backend up.

Reaching a worker's full 32,768 takes more than one source address can hold: a client has
about 28,000 ephemeral ports to a single destination. On Linux the whole of 127.0.0.0/8 is
loopback, so `--source-ips` spreads the connections across many client addresses, each with
its own port range; it is on by default when the target is loopback. The quicker way to see
the rung is to give the proxy a small memory limit (a cgroup, or a low `--workers` on a
limited pod) so its cap is a few thousand, within one address's reach.

    bench/storm.py --target 127.0.0.1:8080 --metrics 127.0.0.1:9100 --count 40000
    bench/storm.py --target 127.0.0.1:8080 --metrics 127.0.0.1:9100 --probe 127.0.0.1:8443

Run the proxy with its metrics socket open, e.g.

    edgerush proxy --config bench/proxy.yaml --workers 4 --metrics 127.0.0.1:9100
"""

import argparse
import errno
import ipaddress
import platform
import socket
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass, field

# The client ran out of ephemeral ports or source addresses — a limit of this tool, not of
# the gateway. The errno module gives POSIX values on Linux and the matching Winsock values
# on Windows, so one set serves both.
EXHAUSTED_ERRNOS = {errno.EADDRNOTAVAIL, errno.EADDRINUSE}

# The series this tool reads from /metrics. Everything else is ignored.
WATCHED = {
    "edgerush_listener_connections_active",
    "edgerush_listener_connections_accepted_total",
    "edgerush_listener_accept_paused_total",
    "edgerush_listener_accept_errors_total",
}


def parse_metrics(text):
    """Parses the Prometheus text we care about into (name, labels, value) triples.

    Only the series in `WATCHED` are kept; comments, help and everything else are dropped.
    `labels` is a dict, empty when the series has none."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        series, _, value = line.rpartition(" ")
        if not series:
            continue
        name, _, rest = series.partition("{")
        if name not in WATCHED:
            continue
        labels = {}
        if rest:
            for pair in rest.rstrip("}").split(","):
                key, _, raw = pair.partition("=")
                labels[key.strip()] = raw.strip().strip('"')
        try:
            out.append((name, labels, float(value)))
        except ValueError:
            continue
    return out


def metric_sum(triples, name, **labels):
    """Sums the value of every `name` triple whose labels include all of `labels`."""
    total = 0.0
    for got_name, got_labels, value in triples:
        if got_name != name:
            continue
        if all(got_labels.get(key) == want for key, want in labels.items()):
            total += value
    return total


def source_addresses(many):
    """`many` distinct loopback addresses in 127.0.0.0/8, from 127.0.0.2 up, skipping those
    that end in .0 or .255. Deterministic, so a run is repeatable."""
    first = int(ipaddress.IPv4Address("127.0.0.2"))
    last = int(ipaddress.IPv4Address("127.255.255.254"))
    out = []
    candidate = first
    while len(out) < many:
        if candidate > last:
            raise ValueError(f"127.0.0.0/8 holds fewer than {many} usable addresses")
        text = str(ipaddress.IPv4Address(candidate))
        candidate += 1
        if int(text.rsplit(".", 1)[1]) in (0, 255):
            continue
        out.append(text)
    return out


def percentiles(samples, points):
    """Nearest-rank percentiles (`points` in 0..100) of `samples`, in milliseconds as given.

    Returns a dict keyed by each point; empty samples give zeros."""
    if not samples:
        return {point: 0.0 for point in points}
    ordered = sorted(samples)
    out = {}
    for point in points:
        rank = max(1, min(len(ordered), (point * len(ordered) + 99) // 100))
        out[point] = ordered[rank - 1]
    return out


@dataclass
class Shared:
    """What the connect threads and the reporter share. Guarded by `lock`."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    held: list = field(default_factory=list)  # established sockets, held open
    connect_ms: list = field(default_factory=list)  # connect() wall time, milliseconds
    established: int = 0
    refused: int = 0
    exhausted: int = 0  # no ephemeral port / address left (EADDRNOTAVAIL, EADDRINUSE)
    timed_out: int = 0
    other_errors: int = 0
    stop: bool = False


def one_connection(target, sources, index, timeout, request, shared):
    """Opens one connection, binding to a rotating source address when `sources` is given,
    records it, and returns True on success. Counts the failure and returns False otherwise."""
    host, port = target
    family = socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        if sources:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((sources[index % len(sources)], 0))
        sock.settimeout(timeout)
        started = time.monotonic()
        sock.connect((host, port))
        elapsed_ms = (time.monotonic() - started) * 1000.0
        if request is not None:
            sock.sendall(request)
        with shared.lock:
            shared.held.append(sock)
            shared.connect_ms.append(elapsed_ms)
            shared.established += 1
        return True
    except (socket.timeout, TimeoutError):
        shared_count(shared, "timed_out")
    except (ConnectionRefusedError, ConnectionResetError):
        shared_count(shared, "refused")
    except OSError as error:
        # Port/address exhaustion is kept apart so it does not hide a real refusal.
        shared_count(shared, "exhausted" if error.errno in EXHAUSTED_ERRNOS else "other_errors")
    sock.close()
    return False


def shared_count(shared, field_name):
    with shared.lock:
        setattr(shared, field_name, getattr(shared, field_name) + 1)


def cannot_progress(exhausted, refused, timed_out, concurrency):
    """An unbounded storm gives up once persistent connect failures — ports or addresses
    exhausted, connections refused, or connects timed out — reach the storm's width: at that
    point every thread is failing rather than establishing, so the client has grown as far
    as the kernel and the gateway will let it. A healthy run against a listener with room
    has no such failures and keeps going until the client runs out of ports."""
    return exhausted + refused + timed_out >= concurrency


def storm(target, count, concurrency, rate, sources, timeout, request, shared):
    """Runs `concurrency` threads opening connections until `count` are established (or, when
    `count` is None, until the client runs out of ports), pacing to `rate` a second if set."""
    counter = {"next": 0}
    counter_lock = threading.Lock()
    pace_lock = threading.Lock()
    pace = {"next_at": time.monotonic()}
    unbounded = count is None

    def worker():
        while not shared.stop:
            with shared.lock:
                enough = count is not None and shared.established >= count
                stuck = unbounded and cannot_progress(
                    shared.exhausted, shared.refused, shared.timed_out, concurrency
                )
            # Stop when the target is reached, or when an unbounded run can grow no further
            # — whether the client ran out of ports or the server stopped taking connections.
            if enough or stuck:
                return
            if rate:
                with pace_lock:
                    now = time.monotonic()
                    wait = pace["next_at"] - now
                    pace["next_at"] = max(now, pace["next_at"]) + 1.0 / rate
                if wait > 0:
                    time.sleep(wait)
                if shared.stop:
                    return
            with counter_lock:
                mine = counter["next"]
                counter["next"] += 1
            one_connection(target, sources, mine, timeout, request, shared)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    for thread in threads:
        thread.start()
    return threads


def scrape(url, timeout=5):
    """Fetches and parses /metrics, or returns None if it cannot be reached."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as answer:
            return parse_metrics(answer.read().decode("utf-8", "replace"))
    except (OSError, ValueError):
        return None


def probe_once(target, timeout):
    """Opens and closes one connection to `target`; returns its connect time in ms, or None
    if it did not accept within `timeout`."""
    try:
        started = time.monotonic()
        with socket.create_connection(target, timeout=timeout):
            return (time.monotonic() - started) * 1000.0
    except OSError:
        return None


def raise_open_file_limit():
    """Raises this process's open-file soft limit to its hard limit, so the client can hold
    as many sockets as it opens. Linux and macOS only; a no-op elsewhere. Returns the soft
    limit now in force."""
    try:
        import resource
    except ImportError:
        return None
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < hard:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            soft = hard
        except (ValueError, OSError):
            pass
    return soft


def address(text):
    host, _, port = text.rpartition(":")
    if not host or not port:
        raise argparse.ArgumentTypeError(f"expected HOST:PORT, got {text!r}")
    return (host, int(port))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Storm a listener with held connections.")
    parser.add_argument("--target", type=address, required=True, help="the listener to storm, HOST:PORT")
    parser.add_argument("--count", type=int, default=None, help="connections to reach (default: as many as the client can)")
    parser.add_argument("--concurrency", type=int, default=200, help="parallel connect threads (default 200)")
    parser.add_argument("--rate", type=float, default=0.0, help="cap new connects a second (default: unlimited)")
    parser.add_argument("--hold", type=float, default=30.0, help="seconds to hold the storm after it stops growing (default 30)")
    parser.add_argument("--request", action="store_true", help="send one GET before holding (needs the backend up)")
    parser.add_argument("--host", default="bench.example.com", help="Host header for --request")
    parser.add_argument("--connect-timeout", type=float, default=5.0, help="per-connection timeout, seconds (default 5)")
    parser.add_argument("--slow-ms", type=float, default=100.0, help="count connects slower than this, ms (default 100)")
    parser.add_argument("--metrics", type=address, default=None, help="scrape /metrics here, HOST:PORT")
    parser.add_argument("--probe", type=address, default=None, help="check this listener still accepts, once a second")
    parser.add_argument("--source-ips", type=int, default=None, help="rotate client source addresses across 127.0.0.0/8 (Linux; default: 250 for a loopback target, else 0)")
    args = parser.parse_args(argv)

    host = args.target[0]
    loopback = host.startswith("127.") or host in ("localhost", "::1")
    on_linux = platform.system() == "Linux"
    want_sources = args.source_ips if args.source_ips is not None else (250 if loopback else 0)
    sources = []
    if want_sources:
        if not on_linux:
            print("source-ip rotation is Linux-only; one address will be used", file=sys.stderr)
        elif not loopback:
            print("source-ip rotation only makes sense for a loopback target; ignored", file=sys.stderr)
        else:
            sources = source_addresses(want_sources)

    soft = raise_open_file_limit()
    if soft is not None and args.count is not None and soft < args.count + 128:
        print(f"open-file limit is {soft}; may not hold {args.count} connections", file=sys.stderr)

    request = None
    if args.request:
        request = (
            f"GET / HTTP/1.1\r\nhost: {args.host}\r\nconnection: keep-alive\r\n\r\n".encode()
        )

    shared = Shared()
    baseline = scrape(f"http://{args.metrics[0]}:{args.metrics[1]}/metrics") if args.metrics else None
    peak_active = 0.0
    probe_down = 0

    print(
        f"storming {host}:{args.target[1]} "
        f"{'as many as possible' if args.count is None else f'to {args.count}'}, "
        f"{len(sources) or 1} source address(es), concurrency {args.concurrency}",
        flush=True,
    )
    started = time.monotonic()
    threads = storm(
        args.target, args.count, args.concurrency, args.rate, sources,
        args.connect_timeout, request, shared,
    )

    # Report once a second while the storm grows, then for `--hold` seconds after it is done.
    settled_at = None
    try:
        while True:
            time.sleep(1.0)
            with shared.lock:
                established = shared.established
                refused, exhausted, timed_out, others = (
                    shared.refused, shared.exhausted, shared.timed_out, shared.other_errors,
                )
            triples = scrape(f"http://{args.metrics[0]}:{args.metrics[1]}/metrics") if args.metrics else None
            active = worker_cap = share = errors = None
            if triples is not None:
                active = metric_sum(triples, "edgerush_listener_connections_active")
                worker_cap = metric_sum(triples, "edgerush_listener_accept_paused_total", reason="worker_cap")
                share = metric_sum(triples, "edgerush_listener_accept_paused_total", reason="share")
                errors = metric_sum(triples, "edgerush_listener_accept_errors_total")
                peak_active = max(peak_active, active)
            probe_ms = probe_once(args.probe, args.connect_timeout) if args.probe else None
            if args.probe and probe_ms is None:
                probe_down += 1

            line = [f"t={time.monotonic() - started:5.1f}s established={established}"]
            if active is not None:
                gap = established - active
                line.append(f"server_active={active:.0f} backlog_gap={gap:.0f}")
                line.append(f"paused[worker_cap={worker_cap:.0f} share={share:.0f}] accept_errors={errors:.0f}")
            if refused or exhausted or timed_out or others:
                line.append(f"client[refused={refused} exhausted={exhausted} timeout={timed_out} other={others}]")
            if args.probe:
                line.append(f"probe={'DOWN' if probe_ms is None else f'{probe_ms:.1f}ms'}")
            print("  ".join(line), flush=True)

            alive = any(thread.is_alive() for thread in threads)
            if not alive and settled_at is None:
                settled_at = time.monotonic()
                print(f"storm settled at {established} established; holding {args.hold:.0f}s", flush=True)
            if settled_at is not None and time.monotonic() - settled_at >= args.hold:
                break
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)

    shared.stop = True
    with shared.lock:
        held = len(shared.held)
        connect_ms = list(shared.connect_ms)
        slow = sum(1 for ms in connect_ms if ms > args.slow_ms)
        totals = (shared.established, shared.refused, shared.exhausted, shared.timed_out, shared.other_errors)

    # Let go of every connection, then read the metrics once more to see accepting resume.
    with shared.lock:
        for sock in shared.held:
            sock.close()
        shared.held.clear()
    recovered = None
    if args.metrics:
        time.sleep(1.0)
        triples = scrape(f"http://{args.metrics[0]}:{args.metrics[1]}/metrics")
        if triples is not None:
            recovered = metric_sum(triples, "edgerush_listener_connections_active")

    pct = percentiles(connect_ms, [50, 90, 99, 100])
    print("\n--- storm over ---")
    print(f"established={totals[0]} held_at_end={held}")
    print(f"client failures: refused={totals[1]} port/addr-exhausted={totals[2]} timeout={totals[3]} other={totals[4]}")
    print(f"connect ms: p50={pct[50]:.1f} p90={pct[90]:.1f} p99={pct[99]:.1f} max={pct[100]:.1f}  slower-than-{args.slow_ms:.0f}ms={slow}")
    if args.metrics:
        base_active = metric_sum(baseline, "edgerush_listener_connections_active") if baseline else 0.0
        print(f"server connections_active: baseline={base_active:.0f} peak={peak_active:.0f}"
              + (f" after-release={recovered:.0f}" if recovered is not None else ""))
        if baseline is not None:
            base_cap = metric_sum(baseline, "edgerush_listener_accept_paused_total", reason="worker_cap")
            base_share = metric_sum(baseline, "edgerush_listener_accept_paused_total", reason="share")
            print(f"accept_paused over the run: worker_cap+={worker_cap - base_cap:.0f} share+={share - base_share:.0f}")
    if args.probe:
        print(f"probe listener failed to accept on {probe_down} of the second-by-second checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
