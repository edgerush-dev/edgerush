#!/usr/bin/env python3
"""What the proxy's user-space instructions went on, by the part of the program they are in.

Reads `perf report -s sym` on the standard input — samples of `instructions:u`, so a
percentage here is a share of the instructions a worker actually ran, not of its time. A
symbol says which crate it came from, so the parts below are crates; what the compiler
inlined into a caller is counted with the caller, which is worth remembering before
reading much into a small difference.

    perf report -i perf.data --stdio --no-children -s sym --percent-limit 0 | bench/buckets.py
"""

import re
import sys
from collections import defaultdict

PARTS = {
    # The engine proper: what serves the client and what carried a request upstream
    # before EdgeRush's own path did.
    "the HTTP engine": {"hyper", "hyper_util", "h2", "http_body", "http_body_util"},
    # Header types and head parsing, kept apart because both paths call them directly:
    # counting them with the engine would flatter whichever path is not hyper's.
    "headers and parsing": {"http", "httparse"},
    "the runtime": {
        "tokio", "mio", "futures_util", "futures_core", "futures_task", "socket2", "slab",
    },
    "EdgeRush": {
        "edgerush", "edgerush_proxy", "edgerush_router", "edgerush_config",
        "edgerush_telemetry", "edgerush_filters",
    },
    "std and core": {"core", "std", "alloc", "hashbrown", "bytes", "memchr", "regex"},
}

# What the C library is doing on our behalf, which has no crate in its name.
MEMORY = re.compile(r"^(__)?(memmove|memcpy|memset|malloc|free|cfree|realloc|_int_|tcache|arena_|calloc)")

# A sample line: "     7.14%  [.] <edgerush_proxy::…>::read"
SAMPLE = re.compile(r"^\s*(\d+\.\d+)%\s+\[([.k])\]\s+(.*?)\s*$")


def crate_of(symbol: str) -> str:
    """The crate a symbol came from: what stands before the first path separator."""
    name = symbol.lstrip("<&*_ ")
    return re.split(r"[:<>(,\[ ]", name, maxsplit=1)[0]


def part_of(symbol: str, where: str) -> str:
    if where == "k":
        return "the kernel"
    if MEMORY.match(symbol):
        return "memory"
    crate = crate_of(symbol)
    for name, crates in PARTS.items():
        if crate in crates:
            return name
    return "everything else"


def main() -> int:
    parts = defaultdict(float)
    symbols = defaultdict(float)
    for line in sys.stdin:
        found = SAMPLE.match(line)
        if not found:
            continue
        share, where, symbol = float(found.group(1)), found.group(2), found.group(3)
        parts[part_of(symbol, where)] += share
        symbols[(part_of(symbol, where), symbol)] += share

    if not parts:
        print("no samples read", file=sys.stderr)
        return 1

    print("| part | share of user instructions |")
    print("|---|---|")
    for name, share in sorted(parts.items(), key=lambda pair: -pair[1]):
        print(f"| {name} | {share:.1f}% |")

    print()
    print("| part | symbol | share |")
    print("|---|---|---|")
    for (part, symbol), share in sorted(symbols.items(), key=lambda pair: -pair[1])[:20]:
        # A generic symbol can be a paragraph long; what is wanted is which function.
        short = symbol if len(symbol) <= 90 else symbol[:87] + "..."
        print(f"| {part} | `{short}` | {share:.1f}% |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
