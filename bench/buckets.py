#!/usr/bin/env python3
"""What the proxy's user-space instructions went on, by the part of the program they are in.

Reads `perf report -s sym` on the standard input — samples of `instructions:u`, so a
percentage here is a percentage of the instructions a worker actually ran, not of time.
Symbols say which crate they came from, so the parts below are prefixes; what is inlined
into a caller is counted with the caller, which is worth remembering before reading much
into a small difference.

    perf report -i perf.data --stdio --no-children -s sym --percent-limit 0 | bench/buckets.py
"""

import re
import sys
from collections import defaultdict

# In order: the first that matches wins, so the narrower ones come first.
PARTS = [
    ("the HTTP engine", ("hyper::", "hyper_util::", "h2::", "http::", "httparse::", "http_body")),
    ("the runtime", ("tokio::", "mio::", "futures_util::", "futures_core::", "<tokio")),
    ("EdgeRush", ("edgerush", "<edgerush")),
    ("memory", ("malloc", "free", "cfree", "_int_", "tcache", "alloc::", "__libc_malloc", "arena")),
    ("std and core", ("core::", "std::", "__rust", "<core", "<std", "memcpy", "memset", "memmove", "__memmove", "__memcpy", "__memset")),
]

# A sample line: "    12.34%  edgerush  [.] edgerush_proxy::request::decide"
SAMPLE = re.compile(r"^\s*(\d+\.\d+)%\s+\S+\s+\[([.k])\]\s+(.*?)\s*$")


def part_of(symbol: str, where: str) -> str:
    if where == "k":
        return "the kernel"
    for name, prefixes in PARTS:
        if symbol.startswith(prefixes):
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
        symbols[symbol] += share

    if not parts:
        print("no samples read", file=sys.stderr)
        return 1

    print("| part | share of user instructions |")
    print("|---|---|")
    for name, share in sorted(parts.items(), key=lambda pair: -pair[1]):
        print(f"| {name} | {share:.1f}% |")

    print()
    print("| symbol | share |")
    print("|---|---|")
    for symbol, share in sorted(symbols.items(), key=lambda pair: -pair[1])[:25]:
        print(f"| `{symbol}` | {share:.1f}% |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
