#!/usr/bin/env python3
"""Sum resident KiB for a Linux process and its descendants, including nginx workers.

This is summed RSS, not unique physical memory: shared pages can be counted twice.
"""

import pathlib
import sys


def rss_tree(pid: int, proc: pathlib.Path = pathlib.Path('/proc')) -> int:
    pending = [pid]
    seen = set()
    total = 0
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        process = proc / str(current)
        try:
            status = (process / 'status').read_text()
            for line in status.splitlines():
                if line.startswith('VmRSS:'):
                    total += int(line.split()[1])
            for children in (process / 'task').glob('*/children'):
                pending.extend(map(int, children.read_text().split()))
        except FileNotFoundError:
            # A child may exit between discovery and reading its status.
            if current == pid:
                raise
    return total


if __name__ == '__main__':
    print(rss_tree(int(sys.argv[1])))
