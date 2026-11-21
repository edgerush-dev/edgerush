#!/usr/bin/env python3
"""Whether a future's field comes first in the state it starts in, from rustc's
`-Zprint-type-sizes`, for check.sh.

A request's future is made in its slot only while its head is first in it (14 §3): put
anywhere else, the future is made beside the slot and all 6 KB of it copied in, and which
it is is the compiler's choice of field order. So the check reads the layout the binary
was built with, rather than a benchmark after the fact.

    layout.py TYPE FIELD    reads -Zprint-type-sizes output on stdin; exits 0 if FIELD is
                            at offset 0 of TYPE's `Unresumed` variant, 1 if it is not, 2
                            if TYPE or its variant is not there to look at
"""

import sys

PREFIX = "print-type-size "


def first_field(text, type_name):
    """The first field of `type_name`'s `Unresumed` variant, as (name, offset), the offset
    None where rustc says none; None if the type or the variant is not in `text`."""
    lines = [line[len(PREFIX):] for line in text.splitlines() if line.startswith(PREFIX)]
    heading = f"type: `{type_name}`: "
    for at, line in enumerate(lines):
        if not line.startswith(heading):
            continue
        variant = None
        for field in lines[at + 1:]:
            if field.startswith("type: "):
                return None
            entry = field.strip()
            if entry.startswith("variant "):
                variant = entry.split("`")[1]
                continue
            if variant != "Unresumed":
                continue
            if entry.startswith("padding:"):
                return (f"padding of {entry.split()[1]} bytes", 0)
            name = entry.split("`")[1]
            offset = None
            for part in entry.split(", "):
                if part.startswith("offset: "):
                    offset = int(part.split()[1])
            return (name, offset)
        return None
    return None


def main(argv, text):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    type_name, field = argv[1], argv[2]
    found = first_field(text, type_name)
    if found is None:
        print(f"no `Unresumed` variant of `{type_name}` in the layouts read", file=sys.stderr)
        return 2
    if found != (field, 0):
        name, offset = found
        print(
            f"`{field}` is not first in `{type_name}`: `{name}` is, at offset {offset}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv, sys.stdin.read()))
