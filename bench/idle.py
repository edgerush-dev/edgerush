#!/usr/bin/env python3
"""Opens connections to the proxy and holds them, so that what idle ones cost can be read.

What each connection does first is the fourth argument:

- `one-request` (the default): one request and then nothing more, which is what leaves the
  proxy holding a client connection and an upstream connection it may keep;
- `large-head`: the same, with a head of most of the 64 KiB a head may be, so that what a
  large head grew is seen to be let go of once it is answered;
- `silent`: nothing at all, which is what a connection costs before its first byte;
- `refreshed`: one request, and then another on each connection every 20 seconds, within
  the proxy's keep-alive deadline, so that a long run holds them the whole time; one the
  proxy closes anyway is opened again.

Says `ready` on its standard output once they are all answered, or all open, and then waits
to be killed.

    bench/idle.py 127.0.0.1:8080 bench.example.com 10000 [one-request|large-head|silent|refreshed]
"""

import socket
import sys
import time

READY = b"HTTP/1.1 "
# Within the proxy's 30-second keep-alive deadline, with room to spare.
REFRESH = 20


def answer(connection) -> bytes:
    """Reads one whole answer, its head and the body its length says, and returns the head.

    Raises `ConnectionError` if the connection ends first."""
    received = b""
    while b"\r\n\r\n" not in received:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("closed before the head ended")
        received += chunk
    head, rest = received.split(b"\r\n\r\n", 1)
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(rest) < length:
        chunk = connection.recv(4096)
        if not chunk:
            raise ConnectionError("closed before the body ended")
        rest += chunk
    return head


def opened(address, port, request):
    """A connection, asked once and answered."""
    connection = socket.create_connection((address, int(port)), timeout=30)
    if request is not None:
        connection.sendall(request)
        # Read the answer so that the exchange is over and the upstream connection is
        # back in the pool; an exchange still under way is not an idle connection.
        head = answer(connection)
        if not head.startswith(READY):
            raise ConnectionError(f"unexpected answer: {head[:40]!r}")
    return connection


def main() -> int:
    where, host, many = sys.argv[1], sys.argv[2], int(sys.argv[3])
    kind = sys.argv[4] if len(sys.argv) > 4 else "one-request"
    address, port = where.split(":")
    # Most of a head's 64 KiB, in fields of 4 KiB, as a large cookie or token would come.
    padding = "".join(f"x-pad-{n}: {'a' * 4000}\r\n" for n in range(14)) if kind == "large-head" else ""
    request = (
        f"GET / HTTP/1.1\r\nhost: {host}\r\nconnection: keep-alive\r\n{padding}\r\n".encode()
    )
    asked = None if kind == "silent" else request

    try:
        held = [opened(address, port, asked) for _ in range(many)]
    except (OSError, ConnectionError) as error:
        print(error, file=sys.stderr)
        return 1

    print(f"ready {len(held)}", flush=True)
    # Held until this process is killed. The sockets go when it does.
    try:
        if kind != "refreshed":
            while True:
                held[0].recv(1)
        while True:
            time.sleep(REFRESH)
            reopened = 0
            for at, connection in enumerate(held):
                try:
                    connection.sendall(request)
                    answer(connection)
                except (OSError, ConnectionError):
                    connection.close()
                    held[at] = opened(address, port, request)
                    reopened += 1
            if reopened:
                print(f"reopened {reopened}", file=sys.stderr, flush=True)
    except (OSError, ConnectionError, KeyboardInterrupt):
        return 0


if __name__ == "__main__":
    sys.exit(main())
