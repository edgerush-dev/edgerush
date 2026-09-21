#!/usr/bin/env python3
"""Opens connections to the proxy and holds them, so that what idle ones cost can be read.

Every connection carries one request and then nothing more, which is what leaves the proxy
holding a client connection and an upstream connection it may keep. Says `ready` on its
standard output once they are all answered, and then waits to be killed.

    bench/idle.py 127.0.0.1:8080 bench.example.com 10000
"""

import socket
import sys

READY = b"HTTP/1.1 "


def main() -> int:
    where, host, many = sys.argv[1], sys.argv[2], int(sys.argv[3])
    address, port = where.split(":")
    request = (
        f"GET / HTTP/1.1\r\nhost: {host}\r\nconnection: keep-alive\r\n\r\n".encode()
    )

    held = []
    for _ in range(many):
        connection = socket.create_connection((address, int(port)), timeout=30)
        connection.sendall(request)
        # Read the answer so that the exchange is over and the upstream connection is
        # back in the pool; an exchange still under way is not an idle connection.
        answered = connection.recv(4096)
        if not answered.startswith(READY):
            print(f"unexpected answer: {answered[:40]!r}", file=sys.stderr)
            return 1
        held.append(connection)

    print(f"ready {len(held)}", flush=True)
    # Held until this process is killed. The sockets go when it does.
    try:
        while True:
            held[0].recv(1)
    except (OSError, KeyboardInterrupt):
        return 0


if __name__ == "__main__":
    sys.exit(main())
