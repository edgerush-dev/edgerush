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
  proxy closes anyway is opened again;
- `h2`: one request over cleartext HTTP/2 with prior knowledge, answered, and then nothing
  more: what an idle HTTP/2 connection costs;
- `websocket`: a WebSocket handshake at `/ws`, switched, and then nothing more: what an open
  WebSocket costs while no message moves (a tunnel, and its backend's connection);
- `tls` and `tls-h2`: `one-request` and `h2` over TLS, to a listener that speaks it (the
  bench's certificate is not checked), HTTP/2 asked for by ALPN: what an idle TLS
  connection costs, which a pod's memory pays for too.

Says `ready` on its standard output once they are all answered, or all open, and then waits
to be killed.

    bench/idle.py 127.0.0.1:8080 bench.example.com 10000 [one-request|large-head|silent|refreshed|h2|websocket|tls|tls-h2]
"""

import socket
import ssl
import sys
import time

READY = b"HTTP/1.1 "
SWITCHED = b"HTTP/1.1 101 "
# Any key will do: the backend answers whichever it is sent (RFC 6455 §4.1).
WEBSOCKET_KEY = "dGhlIHNhbXBsZSBub25jZQ=="
H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
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


def h2_frame(kind: int, flags: int, stream: int, payload: bytes) -> bytes:
    """One HTTP/2 frame (RFC 9113 §4.1)."""
    return len(payload).to_bytes(3, "big") + bytes([kind, flags]) + stream.to_bytes(4, "big") + payload


def h2_request(host: str, scheme: str = "http") -> bytes:
    """A client's preface, empty SETTINGS and a GET on stream 1 that ends it.

    The header block is literal fields without indexing or Huffman coding (RFC 7541
    §6.2.2), each name and value under 127 bytes so that its length is one byte."""
    fields = [(":method", "GET"), (":scheme", scheme), (":authority", host), (":path", "/")]
    block = b"".join(
        b"\x00" + bytes([len(name)]) + name.encode() + bytes([len(value)]) + value.encode()
        for name, value in fields
    )
    end_stream_and_headers = 0x1 | 0x4
    return H2_PREFACE + h2_frame(0x4, 0, 0, b"") + h2_frame(0x1, end_stream_and_headers, 1, block)


def h2_answer(connection) -> bytes:
    """Reads frames until stream 1 ends, acknowledging the server's SETTINGS on the way,
    and returns the first header block on stream 1.

    Raises `ConnectionError` if the connection ends first, or the stream is reset."""
    received = b""
    head = None
    while True:
        while len(received) < 9 or len(received) < 9 + int.from_bytes(received[:3], "big"):
            chunk = connection.recv(4096)
            if not chunk:
                raise ConnectionError("closed before stream 1 ended")
            received += chunk
        length = int.from_bytes(received[:3], "big")
        kind, flags = received[3], received[4]
        stream = int.from_bytes(received[5:9], "big") & 0x7FFFFFFF
        payload, received = received[9 : 9 + length], received[9 + length :]
        if kind == 0x4 and not flags & 0x1:
            connection.sendall(h2_frame(0x4, 0x1, 0, b""))
        elif kind == 0x3 and stream == 1:
            raise ConnectionError("stream 1 was reset")
        elif stream == 1 and kind in (0x0, 0x1):
            if kind == 0x1 and head is None:
                head = payload
            if flags & 0x1:
                return head


def opened(address, port, request, http2=False, expect=READY, tls=None):
    """A connection, asked once and answered with a head that starts with `expect`; over
    TLS to the name `tls`, if it is given."""
    connection = socket.create_connection((address, int(port)), timeout=30)
    if tls is not None:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.set_alpn_protocols(["h2"] if http2 else ["http/1.1"])
        connection = context.wrap_socket(connection, server_hostname=tls)
    if request is not None:
        connection.sendall(request)
        # Read the answer so that the exchange is over and the upstream connection is
        # back in the pool; an exchange still under way is not an idle connection.
        if http2:
            if h2_answer(connection) is None:
                raise ConnectionError("stream 1 ended without a head")
        else:
            head = answer(connection)
            if not head.startswith(expect):
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
    handshake = (
        f"GET /ws HTTP/1.1\r\nhost: {host}\r\nupgrade: websocket\r\nconnection: upgrade\r\n"
        f"sec-websocket-version: 13\r\nsec-websocket-key: {WEBSOCKET_KEY}\r\n\r\n".encode()
    )
    asked = {
        "silent": None,
        "h2": h2_request(host),
        "tls-h2": h2_request(host, "https"),
        "websocket": handshake,
    }.get(kind, request)
    expect = SWITCHED if kind == "websocket" else READY
    http2 = kind in ("h2", "tls-h2")
    tls = host if kind in ("tls", "tls-h2") else None

    try:
        held = [opened(address, port, asked, http2, expect, tls) for _ in range(many)]
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
