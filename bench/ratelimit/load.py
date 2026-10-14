"""Plays one instance's share of a run's arrivals, on HTTP/1.1 keep-alive connections, and
reports what became of each request. One process per instance, so that a burst on every
instance starts at once; all of them keep to the same monotonic clock (Linux's is
system-wide).

A request's target can change while it plays: `route[pod]` names the instance that pod's
traffic goes to, as a load balancer would steer it away from an instance restarting.
"""

import asyncio
import json
import time

TIMEOUT = 5.0


def http_request(ask, key):
    if ask == "gubernator":
        body = json.dumps(
            {
                "requests": [
                    {
                        "name": "rl",
                        "unique_key": key,
                        "hits": 1,
                        "limit": 100,
                        "duration": 1000,
                        "algorithm": "LEAKY_BUCKET",
                        "behavior": "GLOBAL",
                        "burst": 100,
                    }
                ]
            }
        ).encode()
        head = (
            "POST /v1/GetRateLimits HTTP/1.1\r\nHost: harness\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n"
        )
        return head.encode() + body
    return f"GET / HTTP/1.1\r\nHost: harness\r\nX-Key: {key}\r\n\r\n".encode()


async def read_response(reader):
    head = await reader.readuntil(b"\r\n\r\n")
    status = int(head[9:12])
    lower = head.lower()
    body = b""
    if b"transfer-encoding: chunked" in lower:
        while True:
            size = int((await reader.readuntil(b"\r\n")).strip().split(b";")[0], 16)
            body += (await reader.readexactly(size + 2))[:size]
            if size == 0:
                break
    else:
        index = lower.find(b"content-length:")
        if index >= 0:
            end = lower.find(b"\r\n", index)
            body = await reader.readexactly(int(lower[index + 15 : end]))
    return status, body


def outcome(ask, status, body):
    """'admitted', 'refused' or 'failed <why>'."""
    if ask == "gubernator":
        if status != 200:
            return f"failed {status}"
        answer = json.loads(body)["responses"][0]
        if answer.get("error"):
            return "failed error"
        return "refused" if answer.get("status") == "OVER_LIMIT" else "admitted"
    if status == 200:
        return "admitted"
    if status == 429:
        return "refused"
    return f"failed {status}"


class Connections:
    """Idle keep-alive connections per instance, opened as needed."""

    def __init__(self, targets):
        self.targets = targets
        self.idle = {i: [] for i in range(len(targets))}

    async def open(self, i):
        host, port = self.targets[i]
        return await asyncio.open_connection(host, port)

    async def take(self, i):
        if self.idle[i]:
            return self.idle[i].pop()
        return await self.open(i)

    def give(self, i, connection):
        self.idle[i].append(connection)

    def drop(self, i):
        """Closes every idle connection to instance `i`."""
        for _, writer in self.idle[i]:
            writer.close()
        self.idle[i] = []

    def close(self):
        for connections in self.idle.values():
            for _, writer in connections:
                writer.close()


async def one(connections, i, payload, ask, t0, at, pod, results):
    sent = time.monotonic_ns() - t0
    connection = None
    try:
        connection = await asyncio.wait_for(connections.take(i), TIMEOUT)
        reader, writer = connection
        writer.write(payload)
        status, body = await asyncio.wait_for(read_response(reader), TIMEOUT)
        result = outcome(ask, status, body)
        connections.give(i, connection)
    except Exception as error:  # noqa: BLE001 - counted, not raised
        result = f"failed {type(error).__name__}"
        if connection is not None:
            connection[1].close()
    done = time.monotonic_ns() - t0
    results.append((at, pod, i, sent, done, result))


async def play_async(pod, arrivals, targets, ask, key, t0, route, warm):
    connections = Connections(targets)
    for _ in range(warm):
        try:
            connections.give(pod, await connections.open(pod))
        except OSError:
            break
    payload = http_request(ask, key)
    results, tasks = [], []
    steered = False
    for at in arrivals:
        delay = (t0 + at - time.monotonic_ns()) / 1e9
        if delay > 0:
            await asyncio.sleep(delay)
        target = route[pod]
        # Back on its own instance after it restarted: the connections kept to it died with
        # the process it had, and a client would open new ones, as a load balancer's would.
        if target != pod:
            steered = True
        elif steered:
            steered = False
            connections.drop(pod)
        tasks.append(
            asyncio.create_task(one(connections, target, payload, ask, t0, at, pod, results))
        )
    await asyncio.gather(*tasks)
    connections.close()
    return results


def play(pod, arrivals, targets, ask, key, t0, route, warm, out):
    """Process entry: plays `arrivals` (ns from `t0`) for `pod`; puts its results on `out`."""
    out.put(asyncio.run(play_async(pod, arrivals, targets, ask, key, t0, route, warm)))
