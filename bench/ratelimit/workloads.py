"""The traffic of a run and how it is judged: the simulator's workloads
(crates/ratelimit-sim/src/workload.rs) played against real systems.

Times are integer nanoseconds from the start of a run, so that the arithmetic is exact.
"""

import random

SECOND = 1_000_000_000

# The contract every system is configured with, and the simulator measures against.
LIMIT = 100
WINDOW = SECOND
BURST = 100
# Ten windows a run, as the simulator's.
SPAN = 10 * WINDOW


class Bucket:
    """A token bucket holding the contract: `limit` every `window`, `burst` at most at
    once, full when new. Tokens are kept multiplied by the window, so refills are exact."""

    def __init__(self, limit=LIMIT, window=WINDOW, burst=BURST):
        self.limit, self.window = limit, window
        self.capacity = burst * window
        self.tokens = self.capacity
        self.at = None

    def decide(self, at):
        if self.at is not None:
            self.tokens = min(self.capacity, self.tokens + self.limit * (at - self.at))
        self.at = at
        if self.tokens >= self.window:
            self.tokens -= self.window
            return True
        return False


def worst_excess(times, limit=LIMIT, window=WINDOW, burst=BURST):
    """The most requests admitted at `times` (sorted) over what the contract allows in any
    span from one admission to another: count − (limit × span / window + burst), floored.
    Negative when every span keeps the contract; None with nothing admitted."""
    if not times:
        return None
    best = None
    lowest = None
    for j, t in enumerate(times):
        here = j * window - limit * t
        lowest = here if lowest is None else min(lowest, here)
        excess = (j + 1) * window - limit * t - lowest
        best = excess if best is None else max(best, excess)
    return (best - burst * window) // window


def spread(rng, start, until, per_window, pick):
    """About `per_window` requests a window, each gap between half and one and a half times
    the mean, landing on the pod `pick` chooses."""
    out = []
    mean = max(WINDOW // per_window, 1)
    at = start
    while at < until:
        out.append((at, pick()))
        at += mean // 2 + rng.randrange(mean + 1)
    return out


def bursts(start, until, each, pods):
    """`each` requests on every pod at once, every window."""
    out = []
    at = start
    while at < until:
        out.extend((at, pod) for pod in range(pods) for _ in range(each))
        at += WINDOW
    return out


# Name: (shape, conforming). Conforming traffic keeps only what one bucket holding the
# contract admits, so every refusal of it is a false one.
WORKLOADS = {
    "within-spread": ("spread", LIMIT, True),
    "within-one": ("skewed", LIMIT, True),
    "edge-spread": ("spread", 3 * LIMIT, True),
    "edge-one": ("skewed", 3 * LIMIT, True),
    "edge-uneven": ("uneven", 3 * LIMIT, True),
    "over-spread": ("spread", 3 * LIMIT, False),
    "bursts": ("bursts", BURST, False),
    "one-then-bursts": ("skewed-then-bursts", LIMIT, False),
}


def arrivals(workload, pods, seed):
    """The requests of a run, (nanoseconds, pod), in the order they arrive."""
    shape, rate, conforming = WORKLOADS[workload]
    rng = random.Random(seed)
    last = pods - 1

    def anywhere():
        return rng.randrange(pods)

    def uneven():
        return last if rng.randrange(100) < 50 else rng.randrange(pods)

    if shape == "spread":
        out = spread(rng, 0, SPAN, rate, anywhere)
    elif shape == "skewed":
        out = spread(rng, 0, SPAN, rate, lambda: last)
    elif shape == "uneven":
        out = spread(rng, 0, SPAN, rate, uneven)
    elif shape == "bursts":
        out = bursts(0, SPAN, rate, pods)
    else:
        middle = SPAN // 2
        out = spread(rng, 0, middle, rate, lambda: last) + bursts(middle, SPAN, BURST, pods)
    out.sort(key=lambda arrival: arrival[0])
    if conforming:
        bucket = Bucket()
        out = [arrival for arrival in out if bucket.decide(arrival[0])]
    return out


def one_bucket(times):
    """How many of the requests sent at `times` (sorted) one bucket holding the contract
    admits: the simulator's 'central', what the fleet's admissions are measured against."""
    bucket = Bucket()
    return sum(1 for t in times if bucket.decide(t))
