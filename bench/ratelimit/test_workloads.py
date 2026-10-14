"""The harness's traffic and yardsticks, checked as the simulator's are."""

import random
import unittest

import workloads
from workloads import BURST, LIMIT, SECOND, SPAN, Bucket, arrivals, one_bucket, worst_excess


def brute_excess(times, limit, window, burst):
    best = None
    for i in range(len(times)):
        for j in range(i, len(times)):
            count = j - i + 1
            excess = (count * window - limit * (times[j] - times[i]) - burst * window) // window
            best = excess if best is None else max(best, excess)
    return best


class Excess(unittest.TestCase):
    def test_eight_at_once_against_a_bucket_of_five_is_three_over(self):
        self.assertEqual(worst_excess([0] * 8, limit=10, window=SECOND, burst=5), 3)

    def test_traffic_at_the_rate_after_a_full_bucket_is_never_over(self):
        times = [0] * BURST + [SECOND * k // LIMIT for k in range(1, 5 * LIMIT)]
        self.assertLessEqual(worst_excess(times), 0)

    def test_one_pass_finds_what_every_span_finds(self):
        rng = random.Random(7)
        for _ in range(200):
            times = sorted(rng.randrange(3 * SECOND) for _ in range(rng.randrange(1, 40)))
            limit, burst = rng.randrange(1, 20), rng.randrange(1, 10)
            self.assertEqual(
                worst_excess(times, limit=limit, window=SECOND, burst=burst),
                brute_excess(times, limit, SECOND, burst),
            )

    def test_nothing_admitted_has_no_excess(self):
        self.assertIsNone(worst_excess([]))


class TheBucket(unittest.TestCase):
    def test_a_new_bucket_admits_its_burst_at_once_and_no_more(self):
        bucket = Bucket()
        self.assertEqual(sum(bucket.decide(0) for _ in range(BURST + 5)), BURST)

    def test_it_refills_at_the_rate(self):
        bucket = Bucket()
        for _ in range(BURST):
            bucket.decide(0)
        self.assertFalse(bucket.decide(SECOND // LIMIT - 1))
        self.assertTrue(bucket.decide(SECOND // LIMIT))


class Traffic(unittest.TestCase):
    def test_spread_traffic_comes_at_about_its_rate_to_every_pod(self):
        sent = arrivals("over-spread", 4, 1)
        self.assertAlmostEqual(len(sent) / (SPAN / SECOND), 3 * LIMIT, delta=3 * LIMIT * 0.05)
        self.assertEqual({pod for _, pod in sent}, {0, 1, 2, 3})

    def test_uneven_traffic_sends_about_half_to_the_last_pod(self):
        sent = arrivals("edge-uneven", 8, 1)
        last = sum(1 for _, pod in sent if pod == 7) / len(sent)
        self.assertAlmostEqual(last, 0.5 + 0.5 / 8, delta=0.05)

    def test_skewed_traffic_lands_on_the_last_pod(self):
        self.assertEqual({pod for _, pod in arrivals("within-one", 4, 1)}, {3})

    def test_bursts_hit_every_pod_at_the_same_instant(self):
        sent = arrivals("bursts", 4, 1)
        self.assertEqual(len(sent), 10 * 4 * BURST)
        first = [pod for at, pod in sent if at == 0]
        self.assertEqual(sorted(set(first)), [0, 1, 2, 3])
        self.assertEqual(len(first), 4 * BURST)

    def test_one_pod_then_bursts_switches_half_way(self):
        sent = arrivals("one-then-bursts", 4, 1)
        self.assertEqual({pod for at, pod in sent if at < SPAN // 2}, {3})
        self.assertEqual(sum(1 for at, _ in sent if at == SPAN // 2), 4 * BURST)

    def test_conforming_traffic_is_all_admitted_by_one_bucket(self):
        for workload, (_, _, conforming) in workloads.WORKLOADS.items():
            if conforming:
                times = [at for at, _ in arrivals(workload, 8, 3)]
                self.assertEqual(one_bucket(times), len(times), workload)
                self.assertLessEqual(worst_excess(times), 0, workload)

    def test_the_same_seed_draws_the_same_traffic(self):
        self.assertEqual(arrivals("edge-uneven", 8, 5), arrivals("edge-uneven", 8, 5))
        self.assertNotEqual(arrivals("edge-uneven", 8, 5), arrivals("edge-uneven", 8, 6))


if __name__ == "__main__":
    unittest.main()
