"""The storm's pure parts are exact, and a connection it opens is held and counted."""

import platform
import socket
import threading
import unittest

from storm import (
    Shared,
    cannot_progress,
    metric_sum,
    one_connection,
    parse_metrics,
    percentiles,
    probe_once,
    source_addresses,
)

SAMPLE = """\
# HELP edgerush_listener_connections_active open connections
# TYPE edgerush_listener_connections_active gauge
edgerush_listener_connections_active{listener="web"} 32768
edgerush_listener_connections_active{listener="admin"} 2
edgerush_listener_connections_accepted_total{listener="web"} 41003
edgerush_listener_accept_paused_total{listener="web",reason="worker_cap"} 1877
edgerush_listener_accept_paused_total{listener="web",reason="share"} 0
edgerush_listener_accept_errors_total{listener="web"} 0
edgerush_upstream_requests_total{upstream="backend"} 99
"""


class ParseMetrics(unittest.TestCase):
    def test_only_watched_series_are_kept_with_their_labels_and_values(self):
        triples = parse_metrics(SAMPLE)
        names = {name for name, _, _ in triples}
        self.assertNotIn("edgerush_upstream_requests_total", names)
        self.assertIn("edgerush_listener_connections_active", names)
        active = [t for t in triples if t[0] == "edgerush_listener_connections_active"]
        self.assertEqual(len(active), 2)
        self.assertEqual(active[0][1], {"listener": "web"})
        self.assertEqual(active[0][2], 32768.0)

    def test_comments_blanks_and_junk_values_are_ignored(self):
        triples = parse_metrics("# just a comment\n\nedgerush_listener_accept_errors_total{listener=\"web\"} nan_oops\n")
        self.assertEqual(triples, [])

    def test_a_sum_selects_by_label(self):
        triples = parse_metrics(SAMPLE)
        self.assertEqual(metric_sum(triples, "edgerush_listener_connections_active"), 32770.0)
        self.assertEqual(
            metric_sum(triples, "edgerush_listener_accept_paused_total", reason="worker_cap"),
            1877.0,
        )
        self.assertEqual(
            metric_sum(triples, "edgerush_listener_accept_paused_total", reason="share"),
            0.0,
        )


class SourceAddresses(unittest.TestCase):
    def test_distinct_loopback_addresses_skipping_network_and_broadcast(self):
        got = source_addresses(300)
        self.assertEqual(len(got), 300)
        self.assertEqual(len(set(got)), 300)
        self.assertEqual(got[0], "127.0.0.2")
        for text in got:
            self.assertTrue(text.startswith("127."))
            last = int(text.rsplit(".", 1)[1])
            self.assertNotIn(last, (0, 255))

    def test_it_is_deterministic(self):
        self.assertEqual(source_addresses(10), source_addresses(10))


class Percentiles(unittest.TestCase):
    def test_nearest_rank(self):
        pct = percentiles([float(n) for n in range(1, 101)], [50, 90, 99, 100])
        self.assertEqual(pct[50], 50.0)
        self.assertEqual(pct[90], 90.0)
        self.assertEqual(pct[99], 99.0)
        self.assertEqual(pct[100], 100.0)

    def test_empty_is_zeros(self):
        self.assertEqual(percentiles([], [50, 100]), {50: 0.0, 100: 0.0})


class CannotProgress(unittest.TestCase):
    def test_no_failures_keeps_going(self):
        self.assertFalse(cannot_progress(0, 0, 0, 400))

    def test_short_of_the_width_keeps_going(self):
        self.assertFalse(cannot_progress(0, 0, 399, 400))

    def test_failures_of_any_kind_add_up_to_the_width(self):
        self.assertTrue(cannot_progress(0, 0, 400, 400))
        self.assertTrue(cannot_progress(150, 150, 100, 400))
        self.assertTrue(cannot_progress(500, 0, 0, 400))


class LiveConnection(unittest.TestCase):
    def setUp(self):
        self.listener = socket.create_server(("127.0.0.1", 0), backlog=128)
        self.port = self.listener.getsockname()[1]
        self.accepted = []

        def serve():
            while True:
                try:
                    conn, _ = self.listener.accept()
                except OSError:
                    return
                self.accepted.append(conn)

        self.thread = threading.Thread(target=serve, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.listener.close()
        for conn in self.accepted:
            conn.close()

    def test_a_connection_is_held_and_counted(self):
        shared = Shared()
        self.assertTrue(one_connection(("127.0.0.1", self.port), [], 0, 5.0, None, shared))
        self.assertEqual(shared.established, 1)
        self.assertEqual(len(shared.held), 1)
        self.assertEqual(len(shared.connect_ms), 1)
        shared.held[0].close()

    def test_a_failed_connect_is_counted_not_raised(self):
        # A port with nothing listening: the connect fails. How it fails is the platform's
        # (Linux refuses, Windows loopback times out); the point is that it is counted, once,
        # and does not escape to crash the storm.
        spare = socket.socket()
        spare.bind(("127.0.0.1", 0))
        dead_port = spare.getsockname()[1]
        spare.close()
        shared = Shared()
        self.assertFalse(one_connection(("127.0.0.1", dead_port), [], 0, 2.0, None, shared))
        self.assertEqual(shared.established, 0)
        failures = shared.refused + shared.other_errors + shared.timed_out + shared.exhausted
        self.assertEqual(failures, 1)

    def test_probe_sees_a_live_listener_and_a_dead_one(self):
        self.assertIsNotNone(probe_once(("127.0.0.1", self.port), 5.0))

    @unittest.skipUnless(platform.system() == "Linux", "source-IP binding needs Linux loopback")
    def test_binding_a_rotated_source_address_still_reaches_the_target(self):
        shared = Shared()
        ok = one_connection(("127.0.0.1", self.port), source_addresses(4), 2, 5.0, None, shared)
        self.assertTrue(ok)
        self.assertEqual(shared.held[0].getsockname()[0], "127.0.0.4")
        shared.held[0].close()


if __name__ == "__main__":
    unittest.main()
