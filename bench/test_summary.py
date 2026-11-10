"""The run table reads what run.sh writes: idle memory, curl's probe, perf's counts, a
mixed load's whole and wsbench's line."""

import contextlib
import io
import pathlib
import tempfile
import unittest

import summary

H2LOAD = """finished in 10.00s, {rate} req/s, 1MB/s
requests: 100 total, 100 started, 100 done, 100 succeeded, 0 failed, 0 errored, 0 timeout
status codes: 100 2xx, 0 3xx, 0 4xx, 0 5xx
"""


def table(directory):
    (directory / "environment.txt").write_text("the environment\n")
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed), contextlib.redirect_stderr(io.StringIO()):
        summary.main(directory)
    return printed.getvalue()


class IdleMemory(unittest.TestCase):
    def test_what_idle_memory_writes_is_a_row_of_the_memory_table(self):
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            (directory / "ours.1.idle-silent-2000.out").write_text(
                "kind silent\nconnections 2000\nopen 1990\nrss_quiet_kb 1000\n"
                "rss_held_kb 3000\nper_connection_bytes 1029\n")
            printed = table(directory)
            self.assertIn("| idle-silent-2000 | ours | 2,000 | 1,000 KiB | 3,000 KiB | 1,029 |",
                          printed)


class Probe(unittest.TestCase):
    def test_curls_times_are_read_in_milliseconds_with_its_connections(self):
        with tempfile.TemporaryDirectory() as name:
            path = pathlib.Path(name) / "probe"
            lines = ["1 0.004 0.005"] + [f"0 0.0003 0.000{n}" for n in range(1, 10)]
            path.write_text("\n".join(lines) + "\n")
            read = summary.probed(path)
            self.assertEqual(read["probe_connects"], 1)
            self.assertAlmostEqual(read["probe_p50"], 0.6)
            self.assertAlmostEqual(read["probe_p99"], 5.0)


class Counts(unittest.TestCase):
    def test_counts_are_divided_by_the_window_and_the_rate(self):
        with tempfile.TemporaryDirectory() as name:
            path = pathlib.Path(name) / "stat"
            path.write_text(
                "# started on a day\n\n"
                "600000000,,instructions:u,1,100.00,,\n200000000,,instructions:k,1,100.00,,\n"
                "900000000,,cycles:u,1,100.00,,\n300000000,,cycles:k,1,100.00,,\n# window 6\n")
            read = summary.counts(path, 10_000)
            self.assertAlmostEqual(read["instructions_user"], 10.0)
            self.assertAlmostEqual(read["instructions_kernel"], 200 / 60)
            self.assertAlmostEqual(read["cycles"], 20.0)

    def test_without_the_window_there_is_no_count(self):
        with tempfile.TemporaryDirectory() as name:
            path = pathlib.Path(name) / "stat"
            path.write_text("600000000,,instructions:u,1,100.00,,\n")
            self.assertEqual(summary.counts(path, 10_000), {})


class Mixed(unittest.TestCase):
    def test_the_whole_is_a_row_at_the_three_rates_summed(self):
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            for protocol, rate in (("h1", 1000), ("h2", 2000), ("h3", 3000)):
                (directory / f"ours.1.mixed-5000-{protocol}.out").write_text(H2LOAD.format(rate=rate))
            (directory / "ours.1.mixed-5000-all.cpu-before").write_text(
                "time 100.0 0\n/proc/1/task/1 0 0\n")
            (directory / "ours.1.mixed-5000-all.cpu-after").write_text(
                "time 110.0 0\n/proc/1/task/1 600 300\n")
            printed = table(directory)
            # 900 ticks over 10 s is 0.9 CPUs; at 6,000 a second, 150 µs a request.
            row = next(line for line in printed.splitlines() if "mixed-5000-all" in line)
            self.assertIn("| 6,000 |", row)
            self.assertIn("| 0.90 | 150.00 |", row)


class WsBench(unittest.TestCase):
    def test_wsbenchs_line_is_a_row_of_messages_and_their_latency(self):
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            (directory / "ours.1.ws-busy.out").write_text(
                '{"wsbench": "echo", "messages": 1000, "failed": 3, "seconds": 10, '
                '"rate": 100.0, "p50": 0.25, "p99": 1.5, "p99.9": null}\n')
            row = next(line for line in table(directory).splitlines() if "ws-busy" in line)
            self.assertIn("| 100 | 3 | 0.25 | 1.50 |", row)

    def test_wsbenchs_http_line_counts_429s_others_and_failures_as_not_2xx(self):
        line = ('{"wsbench": "http", "ok": 900, "refused": 60, "other": 30, "failed": 10, '
                '"seconds": 10, "rate": 99.0, "p50": 0.25, "p99": 1.5, "p99.9": 2.0, '
                '"resident_met": 0, "resident_missed": 0}\n')
        self.assertEqual(summary.generator(line)["requests"], 1000)
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            (directory / "ours.1.keys-h1.out").write_text(line)
            row = next(line for line in table(directory).splitlines() if "keys-h1" in line)
            self.assertIn("| 99 | 100 | 0.25 | 1.50 |", row)


class PastThePorts(unittest.TestCase):
    CHURNED = ('{{"wsbench": "udp-churn", "messages": {answered}, "failed": {lost}, '
               '"seconds": 5, "rate": 10.0, "p50": 0.1, "p99": 0.2, "p99.9": 0.3, '
               '"by_second": {by_second}}}\n')

    def line(self, by_second):
        return self.CHURNED.format(
            answered=sum(answered for answered, _ in by_second),
            lost=sum(lost for _, lost in by_second),
            by_second=by_second,
        )

    def test_the_seconds_when_datagrams_began_to_be_lost_and_were_answered_again(self):
        # A loss under a hundredth of a second's datagrams is not the ports running short.
        read = summary.ports(summary.json.loads(self.line(
            [[1000, 0], [999, 1], [400, 600], [0, 1000], [995, 5], [1000, 0]])))
        self.assertEqual(read, {"answered": 4394, "lost": 1606, "short_from": 2, "again_from": 4})

    def test_a_run_short_to_its_end_or_never_short_has_no_second_for_what_did_not_happen(self):
        never = summary.ports(summary.json.loads(self.line([[10, 0], [10, 0]])))
        self.assertEqual((never["short_from"], never["again_from"]), (None, None))
        to_the_end = summary.ports(summary.json.loads(self.line([[10, 0], [0, 10], [0, 10]])))
        self.assertEqual((to_the_end["short_from"], to_the_end["again_from"]), (1, None))
        # The other clients' lines do not count by the second.
        self.assertIsNone(summary.ports({"wsbench": "udp", "messages": 1}))

    def test_edgerushs_drops_by_reason_and_nginxs_commonest_error(self):
        metrics = (
            'edgerush_listener_udp_datagrams_dropped_total{listener="datagrams",reason="exhausted"} 31000\n'
            'edgerush_listener_udp_datagrams_dropped_total{listener="datagrams",reason="no_room"} 0\n'
            'edgerush_listener_udp_datagrams_dropped_total{listener="datagrams",reason="send_full"} 2\n'
            'edgerush_listener_udp_sessions_total{listener="datagrams",outcome="idle"} 28000\n')
        self.assertEqual(summary.dropped(metrics), "exhausted 31,000; send_full 2")
        log = (
            "2026/11/10 10:00:01 [crit] 11#11: *5 connect() to 127.0.0.1:9053 failed (99: Cannot "
            "assign requested address) while connecting to upstream, udp client: 127.20.0.1, "
            "server: 127.0.0.1:8053, upstream: \"127.0.0.1:9053\", bytes from/to client:64/0\n"
        ) * 2 + "2026/11/10 10:00:02 [alert] 12#12: something else\n"
        self.assertEqual(
            summary.errors(log),
            "3 lines; 2: connect() to 127.0.0.1:9053 failed (99: Cannot assign requested "
            "address) while connecting to upstream")

    def test_a_run_past_the_ports_is_a_row_of_its_own_with_what_the_proxy_said(self):
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            (directory / "ours.2.udp-ports.out").write_text(self.line([[10, 0], [0, 10], [10, 0]]))
            (directory / "ours.2.udp-ports.metrics").write_text(
                'edgerush_listener_udp_datagrams_dropped_total{listener="datagrams",reason="exhausted"} 10\n')
            (directory / "nginx.1.udp-ports.out").write_text(self.line([[10, 0], [10, 0]]))
            (directory / "nginx.1.udp-ports.errors").write_text("")
            printed = table(directory)
            self.assertIn("| udp-ports | ours | 2 | 20 | 10 | 1 | 2 | exhausted 10 |", printed)
            self.assertIn("| udp-ports | nginx | 1 | 20 | 0 |  |  |  |", printed)
            # And a row of the run table, as any other client's line is.
            self.assertTrue(any(line.startswith("| udp-ports | ours | 1 |")
                                for line in printed.splitlines()))


if __name__ == "__main__":
    unittest.main()
