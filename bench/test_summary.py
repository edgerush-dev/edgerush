"""The run table reads what run.sh writes: idle memory, curl's probe, perf's counts and a
mixed load's whole."""

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


if __name__ == "__main__":
    unittest.main()
