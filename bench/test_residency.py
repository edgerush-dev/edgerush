"""Idle policies take whole cores and compare states by exit latency; a window's shares are
of the monotonic time it lasted, the hardware's of TSC; the run table shows them."""

import json
import pathlib
import tempfile
import unittest

import residency
from test_summary import H2LOAD, table

# Two cores of two threads, CPUs 0 and 2 one core, 1 and 3 the other, as the laptop pairs
# n and n + 4; the states and exit latencies of its driver.
STATES = [("POLL", 0), ("C1", 2), ("C1E", 10), ("C6", 85), ("C10", 890)]
CORES = {0: "0,2", 1: "1,3", 2: "0,2", 3: "1,3"}


def fake_cpus(root, disabled=()):
    for cpu, siblings in CORES.items():
        topology = root / f"cpu{cpu}" / "topology"
        topology.mkdir(parents=True)
        (topology / "thread_siblings_list").write_text(siblings + "\n")
        for index, (name, latency) in enumerate(STATES):
            state = root / f"cpu{cpu}" / "cpuidle" / f"state{index}"
            state.mkdir(parents=True)
            (state / "name").write_text(name + "\n")
            (state / "latency").write_text(f"{latency}\n")
            (state / "disable").write_text("1\n" if (cpu, name) in disabled else "0\n")
            (state / "usage").write_text("0\n")
            (state / "time").write_text("0\n")


def disabled_by(writes, root):
    """Of a plan, the `cpu:state` names it disables."""
    names = []
    for path, value in writes:
        if value:
            cpu = path.parent.parent.parent.name
            names.append(f"{cpu}:{(path.parent / 'name').read_text().strip()}")
    return sorted(names)


class Policies(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.directory.name)
        fake_cpus(self.root)

    def tearDown(self):
        self.directory.cleanup()

    def test_normal_enables_every_state(self):
        writes = residency.plan("normal", [0], self.root)
        self.assertEqual(len(writes), 4 * len(STATES))
        self.assertEqual(disabled_by(writes, self.root), [])

    def test_a_named_state_disables_the_deeper_ones_everywhere(self):
        writes = residency.plan("C1E", [0], self.root)
        self.assertEqual(disabled_by(writes, self.root),
                         sorted(f"cpu{cpu}:{name}" for cpu in range(4) for name in ("C6", "C10")))

    def test_the_proxy_scope_takes_its_whole_cores(self):
        writes = residency.plan("C1E-proxy", [0], self.root)
        self.assertEqual(disabled_by(writes, self.root),
                         ["cpu0:C10", "cpu0:C6", "cpu2:C10", "cpu2:C6"])

    def test_the_others_scope_takes_every_other_core(self):
        writes = residency.plan("C6-others", [0], self.root)
        self.assertEqual(disabled_by(writes, self.root), ["cpu1:C10", "cpu3:C10"])

    def test_a_state_the_driver_has_not_is_refused(self):
        with self.assertRaises(ValueError):
            residency.plan("C3", [0], self.root)
        with self.assertRaises(ValueError):
            residency.plan("C1E-nowhere", [0], self.root)

    def test_what_was_disabled_is_saved_to_be_put_back(self):
        root = self.root / "again"
        fake_cpus(root, disabled={(1, "C10")})
        saved = residency.save(root)
        self.assertEqual([str(path.relative_to(root)) for path, value in saved if value],
                         [str(pathlib.Path("cpu1/cpuidle/state4/disable"))])


def entry(name, usage, time_us):
    return {"name": name, "usage": usage, "time": time_us, "disable": 0}


PERF = """CPU0,1150000000,,cstate_core/c6-residency/,6000000000,100.00,,
CPU1,<not counted>,,cstate_core/c6-residency/,0,0.00,,
CPU0,2300000000,,cstate_core/c7-residency/,6000000000,100.00,,
CPU1,4600000000,,cstate_core/c7-residency/,6000000000,100.00,,
CPU0,1380000000,,cstate_pkg/c3-residency/,6000000000,100.00,,
CPU0,0,,cstate_pkg/c8-residency/,6000000000,100.00,,
CPU0,11500000000,,msr/tsc/,6000000000,100.00,,
CPU1,11500000000,,msr/tsc/,6000000000,100.00,,
CPU2,11500000000,,msr/tsc/,6000000000,100.00,,
CPU0,2300000000,,msr/mperf/,6000000000,100.00,,
CPU2,1150000000,,msr/mperf/,6000000000,100.00,,
"""


def record(elapsed_ns=6_200_000_000):
    """A window of 6.2 s by the monotonic clock: CPU 0 asked for C6 for 3.1 s over 620
    entries and for C10 for 0.62 s over 62; CPU 2 for C1E alone."""
    before = {"at_ns": 1_000_000_000, "cpus": {
        "0": [entry("C1E", 0, 0), entry("C6", 100, 1_000_000), entry("C10", 10, 0)],
        "2": [entry("C1E", 0, 0), entry("C6", 0, 0), entry("C10", 0, 0)]}}
    after = {"at_ns": 1_000_000_000 + elapsed_ns, "cpus": {
        "0": [entry("C1E", 0, 0), entry("C6", 720, 4_100_000), entry("C10", 72, 620_000)],
        "2": [entry("C1E", 1240, 5_580_000), entry("C6", 0, 0), entry("C10", 0, 0)]}}
    return {"proxy_cpus": [0, 2], "cores": {"0": [0, 2], "1": [1, 3], "2": [0, 2], "3": [1, 3]},
            "before": before, "after": after, "perf": PERF}


class Shares(unittest.TestCase):
    def test_the_os_view_is_of_the_monotonic_time_the_window_lasted(self):
        seen = residency.shares(record())
        self.assertAlmostEqual(seen["elapsed"], 6.2)
        self.assertAlmostEqual(seen["os_time"][0]["C6"], 50.0)
        self.assertAlmostEqual(seen["os_entries"][0]["C6"], 100.0)
        self.assertAlmostEqual(seen["os_time"][0]["C10"], 10.0)
        self.assertAlmostEqual(seen["os_entries"][2]["C1E"], 200.0)

    def test_the_hardware_view_is_of_the_tsc_where_it_was_counted(self):
        seen = residency.shares(record())
        self.assertAlmostEqual(seen["core"][0]["c6"], 10.0)
        self.assertAlmostEqual(seen["core"][0]["c7"], 20.0)
        self.assertAlmostEqual(seen["core"][1]["c7"], 40.0)
        self.assertNotIn("c6", seen["core"][1])  # not counted is not zero
        self.assertAlmostEqual(seen["package"]["c3"], 12.0)
        self.assertAlmostEqual(seen["c0"][0], 20.0)

    def test_the_proxy_view_sets_its_cores_against_the_others(self):
        view = residency.proxy_view(record())
        self.assertAlmostEqual(view["proxy_cc7"], 20.0)
        self.assertAlmostEqual(view["other_cc7"], 40.0)
        self.assertAlmostEqual(view["proxy_c0"], 15.0)
        self.assertAlmostEqual(view["pc3"], 12.0)
        self.assertAlmostEqual(view["pc6_plus"], 0.0)
        # CPU 0 entered 110 times a second, CPU 2 200 times.
        self.assertAlmostEqual(view["proxy_entries"], 155.0)
        # C10 for 10% of CPU 0's window is 5% of the proxy's two CPUs'.
        self.assertEqual(view["proxy_deepest_asked"], "C10 5%")

    def test_without_perf_the_os_view_is_still_read(self):
        without = record()
        without["perf"] = None
        view = residency.proxy_view(without)
        self.assertIsNone(view["proxy_cc7"])
        self.assertAlmostEqual(view["proxy_entries"], 155.0)


class Table(unittest.TestCase):
    def test_a_measurement_with_its_window_is_a_row_of_the_idle_table(self):
        with tempfile.TemporaryDirectory() as name:
            directory = pathlib.Path(name)
            (directory / "ours@C1E.1.latency-h3s-1000.out").write_text(H2LOAD.format(rate=1000))
            (directory / "ours@C1E.1.latency-h3s-1000.idle").write_text(json.dumps(record()))
            printed = table(directory)
            row = next(line for line in printed.splitlines()
                       if "latency-h3s-1000" in line and "C10 5%" in line)
            # Others' CC6 was not counted: an empty cell, not a zero.
            self.assertIn("| ours@C1E | 1 | 15 | 10 | 20 |  | 40 |", row)


if __name__ == "__main__":
    unittest.main()
