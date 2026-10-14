"""The report reads what the harness writes, and measures each fleet against its own
system's single instance as well as against the contract."""

import contextlib
import io
import json
import pathlib
import tempfile
import unittest

import report


def row(system, n, traffic, workload, fault, seed, sent, admitted, refused, one_bucket,
        worst, failed=0):
    return {
        "system": system, "instances": n, "traffic_pods": traffic, "yardstick": n == 1,
        "workload": workload, "fault": fault, "seed": seed, "events": [], "sent": sent,
        "admitted": admitted, "refused": refused, "failed": failed, "failures": [],
        "admitted_by_instance": [], "one_bucket": one_bucket, "worst_excess": worst,
        "latency_ms": {"mean": 1.0, "p50": 1.0, "p99": 2.0}, "late_ms_p99": 0.1,
        "packets_per_s": 10.0,
    }


class Describe(unittest.TestCase):
    def test_overload_is_measured_against_one_instance_and_against_the_contract(self):
        table = report.cells([
            row("x", 1, 2, "bursts", "none", 0, 2000, 1000, 1000, 1000, 0),
            row("x", 1, 2, "bursts", "none", 1, 2000, 1000, 1000, 1000, 2),
            row("x", 2, 2, "bursts", "none", 0, 2000, 1500, 500, 1000, 400),
            row("x", 2, 2, "bursts", "none", 1, 2000, 1700, 300, 1000, 700),
        ])
        self.assertEqual(report.describe(table, "x", 2, "bursts", "none"), "1.60× / 1.60× (700)")

    def test_a_fixed_window_is_not_charged_for_its_boundary_by_its_own_yardstick(self):
        table = report.cells([
            row("w", 1, 2, "over-spread", "none", 0, 3000, 1100, 1900, 1000, 90),
            row("w", 2, 2, "over-spread", "none", 0, 3000, 1100, 1900, 1000, 95),
        ])
        self.assertEqual(report.describe(table, "w", 2, "over-spread", "none"), "1.00× / 1.10× (95)")

    def test_conforming_traffic_shows_what_the_fleet_and_one_instance_refuse(self):
        table = report.cells([
            row("x", 1, 2, "edge-spread", "none", 0, 1000, 990, 10, 1000, -1),
            row("x", 2, 2, "edge-spread", "none", 0, 1000, 970, 20, 1000, -1, failed=10),
        ])
        self.assertEqual(report.describe(table, "x", 2, "edge-spread", "none"), "2.0% / 1.0%, 1.0% failed")

    def test_a_fault_is_measured_against_one_instance_with_no_fault(self):
        table = report.cells([
            row("x", 1, 8, "bursts", "none", 0, 8000, 1000, 7000, 1000, 0),
            row("x", 8, 8, "bursts", "partition", 0, 8000, 2000, 6000, 1000, 900),
        ])
        self.assertEqual(report.describe(table, "x", 8, "bursts", "partition"), "2.00× / 2.00× (900)")

    def test_what_was_not_run_is_a_dash(self):
        self.assertEqual(report.describe(report.cells([]), "x", 2, "bursts", "none"), "—")


class Whole(unittest.TestCase):
    def test_a_results_file_becomes_tables(self):
        with tempfile.TemporaryDirectory() as name:
            path = pathlib.Path(name) / "r.jsonl"
            path.write_text("\n".join(json.dumps(r) for r in [
                row("a", 1, 2, "bursts", "none", 0, 2000, 1000, 1000, 1000, 0),
                row("a", 2, 2, "bursts", "none", 0, 2000, 1200, 800, 1000, 150),
            ]) + "\n")
            printed = io.StringIO()
            with contextlib.redirect_stdout(printed):
                report.main([str(path)])
        text = printed.getvalue()
        self.assertIn("| A whole bucket on every instance at once, every second | 1.20× / 1.20× (150) |", text)
        self.assertIn("| a | 1.20× / 1.20× (150) |", text)


if __name__ == "__main__":
    unittest.main()
