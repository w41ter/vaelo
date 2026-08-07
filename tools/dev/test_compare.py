# SPDX-License-Identifier: Apache-2.0

import copy
import json
from pathlib import Path
import tempfile
import unittest

from compare import compare, explain, gate, throughput


class BenchmarkEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.round = {
            "status": "PASS", "command": ["perf", "--iters", "10"],
            "metrics": {"errors": "0", "operations": "10", "success": "10",
                        "throughput": "100 ops/s", "p99 latency": "10.00us"},
            "resources": {"cpu_ns_per_operation": 1000.0},
        }

    def test_failed_round_cannot_be_hidden_by_passing_rounds(self):
        failed = copy.deepcopy(self.round)
        failed["status"] = "FAIL"
        with self.assertRaisesRegex(ValueError, "failed"):
            throughput([self.round, failed])

    def test_rpc_errors_and_partial_success_are_rejected(self):
        for change in ({"errors": "1"}, {"success": "9"}, {"throughput": "nan ops/s"}):
            changed = copy.deepcopy(self.round)
            changed["metrics"].update(change)
            with self.assertRaises(ValueError):
                throughput([changed])

    def test_different_workloads_are_rejected(self):
        changed = copy.deepcopy(self.round)
        changed["command"][-1] = "100"
        with self.assertRaisesRegex(ValueError, "workload"):
            throughput([self.round, changed])

    def test_median_is_computed_from_matching_valid_rounds(self):
        changed = copy.deepcopy(self.round)
        changed["metrics"]["throughput"] = "300 ops/s"
        self.assertEqual(throughput([self.round, changed])[0], 200)

    def reports(self, directory, candidate_patch):
        environment = {"platform": "test", "machine": "test", "logical_cpus": 2,
                       "rustc": "test", "cargo": "test", "arguments": {}}
        result = self.round | {"name": "case-run1"}
        paths = [Path(directory) / name for name in ("before", "after")]
        for path in paths:
            path.mkdir()
            (path / "results.json").write_text(json.dumps([result]))
            value = environment if path == paths[0] else environment | candidate_patch
            (path / "environment.json").write_text(json.dumps(value))
        return paths

    def test_diagnostic_measurements_cannot_be_compared_as_baselines(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = self.reports(directory, {"arguments": {"diagnostic": True}})
            with self.assertRaisesRegex(ValueError, "diagnostic"):
                compare(*paths)

    def test_environment_changes_prevent_performance_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = self.reports(directory, {"logical_cpus": 4})
            with self.assertRaisesRegex(ValueError, "logical_cpus"):
                compare(*paths)

    def test_gate_checks_throughput_and_tail_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            before, after = self.reports(directory, {})
            results = json.loads((after / "results.json").read_text())
            results[0]["metrics"]["throughput"] = "94 ops/s"
            results[0]["metrics"]["p99 latency"] = "12.00us"
            (after / "results.json").write_text(json.dumps(results))
            report, passed = gate(before, after, {
                "throughput_regression_percent": -5.0, "p99_regression_percent": 10.0,
                "cpu_regression_percent": 10.0})
            self.assertFalse(passed)
            self.assertIn("REGRESSION", report)

    def test_explain_rejects_single_round_as_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            before, _ = self.reports(directory, {})
            (before / "report.md").write_text("report")
            report, admissible = explain(before, {
                "minimum_repeats": 3, "maximum_throughput_cv_percent": 5.0})
            self.assertFalse(admissible)
            self.assertIn("Baseline admission: FAIL", report)

    def test_gate_rejects_insufficient_evidence_before_deltas(self):
        with tempfile.TemporaryDirectory() as directory:
            before, after = self.reports(directory, {})
            report, passed = gate(before, after, {
                "minimum_repeats": 2, "maximum_throughput_cv_percent": 5.0,
                "throughput_regression_percent": -5.0, "p99_regression_percent": 10.0,
                "cpu_regression_percent": 10.0})
            self.assertFalse(passed)
            self.assertIn("too few rounds", report)
