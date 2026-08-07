# SPDX-License-Identifier: Apache-2.0

import copy
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks import expected_failure, load_cases, load_suite, parse_metrics, select_cases


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        self.case = {"id": "large", "scenario": "window", "request_size": 65536,
                     "response_size": 65536,
                     "protocols": ["grpc"], "connections": {"grpc": "single"}}

    def load(self, cases):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scenarios.json"
            path.write_text(json.dumps(cases))
            return load_cases(path)

    def test_duplicate_ids_and_invalid_shapes_are_rejected(self):
        for cases in ([], {}, [None], [self.case, self.case]):
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                self.load(cases)

    def test_unsupported_connection_layout_cannot_be_silently_measured(self):
        case = copy.deepcopy(self.case)
        case["connections"]["grpc"] = "tagged"
        with self.assertRaisesRegex(ValueError, "only implements single"):
            self.load([case])

    def test_silent_parameter_ignoring_and_log_name_collisions_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "fields"):
            self.load([self.case | {"threads": 100}])
        with self.assertRaisesRegex(ValueError, "duplicate workload"):
            self.load([self.case, self.case | {"id": "same-workload"}])

    def test_invalid_payload_and_connection_types_are_rejected(self):
        for patch in ({"request_size": True}, {"response_size": -1}, {"scenario": {}},
                      {"connections": {"grpc": []}}):
            case = self.case | patch
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                self.load([case])

    def test_selection_keeps_only_requested_case_and_protocol(self):
        other = self.case | {"id": "small", "request_size": 16, "response_size": 16}
        selected = select_cases(self.load([self.case, other]), "large", "grpc")
        self.assertEqual(selected, [(self.case, "grpc")])
        with self.assertRaisesRegex(ValueError, "no scenarios"):
            select_cases([self.case], "large", "brpc")

    def test_suite_validates_case_names_and_positive_limits(self):
        suite = {"name": "quick", "cases": ["large"], "repeat": 1, "iters": 10,
                 "warmup": 1, "threads": 1, "server_threads": 1, "window": 1,
                 "timeout_ms": 100}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quick.json"
            path.write_text(json.dumps(suite))
            self.assertEqual(load_suite(path, [self.case])["name"], "quick")
            path.write_text(json.dumps(suite | {"cases": ["missing"]}))
            with self.assertRaisesRegex(ValueError, "invalid cases"):
                load_suite(path, [self.case])


class EvidenceTests(unittest.TestCase):
    SUMMARY = "operations: 10\nsuccess: 10\nerrors: 0\nthroughput: 100.00 ops/s\np50 latency: 2.00us\np99 latency: 1.00ms\n"

    def test_complete_zero_error_summary_passes(self):
        metrics, valid = parse_metrics(self.SUMMARY, 10)
        self.assertTrue(valid)
        self.assertEqual(metrics["throughput"], "100.00 ops/s")

    def test_failed_rpc_counts_are_retained_as_failure_evidence(self):
        summary = self.SUMMARY.replace("success: 10", "success: 9").replace("errors: 0", "errors: 1")
        metrics, valid = parse_metrics(summary, 10)
        self.assertFalse(valid)
        self.assertEqual(metrics["errors"], "1")

    def test_malformed_ambiguous_or_nonfinite_metrics_never_pass(self):
        for summary in (self.SUMMARY + "errors: 0\n",
                        self.SUMMARY.replace("100.00 ops/s", "nan ops/s"),
                        self.SUMMARY.replace("1.00ms", "bad"),
                        self.SUMMARY.replace("1.00ms", "1.00ns")):
            with self.subTest(summary=summary), self.assertRaises(ValueError):
                parse_metrics(summary, 10)

    def test_expected_compile_failure_requires_only_the_intended_diagnostic(self):
        message = "vaelo codegen currently supports unary RPCs only"
        output = f"error: {message}\nerror: could not compile `fixture` due to 1 previous error\n"
        self.assertTrue(expected_failure(101, output, message))
        self.assertFalse(expected_failure(0, output, message))
        self.assertFalse(expected_failure(None, output, message))
        self.assertFalse(expected_failure(101, output + "error[E0432]: unresolved import\n", message))
        self.assertFalse(expected_failure(101, "error: missing protoc\n", message))
