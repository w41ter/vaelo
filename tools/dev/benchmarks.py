# SPDX-License-Identifier: Apache-2.0

"""Scenario validation and benchmark evidence parsing, shared by the workflow."""
import json
import math
import re


PROTOCOLS = {"brpc", "grpc", "http2", "tonic"}


def load_cases(path):
    cases = json.loads(path.read_text())
    if not isinstance(cases, list) or not cases:
        raise ValueError("scenario configuration must be a nonempty list")
    seen = set()
    workloads = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("each scenario must be an object")
        required = {"id", "scenario", "request_size", "response_size", "protocols", "connections"}
        if set(case) != required:
            raise ValueError("scenario fields must be id, scenario, request_size, response_size, protocols, connections")
        name = case.get("id", "")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9-]*", name) or name in seen:
            raise ValueError("scenario IDs must be unique lowercase names")
        seen.add(name)
        if not isinstance(case.get("scenario"), str) or case["scenario"] not in {"ping-pong", "window"}:
            raise ValueError(f"unsupported scenario in {name}")
        request_size = case.get("request_size")
        response_size = case.get("response_size")
        if type(request_size) is not int or request_size < 0:
            raise ValueError(f"request_size must be a nonnegative integer in {name}")
        if type(response_size) is not int or response_size < 0:
            raise ValueError(f"response_size must be a nonnegative integer in {name}")
        protocols = case.get("protocols")
        if (not isinstance(protocols, list) or not protocols or
                any(not isinstance(p, str) or p not in PROTOCOLS for p in protocols) or
                len(set(protocols)) != len(protocols)):
            raise ValueError(f"invalid protocols in {name}")
        connections = case.get("connections")
        if not isinstance(connections, dict) or set(connections) != set(protocols):
            raise ValueError(f"connections must specify exactly the selected protocols in {name}")
        for protocol, mode in connections.items():
            if not isinstance(mode, str) or mode not in {"single", "tagged", "short"}:
                raise ValueError(f"invalid connection mode in {name}")
            if protocol != "brpc" and mode != "single":
                raise ValueError(f"{protocol} only implements single in the perf tool")
            signature = (protocol, case["scenario"], request_size, response_size, mode)
            if signature in workloads:
                raise ValueError(f"duplicate workload in {name}")
            workloads.add(signature)
    return cases


def load_suite(path, cases):
    suite = json.loads(path.read_text())
    required = {"name", "cases", "repeat", "iters", "warmup", "threads",
                "server_threads", "window", "timeout_ms"}
    if not isinstance(suite, dict) or set(suite) != required:
        raise ValueError(f"suite fields must be {', '.join(sorted(required))}")
    if suite["name"] != path.stem:
        raise ValueError("suite name must match its file name")
    known = {case["id"] for case in cases}
    selected = suite["cases"]
    if (not isinstance(selected, list) or not selected or len(set(selected)) != len(selected)
            or any(case not in known for case in selected)):
        raise ValueError(f"suite {suite['name']} contains invalid cases")
    for field in required - {"name", "cases"}:
        if type(suite[field]) is not int or suite[field] <= 0:
            raise ValueError(f"suite {field} must be a positive integer")
    return suite


def select_cases(cases, case_id=None, protocol=None, suite_cases=None):
    selected = []
    for case in cases:
        if case_id and case["id"] != case_id:
            continue
        if suite_cases is not None and case["id"] not in suite_cases:
            continue
        for choice in case["protocols"]:
            if not protocol or choice == protocol:
                selected.append((case, choice))
    if not selected:
        raise ValueError("no scenarios match the selection")
    return selected


def parse_metrics(output, expected_operations):
    metrics = {}
    for key in ("operations", "success", "errors", "throughput", "p50 latency", "p99 latency"):
        values = re.findall(r"^" + re.escape(key) + r": (.+)$", output, re.MULTILINE)
        if len(values) != 1:
            raise ValueError(f"missing or ambiguous metric: {key}")
        metrics[key] = values[0]
    warmup_errors = re.findall(r"^warmup errors: (.+)$", output, re.MULTILINE)
    if len(warmup_errors) > 1:
        raise ValueError("ambiguous metric: warmup errors")
    if warmup_errors:
        metrics["warmup errors"] = warmup_errors[0]
    if (metrics["operations"] != str(expected_operations) or
            metrics["success"] != str(expected_operations) or metrics["errors"] != "0" or
            metrics.get("warmup errors", "0") != "0"):
        # Return the raw evidence even when the RPCs themselves failed.
        return metrics, False
    throughput = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?) ops/s", metrics["throughput"])
    if not throughput or not math.isfinite(float(throughput[1])) or float(throughput[1]) <= 0:
        raise ValueError("invalid throughput")
    factors = {"ns": 1, "us": 1000, "ms": 1_000_000, "s": 1_000_000_000}
    latencies = []
    for key in ("p50 latency", "p99 latency"):
        value = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s)", metrics[key])
        if not value or not math.isfinite(float(value[1])):
            raise ValueError(f"invalid {key}")
        latencies.append(float(value[1]) * factors[value[2]])
    if latencies[0] > latencies[1]:
        raise ValueError("p50 latency exceeds p99")
    return metrics, True


def expected_failure(code, output, message):
    diagnostics = re.findall(r"^error(?:\[[^]]+\])?: (.+)$", output, re.MULTILINE)
    diagnostics = [line for line in diagnostics if not line.startswith("could not compile ")]
    return code is not None and code != 0 and diagnostics == [message]
