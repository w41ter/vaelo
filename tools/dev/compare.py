# SPDX-License-Identifier: Apache-2.0

"""Compare valid benchmark rounds, refusing changed environments or workloads."""
import json
import math
from pathlib import Path
import re
import shutil
import statistics
import datetime


def load(directory):
    environment = json.loads((directory / "environment.json").read_text())
    results = json.loads((directory / "results.json").read_text())
    groups = {}
    for result in results:
        if "-run" in result["name"]:
            case = result["name"].rsplit("-run", 1)[0]
            groups.setdefault(case, []).append(result)
    if not groups:
        raise ValueError(f"no benchmark rounds in {directory}")
    return environment, groups


def throughput(rounds):
    values = []
    signatures = set()
    for result in rounds:
        metrics = result["metrics"]
        if result["status"] != "PASS" or metrics.get("errors") != "0":
            raise ValueError("one or more rounds failed")
        operations = int(metrics["operations"])
        if operations <= 0 or int(metrics["success"]) != operations:
            raise ValueError("invalid operation counts")
        value = float(metrics["throughput"].split()[0])
        if not math.isfinite(value) or value <= 0:
            raise ValueError("invalid throughput")
        signatures.add(tuple(result["command"][1:]))
        values.append(value)
    if len(signatures) != 1:
        raise ValueError("workload changed within a run")
    return statistics.median(values), signatures.pop()


def latency_ns(value):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s)", value)
    if not match:
        raise ValueError("invalid latency")
    factors = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
    return float(match[1]) * factors[match[2]]


def summarize(rounds):
    median, signature = throughput(rounds)
    values = [float(result["metrics"]["throughput"].split()[0]) for result in rounds]
    p99 = statistics.median(latency_ns(result["metrics"]["p99 latency"]) for result in rounds)
    cv = statistics.stdev(values) / statistics.mean(values) * 100 if len(values) > 1 else 0.0
    cpu_values = [result.get("resources", {}).get("cpu_ns_per_operation") for result in rounds]
    cpu_ns = None if any(value is None for value in cpu_values) else statistics.median(cpu_values)
    return {"throughput": median, "p99_ns": p99, "cv_percent": cv,
            "cpu_ns_per_operation": cpu_ns, "rounds": len(values), "signature": signature}


def validate_environment(old_environment, new_environment):
    if any(environment.get("arguments", {}).get("diagnostic")
           for environment in (old_environment, new_environment)):
        raise ValueError("diagnostic runs are not performance baselines")
    for field in ("platform", "machine", "logical_cpus", "rustc", "cargo"):
        if field not in old_environment or old_environment[field] != new_environment.get(field):
            raise ValueError(f"environment differs or is missing: {field}")


def compare(baseline: Path, candidate: Path):
    old_environment, old = load(baseline)
    new_environment, new = load(candidate)
    validate_environment(old_environment, new_environment)
    lines = ["| Case | Baseline median ops/s | Candidate median ops/s | Change |",
             "| --- | ---: | ---: | ---: |"]
    compared = 0
    for case in sorted(old.keys() | new.keys()):
        try:
            if case not in old or case not in new:
                raise ValueError("case missing from one report")
            before, old_signature = throughput(old[case])
            after, new_signature = throughput(new[case])
            if old_signature != new_signature:
                raise ValueError("workload arguments differ")
            lines.append(f"| {case} | {before:.2f} | {after:.2f} | {(after / before - 1) * 100:+.2f}% |")
            compared += 1
        except (ValueError, KeyError) as error:
            lines.append(f"| {case} | excluded: {error} | — | — |")
    if not compared:
        raise ValueError("no valid matching cases")
    lines += ["", "Descriptive comparison only; use --gate for the versioned regression policy. "
              "Host load and temperature are not controlled. Excluded cases are not performance evidence."]
    return "\n".join(lines)


def gate(baseline: Path, candidate: Path, policy):
    old_environment, old = load(baseline)
    new_environment, new = load(candidate)
    validate_environment(old_environment, new_environment)
    lines = ["| Case | Throughput change | CPU/op change | p99 change | Result |",
             "| --- | ---: | ---: | ---: | --- |"]
    passed = True
    for case in sorted(old.keys() | new.keys()):
        try:
            if case not in old or case not in new:
                raise ValueError("case missing from one report")
            before, after = summarize(old[case]), summarize(new[case])
            if before["signature"] != after["signature"]:
                raise ValueError("workload arguments differ")
            minimum_repeats = policy.get("minimum_repeats", 1)
            maximum_cv = policy.get("maximum_throughput_cv_percent", math.inf)
            for label, summary in (("baseline", before), ("candidate", after)):
                if summary["rounds"] < minimum_repeats:
                    raise ValueError(f"{label} has too few rounds")
                if summary["cv_percent"] > maximum_cv:
                    raise ValueError(f"{label} throughput is unstable")
            throughput_change = (after["throughput"] / before["throughput"] - 1) * 100
            p99_change = (after["p99_ns"] / before["p99_ns"] - 1) * 100
            if before["cpu_ns_per_operation"] is None or after["cpu_ns_per_operation"] is None:
                raise ValueError("CPU/op evidence missing")
            cpu_change = (after["cpu_ns_per_operation"] / before["cpu_ns_per_operation"] - 1) * 100
            result = (throughput_change >= policy["throughput_regression_percent"] and
                      p99_change <= policy["p99_regression_percent"] and
                      cpu_change <= policy["cpu_regression_percent"])
            passed &= result
            lines.append(f"| {case} | {throughput_change:+.2f}% | {cpu_change:+.2f}% | "
                         f"{p99_change:+.2f}% | "
                         f"{'PASS' if result else 'REGRESSION'} |")
        except (ValueError, KeyError, ZeroDivisionError) as error:
            passed = False
            lines.append(f"| {case} | excluded: {error} | — | — | INVALID |")
    lines += ["", f"Thresholds: throughput >= {policy['throughput_regression_percent']:+.1f}%, "
              f"CPU/op <= +{policy['cpu_regression_percent']:.1f}%, "
              f"p99 <= +{policy['p99_regression_percent']:.1f}%. Result: "
              f"{'PASS' if passed else 'FAIL'}. Flaky reruns must be recorded as new evidence."]
    return "\n".join(lines), passed


def explain(directory: Path, policy):
    environment, groups = load(directory)
    lines = [f"# Performance evidence: {directory}", "",
             "| Case | Valid | Rounds | Median ops/s | CPU ns/op | Throughput CV | Median p99 |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    arguments = environment.get("arguments", {})
    admissible = not arguments.get("diagnostic", False)
    for case in sorted(groups):
        try:
            summary = summarize(groups[case])
            stable = (summary["rounds"] >= policy["minimum_repeats"] and
                      summary["cv_percent"] <= policy["maximum_throughput_cv_percent"] and
                      summary["cpu_ns_per_operation"] is not None)
            admissible &= stable
            cpu = summary["cpu_ns_per_operation"]
            cpu_text = "missing" if cpu is None else f"{cpu:.2f}"
            lines.append(f"| {case} | {'yes' if stable else 'no'} | {summary['rounds']} | "
                         f"{summary['throughput']:.2f} | {cpu_text} | "
                         f"{summary['cv_percent']:.2f}% | "
                         f"{summary['p99_ns'] / 1000:.2f}us |")
        except (ValueError, KeyError) as error:
            admissible = False
            lines.append(f"| {case} | no: {error} | — | — | — | — | — |")
    failed_steps = [result["name"] for result in json.loads((directory / "results.json").read_text())
                    if result["status"] != "PASS"]
    if failed_steps:
        admissible = False
        lines += ["", "Failed steps: " + ", ".join(failed_steps) + "."]
    if arguments.get("diagnostic"):
        lines += ["", "Diagnostic logging is enabled, so this run cannot become a baseline."]
    allowed_suites = policy.get("admissible_suites", ["standard", "stress"])
    if arguments.get("suite") not in allowed_suites:
        admissible = False
        lines += ["", f"Suite {arguments.get('suite')!r} is not eligible for baseline admission."]
    if arguments.get("case") is not None or arguments.get("protocol") is not None:
        admissible = False
        lines += ["", "Filtered case or protocol runs cannot become a suite baseline."]
    lines += ["", "Baseline admission: " + ("PASS" if admissible else "FAIL") + "."]
    return "\n".join(lines), admissible


def resolve_baseline(root: Path, value):
    direct = Path(value).expanduser()
    if direct.exists():
        return direct.resolve()
    index = json.loads((root / "perf/baselines/index.json").read_text())
    matches = [entry for entry in index["baselines"] if entry["id"] == value]
    if len(matches) != 1:
        raise ValueError(f"unknown baseline: {value}")
    return (root / "perf/baselines" / matches[0]["path"]).resolve()


def promote(root: Path, candidate: Path, reason: str, baseline_id=None):
    policy = json.loads((root / "perf/baselines/policy.json").read_text())
    _, admissible = explain(candidate, policy)
    if not admissible:
        raise ValueError("candidate does not satisfy baseline admission policy")
    environment = json.loads((candidate / "environment.json").read_text())
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    baseline_id = baseline_id or f"{stamp}-{environment['source_sha256'][:8]}"
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", baseline_id):
        raise ValueError("baseline ID contains unsupported characters")
    index_path = root / "perf/baselines/index.json"
    index = json.loads(index_path.read_text())
    if any(entry["id"] == baseline_id for entry in index["baselines"]):
        raise ValueError(f"baseline already exists: {baseline_id}")
    destination = root / "perf/baselines/accepted" / baseline_id
    destination.mkdir(parents=True, exist_ok=False)
    for name in ("environment.json", "results.json", "report.md"):
        shutil.copy2(candidate / name, destination / name)
    manifest = {"id": baseline_id, "promoted_utc": stamp, "reason": reason,
                "source": str(candidate.resolve()), "source_sha256": environment["source_sha256"]}
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    index["baselines"].append({"id": baseline_id, "path": f"accepted/{baseline_id}",
                               "promoted_utc": stamp, "reason": reason,
                               "source_sha256": environment["source_sha256"]})
    index_path.write_text(json.dumps(index, indent=2) + "\n")
    return baseline_id, destination
