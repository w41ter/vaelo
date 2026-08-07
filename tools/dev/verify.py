#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Run existing checks and repeatable loopback benchmarks using only the stdlib."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shlex
import signal
import statistics
import subprocess
import sys
import time
import compare
from benchmarks import expected_failure, load_cases, load_suite, parse_metrics, select_cases

ROOT = Path(__file__).resolve().parents[2]


def execute(command, logfile, timeout):
    """Keep child processes scoped to a step, including timeout/interruption."""
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with logfile.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
            status = "PASS" if code == 0 else "FAIL"
        except subprocess.TimeoutExpired:
            code, status = None, "TIMEOUT"
        finally:
            # Also stop descendants still alive after their immediate parent exits.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_seconds = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    return code, status, cpu_seconds


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def interrupted(*_):
    raise KeyboardInterrupt


def capture(command):
    try:
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, check=False, timeout=30)
        return result.stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"unavailable: {error}"


def source_digest():
    digest = hashlib.sha256()
    files = []
    for directory in ("crates", "examples", "perf", "tests", "tools", ".cargo"):
        files.extend(p for p in (ROOT / directory).rglob("*")
                     if p.is_file() and not {"target", "__pycache__"}.intersection(p.parts)
                     and "perf/baselines" not in p.relative_to(ROOT).as_posix())
    files.extend(ROOT / name for name in
                 ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml"))
    for path in sorted(files):
        if path.is_file():
            digest.update(str(path.relative_to(ROOT)).encode() + b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()


def perf_control(arguments):
    """Handle the readable perf subcommands while preserving the original flat entrypoint."""
    if arguments and arguments[0] == "perf" and (
        len(arguments) == 1 or arguments[1] in ("-h", "--help")
    ):
        print("""usage: cargo xtask perf <command> [options]

commands:
  run                 run a quick, standard, or stress suite
  list                show effective cases without building
  explain RESULT      validate evidence and show measurement noise
  compare BASE CAND   compare evidence; add --gate for regression policy
  baseline promote    admit and snapshot a standard/stress result""")
        return 0, arguments
    if len(arguments) < 2 or arguments[0] != "perf" or arguments[1].startswith("-"):
        return None, arguments
    action = arguments[1]
    if action == "run":
        return None, ["perf", *arguments[2:]]
    if action == "list":
        return None, ["perf", "--list", *arguments[2:]]
    if action == "compare":
        parser = argparse.ArgumentParser(prog="cargo xtask perf compare")
        parser.add_argument("baseline")
        parser.add_argument("candidate", type=Path)
        parser.add_argument("--gate", action="store_true")
        parsed = parser.parse_args(arguments[2:])
        try:
            baseline = compare.resolve_baseline(ROOT, parsed.baseline)
            if parsed.gate:
                policy = json.loads((ROOT / "perf/baselines/policy.json").read_text())
                report, passed = compare.gate(baseline, parsed.candidate, policy)
                print(report)
                return int(not passed), arguments
            print(compare.compare(baseline, parsed.candidate))
            return 0, arguments
        except (ValueError, OSError, KeyError) as error:
            print(f"Cannot compare: {error}", file=sys.stderr)
            return 1, arguments
    if action == "explain":
        parser = argparse.ArgumentParser(prog="cargo xtask perf explain")
        parser.add_argument("result", type=Path)
        parsed = parser.parse_args(arguments[2:])
        try:
            policy = json.loads((ROOT / "perf/baselines/policy.json").read_text())
            report, _ = compare.explain(parsed.result, policy)
            print(report)
            return 0, arguments
        except (ValueError, OSError, KeyError) as error:
            print(f"Cannot explain: {error}", file=sys.stderr)
            return 1, arguments
    if action == "baseline" and len(arguments) > 2 and arguments[2] == "promote":
        parser = argparse.ArgumentParser(prog="cargo xtask perf baseline promote")
        parser.add_argument("candidate", type=Path)
        parser.add_argument("--reason", required=True)
        parser.add_argument("--id")
        parsed = parser.parse_args(arguments[3:])
        try:
            baseline_id, destination = compare.promote(
                ROOT, parsed.candidate, parsed.reason, parsed.id)
            print(f"Promoted baseline {baseline_id}: {destination}")
            return 0, arguments
        except (ValueError, OSError, KeyError) as error:
            print(f"Cannot promote baseline: {error}", file=sys.stderr)
            return 1, arguments
    parser = argparse.ArgumentParser(prog="cargo xtask perf")
    parser.error("expected run, list, compare, explain, or baseline promote")


def main():
    controlled, arguments = perf_control(sys.argv[1:])
    if controlled is not None:
        return controlled
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=("doctor", "check", "test", "codegen", "interop", "perf", "compare", "all"),
                        nargs="?", default="check")
    parser.add_argument("selection", nargs="?", help="test group (grpc/tcp/buffer/pool) or baseline directory")
    parser.add_argument("candidate", nargs="?", help="candidate directory for compare")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--suite", default="standard", help="performance suite name")
    parser.add_argument("--repeat", type=positive)
    parser.add_argument("--iters", type=positive)
    parser.add_argument("--warmup", type=positive)
    parser.add_argument("--threads", type=positive)
    parser.add_argument("--server-threads", type=positive)
    parser.add_argument("--window", type=positive)
    parser.add_argument("--timeout-ms", type=positive)
    parser.add_argument("--timeout", type=positive, default=600,
                        help="seconds per command, including builds")
    parser.add_argument("--case", help="performance scenario ID; see --list")
    parser.add_argument("--protocol", choices=("brpc", "grpc", "http2", "tonic"))
    parser.add_argument("--list", action="store_true", help="list selected performance scenarios without building")
    parser.add_argument("--diagnostic", action="store_true", help="enable debug logging; results are not performance baselines")
    args = parser.parse_args(arguments)
    if args.profile == "compare":
        if not args.selection or not args.candidate:
            parser.error("compare requires baseline and candidate directories")
        try:
            print(compare.compare(Path(args.selection), Path(args.candidate)))
        except (ValueError, OSError, KeyError) as error:
            print(f"Cannot compare: {error}", file=sys.stderr)
            return 1
        return 0
    filters = {"grpc": "protocol::", "tcp": "transport::tcp::", "buffer": "buffer::", "pool": "foundation::"}
    if args.candidate or (args.selection and (args.profile != "test" or args.selection not in filters)):
        parser.error("only test accepts a selection: grpc, tcp, buffer, pool")
    if (args.case or args.protocol or args.list or args.diagnostic) and args.profile not in ("perf", "all"):
        parser.error("--case, --protocol, --list and --diagnostic apply only to perf/all")
    try:
        cases = load_cases(ROOT / "perf" / "scenarios" / "cases.json")
        suite_path = ROOT / "perf" / "suites" / f"{args.suite}.json"
        if not suite_path.is_file():
            parser.error(f"unknown performance suite: {args.suite}")
        suite = load_suite(suite_path, cases)
        for field in ("repeat", "iters", "warmup", "threads", "server_threads", "window", "timeout_ms"):
            if getattr(args, field) is None:
                setattr(args, field, suite[field])
        if args.case and args.case not in {case["id"] for case in cases}:
            parser.error(f"unknown scenario: {args.case}")
        selected_cases = select_cases(cases, args.case, args.protocol,
                                      None if args.case else suite["cases"])
    except (ValueError, OSError) as error:
        parser.error(str(error))
    if args.list:
        for case, protocol in selected_cases:
            print(f"{case['id']}: {protocol}, {case['request_size']} -> "
                  f"{case['response_size']} bytes, {case['connections'][protocol]}")
        return 0
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    out = args.out or ROOT / "target" / "verification" / stamp
    out = out.resolve()
    # Keep historical runs intact, including their environment and raw logs.
    out.mkdir(parents=True, exist_ok=False)
    metadata = {
        "utc": stamp, "platform": platform.platform(), "machine": platform.machine(),
        "logical_cpus": os.cpu_count(), "rustc": capture(["rustc", "-Vv"]),
        "cargo": capture(["cargo", "-V"]), "revision": capture(["git", "rev-parse", "HEAD"]),
        "git_status": capture(["git", "status", "--short"]),
        "source_sha256": source_digest(), "arguments": vars(args) | {"out": str(out)},
        "limitations": "Loopback, client/server share host. No CPU/RSS/allocation measurement. "
                        "Successful and failed call latencies are separated; baseline rounds require zero errors.",
    }
    (out / "environment.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")
    steps = []

    def run(name, command, expected_operations=None, expected_error=None):
        logfile = out / (name + ".log")
        print(f"Running {name}: {shlex.join(map(str, command))}", flush=True)
        started = time.monotonic()
        status = "PASS"
        code = None
        cpu_seconds = 0.0
        interrupted = False
        try:
            code, status, cpu_seconds = execute(command, logfile, args.timeout)
        except KeyboardInterrupt:
            status = "INTERRUPTED"
            interrupted = True
        except OSError as error:
            with logfile.open("a") as log:
                log.write(str(error) + "\n")
            status = "FAIL"
        output = logfile.read_text(errors="replace")
        metrics = {}
        reason = None
        if expected_error is not None and status not in ("TIMEOUT", "INTERRUPTED"):
            status = "PASS" if expected_failure(code, output, expected_error) else "FAIL"
            if status == "FAIL":
                reason = "expected compiler rejection was not observed exclusively"
        if expected_operations is not None and status == "PASS":
            try:
                metrics, valid = parse_metrics(output, expected_operations)
                if not valid:
                    status = "FAIL"
                    reason = "RPC errors or unexpected operation counts"
            except ValueError as error:
                status = "FAIL"
                reason = str(error)
        resources = {"cpu_seconds": round(cpu_seconds, 6)} if code is not None else {}
        if expected_operations and cpu_seconds > 0:
            resources["cpu_ns_per_operation"] = round(cpu_seconds * 1_000_000_000 / expected_operations, 2)
        steps.append({"name": name, "command": list(map(str, command)), "status": status,
                      "exit_code": code, "seconds": round(time.monotonic() - started, 3),
                      "log": logfile.name, "metrics": metrics, "reason": reason,
                      "expected_error": expected_error, "resources": resources})
        print(f"{status}: {name} ({steps[-1]['seconds']}s)", flush=True)
        write_report()
        if interrupted:
            raise KeyboardInterrupt
        return status == "PASS"

    def write_report():
        (out / "results.json").write_text(json.dumps(steps, indent=2) + "\n")
        lines = ["# Verification result", "", f"UTC: {stamp}", "",
                 f"Revision: `{metadata['revision']}` (see environment.json for local changes)", "",
                 "| Step | Status | Wall seconds | Raw log |", "| --- | --- | ---: | --- |"]
        for step in steps:
            lines.append(f"| {step['name']} | {step['status']} | {step['seconds']} | "
                         f"[{step['log']}]({step['log']}) |")
        failures = [step for step in steps if step["reason"]]
        if failures:
            lines += ["", "Failure details:", ""]
            lines += [f"- {step['name']}: {step['reason']}" for step in failures]
        if args.diagnostic:
            lines += ["", "Diagnostic logging enabled. These timings are not performance baselines."]
        benchmarks = [s for s in steps if s["metrics"]]
        if benchmarks:
            lines += ["", "## Loopback performance", "",
                      f"{args.threads} client workers, {args.server_threads} server workers; "
                      f"{args.warmup} warmup calls per worker. "
                      "Only successful-call latency is summarized; zero-error runs pass. "
                      "Per-run p99 values below are not a pooled percentile.", "",
                      "| Case | Run | Status | Throughput | CPU ns/op | p50 | p99 | Errors |",
                      "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |"]
            groups = {}
            for step in benchmarks:
                case, repeat = step["name"].rsplit("-run", 1)
                m = step["metrics"]
                lines.append(f"| {case} | {repeat} | {step['status']} | {m.get('throughput', '?')} | "
                             f"{step['resources'].get('cpu_ns_per_operation', '?')} | "
                             f"{m.get('p50 latency', '?')} | {m.get('p99 latency', '?')} | {m.get('errors', '?')} |")
                if step["status"] == "PASS":
                    groups.setdefault(case, []).append(float(m["throughput"].split()[0]))
            lines += ["", "| Case | Passing runs | Throughput min / median / max (ops/s) |",
                      "| --- | ---: | ---: |"]
            for case, values in groups.items():
                lines.append(f"| {case} | {len(values)} | {min(values):.2f} / "
                             f"{statistics.median(values):.2f} / {max(values):.2f} |")
        lines += ["", metadata["limitations"], ""]
        (out / "report.md").write_text("\n".join(lines))

    def cargo(*arguments):
        command = ["cargo", *arguments]
        if args.offline:
            command.append("--offline")
        return command

    if args.profile == "doctor":
        for name, command in (("rustc", ["rustc", "-Vv"]), ("cargo", ["cargo", "-V"]),
                              ("python", [sys.executable, "--version"]), ("protoc", ["protoc", "--version"])):
            run(name, command)
        run("workspace", cargo("metadata", "--no-deps", "--format-version", "1", "--locked"))
    if args.profile in ("check", "all"):
        run("workflow-tests", [sys.executable, "-m", "unittest", "discover", "-s", "tools/dev", "-p", "test_*.py"])
        run("public-audit", [sys.executable, "tools/dev/public_audit.py"])
        run("format", ["cargo", "fmt", "--all", "--", "--check"])
        run("tests", cargo("test", "--workspace", "--locked"))
        run("clippy", cargo("clippy", "--workspace", "--all-targets", "--locked") + ["--", "-D", "warnings"])
        run("docs", cargo("doc", "--workspace", "--no-deps", "--locked"))
    if args.profile == "test":
        command = cargo("test", "--workspace", "--locked") if not args.selection else cargo("test", "-p", "vaelo", "--locked", filters[args.selection])
        run("tests", command)
    if args.profile == "codegen":
        run("codegen", cargo("test", "-p", "vaelo-codegen-tests", "--locked"))
    if args.profile in ("check", "interop", "all"):
        run("interop", cargo("run", "-p", "grpc-interop", "--locked"))
    if args.profile in ("perf", "all"):
        if run("perf-build", cargo("build", "-p", "vaelo-perf", "--release", "--locked")):
            # Ask Cargo for the actual target directory, including local configuration overrides.
            info = json.loads(capture(cargo("metadata", "--format-version", "1", "--no-deps", "--locked")))
            binary = str(Path(info["target_directory"]) / "release" / "vaelo-perf")
            # Interleave protocols within each repetition to reduce order bias.
            for repeat in range(1, args.repeat + 1):
                for case, protocol in selected_cases:
                    scenario = case["scenario"]
                    request_size, response_size = case["request_size"], case["response_size"]
                    connections = case["connections"][protocol]
                    name = f"{protocol}-{case['id']}-{connections}-run{repeat}"
                    command = [binary, "--protocol", protocol, "--scenario", scenario,
                               "--payload-size", str(request_size), "--response-size", str(response_size),
                               "--connections", connections, "--threads", str(args.threads),
                               "--server-threads", str(args.server_threads), "--iters", str(args.iters),
                               "--window", str(args.window), "--warmup", str(args.warmup),
                               "--timeout-ms", str(args.timeout_ms)]
                    if args.diagnostic:
                        command += ["--log-level", "debug"]
                    run(name, command, expected_operations=args.iters * args.threads)
    write_report()
    print(f"Report: {out / 'report.md'}", flush=True)
    return int(any(step["status"] != "PASS" for step in steps))


if __name__ == "__main__":
    # CI cancellation and Ctrl-C must not leave test servers running.
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Verification interrupted; child processes stopped.", file=sys.stderr)
        sys.exit(130)
