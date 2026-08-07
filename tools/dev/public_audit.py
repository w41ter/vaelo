#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Reject internal references and common credential material in tracked files."""

from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
SELF = Path(__file__).relative_to(ROOT).as_posix()
TEST = "tools/dev/test_public_audit.py"

FORBIDDEN_PATHS = (
    re.compile(r"(^|/)\.env($|\.)"),
    re.compile(r"\.(?:key|pem|p12|pfx|jks|keystore)$", re.IGNORECASE),
)

FORBIDDEN_TEXT = (
    ("internal project name", re.compile(r"\bse" r"kas\b", re.IGNORECASE)),
    ("company-internal identifier", re.compile(r"byte[-_.]?dance|byted\.org|toutiao", re.IGNORECASE)),
    ("local absolute path", re.compile(r"/(?:Users|home|data0[0-9])/")),
    ("SSH repository URL", re.compile(r"ssh://")),
    ("private key", re.compile(r"BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY")),
    ("AWS access key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("GitHub token", re.compile(r"(?:github_pat_|gh[pousr]_)[A-Za-z0-9_]{20,}")),
    ("Slack token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
)


def tracked_files(root=ROOT):
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=True, stdout=subprocess.PIPE
    )
    return [Path(item.decode()) for item in result.stdout.split(b"\0") if item]


def audit(paths, root=ROOT):
    findings = []
    for relative in paths:
        name = relative.as_posix()
        if name in (SELF, TEST):
            continue
        for pattern in FORBIDDEN_PATHS:
            if pattern.search(name):
                findings.append(f"{name}: forbidden public path")
        path = root / relative
        try:
            text = path.read_text(errors="replace")
        except OSError as error:
            findings.append(f"{name}: cannot read: {error}")
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for label, pattern in FORBIDDEN_TEXT:
                if pattern.search(line):
                    findings.append(f"{name}:{number}: {label}")
    return findings


def main():
    findings = audit(tracked_files())
    if findings:
        print("Public-content audit failed:", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1
    print("Public-content audit passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
