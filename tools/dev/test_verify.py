# SPDX-License-Identifier: Apache-2.0

import sys
import tempfile
from pathlib import Path
import unittest

from verify import execute


class ProcessLifecycleTests(unittest.TestCase):
    def test_exit_failure_and_output_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "step.log"
            code, status, cpu_seconds = execute(
                [sys.executable, "-c", "print('diagnostic'); raise SystemExit(7)"], log, 5)
            self.assertEqual((code, status), (7, "FAIL"))
            self.assertGreater(cpu_seconds, 0)
            self.assertIn("diagnostic", log.read_text())

    def test_timeout_stops_step_and_retains_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "step.log"
            code, status, cpu_seconds = execute(
                [sys.executable, "-c", "import time; print('waiting', flush=True); time.sleep(30)"],
                log, 0.5)
            self.assertEqual((code, status), (None, "TIMEOUT"))
            self.assertGreaterEqual(cpu_seconds, 0)
            self.assertIn("waiting", log.read_text())
