# SPDX-License-Identifier: Apache-2.0

import tempfile
from pathlib import Path
import unittest

from public_audit import audit


class PublicAuditTests(unittest.TestCase):
    def test_clean_file_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = Path("README.md")
            (root / path).write_text("Public documentation\n")
            self.assertEqual(audit([path], root), [])

    def test_internal_reference_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = Path("notes.md")
            (root / path).write_text("internal: se" "kas\n")
            self.assertIn("internal project name", audit([path], root)[0])

    def test_private_key_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = Path("credentials/server.pem")
            (root / path).parent.mkdir()
            (root / path).write_text("fixture\n")
            self.assertIn("forbidden public path", audit([path], root)[0])
