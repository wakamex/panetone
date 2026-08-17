#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# ///

import subprocess
import sys
import unittest
from pathlib import Path


class ControlConformanceTests(unittest.TestCase):
    def test_python_backend_matches_current_profile(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [
                sys.executable,
                str(root / "tests" / "run_control_conformance.py"),
                "--profile",
                "python-current",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
