#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# ///

import ast
import configparser
import sys
import unittest
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class DeploymentContractTests(unittest.TestCase):
    def test_runtime_lock_covers_every_declared_dependency(self):
        lock = tomllib.loads((ROOT / "bridge.py.lock").read_text())
        requirements = {
            requirement["name"] for requirement in lock["manifest"]["requirements"]
        }

        self.assertEqual(
            requirements,
            {"aiohttp", "python-telegram-bot", "slack-sdk"},
        )

    def test_system_unit_runs_locked_source_as_mihai(self):
        config = configparser.ConfigParser(interpolation=None, strict=False)
        config.optionxform = str
        config.read(ROOT / "deploy" / "panetone.service")
        service = config["Service"]

        self.assertEqual(service["User"], "mihai")
        self.assertEqual(service["Group"], "mihai")
        self.assertEqual(service["UMask"], "0077")
        self.assertEqual(service["KillMode"], "control-group")
        self.assertEqual(service["Restart"], "on-failure")
        self.assertEqual(
            service["ExecStart"],
            "/usr/local/bin/uv --no-config run --locked --script /code/msger/bridge.py",
        )

    def test_poll_loop_cannot_reexecute_the_process(self):
        tree = ast.parse((ROOT / "bridge.py").read_text())
        poll_loop = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "poll_loop"
        )
        calls = [node for node in ast.walk(poll_loop) if isinstance(node, ast.Call)]

        self.assertFalse(
            any(
                isinstance(call.func, ast.Attribute)
                and call.func.attr in {"execv", "execve", "execl", "execlp"}
                for call in calls
            )
        )


if __name__ == "__main__":
    unittest.main()
