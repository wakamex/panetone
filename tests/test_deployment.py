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
            {"aiohttp", "python-telegram-bot"},
        )

    def test_system_unit_runs_the_installed_rust_binary_as_mihai(self):
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
            "/usr/local/bin/panetone daemon --socket /run/panetone/control.sock "
            "--database /var/lib/panetone/panetone.sqlite3 "
            "--wakterm-bin /usr/local/bin/wakterm "
            "--wakterm-socket /run/wakterm/sock",
        )
        self.assertEqual(service["EnvironmentFile"], "/etc/panetone/panetone.env")
        self.assertEqual(service["RuntimeDirectoryMode"], "0700")
        self.assertEqual(service["StateDirectoryMode"], "0700")
        self.assertNotIn("/code/", service["ExecStart"])

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

    def test_system_installer_has_failure_rollback(self):
        installer = (ROOT / "deploy" / "install-system-service.sh").read_text()

        self.assertIn("Without --apply or --start-held", installer)
        self.assertIn("rollback_install", installer)
        self.assertIn("wakterm-mux-server.service", installer)
        self.assertIn("refusing to overwrite durable state", installer)
        self.assertIn("systemctl disable --now panetone.service", installer)
        self.assertIn("runuser --user mihai", installer)
        self.assertIn('delivery_hold"] is True', installer)

    def test_wakterm_preflight_is_pinned_and_production_isolated(self):
        script = (ROOT / "dev" / "phase5b-wakterm-preflight").read_text()

        self.assertIn("cd0a9225c5a9f60fccde69796f8f0c52fa47ba4b", script)
        self.assertIn("worktree add --detach", script)
        self.assertIn("PANETONE_WAKTERM_SOURCE", script)
        self.assertIn("CARGO_TARGET_DIR", script)
        self.assertIn("env -u CARGO_TARGET_DIR", script)
        self.assertIn("PANETONE_WAKTERM_DEV_RUNTIME_ROOT", script)
        self.assertIn("PANETONE_WAKTERM_DEV_STATE_ROOT", script)
        self.assertIn("agent capabilities", script)
        self.assertIn("agent catalog", script)
        self.assertIn("agent events", script)
        self.assertIn("production_effects", script)
        self.assertNotIn("systemctl", script)
        self.assertNotIn("agent admit", script)

    def test_wakterm_production_checklist_requires_manual_agent_restoration(self):
        checklist = (ROOT / "docs" / "wakterm-phase5b-promotion.md").read_text()

        self.assertIn("reliable automatic", checklist)
        self.assertIn("Manually run each recorded exact", checklist)
        self.assertIn("mixed-codec", checklist)
        self.assertIn("keep Panetone stopped", checklist)


if __name__ == "__main__":
    unittest.main()
