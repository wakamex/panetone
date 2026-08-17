#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "aiohttp"]
# ///

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tests" / "run_codex_shadow.py"
spec = importlib.util.spec_from_file_location("run_codex_shadow", SCRIPT)
shadow = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = shadow
spec.loader.exec_module(shadow)
bridge = shadow._load_bridge()


class FakeClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.cursors = []

    def read(self, cursor):
        self.cursors.append(cursor)
        return self.pages.pop(0)


def page(events, *, cursor="next", has_more=False, status="ok"):
    return {
        "schema": shadow.WAKTERM_SCHEMA,
        "status": status,
        "agent_id": "agent-1",
        "session_id": "session-1",
        "baseline": False,
        "events": events,
        "next_cursor": cursor,
        "has_more": has_more,
        "detail": None,
    }


def event(identifier, text):
    return {
        "event_id": identifier,
        "kind": "assistant_message",
        "turn_id": "turn-1",
        "timestamp": None,
        "text": text,
    }


class CodexShadowTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.session = Path(self.tempdir.name) / "rollout.jsonl"
        self.session.write_text(json.dumps({"type": "session_meta"}) + "\n")
        self.harness = shadow._codex_harness(bridge)
        self.cursor = bridge._jsonl_tail_cursor(self.harness, self.session)
        self.state = {
            "schema": shadow.SCHEMA,
            "target": "test-agent",
            "cwd": "/code/test",
            "agent_id": "agent-1",
            "session_id": "session-1",
            "wakterm_cursor": "baseline",
            "legacy_session": str(self.session),
            "legacy_cursor": self.cursor,
        }

    def tearDown(self):
        self.tempdir.cleanup()

    def append_assistant(self, text):
        record = {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            },
        }
        with self.session.open("a") as stream:
            stream.write(json.dumps(record) + "\n")

    def test_exact_messages_match_across_multiple_pages(self):
        self.append_assistant(" first ")
        self.append_assistant("second")
        client = FakeClient(
            [
                page([], cursor="middle", has_more=True),
                page([event("e1", "first"), event("e2", "second")]),
                page([], cursor="middle", has_more=True),
                page([event("e1", "first"), event("e2", "second")]),
            ]
        )

        result = shadow.compare_state(self.state, client, bridge)

        self.assertEqual(result["status"], "match")
        self.assertEqual(result["classification"], "equivalent")
        self.assertEqual(
            client.cursors, ["baseline", "middle", "baseline", "middle"]
        )
        self.assertTrue(result["wakterm_replay_stable"])

    def test_message_difference_is_unexplained(self):
        self.append_assistant("legacy")
        client = FakeClient(
            [page([event("e1", "wakterm")]), page([event("e1", "wakterm")])]
        )

        result = shadow.compare_state(self.state, client, bridge)

        self.assertEqual(result["status"], "difference")
        self.assertEqual(result["classification"], "unexplained")

    def test_wakterm_gap_is_indeterminate(self):
        client = FakeClient([page([], status="session_changed")])

        result = shadow.compare_state(self.state, client, bridge)

        self.assertEqual(result["status"], "indeterminate")
        self.assertEqual(result["reason"], "wakterm_session_changed")

    def test_changed_replay_is_indeterminate(self):
        self.append_assistant("first")
        client = FakeClient(
            [page([event("e1", "first")]), page([event("e2", "changed")])]
        )

        result = shadow.compare_state(self.state, client, bridge)

        self.assertEqual(result["status"], "indeterminate")
        self.assertEqual(result["reason"], "wakterm_replay_changed")

    def test_legacy_inode_change_is_indeterminate(self):
        replacement = self.session.with_suffix(".replacement")
        replacement.write_text(self.session.read_text())
        os.replace(replacement, self.session)

        result = shadow.compare_state(self.state, FakeClient([]), bridge)

        self.assertEqual(result["status"], "indeterminate")
        self.assertEqual(result["reason"], "legacy_source_changed")

    def test_private_state_writer_refuses_overwrite(self):
        state_path = Path(self.tempdir.name) / "state.json"
        shadow._write_private_json(state_path, self.state)
        self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            shadow._write_private_json(state_path, self.state)


if __name__ == "__main__":
    unittest.main()
