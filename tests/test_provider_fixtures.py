#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "aiohttp"]
# ///

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

os.environ.update({
    "WEZ_TG_TOKEN_CLAUDE": "test-claude-token",
    "WEZ_TG_TOKEN_CODEX": "test-codex-token",
    "WEZ_TG_CHAT": "1",
    "WEZ_TG_OWNER": "1",
    "WEZ_TG_STATE": "/tmp/panetone-provider-fixture-state.json",
    "WEZ_TG_PENDING": "/tmp/panetone-provider-fixture-pending.json",
    "WEZ_SIG_SOCKET": "",
    "WEZ_SIG_ACCOUNT": "",
    "WEZ_SIG_OWNER": "",
    "WEZ_TG_DEBATE_CHAT": "0",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bridge


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "agent-observation"


class ProviderObservationFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(
            (FIXTURES / "provider-observations.json").read_text()
        )

    def test_formatter_golden_cases(self):
        formatters = {
            "claude": bridge._claude_format,
            "codex": bridge._codex_format,
            "opencode": bridge._opencode_format,
        }
        for case in self.fixture["format_cases"]:
            with self.subTest(case=case["name"]):
                self.assertEqual(
                    formatters[case["provider"]](case["record"]),
                    case["expected"],
                )

    def test_jsonl_partial_record_and_ordering(self):
        formatters = {
            "claude": bridge._claude_format,
            "codex": bridge._codex_format,
        }
        with tempfile.TemporaryDirectory() as tmp:
            for case in self.fixture["jsonl_cases"]:
                with self.subTest(case=case["name"]):
                    path = Path(tmp) / f"{case['name']}.jsonl"
                    complete = b"".join(
                        json.dumps(record).encode() + b"\n"
                        for record in case["complete_records"]
                    )
                    partial = json.dumps(case["partial_record"]).encode()
                    split = max(1, len(partial) // 2)
                    path.write_bytes(complete + partial[:split])
                    stat = path.stat()
                    cursor = {
                        "kind": "jsonl",
                        "harness": case["provider"],
                        "path": str(path),
                        "offset": 0,
                        "dev": stat.st_dev,
                        "ino": stat.st_ino,
                    }
                    harness = SimpleNamespace(
                        name=case["provider"],
                        format_record=formatters[case["provider"]],
                    )
                    messages, cursor, _records = bridge._read_jsonl_new(
                        harness, path, cursor
                    )
                    self.assertEqual(messages, case["expected_first"])
                    self.assertEqual(cursor["offset"], len(complete))

                    with path.open("ab") as stream:
                        stream.write(partial[split:] + b"\n")
                    messages, cursor, _records = bridge._read_jsonl_new(
                        harness, path, cursor
                    )
                    self.assertEqual(messages, case["expected_after_completion"])
                    self.assertEqual(cursor["offset"], path.stat().st_size)

    def test_gemini_cursor_golden_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            for case in self.fixture["gemini_cases"]:
                with self.subTest(case=case["name"]):
                    path = Path(tmp) / f"{case['name']}.json"
                    path.write_text(json.dumps({"messages": case["messages"]}))
                    messages, cursor = bridge._gemini_read_new(path, case["cursor"])
                    self.assertEqual(messages, case["expected"])
                    self.assertEqual(cursor["index"], case["expected_index"])
                    self.assertEqual(cursor["last_id"], case["expected_last_id"])

    def test_opencode_cursor_golden_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            for case in self.fixture["opencode_cases"]:
                with self.subTest(case=case["name"]):
                    path = Path(tmp) / f"{case['name']}.sqlite3"
                    with sqlite3.connect(path) as db:
                        db.execute("CREATE TABLE message (id TEXT PRIMARY KEY, data TEXT)")
                        db.execute(
                            "CREATE TABLE part (id TEXT, session_id TEXT, "
                            "message_id TEXT, data TEXT)"
                        )
                        for part in case["parts"]:
                            db.execute(
                                "INSERT INTO message(id, data) VALUES (?, ?)",
                                (
                                    part["message_id"],
                                    json.dumps({"role": part["role"]}),
                                ),
                            )
                            db.execute(
                                "INSERT INTO part(id, session_id, message_id, data) "
                                "VALUES (?, ?, ?, ?)",
                                (
                                    part["part_id"],
                                    case["session_id"],
                                    part["message_id"],
                                    json.dumps(part["data"]),
                                ),
                            )
                    messages, cursor = bridge._opencode_read_new(
                        (path, case["session_id"]), case["cursor"]
                    )
                    self.assertEqual(messages, case["expected"])
                    self.assertEqual(cursor["rowid"], case["expected_rowid"])
                    self.assertEqual(cursor["part_id"], case["expected_part_id"])

    def test_consumer_requirements_cover_phase1_failure_boundaries(self):
        requirements = json.loads(
            (FIXTURES / "consumer-requirements.json").read_text()
        )
        self.assertEqual(
            requirements["schema"],
            "panetone.wakterm-consumer-requirements.v1",
        )
        joined = "\n".join(
            item
            for key, values in requirements.items()
            if isinstance(values, list)
            for item in values
        )
        for required in (
            "process incarnation",
            "indeterminate",
            "cursor_too_old",
            "incompatible major versions",
            "recording sink",
            "not production promotion evidence",
        ):
            self.assertIn(required, joined)


if __name__ == "__main__":
    unittest.main()
