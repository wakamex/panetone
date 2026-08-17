#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "slack-sdk>=3.0", "aiohttp"]
# ///

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.update({
    "WEZ_TG_TOKEN_CLAUDE": "test-claude-token",
    "WEZ_TG_TOKEN_CODEX": "test-codex-token",
    "WEZ_TG_CHAT": "1",
    "WEZ_TG_OWNER": "1",
    "WEZ_TG_STATE": "/tmp/panetone-state-fixture.json",
    "WEZ_TG_PENDING": "/tmp/panetone-pending-fixture.json",
    "WEZ_SIG_SOCKET": "",
    "WEZ_SIG_ACCOUNT": "",
    "WEZ_SIG_OWNER": "",
    "WEZ_TG_DEBATE_CHAT": "0",
    "WEZ_SLACK_BOT_TOKEN": "",
    "WEZ_SLACK_APP_TOKEN": "",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bridge


FIXTURES = Path(__file__).resolve().parent / "fixtures"


class LegacyStateFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(
            (FIXTURES / "legacy-state" / "representative.json").read_text()
        )

    def tearDown(self):
        bridge._pending_sends.clear()
        bridge._source_cursors.clear()

    def test_fixture_covers_every_legacy_state_source_and_terminal_class(self):
        self.assertEqual(
            self.fixture["schema"], "panetone.legacy-state-fixture.v1"
        )
        self.assertEqual(
            set(self.fixture["state_json"]),
            {
                "telegram_topics",
                "collab",
                "clod_off_groups",
                "signal_groups",
                "last_sources",
                "clod_off",
                "clod_history",
                "signal_group_names",
            },
        )
        self.assertEqual(
            {row["state"] for row in self.fixture["control_rows"]},
            {"in_progress", "succeeded", "failed", "indeterminate"},
        )
        self.assertEqual(
            {item["kind"] for item in self.fixture["pending_json"]["items"]},
            {"tg", "sig", "debate", "slack"},
        )

    def test_pending_state_round_trips_through_current_decoder(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pending.json"
            path.write_text(json.dumps(self.fixture["pending_json"]))
            with patch.object(bridge, "PENDING_STATE", path):
                self.assertTrue(bridge._load_pending())
                encoded = bridge._pending_json()

        self.assertEqual(encoded["schema"], "panetone.delivery-state.v2")
        self.assertEqual(encoded["cursors"], self.fixture["pending_json"]["cursors"])
        self.assertEqual(encoded["items"], self.fixture["pending_json"]["items"])

    def test_signal_fixture_reproduces_pending_inbox_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "signal.sqlite3"
            with patch.object(bridge, "SIGNAL_DB", db_path):
                bridge._signal_db_init()
                columns = tuple(self.fixture["signal_rows"][0])
                placeholders = ",".join("?" for _ in columns)
                with sqlite3.connect(db_path) as db:
                    db.executemany(
                        f"INSERT INTO signal_messages ({','.join(columns)}) "
                        f"VALUES ({placeholders})",
                        [tuple(row[column] for column in columns)
                         for row in self.fixture["signal_rows"]],
                    )
                pending = bridge._signal_db_pending("signal-alpha")

        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["formatted_text"], "[Alice] pending input")
        self.assertTrue(pending[0]["is_mention"])


class RouteResolutionFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(
            (FIXTURES / "routes" / "control-resolution.json").read_text()
        )

    def setUp(self):
        bridge.tab_topic_name.clear()
        bridge.tab_topic.clear()
        bridge.pane_tab.clear()
        bridge.pane_harness.clear()
        bridge.tab_last_pid.clear()

    def tearDown(self):
        self.setUp()

    def test_control_resolution_golden_cases(self):
        for case in self.fixture["cases"]:
            with self.subTest(case=case["name"]):
                self.setUp()
                for tab in case["tabs"]:
                    tab_id = tab["tab_id"]
                    pane_id = tab["pane_id"]
                    bridge.tab_topic_name[tab_id] = tab["title"]
                    if tab["topic_id"] is not None:
                        bridge.tab_topic[tab_id] = tab["topic_id"]
                    bridge.pane_tab[pane_id] = tab_id
                    bridge.tab_last_pid[tab_id] = pane_id
                    if tab["harness"] is not None:
                        bridge.pane_harness[pane_id] = tab["harness"]

                if "error" in case:
                    with self.assertRaises(bridge.RequestFailure) as raised:
                        bridge._control_route(case["lookup"])
                    self.assertEqual(raised.exception.code, case["error"])
                else:
                    self.assertEqual(
                        bridge._control_route(case["lookup"]), case["expected"]
                    )


if __name__ == "__main__":
    unittest.main()
