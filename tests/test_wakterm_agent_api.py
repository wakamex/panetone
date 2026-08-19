#!/usr/bin/env python3

import json
import os
import unittest
from pathlib import Path


DEFAULT_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "wakterm"
    / "docs"
    / "agent-api"
    / "v1"
    / "golden-fixtures.json"
)


class WaktermAgentApiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path(
            os.environ.get("WAKTERM_AGENT_API_FIXTURE", str(DEFAULT_FIXTURE))
        )
        if not cls.path.is_file():
            raise unittest.SkipTest(
                "set WAKTERM_AGENT_API_FIXTURE to Wakterm's golden-fixtures.json"
            )
        cls.fixture = json.loads(cls.path.read_text())

    def test_live_capabilities_advertise_the_general_event_stream(self):
        fixture = self.fixture
        current = fixture["current_capabilities"]

        self.assertEqual(fixture["fixture_schema"], "wakterm.agent-api-golden.v1")
        self.assertEqual(current["schema"], fixture["contract_schema"])
        self.assertEqual(current["api_major"], 1)
        self.assertTrue(
            {
                "catalog.v1",
                "prompt_admission.v1",
                "return_request_terminal_stream.v1",
            }.issubset(current["capabilities"])
        )
        self.assertIn("event_stream.v1", current["capabilities"])
        self.assertEqual(
            fixture["event_stream_capabilities"]["availability"], "live"
        )
        self.assertIn(
            "event_stream.v1",
            fixture["event_stream_capabilities"]["capabilities"],
        )

    def test_catalog_exposes_only_live_join_and_admission_identity(self):
        agents = self.fixture["catalog"]["agents"]

        self.assertTrue(agents)
        self.assertEqual(len({agent["agent_id"] for agent in agents}), len(agents))
        self.assertEqual(len({agent["pane_id"] for agent in agents}), len(agents))
        for agent in agents:
            self.assertIsInstance(agent["pane_id"], int)
            self.assertGreaterEqual(agent["pane_id"], 0)
            self.assertIsInstance(agent["agent_id"], str)
            if agent["alive"]:
                self.assertIsInstance(agent["incarnation_id"], str)

    def test_admission_receipts_classify_safe_retry_boundary(self):
        receipts = self.fixture["admission_receipts"]
        accepted = receipts["accepted"]
        busy = receipts["busy"]
        unavailable = receipts["unavailable"]
        indeterminate = receipts["indeterminate"]

        self.assertEqual(
            (accepted["status"], accepted["definitive"], accepted["prompt_written"]),
            ("accepted", True, True),
        )
        self.assertEqual(
            (busy["status"], busy["definitive"], busy["prompt_written"]),
            ("busy", True, False),
        )
        self.assertEqual(
            (
                unavailable["status"],
                unavailable["definitive"],
                unavailable["prompt_written"],
            ),
            ("unavailable", True, False),
        )
        self.assertEqual(
            (
                indeterminate["status"],
                indeterminate["definitive"],
                indeterminate["prompt_written"],
            ),
            ("indeterminate", False, None),
        )
        classified = {
            error["code"]: (error["definitive"], error.get("prompt_written"))
            for error in self.fixture["classified_errors"]
            if error["operation"] == "admit_prompt"
        }
        self.assertEqual(classified["busy"], (True, False))
        self.assertEqual(classified["indeterminate"], (False, None))

    def test_live_event_examples_define_order_gap_and_lifecycle(self):
        page = self.fixture["event_page"]
        events = page["events"]
        sequences = [event["sequence"] for event in events]

        self.assertEqual(page["availability"], "live_example")
        self.assertEqual(sequences, sorted(sequences))
        self.assertEqual(len(sequences), len(set(sequences)))
        self.assertEqual(page["next_after_sequence"], sequences[-1])
        self.assertTrue(
            {
                "agent_lifecycle",
                "turn_started",
                "turn_state_changed",
                "plan",
                "assistant_message",
                "observer_failure",
                "turn_final",
            }.issubset(event["kind"] for event in events)
        )

        lifecycle = self.fixture["lifecycle_page"]
        self.assertEqual(lifecycle["availability"], "live_example")
        self.assertGreater(lifecycle["events"][0]["sequence"], sequences[-1])
        self.assertEqual(lifecycle["events"][0]["lifecycle"], "unavailable")

        gap = self.fixture["cursor_too_old"]
        retention = self.fixture["retention"]
        self.assertEqual(gap["status"], "cursor_too_old")
        self.assertLess(
            gap["requested_after_sequence"], gap["oldest_available_sequence"]
        )
        self.assertEqual(gap["recovery"]["kind"], "catalog_snapshot")
        self.assertEqual(retention["mode"], "bounded")
        self.assertEqual(retention["gap_behavior"], "cursor_too_old")


if __name__ == "__main__":
    unittest.main()
