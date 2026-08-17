#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "slack-sdk>=3.0", "aiohttp"]
# ///

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

os.environ.update({
    "WEZ_TG_TOKEN_CLAUDE": "test-claude-token",
    "WEZ_TG_TOKEN_CODEX": "test-codex-token",
    "WEZ_TG_CHAT": "1",
    "WEZ_TG_OWNER": "1",
    "WEZ_TG_STATE": "/tmp/panetone-channel-fixture-state.json",
    "WEZ_TG_PENDING": "/tmp/panetone-channel-fixture-pending.json",
    "WEZ_SIG_SOCKET": "",
    "WEZ_SIG_ACCOUNT": "",
    "WEZ_SIG_OWNER": "",
    "WEZ_TG_DEBATE_CHAT": "0",
    "WEZ_SLACK_BOT_TOKEN": "",
    "WEZ_SLACK_APP_TOKEN": "",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bridge


FIXTURE = json.loads(
    (
        Path(__file__).resolve().parent
        / "fixtures"
        / "channels"
        / "routing.json"
    ).read_text()
)


class ChannelRoutingFixtureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bridge.pane_tab.clear()
        bridge.tab_topic.clear()
        bridge.tab_topic_name.clear()
        bridge.sig_tab_group.clear()
        bridge.sig_tab_name.clear()
        bridge.tab_last_source.clear()
        bridge.last_source_name.clear()
        bridge._slack_direct_tab.clear()
        bridge._pending_sends.clear()

    def tearDown(self):
        self.setUp()

    def test_main_route_golden_cases(self):
        for case in FIXTURE["route_cases"]:
            with self.subTest(case=case["name"]):
                self.setUp()
                pane_id = 11
                tab_id = 1
                bridge.pane_tab[pane_id] = tab_id
                bridge.tab_topic_name[tab_id] = "Alpha"
                if case["telegram_topic"] is not None:
                    bridge.tab_topic[tab_id] = case["telegram_topic"]
                if case["signal_group"] is not None:
                    bridge.sig_tab_group[tab_id] = case["signal_group"]
                    bridge.sig_tab_name[tab_id] = "Alpha"
                if case["source"] is not None:
                    bridge.tab_last_source[tab_id] = case["source"]
                if case["slack_channel"] is not None:
                    bridge._slack_direct_tab[tab_id] = case["slack_channel"]

                with patch.multiple(
                    bridge,
                    SIGNAL_ENABLED=case["signal_enabled"],
                    DEBATE_ENABLED=case["debate_enabled"],
                    DEBATE_TABS=case["debate_tabs"],
                    DEBATE_CHAT=case.get("debate_chat", 0),
                    SLACK_ENABLED=case["slack_enabled"],
                ):
                    actual = bridge._pane_main_route(pane_id)
                if actual is not None:
                    actual = list(actual)
                self.assertEqual(actual, case["expected"])

    def test_text_format_golden_cases(self):
        cases = FIXTURE["format_cases"]
        slack = cases["slack_markdown_table"]
        self.assertEqual(bridge._md_tables_to_slack(slack["input"]), slack["expected"])
        for case in cases["signal_group_ids"]:
            with self.subTest(signal_group=case["input"]):
                self.assertEqual(
                    bridge._normalize_signal_group_id(case["input"]),
                    case["expected"],
                )
        chunks = cases["line_chunks"]
        self.assertEqual(
            list(bridge._chunkify(chunks["input"], chunks["limit"])),
            chunks["expected"],
        )
        utf16 = cases["utf16_chunks"]
        self.assertEqual(
            list(bridge._utf16_chunks(utf16["input"], utf16["limit"])),
            utf16["expected"],
        )

    async def test_retry_fairness_golden_case(self):
        bridge._pending_sends.extend(
            tuple(item) for item in FIXTURE["retry_fairness"]["items"]
        )
        attempts = []

        async def attempt(item):
            attempts.append(item[6])
            return False

        with patch.object(bridge, "_attempt_pending", AsyncMock(side_effect=attempt)):
            await bridge._flush_pending()

        self.assertEqual(
            attempts, FIXTURE["retry_fairness"]["expected_attempt_ids"]
        )


if __name__ == "__main__":
    unittest.main()
