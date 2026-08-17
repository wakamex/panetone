#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "slack-sdk>=3.0", "aiohttp"]
# ///

import asyncio
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telegram.error import BadRequest

os.environ.update({
    "WEZ_TG_TOKEN_CLAUDE": "test-claude-token",
    "WEZ_TG_TOKEN_CODEX": "test-codex-token",
    "WEZ_TG_CHAT": "1",
    "WEZ_TG_OWNER": "1",
    "WEZ_TG_STATE": "/tmp/panetone-test-state.json",
    "WEZ_TG_PENDING": "/tmp/panetone-test-pending.json",
    "WEZ_SIG_SOCKET": "",
    "WEZ_SIG_ACCOUNT": "",
    "WEZ_SIG_OWNER": "",
    "WEZ_TG_DEBATE_CHAT": "0",
    "WEZ_SLACK_BOT_TOKEN": "",
    "WEZ_SLACK_APP_TOKEN": "",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bridge


class FakeTopicBot:
    def __init__(self, edit_error=None, created_id=9001):
        self.edit_error = edit_error
        self.created_id = created_id
        self.created = []
        self.edited = []

    async def edit_forum_topic(self, chat_id, topic_id, name):
        self.edited.append((chat_id, topic_id, name))
        if self.edit_error:
            raise self.edit_error

    async def create_forum_topic(self, chat_id, name):
        self.created.append((chat_id, name))
        return SimpleNamespace(message_thread_id=self.created_id)


class FakeSendBot:
    def __init__(self, pending_path, error=None):
        self.pending_path = pending_path
        self.error = error
        self.saw_durable_item = False

    async def send_message(self, chat_id, chunk, message_thread_id):
        saved = json.loads(self.pending_path.read_text())
        self.saw_durable_item = any(
            item["chunk"] == chunk for item in saved["items"]
        )
        if self.error:
            raise self.error
        return SimpleNamespace(message_id=77, message_thread_id=message_thread_id)


class FakeControlBot:
    def __init__(self, order, fail_on=None):
        self.order = order
        self.fail_on = fail_on
        self.sent = []
        self.edited = []

    async def send_message(self, chat_id, text, message_thread_id, **kwargs):
        number = len(self.sent) + 1
        self.order.append(("telegram", text))
        if self.fail_on == number:
            raise RuntimeError("telegram offline")
        message = SimpleNamespace(message_id=100 + number)
        self.sent.append((chat_id, text, message_thread_id, kwargs))
        return message

    async def edit_message_text(self, text, chat_id, message_id):
        self.edited.append((text, chat_id, message_id))


class BridgeStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_state = bridge.STATE
        self.old_pending_state = bridge.PENDING_STATE
        self.old_primary_bot = bridge._primary_bot
        self.old_codex_harness = bridge.harnesses.get("codex")
        self.old_return_supported = bridge._wakterm_return_supported
        self.old_return_reason = bridge._wakterm_return_unavailable_reason
        bridge.STATE = Path(self.tmp.name) / "state.json"
        bridge.PENDING_STATE = Path(self.tmp.name) / "pending.json"
        bridge._wakterm_return_supported = True
        bridge._wakterm_return_unavailable_reason = None
        bridge._pending_sends.clear()
        bridge._source_cursors.clear()
        bridge.tg_name_topic.clear()
        bridge.tab_topic.clear()
        bridge.tab_topic_name.clear()
        bridge.topic_tab.clear()
        bridge.pane_harness.clear()
        bridge.pane_tab.clear()
        bridge.pane_cwds.clear()
        bridge.tab_last_pid.clear()
        bridge.msg_pane.clear()
        bridge.tab_last_source.clear()
        bridge.last_source_name.clear()
        bridge._tg_verified_names.clear()
        bridge._tg_stale_topics.clear()
        bridge._tg_retry_after_until = 0.0

    def tearDown(self):
        bridge.STATE = self.old_state
        bridge.PENDING_STATE = self.old_pending_state
        bridge._primary_bot = self.old_primary_bot
        bridge._wakterm_return_supported = self.old_return_supported
        bridge._wakterm_return_unavailable_reason = self.old_return_reason
        if self.old_codex_harness is None:
            bridge.harnesses.pop("codex", None)
        else:
            bridge.harnesses["codex"] = self.old_codex_harness
        bridge._pending_sends.clear()
        bridge._source_cursors.clear()
        bridge.tg_name_topic.clear()
        bridge.tab_topic.clear()
        bridge.tab_topic_name.clear()
        bridge.topic_tab.clear()
        bridge.pane_harness.clear()
        bridge.pane_tab.clear()
        bridge.pane_cwds.clear()
        bridge.tab_last_pid.clear()
        bridge.msg_pane.clear()
        bridge.tab_last_source.clear()
        bridge.last_source_name.clear()
        bridge._tg_verified_names.clear()
        bridge._tg_stale_topics.clear()
        self.tmp.cleanup()

    def _configure_output(self, bot=None):
        bridge.pane_harness[9] = "codex"
        bridge.pane_tab[9] = 1
        bridge.pane_cwds[9] = self.tmp.name
        bridge.tab_topic[1] = 42
        bridge.tab_topic_name[1] = "route"
        bridge.tab_last_source[1] = "tg"
        bridge.harnesses["codex"] = SimpleNamespace(
            name="codex",
            display_name="Codex",
            bot=bot,
            find_session=lambda _cwd: None,
        )

    def _configure_control(self, bot):
        bridge.pane_harness.update({11: "codex", 22: "codex"})
        bridge.pane_tab.update({11: 1, 22: 2})
        bridge.pane_cwds.update({11: "/code/source", 22: "/code/target"})
        bridge.tab_topic.update({1: 41, 2: 42})
        bridge.tab_topic_name.update({1: "Source", 2: "Target"})
        bridge._rebuild()
        bridge.harnesses["codex"] = SimpleNamespace(
            name="codex",
            display_name="Codex",
            bot=bot,
            find_session=lambda _cwd: None,
        )

    @staticmethod
    def _agent_catalog():
        return [
            {
                "pane_id": 11,
                "agent_id": "source-agent",
                "incarnation_id": "source-incarnation",
                "name": "source_codex",
                "harness": "codex",
                "alive": True,
            },
            {
                "pane_id": 22,
                "agent_id": "target-agent",
                "incarnation_id": "target-incarnation",
                "name": "target_codex",
                "harness": "codex",
                "alive": True,
            },
        ]

    @staticmethod
    def _output_batch(messages, offset=200):
        return {
            "pid": 9,
            "source_key": "jsonl:codex:/tmp/session.jsonl",
            "cursor": {"kind": "jsonl", "offset": offset},
            "messages": messages,
            "records": len(messages),
            "baseline": False,
        }

    def test_canonical_delivery_state_loads(self):
        state = {
            "schema": "panetone.delivery-state.v2",
            "saved_at": 1,
            "cursors": {"source": {"kind": "jsonl", "offset": 10}},
            "items": [
                {
                    "id": "item-1",
                    "kind": "tg",
                    "target": 10781,
                    "chunk": "preserve me",
                    "pane_id": 20,
                    "harness": "codex",
                    "route_title": "Darpa",
                }
            ],
        }
        bridge._write_json(bridge.PENDING_STATE, state, mode=0o600)

        loaded = bridge._load_pending()

        self.assertTrue(loaded)
        self.assertEqual(
            [item[:6] for item in bridge._pending_sends],
            [("tg", 10781, "preserve me", 20, "codex", "darpa")],
        )
        self.assertEqual(bridge._source_cursors["source"]["offset"], 10)
        self.assertEqual(bridge.PENDING_STATE.stat().st_mode & 0o777, 0o600)

    async def test_missing_legacy_topic_is_recreated_and_backlog_retargeted(self):
        bridge._primary_bot = FakeTopicBot(
            edit_error=BadRequest("Message thread not found"),
            created_id=22001,
        )
        bridge._pending_sends.append(
            ("tg", 10781, "chunk", 20, "codex", "darpa", "test-1")
        )

        topic_id, changed = await bridge._ensure_telegram_topic("darpa", 10781)

        self.assertTrue(changed)
        self.assertEqual(topic_id, 22001)
        self.assertEqual(bridge.tg_name_topic["darpa"], 22001)
        self.assertEqual(bridge._pending_sends[0][1], 22001)

    async def test_sync_adopts_valid_pending_topic_without_duplicate(self):
        bridge._primary_bot = FakeTopicBot(
            edit_error=BadRequest("Topic_not_modified")
        )
        bridge._pending_sends.append(
            ("tg", 10885, "chunk", 22, "codex", "transcribe", "test-1")
        )
        pane = {
            "pane_id": 22,
            "tab_id": 18,
            "cwd": self.tmp.name,
            "tab_title": "Transcribe",
        }

        await bridge.sync_topics([], [(pane, None)])

        self.assertEqual(bridge.tg_name_topic["transcribe"], 10885)
        self.assertEqual(bridge.tab_topic[18], 10885)
        self.assertEqual(bridge._primary_bot.created, [])

    async def test_transient_validation_error_does_not_create_topic(self):
        bridge._primary_bot = FakeTopicBot(edit_error=RuntimeError("Timed out"))

        topic_id, changed = await bridge._ensure_telegram_topic("transcribe", 10885)

        self.assertIsNone(topic_id)
        self.assertFalse(changed)
        self.assertEqual(bridge._primary_bot.created, [])
        self.assertNotIn("transcribe", bridge.tg_name_topic)

    def test_title_route_retargets_every_chunk(self):
        bridge._pending_sends.extend(
            [
                ("tg", 1, "a", 20, "codex", "darpa", "test-a"),
                ("tg", 1, "b", 20, "codex", "darpa", "test-b"),
                ("tg", 2, "c", 22, "codex", "transcribe", "test-c"),
            ]
        )

        changed = bridge._retarget_pending_telegram("DARPA", 99)

        self.assertTrue(changed)
        self.assertEqual([item[1] for item in bridge._pending_sends], [99, 99, 2])

    async def test_production_output_is_durable_before_network_delivery(self):
        bot = FakeSendBot(bridge.PENDING_STATE)
        self._configure_output(bot)
        source_key = "jsonl:codex:/tmp/session.jsonl"
        bridge._source_cursors[source_key] = {"kind": "jsonl", "offset": 10}

        with patch.object(
            bridge, "_peek_new_sync", return_value=self._output_batch(["new output"])
        ):
            await bridge.check_output()

        self.assertTrue(bot.saw_durable_item)
        self.assertEqual(bridge._pending_sends, [])
        saved = json.loads(bridge.PENDING_STATE.read_text())
        self.assertEqual(saved["items"], [])
        self.assertEqual(saved["cursors"][source_key]["offset"], 200)

    async def test_failed_output_survives_memory_reset(self):
        bot = FakeSendBot(bridge.PENDING_STATE, error=RuntimeError("offline"))
        self._configure_output(bot)
        source_key = "jsonl:codex:/tmp/session.jsonl"
        bridge._source_cursors[source_key] = {"kind": "jsonl", "offset": 10}

        with patch.object(
            bridge, "_peek_new_sync", return_value=self._output_batch(["keep output"])
        ):
            await bridge.check_output()
        bridge._pending_sends.clear()
        bridge._source_cursors.clear()
        bridge._load_pending()

        self.assertTrue(bot.saw_durable_item)
        self.assertEqual(
            [item[:6] for item in bridge._pending_sends],
            [("tg", 42, "keep output", 9, "codex", "route")],
        )

    async def test_output_batch_and_cursor_are_checkpointed_together(self):
        self._configure_output()
        source_key = "jsonl:codex:/tmp/session.jsonl"
        bridge._source_cursors[source_key] = {"kind": "jsonl", "offset": 10}
        batch = self._output_batch(["first", "second"])
        checkpoints = []

        async def observe_flush():
            if bridge.PENDING_STATE.exists():
                checkpoints.append(json.loads(bridge.PENDING_STATE.read_text()))

        with (
            patch.object(bridge, "_peek_new_sync", return_value=batch),
            patch.object(bridge, "_flush_pending", AsyncMock(side_effect=observe_flush)),
            patch.object(
                bridge,
                "_chunkify",
                side_effect=lambda text: [f"{text}-a", f"{text}-b"],
            ),
        ):
            await bridge.check_output()

        self.assertEqual(len(checkpoints), 1)
        saved = checkpoints[0]
        self.assertEqual(saved["cursors"][source_key]["offset"], 200)
        self.assertEqual(
            [item["chunk"] for item in saved["items"]],
            ["first-a", "first-b", "second-a", "second-b"],
        )

    async def test_idle_poll_does_not_checkpoint_unchanged_cursor(self):
        self._configure_output()
        source_key = "jsonl:codex:/tmp/session.jsonl"
        cursor = {"kind": "jsonl", "offset": 10}
        bridge._source_cursors[source_key] = cursor
        batch = self._output_batch([], offset=10)

        with (
            patch.object(bridge, "_peek_new_sync", return_value=batch),
            patch.object(bridge, "_write_json") as write_json,
        ):
            await bridge.check_output()

        write_json.assert_not_called()

    def test_failed_checkpoint_keeps_queue_and_cursor_in_memory(self):
        old_item = ("tg", 1, "old", 9, "codex", "route", "old-id")
        new_item = ("tg", 1, "new", 9, "codex", "route", "new-id")
        bridge._pending_sends.append(old_item)
        bridge._source_cursors["source"] = {"offset": 10}

        with (
            patch.object(bridge, "_write_json", side_effect=OSError("disk full")),
            self.assertRaises(OSError),
        ):
            bridge._commit_delivery([new_item], {"source": {"offset": 20}})

        self.assertEqual(bridge._pending_sends, [old_item])
        self.assertEqual(bridge._source_cursors, {"source": {"offset": 10}})

    def test_partial_jsonl_record_is_not_consumed(self):
        path = Path(self.tmp.name) / "session.jsonl"
        first = json.dumps({"text": "one"}).encode() + b"\n"
        second = json.dumps({"text": "two"}).encode()
        path.write_bytes(first + second[:8])
        st = path.stat()
        cursor = {
            "kind": "jsonl",
            "harness": "test",
            "path": str(path),
            "offset": 0,
            "dev": st.st_dev,
            "ino": st.st_ino,
        }
        harness = SimpleNamespace(
            name="test", format_record=lambda record: record.get("text")
        )

        messages, next_cursor, _ = bridge._read_jsonl_new(harness, path, cursor)
        self.assertEqual(messages, ["one"])
        self.assertEqual(next_cursor["offset"], len(first))

        with path.open("ab") as f:
            f.write(second[8:] + b"\n")
        messages, final_cursor, _ = bridge._read_jsonl_new(
            harness, path, next_cursor
        )
        self.assertEqual(messages, ["two"])
        self.assertEqual(final_cursor["offset"], path.stat().st_size)

    def test_unknown_session_baselines_at_current_tail(self):
        path = Path(self.tmp.name) / "new-session.jsonl"
        record = {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "first reply"}],
            },
        }
        path.write_text(json.dumps(record) + "\n")
        bridge.harnesses["codex"] = SimpleNamespace(
            name="codex",
            find_session=lambda _cwd: path,
            format_record=bridge._codex_format,
        )
        bridge.pane_harness[9] = "codex"
        bridge.pane_cwds[9] = self.tmp.name
        batch = bridge._peek_new_sync(9, {})

        self.assertTrue(batch["baseline"])
        self.assertEqual(batch["messages"], [])
        self.assertEqual(batch["cursor"]["offset"], path.stat().st_size)

        record["payload"]["content"][0]["text"] = "next reply"
        with path.open("a") as session:
            session.write(json.dumps(record) + "\n")
        resumed = bridge._peek_new_sync(
            9, {batch["source_key"]: batch["cursor"]}
        )

        self.assertFalse(resumed["baseline"])
        self.assertEqual(resumed["messages"], ["next reply"])

    def test_codex_session_discovery_ignores_newer_subagent(self):
        sessions = Path(self.tmp.name) / "sessions"
        sessions.mkdir()
        parent = sessions / "rollout-parent.jsonl"
        child = sessions / "rollout-child.jsonl"
        cwd = "/code/application"
        parent.write_text(json.dumps({
            "type": "session_meta",
            "payload": {"cwd": cwd, "source": "cli"},
        }) + "\n")
        child.write_text(json.dumps({
            "type": "session_meta",
            "payload": {
                "cwd": cwd,
                "source": {"subagent": {"thread_spawn": {"depth": 1}}},
            },
        }) + "\n")
        now = time.time()
        os.utime(parent, (now - 1, now - 1))
        os.utime(child, (now, now))

        with patch.object(bridge, "CODEX_DIR", sessions):
            bridge._codex_cache = {}
            bridge._codex_cache_t = 0.0
            found = bridge._codex_find_session(cwd)

        self.assertEqual(found, parent)

    async def test_control_send_audits_before_async_wakterm_delivery(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        transitions = []

        async def transition(state, progress=None):
            transitions.append((state, progress))

        async def agent_send(pid, text):
            order.append(("wakterm", text))
            return {
                "agent_id": "target-agent",
                "agent_name": "target",
                "pane_id": pid,
                "submitted": True,
                "acknowledgement": {"acknowledged": True},
            }

        request = {
            "id": "00000000-0000-4000-8000-000000000001",
            "params": {"from": "source", "to": "TARGET", "message": "do work"},
        }
        with (
            patch.object(bridge, "_refresh_telegram_routes", AsyncMock()),
            patch.object(bridge, "agent_send", AsyncMock(side_effect=agent_send)),
        ):
            result = await bridge._handle_control_send(request, transition)

        self.assertEqual([kind for kind, _ in order], ["telegram", "wakterm"])
        self.assertEqual(
            bot.sent[0][1],
            "Source → Target:\n"
            "Request: 00000000-0000-4000-8000-000000000001 [pending]\n"
            "do work",
        )
        self.assertEqual(
            order[1][1],
            "[Panetone cross-agent message]\n"
            "From: Source (codex)\n"
            "To: Target (codex)\n"
            "Request ID: 00000000-0000-4000-8000-000000000001\n"
            "Reply mode: one-way\n\n"
            "do work",
        )
        self.assertEqual([state for state, _ in transitions], ["audit_posted", "delivering"])
        self.assertEqual(bridge.tab_last_source[2], "tg")
        self.assertEqual(result["reply_mode"], "one_way")
        self.assertTrue(result["wakterm"]["submitted"])
        self.assertIn("[submitted]", bot.edited[0][0])

    async def test_control_send_marks_visible_failure_when_wakterm_fails(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)

        async def transition(_state, _progress=None):
            return None

        request = {
            "id": "00000000-0000-4000-8000-000000000002",
            "params": {"from": "Source", "to": "Target", "message": "do work"},
        }
        with (
            patch.object(bridge, "_refresh_telegram_routes", AsyncMock()),
            patch.object(
                bridge,
                "agent_send",
                AsyncMock(side_effect=RuntimeError("mux disconnected")),
            ),
            self.assertRaises(bridge.RequestFailure) as raised,
        ):
            await bridge._handle_control_send(request, transition)

        self.assertTrue(raised.exception.indeterminate)
        self.assertEqual(raised.exception.code, "wakterm_delivery_indeterminate")
        self.assertEqual(len(bot.sent), 2)
        self.assertIn("DELIVERY FAILED", bot.sent[1][1])
        self.assertEqual(bot.sent[1][3]["reply_to_message_id"], 101)

    async def test_control_send_does_not_invoke_wakterm_when_audit_fails(self):
        order = []
        bot = FakeControlBot(order, fail_on=1)
        self._configure_control(bot)
        send = AsyncMock()

        async def transition(_state, _progress=None):
            return None

        request = {
            "id": "00000000-0000-4000-8000-000000000003",
            "params": {"from": "Source", "to": "Target", "message": "do work"},
        }
        with (
            patch.object(bridge, "_refresh_telegram_routes", AsyncMock()),
            patch.object(bridge, "agent_send", send),
            self.assertRaises(bridge.RequestFailure) as raised,
        ):
            await bridge._handle_control_send(request, transition)

        self.assertFalse(raised.exception.indeterminate)
        self.assertEqual(raised.exception.code, "telegram_audit_failed")
        send.assert_not_awaited()

    def test_wakterm_return_capability_requires_safe_agent_api_boundary(self):
        with patch.object(
            bridge,
            "_wakterm_json_sync",
            return_value={
                "schema": "wakterm.agent-api.v1",
                "api_major": 1,
                "capabilities": [
                    "catalog.v1",
                    "return_request_terminal_stream.v1",
                ],
            },
        ):
            supported, reason = bridge._wakterm_return_capability_sync()

        self.assertFalse(supported)
        self.assertEqual(
            reason, "missing Wakterm Agent API capabilities: prompt_admission.v1"
        )

    def test_route_binding_joins_fresh_catalog_by_ephemeral_pane(self):
        route = {
            "title": "Source",
            "pane_id": 11,
            "harness": "codex",
            "topic_id": 41,
        }

        bound = bridge._bind_route_agent(route, self._agent_catalog())

        self.assertEqual(bound["agent_id"], "source-agent")
        self.assertEqual(bound["incarnation_id"], "source-incarnation")
        self.assertEqual(bound["agent_name"], "source_codex")
        self.assertEqual(bound["title"], "Source")

    def test_agent_admission_uses_exact_identity_and_stable_request(self):
        route = bridge._bind_route_agent(
            {"title": "Source", "pane_id": 11, "harness": "codex"},
            self._agent_catalog(),
        )
        request_id = bridge._return_callback_request_id(
            "00000000-0000-4000-8000-000000000009"
        )
        response = {
            "schema": "wakterm.agent-api.v1",
            "request_id": request_id,
            "status": "accepted",
            "definitive": True,
            "prompt_written": True,
            "agent_id": "source-agent",
            "incarnation_id": "source-incarnation",
        }

        with patch.object(
            bridge, "_wakterm_json_sync", return_value=response
        ) as command:
            receipt = bridge._agent_admit_sync(
                route, "callback", request_id=request_id
            )

        self.assertEqual(receipt, response)
        command.assert_called_once_with(
            "admit",
            "source-agent",
            "--incarnation",
            "source-incarnation",
            "--request-id",
            request_id,
            input_text="callback",
            timeout=10,
        )

    def test_agent_admission_rejects_unknown_receipt_status(self):
        route = bridge._bind_route_agent(
            {"title": "Source", "pane_id": 11, "harness": "codex"},
            self._agent_catalog(),
        )
        request_id = "00000000-0000-4000-8000-000000000010"
        response = {
            "schema": "wakterm.agent-api.v1",
            "request_id": request_id,
            "status": "maybe",
            "definitive": True,
            "prompt_written": False,
            "agent_id": "source-agent",
            "incarnation_id": "source-incarnation",
        }

        with (
            patch.object(bridge, "_wakterm_json_sync", return_value=response),
            self.assertRaisesRegex(RuntimeError, "unknown status"),
        ):
            bridge._agent_admit_sync(route, "callback", request_id=request_id)

    async def test_return_final_fails_before_any_side_effect_when_unsupported(self):
        bridge._wakterm_return_supported = False
        bridge._wakterm_return_unavailable_reason = "unsupported command"
        refresh = AsyncMock()
        send = AsyncMock()
        request = {
            "id": "00000000-0000-4000-8000-000000000006",
            "params": {
                "from": "Source",
                "to": "Target",
                "message": "do work",
                "return_final": True,
            },
        }

        with (
            patch.object(bridge, "_refresh_telegram_routes", refresh),
            patch.object(bridge, "agent_send", send),
            self.assertRaises(bridge.RequestFailure) as raised,
        ):
            await bridge._handle_control_send(request, AsyncMock())

        self.assertEqual(raised.exception.code, "return_final_unavailable")
        self.assertFalse(raised.exception.indeterminate)
        refresh.assert_not_awaited()
        send.assert_not_awaited()

    async def test_return_final_fails_closed_when_route_binding_changes(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        before = self._agent_catalog()
        after = json.loads(json.dumps(before))
        after[0]["agent_id"] = "replacement-source"
        after[0]["incarnation_id"] = "replacement-incarnation"
        send = AsyncMock()
        request = {
            "id": "00000000-0000-4000-8000-000000000012",
            "params": {
                "from": "Source",
                "to": "Target",
                "message": "do work",
                "return_final": True,
            },
        }

        with (
            patch.object(bridge, "_refresh_telegram_routes", AsyncMock()),
            patch.object(
                bridge,
                "_wakterm_agent_catalog_sync",
                side_effect=[before, after],
            ),
            patch.object(bridge, "agent_send", send),
            self.assertRaises(bridge.RequestFailure) as raised,
        ):
            await bridge._handle_control_send(request, AsyncMock())

        self.assertEqual(raised.exception.code, "return_final_unavailable")
        self.assertFalse(raised.exception.indeterminate)
        self.assertEqual(bot.sent, [])
        send.assert_not_awaited()

    async def test_return_final_registers_route_before_wakterm_submission(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        journal = SimpleNamespace()

        def register_return_route(request_id, source, target):
            order.append(("registered", request_id))
            self.assertEqual(source["title"], "Source")
            self.assertEqual(target["title"], "Target")

        journal.register_return_route = register_return_route

        async def transition(_state, _progress=None):
            return None

        async def agent_send(pid, text, **kwargs):
            order.append(("wakterm", text))
            self.assertEqual(pid, 22)
            self.assertEqual(
                text,
                "[Panetone cross-agent message]\n"
                "From: Source (codex)\n"
                "To: Target (codex)\n"
                "Request ID: 00000000-0000-4000-8000-000000000004\n"
                "Reply mode: asynchronous final callback\n\n"
                "do work",
            )
            self.assertTrue(kwargs["return_final"])
            self.assertEqual(
                kwargs["request_id"], "00000000-0000-4000-8000-000000000004"
            )
            return {
                "request_id": kwargs["request_id"],
                "reply_pending": True,
            }

        request = {
            "id": "00000000-0000-4000-8000-000000000004",
            "params": {
                "from": "Source",
                "to": "Target",
                "message": "do work",
                "return_final": True,
                "timeout_ms": 5000,
            },
        }
        with (
            patch.object(bridge, "_refresh_telegram_routes", AsyncMock()),
            patch.object(
                bridge,
                "_wakterm_agent_catalog_sync",
                return_value=self._agent_catalog(),
            ),
            patch.object(bridge, "agent_send", AsyncMock(side_effect=agent_send)),
        ):
            result = await bridge._handle_control_send(request, transition, journal)

        kinds = [kind for kind, _value in order]
        self.assertLess(kinds.index("registered"), kinds.index("wakterm"))
        self.assertEqual(result["reply_mode"], "return_final")
        self.assertTrue(result["reply_pending"])

    async def test_terminal_return_event_delivers_each_destination_once(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        with tempfile.TemporaryDirectory() as tmp:
            journal = bridge.ControlJournal(Path(tmp) / "journal.sqlite3")
            journal.initialize()
            request_id = "00000000-0000-4000-8000-000000000005"
            journal.register_return_route(
                request_id,
                {
                    "title": "Source",
                    "pane_id": 11,
                    "harness": "codex",
                    "topic_id": 101,
                    "agent_id": "source-agent",
                    "incarnation_id": "source-incarnation",
                },
                {
                    "title": "Target",
                    "pane_id": 22,
                    "harness": "codex",
                    "topic_id": 202,
                },
            )
            result = {
                "request_id": request_id,
                "state": "completed",
                "final_message": "full final response",
                "terminal_event_sequence": 9,
            }
            bridge._return_delivery_lock = asyncio.Lock()
            admit = AsyncMock(
                return_value={
                    "status": "accepted",
                    "definitive": True,
                    "prompt_written": True,
                }
            )
            with patch.object(bridge, "agent_admit", admit):
                await bridge._handle_wakterm_return_event(result, journal)
                await bridge._handle_wakterm_return_event(result, journal)

            admit.assert_awaited_once()
            self.assertEqual(
                admit.await_args.kwargs["request_id"],
                bridge._return_callback_request_id(request_id),
            )
            self.assertEqual(len(bot.sent), 1)
            self.assertIn("full final response", bot.sent[0][1])
            self.assertEqual(journal.event_cursor(), 9)
            row = journal.get_return(request_id)
            self.assertEqual(row["agent_state"], "delivered")
            self.assertEqual(row["telegram_state"], "delivered")

    async def test_busy_source_callback_stays_queued_with_stable_admission_id(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        with tempfile.TemporaryDirectory() as tmp:
            journal = bridge.ControlJournal(Path(tmp) / "journal.sqlite3")
            journal.initialize()
            request_id = "00000000-0000-4000-8000-000000000007"
            source = {
                "title": "Source",
                "pane_id": 11,
                "harness": "codex",
                "topic_id": 101,
                "agent_id": "source-agent",
                "incarnation_id": "source-incarnation",
            }
            journal.register_return_route(
                request_id,
                source,
                {"title": "Target", "harness": "codex"},
            )
            journal.record_return_result(
                request_id,
                {
                    "request_id": request_id,
                    "state": "completed",
                    "final_message": "done",
                },
            )
            bridge._return_delivery_lock = asyncio.Lock()
            admit = AsyncMock(
                side_effect=[
                    {
                        "status": "busy",
                        "definitive": True,
                        "prompt_written": False,
                        "detail": "source is busy",
                    },
                    {
                        "status": "accepted",
                        "definitive": True,
                        "prompt_written": True,
                    },
                ]
            )

            with patch.object(bridge, "agent_admit", admit):
                await bridge._deliver_return(request_id, journal)
                first = journal.get_return(request_id)
                self.assertEqual(first["agent_state"], "pending")
                self.assertEqual(first["telegram_state"], "delivered")
                await bridge._deliver_return(request_id, journal)

            self.assertEqual(admit.await_count, 2)
            callback_ids = {
                call.kwargs["request_id"] for call in admit.await_args_list
            }
            self.assertEqual(
                callback_ids, {bridge._return_callback_request_id(request_id)}
            )
            row = journal.get_return(request_id)
            self.assertEqual(row["agent_state"], "delivered")
            self.assertEqual(len(bot.sent), 1)

    async def test_stale_source_incarnation_is_not_rebound_by_title(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        with tempfile.TemporaryDirectory() as tmp:
            journal = bridge.ControlJournal(Path(tmp) / "journal.sqlite3")
            journal.initialize()
            request_id = "00000000-0000-4000-8000-000000000008"
            journal.register_return_route(
                request_id,
                {
                    "title": "Source",
                    "pane_id": 11,
                    "harness": "codex",
                    "topic_id": 101,
                    "agent_id": "source-agent",
                    "incarnation_id": "source-incarnation",
                },
                {"title": "Target", "harness": "codex"},
            )
            journal.record_return_result(
                request_id,
                {
                    "request_id": request_id,
                    "state": "completed",
                    "final_message": "durable result",
                },
            )
            bridge._return_delivery_lock = asyncio.Lock()
            admit = AsyncMock(
                return_value={
                    "status": "stale_incarnation",
                    "definitive": True,
                    "prompt_written": False,
                    "detail": "source process changed",
                }
            )
            refresh = AsyncMock()

            with (
                patch.object(bridge, "agent_admit", admit),
                patch.object(bridge, "_refresh_telegram_routes", refresh),
            ):
                await bridge._deliver_return(request_id, journal)

            refresh.assert_not_awaited()
            row = journal.get_return(request_id)
            self.assertEqual(row["agent_state"], "failed")
            self.assertEqual(row["telegram_state"], "delivered")
            self.assertIn("durable result", bot.sent[0][1])

    async def test_indeterminate_callback_is_never_retried(self):
        order = []
        bot = FakeControlBot(order)
        self._configure_control(bot)
        with tempfile.TemporaryDirectory() as tmp:
            journal = bridge.ControlJournal(Path(tmp) / "journal.sqlite3")
            journal.initialize()
            request_id = "00000000-0000-4000-8000-000000000011"
            journal.register_return_route(
                request_id,
                {
                    "title": "Source",
                    "pane_id": 11,
                    "harness": "codex",
                    "topic_id": 101,
                    "agent_id": "source-agent",
                    "incarnation_id": "source-incarnation",
                },
                {"title": "Target", "harness": "codex"},
            )
            journal.record_return_result(
                request_id,
                {
                    "request_id": request_id,
                    "state": "completed",
                    "final_message": "durable result",
                },
            )
            bridge._return_delivery_lock = asyncio.Lock()
            admit = AsyncMock(
                return_value={
                    "status": "indeterminate",
                    "definitive": False,
                    "prompt_written": None,
                    "detail": "prompt write receipt was lost",
                }
            )

            with patch.object(bridge, "agent_admit", admit):
                await bridge._deliver_return(request_id, journal)
                await bridge._deliver_return(request_id, journal)

            admit.assert_awaited_once()
            row = journal.get_return(request_id)
            self.assertEqual(row["agent_state"], "indeterminate")
            self.assertEqual(row["telegram_state"], "delivered")

    async def test_shutdown_cancels_owned_background_tasks(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def worker():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = bridge._start_background_task(worker(), "test-worker")
        await started.wait()
        old_server = bridge._control_server
        bridge._control_server = None
        try:
            await bridge.shutdown(None)
        finally:
            bridge._control_server = old_server

        self.assertTrue(task.cancelled())
        self.assertTrue(cancelled.is_set())
        self.assertFalse(bridge._background_tasks)

if __name__ == "__main__":
    unittest.main()
