#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# ///

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from panetone_control import (
    COMPLETED_RETENTION_SECONDS,
    SCHEMA,
    ControlJournal,
    ControlServer,
    DurableDispatcher,
    ProtocolError,
    RequestFailure,
    parse_request,
    request_hash,
)


def make_request(request_id=None, message="hello"):
    return {
        "schema": SCHEMA,
        "id": request_id or str(uuid.uuid4()),
        "method": "send",
        "params": {"from": "source", "to": "target", "message": message},
    }


class DurableDispatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.journal = ControlJournal(Path(self.tmp.name) / "journal.sqlite3")
        self.journal.initialize()

    def tearDown(self):
        self.tmp.cleanup()

    async def test_same_id_returns_cached_response_without_redelivery(self):
        calls = []

        async def handler(request, transition):
            calls.append(request["id"])
            await transition("delivering", {"pane_id": 9})
            return {"submitted": True}

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()

        first = await dispatcher.dispatch(request)
        second = await dispatcher.dispatch(request)

        self.assertEqual(first, second)
        self.assertEqual(calls, [request["id"]])
        self.assertEqual(self.journal.get(request["id"])["state"], "succeeded")

    async def test_same_id_with_different_content_conflicts(self):
        calls = 0

        async def handler(_request, _transition):
            nonlocal calls
            calls += 1
            return {"submitted": True}

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()
        changed = make_request(request["id"], "different")

        await dispatcher.dispatch(request)
        conflict = await dispatcher.dispatch(changed)

        self.assertEqual(calls, 1)
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["error"]["code"], "idempotency_conflict")

    async def test_concurrent_duplicate_waits_for_original(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def handler(_request, _transition):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return {"submitted": True}

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()
        first = asyncio.create_task(dispatcher.dispatch(request))
        await entered.wait()
        duplicate = asyncio.create_task(dispatcher.dispatch(request))
        await asyncio.sleep(0)
        self.assertFalse(duplicate.done())
        release.set()

        self.assertEqual(await first, await duplicate)
        self.assertEqual(calls, 1)

    async def test_restart_turns_unfinished_delivery_indeterminate(self):
        request = make_request()
        self.assertTrue(self.journal.claim(request, request_hash(request)))
        self.journal.transition(
            request["id"], "delivering", {"topic_id": 42, "message_ids": [77]}
        )

        async def must_not_run(_request, _transition):
            self.fail("an uncertain request was redelivered")

        restarted = DurableDispatcher(self.journal, must_not_run)
        response = await restarted.dispatch(request)
        repeated = await restarted.dispatch(request)

        self.assertEqual(response, repeated)
        self.assertEqual(response["error"]["code"], "request_indeterminate")
        self.assertEqual(response["error"]["details"]["last_state"], "delivering")
        self.assertEqual(self.journal.get(request["id"])["state"], "indeterminate")

    async def test_handled_uncertain_failure_is_durable(self):
        calls = 0

        async def handler(_request, transition):
            nonlocal calls
            calls += 1
            await transition("delivering", {"pane_id": 9})
            raise RequestFailure(
                "wakterm_delivery_indeterminate",
                "delivery uncertain",
                indeterminate=True,
            )

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()
        first = await dispatcher.dispatch(request)
        second = await dispatcher.dispatch(request)

        self.assertEqual(first, second)
        self.assertEqual(calls, 1)
        self.assertEqual(self.journal.get(request["id"])["state"], "indeterminate")

    async def test_cancelled_handler_wakes_duplicate_with_indeterminate_result(self):
        entered = asyncio.Event()

        async def handler(_request, transition):
            await transition("delivering", {"pane_id": 9})
            entered.set()
            await asyncio.Event().wait()

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()
        original = asyncio.create_task(dispatcher.dispatch(request))
        await entered.wait()
        duplicate = asyncio.create_task(dispatcher.dispatch(request))
        original.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await original
        response = await duplicate

        self.assertEqual(response["error"]["code"], "request_indeterminate")
        self.assertEqual(self.journal.get(request["id"])["state"], "indeterminate")

    def test_journal_permissions_are_restrictive(self):
        mode = self.journal.path.stat().st_mode & 0o777
        self.assertEqual(mode, 0o600)

    def test_journal_has_a_hard_page_bound(self):
        with self.journal._connect() as db:
            max_pages = db.execute("PRAGMA max_page_count").fetchone()[0]
            page_size = db.execute("PRAGMA page_size").fetchone()[0]

        self.assertEqual(max_pages * page_size, 64 * 1024 * 1024)

    def test_return_route_and_terminal_result_survive_restart(self):
        request = make_request()
        self.assertTrue(self.journal.claim(request, request_hash(request)))
        source = {"title": "Source", "pane_id": 11, "topic_id": 101}
        target = {"title": "Target", "pane_id": 22, "topic_id": 202}
        self.journal.register_return_route(request["id"], source, target)
        result = {
            "request_id": request["id"],
            "state": "completed",
            "final_message": "done",
            "terminal_event_sequence": 7,
        }
        self.journal.record_return_result(request["id"], result)
        self.journal.set_event_cursor(7)

        restarted = ControlJournal(self.journal.path)
        restarted.initialize()
        row = restarted.get_return(request["id"])
        self.assertEqual(json.loads(row["result_json"]), result)
        self.assertEqual(restarted.event_cursor(), 7)
        self.assertEqual(
            [pending["request_id"] for pending in restarted.pending_returns()],
            [request["id"]],
        )

    def test_restart_never_repeats_uncertain_callback_destination(self):
        request = make_request()
        self.assertTrue(self.journal.claim(request, request_hash(request)))
        self.journal.register_return_route(
            request["id"],
            {"title": "Source"},
            {"title": "Target"},
        )
        self.journal.record_return_result(
            request["id"],
            {"request_id": request["id"], "state": "completed"},
        )
        self.journal.set_return_destination(
            request["id"], "agent", "delivering"
        )

        restarted = ControlJournal(self.journal.path)
        restarted.initialize()
        row = restarted.get_return(request["id"])
        self.assertEqual(row["agent_state"], "indeterminate")
        self.assertEqual(row["telegram_state"], "pending")

    def test_pruning_expires_only_completed_success_and_failure(self):
        requests = {
            state: make_request()
            for state in ("succeeded", "failed", "indeterminate", "delivering")
        }
        for state, request in requests.items():
            self.assertTrue(self.journal.claim(request, request_hash(request)))
            if state == "delivering":
                self.journal.transition(request["id"], state)
            else:
                self.journal.finish(
                    request["id"],
                    state,
                    {"schema": SCHEMA, "id": request["id"], "ok": state == "succeeded"},
                )
        with self.journal._connect() as db:
            db.execute("UPDATE control_request SET updated_at = 0")

        removed = self.journal.prune_completed(now=COMPLETED_RETENTION_SECONDS + 1)

        self.assertEqual(removed, 2)
        self.assertIsNone(self.journal.get(requests["succeeded"]["id"]))
        self.assertIsNone(self.journal.get(requests["failed"]["id"]))
        self.assertEqual(
            self.journal.get(requests["indeterminate"]["id"])["state"],
            "indeterminate",
        )
        self.assertEqual(
            self.journal.get(requests["delivering"]["id"])["state"],
            "delivering",
        )

    async def test_expired_completed_uuid_can_deliver_as_a_new_request(self):
        calls = 0

        async def handler(_request, _transition):
            nonlocal calls
            calls += 1
            return {"delivery": calls}

        dispatcher = DurableDispatcher(self.journal, handler)
        request = make_request()
        first = await dispatcher.dispatch(request)
        with self.journal._connect() as db:
            db.execute(
                "UPDATE control_request SET updated_at = ? WHERE request_id = ?",
                (time.time() - COMPLETED_RETENTION_SECONDS - 1, request["id"]),
            )

        second = await dispatcher.dispatch(request)

        self.assertEqual(first["result"]["delivery"], 1)
        self.assertEqual(second["result"]["delivery"], 2)
        self.assertEqual(calls, 2)


class ControlServerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.journal = ControlJournal(self.root / "journal.sqlite3")
        self.journal.initialize()
        self.server = None

    async def asyncTearDown(self):
        if self.server:
            await self.server.close()
        self.tmp.cleanup()

    async def _start(self, handler, socket_path=None):
        dispatcher = DurableDispatcher(self.journal, handler)
        self.server = ControlServer(
            socket_path or self.root / "runtime" / "control.sock", dispatcher
        )
        await self.server.start()
        return self.server

    async def test_stale_socket_is_replaced_with_restrictive_permissions(self):
        socket_path = self.root / "runtime" / "control.sock"
        socket_path.parent.mkdir()
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(str(socket_path))
        stale.close()

        async def handler(_request, _transition):
            return {"submitted": True}

        await self._start(handler, socket_path)

        self.assertEqual(socket_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(socket_path.parent.stat().st_mode & 0o777, 0o700)

    async def test_live_socket_is_not_stolen(self):
        async def handler(_request, _transition):
            return {"submitted": True}

        await self._start(handler)
        other = ControlServer(self.server.path, self.server.dispatcher)

        with self.assertRaisesRegex(RuntimeError, "already active"):
            await other.start()

    async def test_non_socket_path_is_never_replaced(self):
        socket_path = self.root / "runtime" / "control.sock"
        socket_path.parent.mkdir()
        socket_path.write_text("keep me")

        async def handler(_request, _transition):
            return {"submitted": True}

        dispatcher = DurableDispatcher(self.journal, handler)
        server = ControlServer(socket_path, dispatcher)
        with self.assertRaisesRegex(RuntimeError, "refusing to replace non-socket"):
            await server.start()

        self.assertEqual(socket_path.read_text(), "keep me")

    async def test_shutdown_does_not_remove_a_replacement_inode(self):
        async def handler(_request, _transition):
            return {"submitted": True}

        await self._start(handler)
        self.server.path.unlink()
        self.server.path.write_text("replacement")

        await self.server.close()
        self.server = None

        self.assertEqual(
            (self.root / "runtime" / "control.sock").read_text(), "replacement"
        )

    async def test_cli_receives_structured_acknowledgement(self):
        async def handler(request, _transition):
            return {"echo": request["params"]["message"], "reply_mode": "one_way"}

        await self._start(handler)
        env = os.environ.copy()
        env["PANETONE_CONTROL_SOCKET"] = str(self.server.path)
        result = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                str(Path(__file__).resolve().parents[1] / "panetone"),
                "send",
                "--from",
                "source",
                "--to",
                "target",
                "hello",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=5,
        )

        response = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(response["ok"])
        self.assertEqual(response["result"]["echo"], "hello")
        self.assertEqual(response["result"]["reply_mode"], "one_way")

    def test_protocol_normalizes_uuid_and_preserves_one_way_params(self):
        request = make_request()
        encoded = json.dumps(request).encode()

        parsed = parse_request(encoded)

        self.assertEqual(parsed, request)

    def test_protocol_rejects_control_characters_in_route_names(self):
        for route in ("source\nClaimed target: forged", "source\x1btarget"):
            request = make_request()
            request["params"]["from"] = route
            with (
                self.subTest(route=repr(route)),
                self.assertRaises(ProtocolError) as raised,
            ):
                parse_request(json.dumps(request).encode())
            self.assertEqual(raised.exception.code, "invalid_params")


if __name__ == "__main__":
    unittest.main()
