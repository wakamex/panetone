#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "aiohttp"]
# ///
"""Compare one Codex output interval through Wakterm and the legacy reader.

This is a recording-only discovery tool. It reads local state and prints JSON.
It never submits a prompt or invokes a messaging adapter.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


SCHEMA = "panetone.codex-shadow.v1"
WAKTERM_SCHEMA = "wakterm.agent-output-shadow.experimental.v1"


def _load_bridge():
    os.environ.setdefault("WEZ_TG_TOKEN_CLAUDE", "shadow-claude-token")
    os.environ.setdefault("WEZ_TG_TOKEN_CODEX", "shadow-codex-token")
    os.environ.setdefault("WEZ_TG_CHAT", "1")
    os.environ.setdefault("WEZ_TG_OWNER", "1")
    os.environ.setdefault("WEZ_TG_STATE", "/tmp/panetone-shadow-unused-state.json")
    os.environ.setdefault("WEZ_TG_PENDING", "/tmp/panetone-shadow-unused-pending.json")
    os.environ.setdefault("WEZ_SIG_SOCKET", "")
    os.environ.setdefault("WEZ_SIG_ACCOUNT", "")
    os.environ.setdefault("WEZ_SIG_OWNER", "")
    os.environ.setdefault("WEZ_TG_DEBATE_CHAT", "0")
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import bridge

    return bridge


class WaktermOutputClient:
    def __init__(self, executable: str, target: str, limit: int = 100):
        self.executable = executable
        self.target = target
        self.limit = limit

    def read(self, cursor: str | None) -> dict[str, Any]:
        command = [
            self.executable,
            "cli",
            "agent",
            "output",
            self.target,
            "--limit",
            str(self.limit),
        ]
        if cursor is not None:
            command.extend(("--after", cursor))
        completed = subprocess.run(
            command, check=False, capture_output=True, text=True
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(f"Wakterm output read failed: {detail}")
        try:
            page = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("Wakterm output was not valid JSON") from error
        if not isinstance(page, dict) or page.get("schema") != WAKTERM_SCHEMA:
            raise RuntimeError("Wakterm returned an incompatible shadow schema")
        return page


def _codex_harness(bridge):
    harness = bridge.harnesses.get("codex")
    if harness is None:
        bridge._init_harnesses()
        harness = bridge.harnesses.get("codex")
    if harness is None:
        raise RuntimeError("the legacy Codex reader is not configured")
    return harness


def capture_baseline(
    target: str,
    cwd: str,
    client,
    bridge,
    session: Path | None = None,
) -> dict[str, Any]:
    harness = _codex_harness(bridge)
    discovered = session or harness.find_session(cwd)
    if discovered is None:
        raise RuntimeError(f"legacy Codex session is unavailable for {cwd}")
    session = Path(discovered).resolve()
    if not session.is_file():
        raise RuntimeError(f"legacy Codex session is unavailable for {cwd}")

    legacy_cursor = bridge._jsonl_tail_cursor(harness, session)
    page = client.read(None)
    if page.get("status") != "ok":
        raise RuntimeError(
            f"Wakterm baseline failed: {page.get('status')}: {page.get('detail')}"
        )
    if not page.get("baseline") or page.get("events"):
        raise RuntimeError("Wakterm did not return an empty tail baseline")
    if not page.get("next_cursor") or not page.get("session_id"):
        raise RuntimeError("Wakterm baseline omitted its cursor or session identity")

    messages, after_cursor, _records = bridge._read_jsonl_new(
        harness, session, legacy_cursor
    )
    if messages or after_cursor["offset"] != legacy_cursor["offset"]:
        raise RuntimeError("Codex output changed while the two baselines were captured")

    return {
        "schema": SCHEMA,
        "target": target,
        "cwd": cwd,
        "agent_id": page.get("agent_id"),
        "session_id": page.get("session_id"),
        "wakterm_cursor": page["next_cursor"],
        "legacy_session": str(session),
        "legacy_cursor": legacy_cursor,
    }


def compare_state(state: dict[str, Any], client, bridge) -> dict[str, Any]:
    if state.get("schema") != SCHEMA:
        raise RuntimeError("shadow state has an incompatible schema")
    harness = _codex_harness(bridge)
    session = Path(state["legacy_session"]).resolve()
    legacy_cursor = state["legacy_cursor"]
    try:
        stat = session.stat()
    except OSError:
        return _gap_result(state, "legacy_source_changed")
    if (
        legacy_cursor.get("dev") != stat.st_dev
        or legacy_cursor.get("ino") != stat.st_ino
        or int(legacy_cursor.get("offset", 0)) > stat.st_size
    ):
        return _gap_result(state, "legacy_source_changed")

    legacy_messages, _next_legacy_cursor, legacy_records = bridge._read_jsonl_new(
        harness, session, legacy_cursor
    )

    first = _read_wakterm_interval(state, client)
    if first[2] is not None:
        return first[2]
    second = _read_wakterm_interval(state, client)
    if second[2] is not None:
        return second[2]
    wakterm_messages, event_ids, _gap = first
    if first[:2] != second[:2]:
        return _gap_result(state, "wakterm_replay_changed")

    matches = legacy_messages == wakterm_messages
    return {
        "schema": SCHEMA,
        "status": "match" if matches else "difference",
        "classification": "equivalent" if matches else "unexplained",
        "target": state["target"],
        "agent_id": state["agent_id"],
        "session_id": state["session_id"],
        "legacy_record_count": legacy_records,
        "legacy_messages": legacy_messages,
        "wakterm_event_ids": event_ids,
        "wakterm_messages": wakterm_messages,
        "wakterm_replay_stable": True,
    }


def _read_wakterm_interval(state: dict[str, Any], client):
    cursor = state["wakterm_cursor"]
    wakterm_messages: list[str] = []
    event_ids: list[str] = []
    seen_event_ids: set[str] = set()
    for _page_number in range(10_000):
        page = client.read(cursor)
        if page.get("status") != "ok":
            return [], [], _gap_result(
                state,
                f"wakterm_{page.get('status', 'unknown')}",
                page.get("detail"),
            )
        if page.get("agent_id") != state.get("agent_id"):
            return [], [], _gap_result(state, "wakterm_agent_changed")
        if page.get("session_id") != state.get("session_id"):
            return [], [], _gap_result(state, "wakterm_session_changed")
        for item in page.get("events", []):
            if item.get("kind") != "assistant_message":
                return [], [], _gap_result(state, "wakterm_unknown_event_kind")
            event_id = item.get("event_id")
            if not isinstance(event_id, str) or not event_id or event_id in seen_event_ids:
                return [], [], _gap_result(state, "wakterm_event_identity_invalid")
            seen_event_ids.add(event_id)
            wakterm_messages.append(item.get("text", ""))
            event_ids.append(event_id)
        next_cursor = page.get("next_cursor")
        if not next_cursor:
            return [], [], _gap_result(state, "wakterm_cursor_missing")
        if not page.get("has_more"):
            break
        if next_cursor == cursor:
            return [], [], _gap_result(state, "wakterm_cursor_stalled")
        cursor = next_cursor
    else:
        return [], [], _gap_result(state, "wakterm_page_limit_exceeded")
    return wakterm_messages, event_ids, None


def _gap_result(
    state: dict[str, Any], reason: str, detail: str | None = None
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "status": "indeterminate",
        "classification": "correlation_gap",
        "target": state.get("target"),
        "agent_id": state.get("agent_id"),
        "session_id": state.get("session_id"),
        "reason": reason,
        "detail": detail,
    }


def _write_private_json(path: Path, value: dict[str, Any]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wakterm", default="wakterm")
    parser.add_argument("--limit", type=int, default=100)
    subparsers = parser.add_subparsers(dest="command", required=True)

    baseline = subparsers.add_parser("baseline")
    baseline.add_argument("target")
    baseline.add_argument("--cwd", required=True)
    baseline.add_argument("--state", required=True, type=Path)

    compare = subparsers.add_parser("compare")
    compare.add_argument("--state", required=True, type=Path)

    args = parser.parse_args()
    bridge = _load_bridge()
    if args.command == "baseline":
        client = WaktermOutputClient(args.wakterm, args.target, args.limit)
        state = capture_baseline(args.target, args.cwd, client, bridge)
        _write_private_json(args.state, state)
        print(json.dumps({"status": "baseline", **state}, sort_keys=True))
        return 0

    state = json.loads(args.state.read_text())
    client = WaktermOutputClient(args.wakterm, state["target"], args.limit)
    result = compare_state(state, client, bridge)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "match" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, KeyError, TypeError, ValueError) as error:
        print(
            json.dumps(
                {
                    "schema": SCHEMA,
                    "status": "indeterminate",
                    "classification": "runner_error",
                    "detail": str(error),
                }
            )
        )
        raise SystemExit(2) from None
