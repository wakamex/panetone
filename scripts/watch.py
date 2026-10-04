#!/usr/bin/env python3
"""Report new Panetone delivery failures and unobservable Wakterm agents.

Each run reads Panetone's store read-only and Wakterm's agent list, then sends
one Panetone message listing problems it has not reported before. The first run
records existing problems as a baseline without reporting them. A Wakterm agent
problem is reported only when it persists across two runs, because a freshly
started or restored agent is briefly unobserved.
"""

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

DATABASE = Path.home() / ".local/state/panetone-rust/migration/panetone.sqlite3"
STATE = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "panetone-rust/watch.json"
ROUTE = os.environ.get("PANETONE_WATCH_ROUTE", "panetone")
STUCK_INBOX_MS = 10 * 60 * 1000
# Reports are excluded from what the watcher reports, so an unconfirmed report
# cannot trigger another one.
REPORT_HEADER = "Panetone watch found new problems:"


def store_problems(connection, now_ms):
    """Return {key: description} for failed or unconfirmed store records."""
    problems = {}
    for effect_id, channel, error, body in connection.execute(
        "SELECT effect_id, channel, json_extract(record_json, '$.last_error'),"
        " json_extract(record_json, '$.body') FROM outbox WHERE state = 'failed'"
    ):
        problems[f"outbox:{effect_id}"] = f"{channel} post failed ({error}): {snippet(body)}"
    for effect_id, channel, state, created, body in connection.execute(
        "SELECT effect_id, channel, state, created_at_ms, json_extract(record_json, '$.body')"
        " FROM inbox WHERE state = 'indeterminate'"
        " OR (state = 'pending' AND created_at_ms < ?)",
        (now_ms - STUCK_INBOX_MS,),
    ):
        what = "unconfirmed" if state == "indeterminate" else "undelivered for over 10 minutes"
        problems[f"inbox:{effect_id}:{state}"] = f"{channel} message {what}: {snippet(body)}"
    for request_id, source, target in connection.execute(
        "SELECT request_id, json_extract(record_json, '$.command.source'),"
        " json_extract(record_json, '$.command.target')"
        " FROM workflows WHERE state = 'indeterminate'"
        " AND coalesce(json_extract(record_json, '$.command.message'), '') NOT LIKE ?",
        (REPORT_HEADER + "%",),
    ):
        problems[f"workflow:{request_id}"] = f"send {source} -> {target} unconfirmed ({request_id})"
    for request_id, detail in connection.execute(
        "SELECT request_id, json_extract(record_json, '$.result.message')"
        " FROM return_deliveries WHERE agent_state = 'indeterminate'"
    ):
        problems[f"return:{request_id}"] = f"final return to caller unconfirmed ({request_id}): {snippet(detail)}"
    return problems


def agent_problems(agents, routed_titles, pane_titles):
    """Return {key: description} for agents Panetone cannot use fully."""
    problems = {}
    for agent in agents:
        runtime = agent["runtime"]
        if not runtime["alive"] or runtime["harness"] not in ("Claude", "Codex"):
            continue
        metadata = agent["metadata"]
        title = pane_titles.get(agent["pane_id"], "")
        identity = f"{metadata['agent_id']}:{runtime['tty_name']}"
        where = f"{metadata['name']} (pane {agent['pane_id']}, tab {title or '?'})"
        if agent["origin"] == "detected":
            if title.lower() in routed_titles:
                problems[f"detected:{identity}"] = f"{where} is detected but not registered, so Panetone ignores it"
        elif runtime["transport"] == "PlainPty":
            problems[f"unobserved:{identity}"] = f"{where} is registered but unobserved: its output is not forwarded and prompts come back unconfirmed"
    return problems


def snippet(text, limit=80):
    text = " ".join((text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def run_json(*command):
    return json.loads(subprocess.run(command, check=True, capture_output=True, text=True).stdout)


def main():
    now_ms = int(time.time() * 1000)
    connection = sqlite3.connect(f"file:{DATABASE}?mode=ro", uri=True)
    store = store_problems(connection, now_ms)
    connection.close()

    routes = run_json("panetone", "route", "list")["result"]["routes"]
    panes = run_json("wakterm", "cli", "list", "--format", "json")
    agents = run_json("wakterm", "agent", "list", "--format", "json")
    current_agents = agent_problems(
        agents,
        {route["title"].lower() for route in routes},
        {pane["pane_id"]: pane.get("effective_title") or "" for pane in panes},
    )

    first_run = not STATE.exists()
    state = {"seen": [], "agent_candidates": []} if first_run else json.loads(STATE.read_text())
    seen = set(state["seen"])
    candidates = set(state["agent_candidates"])

    confirmed_agents = {key: text for key, text in current_agents.items() if key in candidates}
    found = {**store, **confirmed_agents}
    new = {key: text for key, text in found.items() if key not in seen}

    # Save before sending: an unconfirmed send may still have arrived, so a
    # failed send must not cause the same report on every later run.
    state = {"seen": sorted(seen | found.keys()), "agent_candidates": sorted(current_agents)}
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, indent=1) + "\n")
    print(f"{len(found)} known problems, {len(new)} new{' (baseline run, not reported)' if first_run else ''}")

    if new and not first_run:
        lines = [f"- {text}" for text in sorted(new.values())]
        message = REPORT_HEADER + "\n" + "\n".join(lines)
        sent = subprocess.run(["panetone", "send", "--from", ROUTE, "--to", ROUTE, message], capture_output=True, text=True)
        if sent.returncode != 0:
            print(f"report send was not confirmed:\n{sent.stdout}{sent.stderr}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
