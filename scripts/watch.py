#!/usr/bin/env python3
"""Report new Panetone delivery failures and failing daemon workers.

Each run reads Panetone's store read-only and its status, then sends one
Panetone message listing problems it has not reported before. The first run
records existing problems as a baseline without reporting them. Panetone
itself reports unregistered or unobserved agents in each route's channel.
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
    for effect_id, channel, state, error, body in connection.execute(
        "SELECT effect_id, channel, state, json_extract(record_json, '$.last_error'),"
        " json_extract(record_json, '$.body') FROM outbox WHERE state IN ('failed', 'indeterminate')"
    ):
        what = "failed" if state == "failed" else "gave up after attempts that may have posted"
        problems[f"outbox:{effect_id}"] = f"{channel} post {what} ({error}): {snippet(body)}"
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


def task_problems(tasks):
    """Return {key: description} for daemon tasks that are not running."""
    return {
        f"task:{name}": f"Panetone task {name} is {health['state']}: {snippet(health.get('last_error'))}"
        for name, health in tasks.items()
        if health["state"] != "running"
    }


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

    tasks = task_problems(run_json("panetone", "status", "--json")["result"]["tasks"])

    first_run = not STATE.exists()
    seen = set() if first_run else set(json.loads(STATE.read_text())["seen"])

    found = {**store, **tasks}
    new = {key: text for key, text in found.items() if key not in seen}
    # A recovered task is forgotten, so its next failure is reported again.
    seen = {key for key in seen if not key.startswith("task:") or key in tasks}

    # Save before sending: an unconfirmed send may still have arrived, so a
    # failed send must not cause the same report on every later run.
    state = {"seen": sorted(seen | found.keys())}
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
