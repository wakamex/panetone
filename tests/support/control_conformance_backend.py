#!/usr/bin/env python3
"""Deterministic control backend used by the black-box conformance runner."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from panetone_control import ControlJournal, ControlServer, DurableDispatcher


def append_effect(path, request_id):
    record = json.dumps({"request_id": request_id}, separators=(",", ":")) + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, record.encode())
        os.fsync(fd)
    finally:
        os.close(fd)


async def run(args):
    journal = ControlJournal(args.journal)
    await asyncio.to_thread(journal.initialize)

    async def handle(request, transition):
        params = request["params"]
        await transition("audit_posted", {"visible": True})
        await transition(
            "delivering",
            {"source": params["from"], "target": params["to"]},
        )
        await asyncio.to_thread(append_effect, args.effect_log, request["id"])
        if params["message"] == "__hold_after_effect__":
            await asyncio.Event().wait()
        return {
            "accepted": True,
            "reply_pending": params.get("return_final", False),
        }

    dispatcher = DurableDispatcher(journal, handle)
    server = ControlServer(args.socket, dispatcher)
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--effect-log", required=True)
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
