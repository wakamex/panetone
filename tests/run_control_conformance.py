#!/usr/bin/env python3
"""Run control v1 golden cases against a replaceable backend command."""

import argparse
import copy
import json
import shlex
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = ROOT / "tests" / "fixtures" / "control-v1" / "cases.json"


class Backend:
    def __init__(self, command, socket_path, journal_path, effect_log):
        self.command = command
        self.socket_path = socket_path
        self.journal_path = journal_path
        self.effect_log = effect_log
        self.process = None

    def start(self):
        argv = [
            *self.command,
            "--socket",
            str(self.socket_path),
            "--journal",
            str(self.journal_path),
            "--effect-log",
            str(self.effect_log),
        ]
        self.process = subprocess.Popen(
            argv,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                stdout, stderr = self.process.communicate()
                raise RuntimeError(
                    f"backend exited with {self.process.returncode}: {stdout}{stderr}"
                )
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                    probe.settimeout(0.1)
                    probe.connect(str(self.socket_path))
                return
            except (FileNotFoundError, ConnectionRefusedError, socket.timeout):
                pass
            time.sleep(0.02)
        self.stop(force=True)
        raise RuntimeError("backend did not create its control socket")

    def stop(self, force=False):
        if not self.process:
            return
        if self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None


def send(socket_path, request, timeout=5):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        client.sendall(
            json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode()
            + b"\n"
        )
        chunks = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
    return json.loads(b"".join(chunks).splitlines()[0])


def effect_count(path, request_id):
    if not path.exists():
        return 0
    return sum(
        json.loads(line)["request_id"] == request_id
        for line in path.read_text().splitlines()
        if line
    )


def expected_for(step, case, profile):
    by_profile = step.get("expected_by_profile", case.get("expected_by_profile"))
    if by_profile:
        return by_profile[profile]
    return step.get("expected", case.get("expected"))


def expected_effects_for(case, profile):
    by_profile = case.get("expected_effects_by_profile")
    if by_profile:
        return by_profile[profile]
    return case["expected_effects"]


def run_suite(command, fixture_path, profile):
    fixture = json.loads(fixture_path.read_text())
    if fixture.get("schema") != "panetone.conformance.control.v1":
        raise AssertionError("unsupported conformance fixture schema")

    with tempfile.TemporaryDirectory(prefix="panetone-control-conformance-") as tmp:
        tmp_path = Path(tmp)
        backend = Backend(
            command,
            tmp_path / "control.sock",
            tmp_path / "journal.sqlite3",
            tmp_path / "effects.jsonl",
        )
        backend.start()
        try:
            for case in fixture["cases"]:
                request = case["request"]
                request_id = request["id"]
                if case.get("crash_after_effect"):
                    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    probe.settimeout(5)
                    probe.connect(str(backend.socket_path))
                    probe.sendall(
                        json.dumps(request, separators=(",", ":")).encode() + b"\n"
                    )
                    deadline = time.monotonic() + 5
                    while effect_count(backend.effect_log, request_id) < 1:
                        if time.monotonic() >= deadline:
                            raise AssertionError(
                                f"{case['name']}: effect was not reached before crash"
                            )
                        time.sleep(0.01)
                    backend.stop(force=True)
                    probe.close()
                    backend.start()
                    actual = send(backend.socket_path, request)
                    expected = case["expected_after_restart"]
                    if actual != expected:
                        raise AssertionError(
                            f"{case['name']}:\nactual={actual!r}\nexpected={expected!r}"
                        )
                else:
                    actual = send(backend.socket_path, request)
                    expected = expected_for({}, case, profile)
                    if actual != expected:
                        raise AssertionError(
                            f"{case['name']}:\nactual={actual!r}\nexpected={expected!r}"
                        )
                    for replay in case.get("replays", []):
                        replay_request = replay.get("request", copy.deepcopy(request))
                        actual = send(backend.socket_path, replay_request)
                        expected = expected_for(replay, case, profile)
                        if actual != expected:
                            raise AssertionError(
                                f"{case['name']} replay:\n"
                                f"actual={actual!r}\nexpected={expected!r}"
                            )
                actual_effects = effect_count(backend.effect_log, request_id)
                expected_effects = expected_effects_for(case, profile)
                if actual_effects != expected_effects:
                    raise AssertionError(
                        f"{case['name']}: {actual_effects} effects, "
                        f"expected {expected_effects}"
                    )
        finally:
            backend.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backend",
        default=(
            f"python3 {ROOT / 'tests' / 'support' / 'control_conformance_backend.py'}"
        ),
        help="backend command accepting --socket, --journal, and --effect-log",
    )
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument(
        "--profile", choices=("python-current", "target"), default="python-current"
    )
    args = parser.parse_args()
    run_suite(shlex.split(args.backend), args.fixture, args.profile)
    print(f"control conformance passed for {args.profile}")


if __name__ == "__main__":
    main()
