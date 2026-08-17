#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "slack-sdk>=3.0", "aiohttp"]
# ///
"""panetone: wakterm <> telegram/signal bridge for multiple AI coding agents

Each wakterm tab gets one Telegram forum topic (by tab_title) and/or
one Signal group chat. Multiple harnesses (Claude, Codex, ...) share
the channel but post via their own identity. Replies route to the right pane.

Env (Telegram):
  WEZ_TG_TOKEN_CLAUDE   - bot token for Claude messages (required)
  WEZ_TG_TOKEN_CODEX    - bot token for Codex messages (optional)
  WEZ_TG_TOKEN_OPENCODE - bot token for OpenCode messages (optional)
  WEZ_TG_CHAT           - forum group chat id
  WEZ_TG_OWNER          - your telegram user id (optional lock)
  WEZ_TG_POLL           - poll interval seconds (default 2)

Env (Signal — all three required to enable):
  WEZ_SIG_SOCKET         - path to signal-cli UNIX socket
  WEZ_SIG_ACCOUNT        - signal-cli registered number (the "bot")
  WEZ_SIG_OWNER          - your personal Signal number (invited to groups)

Env (Slack observer — all three required to enable):
  WEZ_SLACK_BOT_TOKEN    - Slack bot user OAuth token (xoxb-...)
  WEZ_SLACK_APP_TOKEN    - Slack app-level token for Socket Mode (xapp-...)
  WEZ_SLACK_CHANNELS     - comma-separated Slack channel IDs to observe
  WEZ_SLACK_TABS         - comma-separated tab name patterns to observe

Env (local control interface):
  PANETONE_CONTROL_SOCKET  - Panetone-owned UNIX socket path
  PANETONE_CONTROL_JOURNAL - durable request/idempotency journal path
"""

import asyncio
import html
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
import traceback
import uuid
from pathlib import Path
from urllib.parse import urlparse

from telegram import Bot, Update
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from panetone_control import (
    ControlJournal,
    ControlServer,
    DurableDispatcher,
    RequestFailure,
    default_socket_path,
)

# --- config ----------------------------------------------------------------

_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

CLAUDE_TOKEN = os.environ["WEZ_TG_TOKEN_CLAUDE"]
CODEX_TOKEN = os.environ.get("WEZ_TG_TOKEN_CODEX", "")
CHAT = int(os.environ["WEZ_TG_CHAT"])
OWNER = int(os.environ.get("WEZ_TG_OWNER", "0"))
POLL = float(os.environ.get("WEZ_TG_POLL", "2"))
STATE = Path(
    os.environ.get("WEZ_TG_STATE", "~/.config/wez-tg/state.json")
).expanduser()
PENDING_STATE = Path(
    os.environ.get("WEZ_TG_PENDING", str(STATE.with_name("pending_sends.json")))
).expanduser()
CONTROL_SOCKET = Path(
    os.environ.get("PANETONE_CONTROL_SOCKET", str(default_socket_path()))
).expanduser()
CONTROL_JOURNAL = Path(
    os.environ.get(
        "PANETONE_CONTROL_JOURNAL",
        str(STATE.with_name("control-journal.sqlite3")),
    )
).expanduser()
OPENCODE_TOKEN = os.environ.get("WEZ_TG_TOKEN_OPENCODE", "")
GEMINI_TOKEN = os.environ.get("WEZ_TG_TOKEN_GEMINI", "")
CLAUDE_DIR = Path.home() / ".claude" / "projects"
CODEX_DIR = Path.home() / ".codex" / "sessions"
OPENCODE_DB = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
GEMINI_DIR = Path.home() / ".gemini" / "tmp"

SIGNAL_SOCKET = os.environ.get("WEZ_SIG_SOCKET", "")
SIGNAL_ACCOUNT = os.environ.get("WEZ_SIG_ACCOUNT", "")
SIGNAL_OWNER = os.environ.get("WEZ_SIG_OWNER", "")
SIGNAL_ENABLED = bool(SIGNAL_SOCKET and SIGNAL_ACCOUNT and SIGNAL_OWNER)
SIGNAL_DB = Path(
    os.environ.get("WEZ_SIG_DB", str(STATE.with_name("signal.db")))
).expanduser()
SIGNAL_TABS = [t.strip().lower() for t in os.environ.get("WEZ_SIG_TABS", "").split(",") if t.strip()]
SIGNAL_ALLOWED = {s.strip() for s in os.environ.get("WEZ_SIG_ALLOWED", "").split(",") if s.strip()}
SIGNAL_MEMBERS = [s.strip() for s in os.environ.get("WEZ_SIG_MEMBERS", "").split(",") if s.strip()]
# per-tab members: WEZ_SIG_MEMBERS_<TAB>=+number also implies tab is signal-enabled
SIGNAL_TAB_MEMBERS = {}
_SIG_PREFIX = "WEZ_SIG_MEMBERS_"
for _k, _v in os.environ.items():
    if _k.startswith(_SIG_PREFIX) and _v.strip():
        _pat = _k[len(_SIG_PREFIX):].lower()
        SIGNAL_TAB_MEMBERS[_pat] = [s.strip() for s in _v.split(",") if s.strip()]
        if _pat not in SIGNAL_TABS:
            SIGNAL_TABS.append(_pat)

DEBATE_CHAT = int(os.environ.get("WEZ_TG_DEBATE_CHAT", "0"))
DEBATE_TABS = [t.strip().lower() for t in os.environ.get("WEZ_TG_DEBATE_TABS", "").split(",") if t.strip()]
DEBATE_ENABLED = bool(DEBATE_CHAT and DEBATE_TABS)

SLACK_BOT_TOKEN = os.environ.get("WEZ_SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.environ.get("WEZ_SLACK_APP_TOKEN", "")
SLACK_CHANNELS = {s.strip() for s in os.environ.get("WEZ_SLACK_CHANNELS", "").split(",") if s.strip()}
SLACK_TABS = [t.strip().lower() for t in os.environ.get("WEZ_SLACK_TABS", "").split(",") if t.strip()]
# direct channels: WEZ_SLACK_DIRECT=C0CHAN1:tab1,C0CHAN2:tab2
SLACK_DIRECT = {}  # channel_id -> tab_pattern
for _entry in os.environ.get("WEZ_SLACK_DIRECT", "").split(","):
    if ":" in _entry:
        _ch, _tab = _entry.strip().split(":", 1)
        if _ch and _tab:
            SLACK_DIRECT[_ch.strip()] = _tab.strip().lower()
            SLACK_CHANNELS.add(_ch.strip())
SLACK_ENABLED = bool(SLACK_BOT_TOKEN and SLACK_APP_TOKEN and (SLACK_CHANNELS or SLACK_DIRECT))

MSG_TZ = os.environ.get("WEZ_MSG_TZ", "America/New_York")
MSG_TIMESTAMPS = os.environ.get("WEZ_MSG_TIMESTAMPS", "1") != "0"

# --- wakterm cli -----------------------------------------------------------

def _get_terminal_env(*names):
    for name in names:
        value = os.environ.get(name, "")
        if value:
            return value
    return ""


def _find_wakterm():
    """Resolve terminal binary: WAKTERM_BIN > WEZTERM_BIN > known paths > PATH."""
    from_env = _get_terminal_env("WAKTERM_BIN", "WEZTERM_BIN")
    if from_env:
        return from_env
    for p in (
        Path.home() / ".local/bin/wakterm",
        Path("/usr/local/bin/wakterm"),
        Path.home() / ".local/bin/wezterm",
        Path("/usr/local/bin/wezterm"),
    ):
        if p.exists():
            return str(p)
    return shutil.which("wakterm") or shutil.which("wezterm") or "wakterm"

WAKTERM_BIN = _find_wakterm()
WAKTERM_SOCKET = _get_terminal_env("WAKTERM_UNIX_SOCKET", "WEZTERM_UNIX_SOCKET")
WAKTERM_AGENT_API_SCHEMA = "wakterm.agent-api.v1"
RETURN_CALLBACK_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://panetone.dev/control/v1/return-agent-callback"
)
print(f"[wakterm] binary: {WAKTERM_BIN}, socket: {WAKTERM_SOCKET or '(auto)'}")


def _wez_sync(*args):
    try:
        r = subprocess.run(
            [WAKTERM_BIN, "cli", *args], capture_output=True, text=True, timeout=5
        )
        if r.returncode != 0:
            print(f"[wakterm] cli {' '.join(args)} failed (rc={r.returncode}): {r.stderr.strip()}")
            return ""
        return r.stdout
    except subprocess.TimeoutExpired:
        print(f"[wakterm] cli {' '.join(args)} timed out")
        return ""
    except FileNotFoundError:
        print(f"[wakterm] binary not found: {WAKTERM_BIN}")
        return ""


def _all_panes_sync():
    out = _wez_sync("list", "--format", "json")
    if not out:
        print("[wakterm] cli list returned empty")
    return json.loads(out) if out else []


def _send_text_sync(pid, text):
    try:
        pane = [WAKTERM_BIN, "cli", "send-text", "--pane-id", str(pid)]
        subprocess.run(pane, input=text.encode(), capture_output=True, timeout=5)
        time.sleep(0.2)
        subprocess.run(
            pane + ["--no-paste"], input=b"\x0d", capture_output=True, timeout=5
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass


def _send_enter_sync(pid):
    try:
        pane = [WAKTERM_BIN, "cli", "send-text", "--pane-id", str(pid), "--no-paste"]
        subprocess.run(pane, input=b"\x0d", capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass


def _agent_send_sync(pid, text, *, return_final=False, request_id=None, timeout_ms=0):
    """Send through Wakterm's agent interface and return its JSON receipt."""
    command = [WAKTERM_BIN, "cli", "agent", "send", str(pid)]
    if return_final:
        command.append("--return-final")
        if request_id:
            command.extend(["--request-id", request_id])
        if timeout_ms:
            command.extend(["--final-timeout-ms", str(timeout_ms)])
    try:
        result = subprocess.run(
            command,
            input=text,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("Wakterm agent send timed out") from error
    except FileNotFoundError as error:
        raise RuntimeError(f"Wakterm binary not found: {WAKTERM_BIN}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown Wakterm error"
        raise RuntimeError(f"Wakterm agent send failed (rc={result.returncode}): {detail}")
    try:
        receipt = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("Wakterm agent send returned invalid JSON") from error
    if not isinstance(receipt, dict):
        raise RuntimeError("Wakterm returned a non-object receipt")
    if return_final:
        if receipt.get("request_id") != request_id or not receipt.get("reply_pending"):
            raise RuntimeError("Wakterm did not durably register the return request")
    elif not receipt.get("submitted"):
        raise RuntimeError("Wakterm did not report a submitted prompt")
    return receipt


def _wakterm_json_sync(*args, input_text=None, timeout=5):
    """Run a Wakterm Agent API command and require one JSON object."""
    try:
        result = subprocess.run(
            [WAKTERM_BIN, "cli", "agent", *args],
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"Wakterm agent {' '.join(args)} timed out") from error
    except FileNotFoundError as error:
        raise RuntimeError(f"Wakterm binary not found: {WAKTERM_BIN}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown Wakterm error"
        raise RuntimeError(
            f"Wakterm agent {' '.join(args)} failed (rc={result.returncode}): {detail}"
        )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"Wakterm agent {' '.join(args)} returned invalid JSON"
        ) from error
    if not isinstance(value, dict):
        raise RuntimeError(f"Wakterm agent {' '.join(args)} returned a non-object")
    return value


def _wakterm_return_capability_sync():
    """Negotiate the complete safe return-final capability set."""
    try:
        response = _wakterm_json_sync("capabilities")
    except RuntimeError as error:
        return False, str(error).splitlines()[0]
    if (
        response.get("schema") != WAKTERM_AGENT_API_SCHEMA
        or response.get("api_major") != 1
    ):
        return False, "incompatible Wakterm Agent API schema"
    capabilities = response.get("capabilities")
    required = {
        "catalog.v1",
        "prompt_admission.v1",
        "return_request_terminal_stream.v1",
    }
    available = (
        set(capabilities)
        if isinstance(capabilities, list)
        and all(isinstance(capability, str) for capability in capabilities)
        else set()
    )
    if not required.issubset(available):
        missing = sorted(required.difference(available))
        return False, f"missing Wakterm Agent API capabilities: {', '.join(missing)}"
    return True, None


def _wakterm_agent_catalog_sync():
    catalog = _wakterm_json_sync("catalog")
    if catalog.get("schema") != WAKTERM_AGENT_API_SCHEMA:
        raise RuntimeError("Wakterm agent catalog has an incompatible schema")
    agents = catalog.get("agents")
    if not isinstance(agents, list):
        raise RuntimeError("Wakterm agent catalog is missing agents")
    return agents


def _bind_route_agent(route, agents):
    matches = [
        agent
        for agent in agents
        if isinstance(agent, dict) and agent.get("pane_id") == route["pane_id"]
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"route {route['title']!r} pane {route['pane_id']} did not match exactly one "
            "Wakterm catalog agent"
        )
    agent = matches[0]
    agent_id = agent.get("agent_id")
    incarnation_id = agent.get("incarnation_id")
    if agent.get("harness") != route["harness"]:
        raise RuntimeError(
            f"route {route['title']!r} and Wakterm catalog disagree on harness"
        )
    if not agent.get("alive") or not isinstance(agent_id, str) or not isinstance(
        incarnation_id, str
    ):
        raise RuntimeError(f"route {route['title']!r} has no live Wakterm incarnation")
    return {
        **route,
        "agent_id": agent_id,
        "incarnation_id": incarnation_id,
        "agent_name": agent.get("name"),
    }


def _return_callback_request_id(request_id):
    return str(uuid.uuid5(RETURN_CALLBACK_NAMESPACE, request_id))


def _agent_admit_sync(route, text, *, request_id):
    command = (
        "admit",
        route["agent_id"],
        "--incarnation",
        route["incarnation_id"],
        "--request-id",
        request_id,
    )
    receipt = _wakterm_json_sync(*command, input_text=text, timeout=10)
    if receipt.get("schema") != WAKTERM_AGENT_API_SCHEMA:
        raise RuntimeError("Wakterm admission receipt has an incompatible schema")
    if receipt.get("request_id") != request_id:
        raise RuntimeError("Wakterm admission receipt has the wrong request ID")
    if receipt.get("agent_id") != route["agent_id"] or receipt.get(
        "incarnation_id"
    ) != route["incarnation_id"]:
        raise RuntimeError("Wakterm admission receipt has the wrong agent identity")
    status = receipt.get("status")
    definitive = receipt.get("definitive")
    prompt_written = receipt.get("prompt_written")
    known_statuses = {
        "accepted",
        "busy",
        "unsupported",
        "unavailable",
        "stale_incarnation",
        "invalid",
        "observer_failure",
        "internal_failure",
        "indeterminate",
    }
    if status not in known_statuses:
        raise RuntimeError(f"Wakterm admission receipt has unknown status {status!r}")
    if status == "accepted":
        valid = definitive is True and prompt_written is True
    elif status == "indeterminate":
        valid = definitive is False and prompt_written is None
    else:
        valid = definitive is True and prompt_written is False
    if not valid:
        raise RuntimeError("Wakterm admission receipt has inconsistent delivery fields")
    return receipt


async def agent_send(pid, text, **kwargs):
    return await asyncio.to_thread(_agent_send_sync, pid, text, **kwargs)


async def agent_admit(route, text, *, request_id):
    return await asyncio.to_thread(
        _agent_admit_sync, route, text, request_id=request_id
    )


async def send_text(pid, text):
    await asyncio.to_thread(_send_text_sync, pid, text)


def _get_session_path(pid):
    """Return the session file path for a pane, or None."""
    h = harnesses.get(pane_harness.get(pid, ""))
    cwd = pane_cwds.get(pid)
    if not h or not cwd:
        return None
    if h.name == "opencode":
        return None  # DB-based, can't watch mtime
    session = h.find_session(cwd)
    return str(session) if session else None


def _watch_mtime_sync(path, baseline_mtime, timeout_s):
    """Poll file mtime until it changes from baseline. Return ms elapsed or None."""
    start = time.monotonic()
    deadline = start + timeout_s
    while time.monotonic() < deadline:
        try:
            mt = os.path.getmtime(path)
            if mt != baseline_mtime:
                return int((time.monotonic() - start) * 1000)
        except OSError:
            pass
        time.sleep(0.01)  # 10ms poll
    return None


async def send_and_verify(pid, text):
    """Send text to pane, watch session file for update, retry Enter if needed."""
    path = _get_session_path(pid)
    if not path:
        h_name = pane_harness.get(pid, "?")
        print(f"[verify] {h_name}/{pid}: no session path, sending blind")
        await send_text(pid, text)
        return "?"
    try:
        baseline = os.path.getmtime(path)
    except OSError:
        await send_text(pid, text)
        return "?"
    await send_text(pid, text)
    ms = await asyncio.to_thread(_watch_mtime_sync, path, baseline, 2.0)
    if ms is not None:
        return f"\u2713 {ms}ms"
    # Enter might not have registered — retry Enter
    await asyncio.to_thread(_send_enter_sync, pid)
    ms = await asyncio.to_thread(_watch_mtime_sync, path, baseline, 2.0)
    if ms is not None:
        return f"\u2713 {ms}ms (enter retry)"
    # paste may have been dropped — full resend
    await send_text(pid, text)
    ms = await asyncio.to_thread(_watch_mtime_sync, path, baseline, 2.0)
    if ms is not None:
        return f"\u2713 {ms}ms (resend)"
    h_name = pane_harness.get(pid, "?")
    print(f"[verify] {h_name}/{pid}: >6s watching {path} (baseline mtime={baseline})")
    return "\u2717 >6s"


def _parse_cwd(cwd_url):
    if cwd_url.startswith("file://"):
        return urlparse(cwd_url).path.rstrip("/")
    return cwd_url.rstrip("/")


# --- harness: session finders ---------------------------------------------


def _claude_is_interactive(path):
    """Check if a Claude session is interactive (not a -p oneshot)."""
    try:
        with open(path) as f:
            first = json.loads(f.readline())
        return first.get("type") != "queue-operation"
    except (json.JSONDecodeError, OSError):
        return True


def _claude_find_session(cwd):
    proj_dir = CLAUDE_DIR / cwd.replace("/", "-")
    if not proj_dir.is_dir():
        return None
    files = [f for f in proj_dir.glob("*.jsonl") if _claude_is_interactive(f)]
    return max(files, key=lambda f: f.stat().st_mtime) if files else None


_codex_cache = {}  # path_str -> cwd
_codex_cache_t = 0.0


def _codex_find_session(cwd):
    global _codex_cache, _codex_cache_t
    now = time.time()

    if now - _codex_cache_t > 5:
        _codex_cache = {}
        for f in sorted(CODEX_DIR.rglob("rollout-*.jsonl"), key=lambda f: f.stat().st_mtime, reverse=True):
            ps = str(f)
            if ps in _codex_cache:
                continue
            try:
                with open(f) as fh:
                    meta = json.loads(fh.readline())
                payload = meta.get("payload", {})
                source = payload.get("source")
                if isinstance(source, dict) and source.get("subagent"):
                    continue
                _codex_cache[ps] = payload.get("cwd", "")
            except (json.JSONDecodeError, OSError):
                pass
        _codex_cache_t = now

    best, best_mt = None, 0.0
    for ps, scwd in _codex_cache.items():
        if scwd == cwd:
            try:
                mt = Path(ps).stat().st_mtime
                if mt > best_mt:
                    best, best_mt = Path(ps), mt
            except OSError:
                pass
    return best


def _opencode_find_session(cwd):
    """Return (db_path, session_id) or None."""
    if not OPENCODE_DB.exists():
        print(f"[opencode] db not found: {OPENCODE_DB}")
        return None
    try:
        con = sqlite3.connect(str(OPENCODE_DB), timeout=2)
        row = con.execute(
            "SELECT id FROM session WHERE directory = ? "
            "ORDER BY rowid DESC LIMIT 1",
            (cwd,),
        ).fetchone()
        con.close()
        if row:
            return (OPENCODE_DB, row[0])
    except (sqlite3.Error, OSError) as e:
        print(f"[opencode] find_session error: {e}")
    return None


def _opencode_tail_cursor(session_info):
    """Return a stable cursor at the current end of an OpenCode session."""
    db_path, session_id = session_info
    con = sqlite3.connect(str(db_path), timeout=2)
    try:
        row = con.execute(
            "SELECT rowid, id FROM part WHERE session_id = ? "
            "ORDER BY rowid DESC LIMIT 1",
            (session_id,),
        ).fetchone()
    finally:
        con.close()
    return {
        "kind": "opencode",
        "db": str(Path(db_path).resolve()),
        "session_id": session_id,
        "rowid": int(row[0]) if row else 0,
        "part_id": row[1] if row else None,
    }


def _opencode_read_new(session_info, cursor):
    """Pure OpenCode reader returning messages and the next durable cursor."""
    db_path, session_id = session_info
    rowid = int(cursor.get("rowid", 0))
    messages = []
    try:
        con = sqlite3.connect(str(db_path), timeout=2)
        if rowid and cursor.get("part_id"):
            found = con.execute(
                "SELECT id FROM part WHERE session_id = ? AND rowid = ?",
                (session_id, rowid),
            ).fetchone()
            if not found or found[0] != cursor["part_id"]:
                print(f"[opencode] cursor reset for rebuilt session {session_id}")
                rowid = 0
        rows = con.execute(
            "SELECT p.rowid, p.id, p.data, m.data FROM part p "
            "JOIN message m ON p.message_id = m.id "
            "WHERE p.session_id = ? AND p.rowid > ? "
            "ORDER BY p.rowid",
            (session_id, rowid),
        ).fetchall()
        con.close()
    except (sqlite3.Error, OSError) as e:
        raise RuntimeError(f"opencode read error: {e}") from e

    max_rowid = rowid
    last_part_id = cursor.get("part_id") if rowid else None
    for part_rowid, part_id, pdata_str, mdata_str in rows:
        max_rowid = max(max_rowid, part_rowid)
        last_part_id = part_id
        try:
            pdata = json.loads(pdata_str)
            mdata = json.loads(mdata_str)
        except json.JSONDecodeError:
            continue
        if mdata.get("role") != "assistant":
            continue
        formatted = _opencode_format(pdata)
        if formatted:
            messages.append(formatted)
    next_cursor = dict(cursor)
    next_cursor.update(rowid=max_rowid, part_id=last_part_id)
    return messages, next_cursor


def _gemini_find_session(cwd):
    """Find most recent Gemini session file for a project cwd."""
    project_name = Path(cwd).name
    chats_dir = GEMINI_DIR / project_name / "chats"
    if not chats_dir.is_dir():
        return None
    files = list(chats_dir.glob("session-*.json"))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None


def _gemini_tail_cursor(session):
    """Return a stable cursor at the current end of a Gemini session."""
    data = json.loads(session.read_text())
    messages = data.get("messages", [])
    return {
        "kind": "gemini",
        "path": str(Path(session).resolve()),
        "index": len(messages),
        "last_id": messages[-1].get("id") if messages else None,
    }


def _gemini_read_new(session, cursor):
    """Pure Gemini reader returning messages and the next durable cursor."""
    data = json.loads(session.read_text())
    messages = data.get("messages", [])
    index = int(cursor.get("index", 0))
    last_id = cursor.get("last_id")
    if index:
        cursor_matches = (
            index <= len(messages)
            and messages[index - 1].get("id") == last_id
        )
        if not cursor_matches:
            positions = [i for i, msg in enumerate(messages) if msg.get("id") == last_id]
            if positions:
                index = positions[-1] + 1
            else:
                print(f"[gemini] cursor reset for rewritten session {session}")
                index = 0
    new_msgs = messages[index:]
    results = []
    for msg in new_msgs:
        if msg.get("type") == "gemini":
            text = (msg.get("content") or "").strip()
            if text:
                results.append(text)
    next_cursor = dict(cursor)
    next_cursor.update(
        index=len(messages),
        last_id=messages[-1].get("id") if messages else None,
    )
    return results, next_cursor


# --- harness: message formatters ------------------------------------------


def _claude_format(record):
    if record.get("type") != "assistant":
        return None
    msg = record.get("message", {})
    if msg.get("model") == "<synthetic>":
        return None
    content = msg.get("content", [])
    parts = []
    for block in content:
        if block.get("type") == "text":
            t = block.get("text", "").strip()
            if t:
                parts.append(t)
        elif block.get("type") == "tool_use" and block.get("name") == "ExitPlanMode":
            plan = block.get("input", {}).get("plan", "")
            if plan:
                parts.append(f"📋 PLAN PROPOSAL:\n{plan}")
    return "\n".join(parts) if parts else None


def _codex_format(record):
    if record.get("type") != "response_item":
        return None
    p = record.get("payload", {})
    if p.get("type") == "message" and p.get("role") == "assistant":
        parts = []
        for block in p.get("content", []):
            if block.get("type") == "output_text":
                t = block.get("text", "").strip()
                if t:
                    parts.append(t)
        return "\n".join(parts) if parts else None
    return None


def _opencode_format(part):
    """Format an opencode part record (from the part.data column)."""
    if part.get("type") == "text":
        t = part.get("text", "").strip()
        return t if t else None
    return None


# --- harness registry ------------------------------------------------------


class Harness:
    def __init__(self, name, token, find_session, format_record,
                 read_new=None, proc_hints=()):
        self.name = name
        self.bot = Bot(token)
        self.find_session = find_session
        self.format_record = format_record
        self.read_new = read_new  # optional override for non-file-based sessions
        self.display_name = name  # updated from bot profile at startup
        self.proc_hints = proc_hints


harnesses = {}  # name -> Harness


def _init_harnesses():
    harnesses["claude"] = Harness(
        "claude", CLAUDE_TOKEN, _claude_find_session, _claude_format,
        proc_hints=("claude",),
    )
    if CODEX_TOKEN:
        harnesses["codex"] = Harness(
            "codex", CODEX_TOKEN, _codex_find_session, _codex_format,
            proc_hints=("codex",),
        )
    if OPENCODE_TOKEN:
        harnesses["opencode"] = Harness(
            "opencode", OPENCODE_TOKEN, _opencode_find_session, _opencode_format,
            read_new=_opencode_read_new,
            proc_hints=("opencode",),
        )
    if GEMINI_TOKEN:
        harnesses["gemini"] = Harness(
            "gemini", GEMINI_TOKEN, _gemini_find_session, None,
            read_new=_gemini_read_new,
            proc_hints=("gemini",),
        )


# --- text helpers ----------------------------------------------------------


def _now_ts():
    """Return short timestamp like '14:03' in configured timezone, or '' if disabled."""
    if not MSG_TIMESTAMPS:
        return ""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo(MSG_TZ)).strftime("%H:%M")


def _ts_from_millis(timestamp):
    """Format an epoch-millisecond Signal timestamp in the configured timezone."""
    if not MSG_TIMESTAMPS or not timestamp:
        return ""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.fromtimestamp(
        int(timestamp) / 1000, ZoneInfo(MSG_TZ)
    ).strftime("%H:%M")


def _md_tables_to_slack(text):
    """Convert markdown tables to monospace code blocks for Slack."""
    lines = text.split("\n")
    out, table, in_table = [], [], False
    for line in lines:
        stripped = line.strip()
        if re.match(r"^\|.*\|$", stripped):
            if re.match(r"^\|[-\s|:]+\|$", stripped):
                continue  # skip separator row
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            table.append(cells)
            in_table = True
        else:
            if in_table:
                # compute column widths and format
                widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
                for row in table:
                    out.append("  ".join(c.ljust(widths[i]) for i, c in enumerate(row)))
                out.append("")
                table, in_table = [], False
            out.append(line)
    if table:
        widths = [max(len(r[i]) for r in table) for i in range(len(table[0]))]
        for row in table:
            out.append("  ".join(c.ljust(widths[i]) for i, c in enumerate(row)))
    return "\n".join(out)


def _chunkify(text, limit=4000):
    buf, length = [], 0
    for line in text.split("\n"):
        if length + len(line) + 1 > limit and buf:
            yield "\n".join(buf)
            buf, length = [], 0
        buf.append(line)
        length += len(line) + 1
    if buf:
        yield "\n".join(buf)


# --- signal-cli JSON-RPC client --------------------------------------------


class SignalClient:
    """Two-connection client: one for RPC calls, one for receiving notifications."""

    def __init__(self, socket_path):
        self._path = socket_path
        # call connection (for send, createGroup, etc.)
        self._call_reader = None
        self._call_writer = None
        self._req_id = 0
        self._call_lock = asyncio.Lock()
        # receive connection (subscribe + notifications)
        self._recv_reader = None
        self._recv_writer = None

    async def connect(self):
        self._call_reader, self._call_writer = (
            await asyncio.open_unix_connection(self._path)
        )

    async def close(self):
        for w in (self._call_writer, self._recv_writer):
            if w:
                try:
                    w.close()
                    await w.wait_closed()
                except Exception:
                    pass
        self._call_writer = self._call_reader = None
        self._recv_writer = self._recv_reader = None

    async def _call(self, method, params=None):
        async with self._call_lock:
            self._req_id += 1
            rid = self._req_id
            req = {"jsonrpc": "2.0", "id": rid, "method": method}
            if params:
                req["params"] = params
            line = json.dumps(req) + "\n"
            self._call_writer.write(line.encode())
            await self._call_writer.drain()
            # read lines until we get our response (skip stale notifications)
            while True:
                resp_line = await self._call_reader.readline()
                if not resp_line:
                    raise ConnectionError("signal-cli call socket closed")
                msg = json.loads(resp_line)
                if msg.get("id") != rid:
                    continue  # skip notifications / stale data
                if "error" in msg:
                    raise RuntimeError(f"signal-cli: {msg['error']}")
                return msg.get("result")

    @property
    def connected(self):
        return self._call_writer is not None

    async def send_message(self, group_id, text):
        result = await self._call("send", {
            "groupId": group_id,
            "message": text,
            "account": SIGNAL_ACCOUNT,
        })
        try:
            _signal_db_archive_outgoing(group_id, text, result)
        except Exception as e:
            print(f"[signal] outgoing archive error: {e}")
        return result

    async def create_group(self, name, members):
        return await self._call("updateGroup", {
            "name": name,
            "members": members,
            "account": SIGNAL_ACCOUNT,
        })

    async def rename_group(self, group_id, name):
        return await self._call("updateGroup", {
            "groupId": group_id,
            "name": name,
            "account": SIGNAL_ACCOUNT,
        })

    async def leave_group(self, group_id):
        return await self._call("quitGroup", {
            "groupId": group_id,
            "admin": [SIGNAL_OWNER],
            "account": SIGNAL_ACCOUNT,
        })

    async def add_members(self, group_id, members):
        return await self._call("updateGroup", {
            "groupId": group_id,
            "addMember": members,
            "account": SIGNAL_ACCOUNT,
        })

    async def remove_members(self, group_id, members):
        return await self._call("updateGroup", {
            "groupId": group_id,
            "removeMember": members,
            "account": SIGNAL_ACCOUNT,
        })

    async def list_groups(self):
        return await self._call("listGroups", {
            "account": SIGNAL_ACCOUNT,
        })

    async def receive_loop(self, callback):
        # open a dedicated connection for receiving
        self._recv_reader, self._recv_writer = (
            await asyncio.open_unix_connection(self._path)
        )
        # subscribe
        req = json.dumps({"jsonrpc": "2.0", "id": 1,
                          "method": "subscribeReceive",
                          "params": {"account": SIGNAL_ACCOUNT}}) + "\n"
        self._recv_writer.write(req.encode())
        await self._recv_writer.drain()
        while True:
            line = await self._recv_reader.readline()
            if not line:
                raise ConnectionError("signal-cli recv socket closed")
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            # skip RPC responses (subscribe ack, etc.)
            if "id" in msg and "method" not in msg:
                continue
            # notification
            if "method" in msg and callback:
                try:
                    await callback(msg)
                except Exception as e:
                    print(f"[signal] callback error: {e}\n{traceback.format_exc()}")


def _normalize_signal_group_id(group_id):
    if group_id is None:
        return ""
    if isinstance(group_id, bytes):
        group_id = group_id.decode(errors="ignore")
    gid = str(group_id).strip()
    if not gid:
        return ""
    # signal-cli may vary padding across endpoints
    return gid.rstrip("=")





# --- persistent state ------------------------------------------------------


def _load():
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {}


def _write_json(path, data, *, mode=None):
    """Atomically replace a JSON file so a crash cannot leave partial state."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(tmp, flags, mode if mode is not None else 0o666)
    try:
        if mode is not None:
            # An existing temp file may have broader permissions. Tighten it
            # before writing any private message content.
            os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as f:
            fd = -1
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        if fd >= 0:
            os.close(fd)
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    try:
        dir_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass


def _save(data):
    _write_json(STATE, data)


def _signal_db_connect():
    SIGNAL_DB.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(SIGNAL_DB)
    db.row_factory = sqlite3.Row
    return db


def _signal_db_init():
    with _signal_db_connect() as db:
        db.execute("PRAGMA journal_mode=WAL")
        schema = db.execute("""
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'signal_messages'
        """).fetchone()
        if schema and "UNIQUE (group_id, envelope_timestamp)" in schema["sql"]:
            columns = {
                row["name"] for row in db.execute(
                    "PRAGMA table_info(signal_messages)"
                ).fetchall()
            }
            direction = "direction" if "direction" in columns else "'incoming'"
            db.execute("ALTER TABLE signal_messages RENAME TO signal_messages_old")
            db.execute("""
                CREATE TABLE signal_messages (
                    id INTEGER PRIMARY KEY,
                    group_id TEXT NOT NULL,
                    envelope_timestamp INTEGER,
                    received_at INTEGER NOT NULL,
                    sender_id TEXT NOT NULL,
                    sender_number TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    formatted_text TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    direction TEXT NOT NULL DEFAULT 'incoming',
                    accepted INTEGER NOT NULL,
                    is_command INTEGER NOT NULL,
                    is_mention INTEGER NOT NULL,
                    delivered_at INTEGER,
                    UNIQUE (group_id, sender_id, envelope_timestamp)
                )
            """)
            db.execute(f"""
                INSERT INTO signal_messages (
                    id, group_id, envelope_timestamp, received_at,
                    sender_id, sender_number, sender_name,
                    text, formatted_text, data_json, direction,
                    accepted, is_command, is_mention, delivered_at
                )
                SELECT
                    id, group_id, envelope_timestamp, received_at,
                    sender_id, sender_number, sender_name,
                    text, formatted_text, data_json, {direction},
                    accepted, is_command, is_mention, delivered_at
                FROM signal_messages_old
            """)
            db.execute("DROP TABLE signal_messages_old")
        else:
            db.execute("""
                CREATE TABLE IF NOT EXISTS signal_messages (
                    id INTEGER PRIMARY KEY,
                    group_id TEXT NOT NULL,
                    envelope_timestamp INTEGER,
                    received_at INTEGER NOT NULL,
                    sender_id TEXT NOT NULL,
                    sender_number TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    text TEXT NOT NULL,
                    formatted_text TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    direction TEXT NOT NULL DEFAULT 'incoming',
                    accepted INTEGER NOT NULL,
                    is_command INTEGER NOT NULL,
                    is_mention INTEGER NOT NULL,
                    delivered_at INTEGER,
                    UNIQUE (group_id, sender_id, envelope_timestamp)
                )
            """)
        columns = {
            row["name"] for row in db.execute(
                "PRAGMA table_info(signal_messages)"
            ).fetchall()
        }
        if "direction" not in columns:
            db.execute("""
                ALTER TABLE signal_messages
                ADD COLUMN direction TEXT NOT NULL DEFAULT 'incoming'
            """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS signal_messages_pending
            ON signal_messages (group_id, delivered_at, id)
        """)
        db.execute("""
            CREATE VIEW IF NOT EXISTS signal_history AS
            SELECT
                id,
                datetime(received_at / 1000, 'unixepoch', 'localtime') AS received,
                group_id,
                direction,
                sender_name,
                text,
                accepted,
                is_command,
                is_mention,
                delivered_at IS NULL AS pending
            FROM signal_messages
            ORDER BY COALESCE(envelope_timestamp, received_at), id
        """)


def _signal_db_archive(
    *,
    group_id,
    envelope_timestamp,
    sender_id,
    sender_number,
    sender_name,
    text,
    formatted_text,
    data,
    accepted,
    is_command,
    is_mention,
):
    """Archive one received message. Return (row_id, inserted, should_process)."""
    received_at = int(time.time() * 1000)
    timestamp = int(envelope_timestamp) if envelope_timestamp else None
    values = (
        group_id,
        timestamp,
        received_at,
        sender_id or "",
        sender_number or "",
        sender_name or "",
        text or "",
        formatted_text or "",
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        int(bool(accepted)),
        int(bool(is_command)),
        int(bool(is_mention)),
    )
    with _signal_db_connect() as db:
        cur = db.execute("""
            INSERT OR IGNORE INTO signal_messages (
                group_id, envelope_timestamp, received_at,
                sender_id, sender_number, sender_name,
                text, formatted_text, data_json,
                direction, accepted, is_command, is_mention
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'incoming', ?, ?, ?)
        """, values)
        if cur.rowcount:
            return (
                cur.lastrowid,
                True,
                bool(accepted and not is_command and formatted_text),
            )
        row = db.execute("""
            SELECT id, accepted, is_command, formatted_text, delivered_at
            FROM signal_messages
            WHERE group_id = ? AND sender_id = ? AND envelope_timestamp = ?
        """, (group_id, sender_id or "", timestamp)).fetchone()
    if not row:
        return None, False, False
    should_process = (
        row["accepted"]
        and not row["is_command"]
        and bool(row["formatted_text"])
        and row["delivered_at"] is None
    )
    return row["id"], False, should_process


def _signal_db_archive_outgoing(group_id, text, result):
    timestamp = result.get("timestamp") if isinstance(result, dict) else None
    sent_at = int(timestamp or time.time() * 1000)
    normalized = _normalize_signal_group_id(group_id)
    with _signal_db_connect() as db:
        db.execute("""
            INSERT OR IGNORE INTO signal_messages (
                group_id, envelope_timestamp, received_at,
                sender_id, sender_number, sender_name,
                text, formatted_text, data_json, direction,
                accepted, is_command, is_mention, delivered_at
            ) VALUES (?, ?, ?, ?, ?, 'Clod', ?, ?, '{}', 'outgoing', 1, 0, 0, ?)
        """, (
            normalized,
            int(timestamp) if timestamp else None,
            sent_at,
            SIGNAL_ACCOUNT,
            SIGNAL_ACCOUNT,
            text or "",
            text or "",
            sent_at,
        ))


def _signal_db_pending(group_id):
    with _signal_db_connect() as db:
        return db.execute("""
            SELECT id, formatted_text, is_mention
            FROM signal_messages
            WHERE group_id = ?
              AND accepted = 1
              AND direction = 'incoming'
              AND is_command = 0
              AND formatted_text != ''
              AND delivered_at IS NULL
            ORDER BY COALESCE(envelope_timestamp, received_at), id
        """, (group_id,)).fetchall()


def _signal_db_mark_delivered(message_ids):
    if not message_ids:
        return
    placeholders = ",".join("?" for _ in message_ids)
    with _signal_db_connect() as db:
        db.execute(
            f"UPDATE signal_messages SET delivered_at = ? "
            f"WHERE id IN ({placeholders}) AND delivered_at IS NULL",
            (int(time.time() * 1000), *message_ids),
        )


def _signal_db_move_pending(old_group_id, new_group_id):
    with _signal_db_connect() as db:
        db.execute("""
            UPDATE signal_messages
            SET group_id = ?
            WHERE group_id = ? AND delivered_at IS NULL
        """, (new_group_id, old_group_id))


def _signal_db_migrate_legacy_history(saved_history):
    """Move the former JSON mute backlog into the durable message archive."""
    base = int(time.time() * 1000)
    with _signal_db_connect() as db:
        for group_id, messages in saved_history.items():
            normalized = _normalize_signal_group_id(group_id)
            if not normalized or not isinstance(messages, list):
                continue
            for offset, message in enumerate(messages):
                formatted = str(message)
                db.execute("""
                    INSERT INTO signal_messages (
                        group_id, received_at,
                        sender_id, sender_number, sender_name,
                        text, formatted_text, data_json,
                        accepted, is_command, is_mention
                    ) VALUES (?, ?, '', '', '', ?, ?, '{}', 1, 0, 0)
                """, (normalized, base + offset, formatted, formatted))


# --- bridge state ----------------------------------------------------------

# Telegram's durable key is the tab title. wakterm tab IDs are reused after a
# mux restart, so the tab-ID maps are rebuilt from the current layout.
tg_name_topic = {}  # title_lower -> Telegram topic_id (persisted)
tab_topic = {}  # tab_id -> topic_id (ephemeral)
tab_topic_name = {}  # tab_id -> current title (ephemeral)
topic_tab = {}  # topic_id -> tab_id (ephemeral)
_tg_verified_names = set()  # title_lower mappings probed during this process
_tg_stale_topics = set()  # topic IDs rejected by Telegram during delivery
_tg_topic_lock = asyncio.Lock()
_tg_retry_after_until = 0.0

# per pane
pane_harness = {}  # pane_id -> harness name
pane_tab = {}  # pane_id -> tab_id
pane_cwds = {}  # pane_id -> cwd
_source_cursors = {}  # stable source key -> durable cursor record

# reply routing
msg_pane = {}  # telegram_msg_id -> pane_id
tab_last_pid = {}  # tab_id -> pane_id of last agent to send a message


# signal bridge state
# Durable source of truth: title -> group_id. wezterm renumbers tab_ids on
# restart, so tab_id is NOT a stable key; the tab TITLE is. sig_tab_group /
# sig_tab_name are ephemeral, rebuilt each sync from the current tab layout.
sig_name_group = {}   # title_lower -> signal group_id (persisted)
sig_tab_group = {}    # tab_id -> signal group_id (ephemeral, rebuilt each sync)
sig_group_tab = {}    # group_id -> tab_id (ephemeral)
sig_tab_name = {}     # tab_id -> group name (ephemeral)
sig_msg_pane = {}     # signal_timestamp -> pane_id
sig_tab_last_pid = {} # tab_id -> pane_id
_signal_client = None
_signal_cmd_queue = []  # [(text, group_id, tab_id), ...]
_signal_input_queue = []  # [(data, tab_id, group_id, is_mention), ...]

# slack observer state
_slack_msg_buffer = {}     # channel_id -> [(ts, user_id_or_None, text), ...]
_slack_obs_queue = []      # [(extra_text, user_id, channel_id), ...] !obs triggers
_slack_reply_channel = None  # set by !obs, cleared after agent responds
_slack_direct_tab = {}     # tab_id -> channel_id (for direct channel output)
_slack_direct_queue = []   # [(text, user_id, channel_id), ...] direct channel messages
_slack_web_client = None   # AsyncWebClient, set in startup
_slack_bot_user_id = None  # our own bot user id, to skip self-echo
last_source_name = {}  # stable title_lower -> last output channel


def _rebuild():
    global topic_tab, sig_group_tab
    topic_tab = {tid: tab for tab, tid in tab_topic.items()}
    sig_group_tab = {}
    for tab, gid in sig_tab_group.items():
        ngid = _normalize_signal_group_id(gid)
        if ngid:
            sig_group_tab[ngid] = tab


def _persist():
    data = {
        "telegram_topics": dict(sorted(tg_name_topic.items())),
        "collab": {str(k): v for k, v in collab_tabs.items()},
        "clod_off_groups": sorted(clod_off_groups),
    }
    if _legacy_clod_off_tabs:
        data["clod_off"] = sorted(str(k) for k in _legacy_clod_off_tabs)
    if sig_name_group:
        # keyed by tab title (stable), not tab_id (renumbers on wezterm restart)
        data["signal_groups"] = dict(sig_name_group)
    if last_source_name:
        data["last_sources"] = dict(sorted(last_source_name.items()))
    _save(data)


def _source_key(h, session):
    if h.name == "opencode":
        db_path, session_id = session
        return f"opencode:{Path(db_path).resolve()}:{session_id}"
    path = Path(session).resolve()
    kind = "gemini" if h.name == "gemini" else "jsonl"
    return f"{kind}:{h.name}:{path}"


def _jsonl_tail_cursor(h, session):
    path = Path(session).resolve()
    with path.open("rb") as f:
        st = os.fstat(f.fileno())
        offset = st.st_size
        if offset:
            f.seek(offset - 1)
            if f.read(1) != b"\n":
                start = max(0, offset - 65536)
                f.seek(start)
                tail = f.read(offset - start)
                newline = tail.rfind(b"\n")
                offset = start + newline + 1 if newline >= 0 else 0
    return {
        "kind": "jsonl",
        "harness": h.name,
        "path": str(path),
        "offset": offset,
        "dev": st.st_dev,
        "ino": st.st_ino,
    }


def _tail_cursor(h, session):
    if h.name == "opencode":
        return _opencode_tail_cursor(session)
    if h.name == "gemini":
        return _gemini_tail_cursor(session)
    return _jsonl_tail_cursor(h, session)


def _seek_to_end(pid, cursors=None):
    """Set this source's durable cursor to its current tail."""
    cwd = pane_cwds.get(pid)
    h = harnesses.get(pane_harness.get(pid, ""))
    if not cwd or not h:
        return False
    session = h.find_session(cwd)
    if not session:
        return False
    cursor_map = _source_cursors if cursors is None else cursors
    cursor_map[_source_key(h, session)] = _tail_cursor(h, session)
    return True


# --- pane discovery --------------------------------------------------------



def _pane_procs(tty_name):
    """Return set of command names running on a pane's TTY."""
    pts = tty_name.replace("/dev/", "") if tty_name else ""
    if not pts:
        return set()
    try:
        r = subprocess.run(
            ["ps", "-t", pts, "-o", "args", "--no-headers"],
            capture_output=True, text=True, timeout=3,
        )
        cmds = set()
        for line in r.stdout.strip().splitlines():
            # extract binary names from all args (catches "node /path/to/gemini --yolo")
            for part in line.strip().split():
                if not part.startswith("-"):
                    cmds.add(Path(part).name)
        return cmds
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return set()


def _discover_sync():
    """Find all panes, classify by harness where possible."""
    all_panes = _all_panes_sync()
    by_tab = {}
    for p in all_panes:
        by_tab.setdefault(p["tab_id"], []).append(p)

    # cache process info per pane (one ps call per pane)
    pane_procs = {}
    for p in all_panes:
        pane_procs[p["pane_id"]] = _pane_procs(p.get("tty_name", ""))

    matched = []  # (pane, harness_name) — have a session file
    unmatched = []  # (pane, None) — no session but not a shell
    claimed_sessions = set()  # global: session keys already taken

    for panes in by_tab.values():
        claimed = set()
        for h_name, h in harnesses.items():
            best_pane, best_mt, best_key = None, 0.0, None
            proc_only_pane = None  # process match but no session
            for p in panes:
                if p["pane_id"] in claimed:
                    continue
                procs = pane_procs.get(p["pane_id"], set())
                proc_match = any(h in procs for h in h.proc_hints)
                if not proc_match:
                    continue
                cwd = _parse_cwd(p.get("cwd", ""))
                session = h.find_session(cwd)
                if session:
                    key = (h_name, str(session) if isinstance(session, Path)
                           else session[1])
                    if key in claimed_sessions:
                        continue
                    mt = (session.stat().st_mtime
                          if isinstance(session, Path) else time.time())
                    if mt > best_mt:
                        best_pane, best_mt, best_key = p, mt, key
                elif proc_only_pane is None:
                    proc_only_pane = p
            if best_pane:
                matched.append((best_pane, h_name))
                claimed.add(best_pane["pane_id"])
                claimed_sessions.add(best_key)
            elif proc_only_pane:
                # process running but no session yet — claim for input routing
                matched.append((proc_only_pane, h_name))
                claimed.add(proc_only_pane["pane_id"])
        # track one pane per tab for topic creation + input routing
        seen_tabs = {p["tab_id"] for p, _ in matched}
        for p in panes:
            if p["tab_id"] not in seen_tabs and p["pane_id"] not in claimed:
                unmatched.append((p, None))
                seen_tabs.add(p["tab_id"])
                claimed.add(p["pane_id"])
    return matched, unmatched


async def discover():
    return await asyncio.to_thread(_discover_sync)


# --- core loop -------------------------------------------------------------

_primary_bot = None  # set in startup, used for topic management
_route_sync_lock = asyncio.Lock()
_control_server = None


def _telegram_topic_key(title):
    return str(title or "").strip().casefold()


def _telegram_topic_missing(error):
    return (
        isinstance(error, BadRequest)
        and "message thread not found" in str(error).lower().replace("_", " ")
    )


def _telegram_topic_unchanged(error):
    return (
        isinstance(error, BadRequest)
        and "topic not modified" in str(error).lower().replace("_", " ")
    )


async def _ensure_telegram_topic(title, preferred_topic=None):
    async with _tg_topic_lock:
        return await _ensure_telegram_topic_unlocked(title, preferred_topic)


async def _ensure_telegram_topic_unlocked(title, preferred_topic=None):
    """Return a verified topic ID for a stable title, creating if missing."""
    title = str(title or "").strip()[:128]
    key = _telegram_topic_key(title)
    if not key:
        return None, False

    topic_id = tg_name_topic.get(key) or preferred_topic
    if (
        topic_id
        and key in _tg_verified_names
        and topic_id not in _tg_stale_topics
    ):
        return topic_id, False

    changed = False
    if topic_id in _tg_stale_topics:
        topic_id = None
    if topic_id and topic_id not in _tg_stale_topics:
        try:
            await _primary_bot.edit_forum_topic(CHAT, topic_id, name=title)
            print(f"[tg] verified topic '{title}' -> {topic_id}")
        except Exception as e:
            if _telegram_topic_unchanged(e):
                pass
            elif _telegram_topic_missing(e):
                _tg_stale_topics.add(topic_id)
                topic_id = None
            else:
                print(f"[tg] verify topic '{title}' failed: {e}")
                return None, False

    if not topic_id:
        try:
            topic = await _primary_bot.create_forum_topic(CHAT, title)
            topic_id = topic.message_thread_id
            if not topic_id:
                raise RuntimeError("create_forum_topic returned no thread ID")
            print(f"[tg] created topic '{title}' -> {topic_id}")
            changed = True
        except Exception as e:
            print(f"[tg] create topic '{title}' failed: {e}")
            return None, False

    previous = tg_name_topic.get(key)
    if previous != topic_id:
        tg_name_topic[key] = topic_id
        changed = True
    _tg_verified_names.add(key)
    _tg_stale_topics.discard(topic_id)
    if previous and previous != topic_id:
        _tg_stale_topics.discard(previous)
    if changed:
        # Persist the new route before another await or any backlog delivery.
        _persist()
    if _retarget_pending_telegram(key, topic_id):
        changed = True
    return topic_id, changed


async def sync_topics(matched, unmatched):
    active_pids = set()
    dirty = False
    tabs = {}

    # Track all panes first, then resolve one stable title mapping per tab.
    for p, h_name in matched + unmatched:
        pid = p["pane_id"]
        tab_id = p["tab_id"]
        cwd = _parse_cwd(p.get("cwd", ""))
        title = str(p.get("tab_title") or "").strip()[:128]

        active_pids.add(pid)
        pane_tab[pid] = tab_id
        pane_cwds[pid] = cwd
        if h_name:
            pane_harness[pid] = h_name
        if not title:
            continue
        tabs.setdefault(tab_id, title)

    new_tab_topic = {}
    new_tab_topic_name = {}
    claimed_names = {}
    pending_routes = _pending_telegram_routes()
    for tab_id, title in tabs.items():
        key = _telegram_topic_key(title)
        if key in claimed_names and claimed_names[key] != tab_id:
            print(f"[tg] duplicate tab title '{title}', skipping tab {tab_id}")
            continue
        claimed_names[key] = tab_id
        preferred = pending_routes.pop(key, None)
        topic_id, topic_changed = await _ensure_telegram_topic(title, preferred)
        if not topic_id:
            continue
        new_tab_topic[tab_id] = topic_id
        new_tab_topic_name[tab_id] = title
        dirty = dirty or topic_changed

    # Pending output can outlive its terminal pane. Its durable route title is
    # enough to restore that topic and deliver the backlog.
    for route, preferred in pending_routes.items():
        _topic_id, topic_changed = await _ensure_telegram_topic(route, preferred)
        dirty = dirty or topic_changed

    tab_topic.clear()
    tab_topic.update(new_tab_topic)
    tab_topic_name.clear()
    tab_topic_name.update(new_tab_topic_name)
    tab_last_source.clear()
    tab_last_source.update({
        tab_id: last_source_name[_telegram_topic_key(title)]
        for tab_id, title in new_tab_topic_name.items()
        if _telegram_topic_key(title) in last_source_name
    })
    for pid in [p for p in list(pane_tab) if p not in active_pids]:
        pane_harness.pop(pid, None)
        pane_tab.pop(pid, None)
        pane_cwds.pop(pid, None)

    _rebuild()
    if dirty:
        _persist()


async def _refresh_telegram_routes():
    """Refresh live panes and Telegram mappings without overlapping a poll sync."""
    async with _route_sync_lock:
        matched, unmatched = await discover()
        await sync_topics(matched, unmatched)
        return matched, unmatched


def _tab_match(title, patterns, empty_means_all=False):
    """Check if a tab title matches any pattern in the list."""
    if not patterns:
        return empty_means_all
    t = title.lower()
    return any(re.search(rf'\b{re.escape(pat)}\b', t) for pat in patterns)


def _sig_members_for(title):
    """Return the member list for a signal group based on tab title."""
    t = title.lower()
    members = SIGNAL_TAB_MEMBERS.get(t)
    if members is not None:
        return [SIGNAL_OWNER] + members
    return [SIGNAL_OWNER] + SIGNAL_MEMBERS


def _find_tab(patterns, **kw):
    """Find the first tab_id matching patterns."""
    for tid, name in tab_topic_name.items():
        if _tab_match(name, patterns, **kw):
            return tid
    return None


_sig_greeted = set()  # group_ids we've sent a startup message to
_sig_groups_verified = False


async def sync_signal_groups(matched, unmatched):
    if not SIGNAL_ENABLED or not _signal_client or not _signal_client.connected:
        return

    # one-time: verify stored groups are still valid on first call
    global _sig_groups_verified
    if not _sig_groups_verified and sig_name_group:
        _sig_groups_verified = True
        try:
            groups = await _signal_client.list_groups()
            valid_ids = {
                _normalize_signal_group_id(g.get("id"))
                for g in (groups or [])
                if g.get("isMember")
            }
            valid_ids.discard("")
            stale = [nm for nm, gid in list(sig_name_group.items())
                     if _normalize_signal_group_id(gid) not in valid_ids]
            if stale:
                for nm in stale:
                    sig_name_group.pop(nm, None)
                _persist()
                print(f"[signal] removed {len(stale)} stale group(s): {stale}")
        except Exception as e:
            print(f"[signal] group verification error: {e}")

    # Resolve groups by tab TITLE, not tab_id. wezterm renumbers tab_ids on
    # restart, so the tab_id -> group maps (sig_tab_group / sig_tab_name) are
    # rebuilt from scratch every sync off the current tab layout; the durable
    # title -> group_id mapping lives in sig_name_group. This makes the bridge
    # robust to tab renumbering and stops a renumbered tab from inheriting a
    # stale mapping (e.g. "trading" landing on the debate group's old tab_id).
    sig_tab_group.clear()
    sig_tab_name.clear()
    dirty = False
    for p, _h in matched + unmatched:
        tab_id = p["tab_id"]
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"
        if not _tab_match(title, SIGNAL_TABS):
            continue
        key = title.lower()
        gid = sig_name_group.get(key)
        if not gid:
            try:
                result = await _signal_client.create_group(
                    title, _sig_members_for(title)
                )
                gid = result.get("groupId") if isinstance(result, dict) else result
                if not gid:
                    print(f"[signal] create group returned no group id: {result}")
                    continue
                sig_name_group[key] = gid
                dirty = True
                print(f"[signal] created group '{title}' -> {gid}")
            except Exception as e:
                print(f"[signal] create group error: {e}")
                continue
        sig_tab_group[tab_id] = gid
        sig_tab_name[tab_id] = title
        _sig_greeted.add(gid)

    # Migrate the old tab-id-based mute state while the original mux layout is
    # still available. New state is keyed by Signal group ID and survives tab
    # renumbering across terminal restarts.
    for tab_id in list(_legacy_clod_off_tabs):
        gid = _normalize_signal_group_id(sig_tab_group.get(tab_id))
        if gid:
            clod_off_groups.add(gid)
            _legacy_clod_off_tabs.discard(tab_id)
            dirty = True

    if dirty:
        _persist()
    _rebuild()
    await _flush_ready_signal_history()


def _read_jsonl_new(h, session, cursor):
    path = Path(session).resolve()
    with path.open("rb") as f:
        st = os.fstat(f.fileno())
        offset = int(cursor.get("offset", 0))
        if (
            cursor.get("dev") != st.st_dev
            or cursor.get("ino") != st.st_ino
            or offset > st.st_size
        ):
            print(f"[{h.name}] source reset detected, replaying {path}")
            offset = 0
        f.seek(offset)
        data = f.read()

    newline = data.rfind(b"\n")
    if newline < 0:
        return [], dict(cursor, offset=offset, dev=st.st_dev, ino=st.st_ino), 0
    complete = data[:newline + 1]
    messages = []
    record_count = 0
    for raw_line in complete.splitlines():
        if not raw_line.strip():
            continue
        record_count += 1
        try:
            record = json.loads(raw_line.decode("utf-8"))
        except json.JSONDecodeError as e:
            print(f"[{h.name}] skipping invalid complete JSONL record: {e}")
            continue
        formatted = h.format_record(record)
        if formatted:
            messages.append(formatted)
    next_cursor = dict(cursor)
    next_cursor.update(
        kind="jsonl",
        harness=h.name,
        path=str(path),
        offset=offset + len(complete),
        dev=st.st_dev,
        ino=st.st_ino,
    )
    return messages, next_cursor, record_count


def _peek_new_sync(pid, cursor_snapshot):
    """Read without mutating durable state; return a batch for one source."""
    h = harnesses.get(pane_harness.get(pid, ""))
    cwd = pane_cwds.get(pid)
    if not h or not cwd:
        return None

    session = h.find_session(cwd)
    if not session:
        return None

    source_key = _source_key(h, session)
    cursor = cursor_snapshot.get(source_key)
    if cursor is None:
        cursor = _tail_cursor(h, session)

    if h.name == "opencode":
        messages, next_cursor = _opencode_read_new(session, cursor)
        records = max(0, int(next_cursor.get("rowid", 0)) - int(cursor.get("rowid", 0)))
    elif h.name == "gemini":
        messages, next_cursor = _gemini_read_new(session, cursor)
        records = max(0, int(next_cursor.get("index", 0)) - int(cursor.get("index", 0)))
    else:
        messages, next_cursor, records = _read_jsonl_new(h, session, cursor)

    if records:
        tab = tab_topic_name.get(pane_tab.get(pid, -1), "?")
        print(f"[{tab}/{h.name}] read {records} records, {len(messages)} messages")
    return {
        "pid": pid,
        "source_key": source_key,
        "cursor": next_cursor,
        "messages": messages,
        "records": records,
        "baseline": source_key not in cursor_snapshot,
    }


# Failed sends awaiting retry:
# (kind, target, chunk, pid, h_name, stable_route_name, item_id).
# The outbox and stable source cursors share one atomic checkpoint. A cursor
# advances only when every output chunk derived before it is in the outbox.
_pending_sends = []


def _pending_item(raw):
    """Decode one canonical durable queue item."""
    if not isinstance(raw, dict):
        raise TypeError("invalid pending-send item")
    item_id = raw.get("id")
    kind = raw.get("kind")
    target = raw.get("target")
    chunk = raw.get("chunk")
    pid = raw.get("pane_id")
    h_name = raw.get("harness")
    route = raw.get("route_title", "")
    if (
        not item_id
        or not kind
        or target is None
        or not isinstance(chunk, str)
        or pid is None
        or not h_name
    ):
        raise ValueError("incomplete pending-send item")
    return (
        str(kind), target, chunk, int(pid), str(h_name),
        _telegram_topic_key(route), str(item_id),
    )


def _pending_json(items=None, cursors=None):
    items = _pending_sends if items is None else items
    cursors = _source_cursors if cursors is None else cursors
    return {
        "schema": "panetone.delivery-state.v2",
        "saved_at": int(time.time() * 1000),
        "cursors": cursors,
        "items": [
            {
                "id": item_id,
                "kind": kind,
                "target": target,
                "chunk": chunk,
                "pane_id": pid,
                "harness": h_name,
                "route_title": route,
            }
            for kind, target, chunk, pid, h_name, route, item_id in items
        ],
    }


def _replace_delivery_state(items, cursors=None):
    """Atomically replace disk state before publishing it in memory."""
    next_items = list(items)
    next_cursors = dict(_source_cursors if cursors is None else cursors)
    _write_json(
        PENDING_STATE,
        _pending_json(next_items, next_cursors),
        mode=0o600,
    )
    _pending_sends[:] = next_items
    _source_cursors.clear()
    _source_cursors.update(next_cursors)


def _commit_delivery(new_items, cursor_updates):
    next_cursors = dict(_source_cursors)
    next_cursors.update(cursor_updates)
    _replace_delivery_state([*_pending_sends, *new_items], next_cursors)


def _load_pending():
    if not PENDING_STATE.exists():
        return False
    try:
        data = json.loads(PENDING_STATE.read_text())
        if not isinstance(data, dict) or data.get("schema") != "panetone.delivery-state.v2":
            raise ValueError("unsupported delivery-state schema")
        raw_items = data.get("items", [])
        if not isinstance(raw_items, list):
            raise TypeError("invalid pending-send list")
        items = [_pending_item(item) for item in raw_items]
        cursors = data.get("cursors", {})
        if not isinstance(cursors, dict) or not all(
            isinstance(key, str) and isinstance(value, dict)
            for key, value in cursors.items()
        ):
            raise ValueError("invalid source cursor map")
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as e:
        raise RuntimeError(f"cannot load durable send queue {PENDING_STATE}: {e}") from e
    _pending_sends[:] = items
    _source_cursors.clear()
    _source_cursors.update(cursors)
    if items:
        routes = sorted({item[5] or str(item[1]) for item in items})
        print(f"[retry] loaded {len(items)} durable chunk(s) for {routes}")
    return True


def _pending_destination(item):
    kind, target, _chunk, _pid, _h_name, route, _item_id = item
    return (kind, route or target)


def _retarget_pending_telegram(route_key, topic_id):
    route_key = _telegram_topic_key(route_key)
    changed = False
    updated = []
    for kind, target, chunk, pid, h_name, route, item_id in _pending_sends:
        if kind == "tg" and _telegram_topic_key(route) == route_key:
            if target != topic_id or route != route_key:
                changed = True
            target, route = topic_id, route_key
        updated.append((kind, target, chunk, pid, h_name, route, item_id))
    if changed:
        _replace_delivery_state(updated)
        print(f"[retry] retargeted '{route_key}' backlog -> topic {topic_id}")
    return changed


def _pending_telegram_routes():
    routes = {}
    for kind, target, _chunk, _pid, _h_name, route, _item_id in _pending_sends:
        if kind == "tg" and route:
            routes.setdefault(route, target)
    return routes


async def _recreate_telegram_topic(tab_id, topic_id, name):
    """Serialize an explicit /refresh replacement with topic discovery."""
    async with _tg_topic_lock:
        await _primary_bot.delete_forum_topic(CHAT, topic_id)
        topic = await _primary_bot.create_forum_topic(CHAT, name)
        new_topic_id = topic.message_thread_id
        tab_topic[tab_id] = new_topic_id
        tab_topic_name[tab_id] = name
        key = _telegram_topic_key(name)
        tg_name_topic[key] = new_topic_id
        _tg_verified_names.add(key)
        _tg_stale_topics.discard(topic_id)
        _retarget_pending_telegram(key, new_topic_id)
        _rebuild()
        _persist()
        return new_topic_id


async def _deliver(kind, target, chunk, pid, h_name, route_name="", _item_id=None):
    """Send one chunk to a channel. Returns True on success."""
    global _tg_retry_after_until
    h = harnesses.get(h_name)
    bot = h.bot if h else _primary_bot
    try:
        if kind == "tg":
            sent = await bot.send_message(CHAT, chunk, message_thread_id=target)
            tab_id = pane_tab.get(pid)
            pane_route = _telegram_topic_key(tab_topic_name.get(tab_id, ""))
            if (
                tab_id is not None
                and pane_route == _telegram_topic_key(route_name)
                and pane_harness.get(pid) == h_name
            ):
                msg_pane[sent.message_id] = pid
        elif kind == "sig":
            result = await _signal_client.send_message(target, chunk)
            ts = result.get("timestamp") if isinstance(result, dict) else None
            if ts:
                sig_msg_pane[ts] = pid
        elif kind == "debate":
            sent = await bot.send_message(target, chunk)
            debate_msg_pane[sent.message_id] = pid
        elif kind == "slack":
            await _slack_web_client.chat_postMessage(channel=target, text=chunk)
        return True
    except Exception as e:
        if kind == "tg" and isinstance(e, RetryAfter):
            delay = e.retry_after
            if hasattr(delay, "total_seconds"):
                delay = delay.total_seconds()
            _tg_retry_after_until = max(
                _tg_retry_after_until,
                time.monotonic() + float(delay) + 1.0,
            )
            print(f"[tg] flood control, pausing delivery for {delay}s")
            return False
        if kind == "tg" and _telegram_topic_missing(e):
            _tg_stale_topics.add(target)
        print(f"send err [{kind}/{h_name}/{pid}]: {e}")
        return False


async def _attempt_pending(item):
    """Deliver the current form of one item, then durably dequeue it."""
    async def attempt_current():
        current = next(
            (pending for pending in _pending_sends if pending[6] == item[6]),
            None,
        )
        if current is None:
            return True
        if not await _deliver(*current):
            return False
        _replace_delivery_state(
            [pending for pending in _pending_sends if pending[6] != current[6]]
        )
        return True

    if item[0] == "tg":
        # /refresh uses this same lock, so it cannot delete or retarget a topic
        # between a successful send and the durable dequeue.
        async with _tg_topic_lock:
            if time.monotonic() < _tg_retry_after_until:
                return False
            return await attempt_current()
    return await attempt_current()


async def _flush_pending():
    """Retry one queued chunk per destination and durably record progress."""
    if not _pending_sends:
        return
    attempted = set()
    candidates = []
    for item in _pending_sends:
        dest = _pending_destination(item)
        if dest in attempted:
            continue
        attempted.add(dest)
        candidates.append(item)

    delivered = 0
    telegram_attempted = False
    for item in candidates:
        if item[0] == "tg":
            if telegram_attempted:
                continue
            telegram_attempted = True
        if await _attempt_pending(item):
            delivered += 1
    if _pending_sends:
        print(f"[retry] {len(_pending_sends)} chunk(s) still pending")
    elif delivered:
        print("[retry] durable queue drained")


def _new_pending_item(kind, target, chunk, pid, h_name, route_name=""):
    return (
        kind, target, chunk, pid, h_name,
        _telegram_topic_key(route_name), uuid.uuid4().hex,
    )


def _tab_route_title(tab_id):
    return tab_topic_name.get(tab_id) or sig_tab_name.get(tab_id, "")


def _set_tab_last_source(tab_id, source):
    if tab_id is None:
        return
    tab_last_source[tab_id] = source
    key = _telegram_topic_key(_tab_route_title(tab_id))
    if key and last_source_name.get(key) != source:
        last_source_name[key] = source
        _persist()


def _pane_main_route(pid):
    tab_id = pane_tab.get(pid)
    title = _tab_route_title(tab_id)
    gid = sig_tab_group.get(tab_id) if SIGNAL_ENABLED else None
    source = tab_last_source.get(tab_id) or last_source_name.get(
        _telegram_topic_key(title)
    )
    if source is None:
        source = "sig" if gid else "tg"
    if source == "tg":
        tid = tab_topic.get(tab_id)
        return ("tg", tid, title) if tid else None
    if source == "sig":
        return ("sig", gid, "") if gid else None
    if source == "debate" and DEBATE_ENABLED and _tab_match(title, DEBATE_TABS):
        return ("debate", DEBATE_CHAT, "")
    if source == "slack" and SLACK_ENABLED and tab_id in _slack_direct_tab:
        return ("slack", _slack_direct_tab[tab_id], "")
    return None


def _pane_output_ready(pid):
    if _pane_main_route(pid):
        return True
    tab_id = pane_tab.get(pid)
    return bool(
        SLACK_ENABLED
        and _slack_reply_channel
        and _tab_match(tab_topic_name.get(tab_id, ""), SLACK_TABS)
    )


async def check_output():
    global _slack_reply_channel
    pids = [pid for pid in pane_harness if _pane_output_ready(pid)]
    cursor_snapshot = {
        key: dict(value) for key, value in _source_cursors.items()
    }
    results = await asyncio.gather(
        *(asyncio.to_thread(_peek_new_sync, pid, cursor_snapshot) for pid in pids),
        return_exceptions=True,
    )
    for pid, result in zip(pids, results):
        if isinstance(result, Exception):
            print(f"[cursor] read error for pane {pid}: {result}")

    new_items = []
    cursor_updates = {}
    collab_batches = []
    slack_reply = _slack_reply_channel
    for result in results:
        if not result or isinstance(result, Exception):
            continue
        pid = result["pid"]
        messages = result["messages"]
        h = harnesses.get(pane_harness.get(pid, ""))
        if not h:
            continue
        tab_id = pane_tab.get(pid)
        main_route = _pane_main_route(pid)
        slack_observer = bool(
            SLACK_ENABLED
            and slack_reply
            and _tab_match(tab_topic_name.get(tab_id, ""), SLACK_TABS)
        )
        # A route may disappear while the worker thread reads. In that case,
        # leave the cursor untouched so the batch is retried after routing is
        # restored.
        if messages and not main_route and not slack_observer:
            continue
        source_key = result["source_key"]
        if cursor_snapshot.get(source_key) != result["cursor"]:
            cursor_updates[source_key] = result["cursor"]
        if tab_id is not None:
            tab_last_pid[tab_id] = pid
            sig_tab_last_pid[tab_id] = pid
        for msg in messages:
            if main_route:
                kind, target, route_name = main_route
                outgoing = msg
                if kind == "sig":
                    outgoing = f"{h.display_name}: {msg}"
                elif kind == "slack":
                    outgoing = _md_tables_to_slack(msg)
                for chunk in _chunkify(outgoing):
                    new_items.append(_new_pending_item(
                        kind, target, chunk, pid, h.name, route_name,
                    ))
            if main_route and main_route[0] == "sig":
                tab = tab_topic_name.get(tab_id, "?")
                print(f"[check_output] sending to signal: {tab}/{h.name} msg={msg[:60]}")
            # slack observer (reply to !obs, then clear)
            if slack_observer and slack_reply:
                slack_msg = _md_tables_to_slack(msg)
                for chunk in _chunkify(slack_msg):
                    new_items.append(_new_pending_item(
                        "slack", slack_reply, chunk, pid, h.name,
                    ))
                slack_reply = None
                slack_observer = False
            collab_batches.append((pid, h, tab_id, msg))

    if new_items or cursor_updates:
        _commit_delivery(new_items, cursor_updates)
        if _slack_reply_channel and slack_reply is None:
            _slack_reply_channel = None
    await _flush_pending()

    for pid, h, tab_id, msg in collab_batches:
        # collab: forward to other harness panes in this tab
        # (auto-enabled for debate tabs)
        is_debate = (
            DEBATE_ENABLED
            and debate_crosspost
            and _tab_match(tab_topic_name.get(tab_id, ""), DEBATE_TABS)
        )
        if tab_id is None or (tab_id not in collab_tabs and not is_debate):
            continue
        tid = tab_topic.get(tab_id)
        if "/signoff" in msg.lower():
            signoffs = collab_signoffs.setdefault(tab_id, set())
            signoffs.add(pid)
            tab_panes = {
                pane_pid for pane_pid, pane_tab_id in pane_tab.items()
                if pane_tab_id == tab_id and pane_pid in pane_harness
            }
            if tab_panes and signoffs >= tab_panes:
                del collab_tabs[tab_id]
                collab_signoffs.pop(tab_id, None)
                _persist()
                if tid:
                    try:
                        await _primary_bot.send_message(
                            CHAT, "all agents signed off, collab done",
                            message_thread_id=tid,
                        )
                    except Exception:
                        pass
                continue
        else:
            signoffs = collab_signoffs.get(tab_id)
            if signoffs:
                signoffs.discard(pid)

        ts = f" [{_now_ts()}]" if MSG_TIMESTAMPS else ""
        prefixed = f"{h.display_name}{ts} says: {msg}"
        for target_pid in _other_panes(tab_id, pid):
            await send_and_verify(target_pid, prefixed)
        rounds = collab_tabs.get(tab_id)
        if rounds and rounds > 0:
            collab_tabs[tab_id] = rounds - 1
            if rounds - 1 <= 0:
                del collab_tabs[tab_id]
                collab_signoffs.pop(tab_id, None)
                _persist()
                if tid:
                    try:
                        await _primary_bot.send_message(
                            CHAT, "collab done", message_thread_id=tid
                        )
                    except Exception:
                        pass


# --- collab mode -----------------------------------------------------------

collab_tabs = {}  # tab_id -> rounds_remaining (0 = infinite)
collab_signoffs = {}  # tab_id -> set of pane_ids that signed off
clod_off_groups = set()  # stable Signal group_ids muted via /clodoff
_legacy_clod_off_tabs = set()  # pre-migration tab_ids loaded from old state
debate_msg_pane = {}  # msg_id -> pane_id (reply routing for debate chat)
debate_crosspost = False  # auto-forward between harnesses in debate tabs (toggle with /crosspost)
tab_last_source = {}  # tab_id -> "tg"|"sig"|"debate"|"slack" (last input channel)


def _other_panes(tab_id, src_pid):
    """Find other panes in the same tab."""
    return [
        p for p, t in pane_tab.items()
        if t == tab_id and p != src_pid
    ]


# --- telegram handlers -----------------------------------------------------


def _is_owner(update):
    return not OWNER or (update.effective_user and update.effective_user.id == OWNER)


def _resolve_pid(tab_id, reply_pid=None):
    """Resolve target pane: explicit reply > last active > sole pane in tab."""
    pid = reply_pid
    if pid is None:
        pid = tab_last_pid.get(tab_id)
        if pid is not None and pane_tab.get(pid) != tab_id:
            pid = None
    if pid is None:
        tab_panes = sorted(p for p, t in pane_tab.items() if t == tab_id and p in pane_harness)
        if tab_panes:
            pid = tab_panes[0]
    if pid is None:
        # fall back to any tracked (non-shell) pane
        tab_panes = sorted(p for p, t in pane_tab.items() if t == tab_id)
        if tab_panes:
            pid = tab_panes[0]
    return pid


async def _route_to_pane(pid, tab_id, text, label="tg"):
    """Send text to pane (or all panes if collab). Returns True if routed."""
    if pid is None:
        tab = tab_topic_name.get(tab_id, "?")
        print(f"[{label}] no target in {tab}, dropped: '{text[:50]}' (no agent has spoken yet)")
        return False
    h_name = pane_harness.get(pid, "?")
    tab = tab_topic_name.get(tab_id, "?")
    snippet = text[:50].replace("\n", " ")
    if tab_id and tab_id in collab_tabs:
        targets = [p for p, t in pane_tab.items() if t == tab_id]
        for p in targets:
            h = pane_harness.get(p, "?")
            status = await send_and_verify(p, text)
            print(f"[{label}>{tab}/{h}] '{snippet}' {status}")
    else:
        status = await send_and_verify(pid, text)
        print(f"[{label}>{tab}/{h_name}] '{snippet}' {status}")
    return True




async def _handle_debate_message(m, sender="?"):
    """Route incoming debate chat message to pane(s)."""
    text = m.text
    if text.strip().startswith("/"):
        await _debate_handle_command(m)
        return

    tab_id = _find_tab(DEBATE_TABS)
    if tab_id is None:
        print(f"[debate] no matching tab, dropped: '{text[:50]}'")
        return

    reply_pid = debate_msg_pane.get(m.reply_to_message.message_id) if m.reply_to_message else None
    pid = _resolve_pid(tab_id, reply_pid)
    ts = f" [{_now_ts()}]" if MSG_TIMESTAMPS else ""
    prefixed = f"{sender}{ts} says: {text}"
    _set_tab_last_source(tab_id, "debate")
    await _route_to_pane(pid, tab_id, prefixed, "debate")


async def _debate_handle_command(m):
    """Handle /commands in the debate chat (currently none — just ignore)."""
    pass


# --- slack observer -------------------------------------------------------

async def _slack_receive_task():
    from slack_sdk.socket_mode.aiohttp import SocketModeClient as SMClient
    from slack_sdk.socket_mode.response import SocketModeResponse

    sm = SMClient(app_token=SLACK_APP_TOKEN, web_client=_slack_web_client)

    async def handler(client, req):
        if req.type == "events_api":
            event = req.payload.get("event", {})
            subtype = event.get("subtype")
            if (event.get("type") == "message"
                    and subtype in (None, "bot_message")
                    and event.get("channel") in SLACK_CHANNELS):
                text = event.get("text", "")
                user_id = event.get("user", "")
                channel = event.get("channel")
                bot_id = event.get("bot_id")
                ts = float(event.get("ts", 0))
                buf = _slack_msg_buffer.setdefault(channel, [])
                if bot_id and bot_id == _slack_bot_user_id:
                    pass  # skip our own output
                elif channel in SLACK_DIRECT:
                    if not bot_id:
                        _slack_direct_queue.append((text, user_id, channel))
                elif bot_id:
                    buf.append((ts, None, text))
                elif text.startswith("!obs"):
                    _slack_obs_queue.append((text[4:].strip(), user_id, channel))
                else:
                    buf.append((ts, user_id, text))
            await client.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id))

    sm.socket_mode_request_listeners.append(handler)
    await sm.connect()
    while True:
        await asyncio.sleep(60)


_slack_user_cache = {}     # user_id -> display_name
_slack_channel_cache = {}  # channel_id -> #channel_name


async def _resolve_slack_user(user_id):
    if user_id not in _slack_user_cache:
        try:
            info = await _slack_web_client.users_info(user=user_id)
            profile = info["user"]["profile"]
            _slack_user_cache[user_id] = (
                profile.get("display_name_normalized")
                or profile.get("real_name_normalized")
                or info["user"].get("real_name")
                or user_id
            )
        except Exception as e:
            print(f"[slack] users_info({user_id}) failed: {e}")
            _slack_user_cache[user_id] = user_id
    return _slack_user_cache[user_id]


async def _resolve_slack_channel(channel_id):
    if channel_id not in _slack_channel_cache:
        try:
            info = await _slack_web_client.conversations_info(channel=channel_id)
            _slack_channel_cache[channel_id] = f"#{info['channel']['name']}"
        except Exception:
            _slack_channel_cache[channel_id] = f"#{channel_id}"
    return _slack_channel_cache[channel_id]


async def _process_slack_queue():
    global _slack_reply_channel
    while _slack_obs_queue:
        extra, obs_user_id, channel = _slack_obs_queue.pop(0)
        tab_id = _find_tab(SLACK_TABS)
        if tab_id is None:
            print("[slack] no matching tab for !obs")
            continue
        # flush Slack message buffer → pane
        all_msgs = []
        for ch_id in list(_slack_msg_buffer):
            buf = _slack_msg_buffer.pop(ch_id)
            for ts, uid, text in buf:
                all_msgs.append((ts, ch_id, uid, text))
        all_msgs.sort(key=lambda x: x[0])
        parts = []
        from datetime import datetime
        for ts, ch_id, uid, text in all_msgs:
            ch_name = await _resolve_slack_channel(ch_id)
            t = datetime.fromtimestamp(ts).strftime("%H:%M") if ts else "??:??"
            if uid:
                name = await _resolve_slack_user(uid)
                parts.append(f"[slack: {ch_name} {t}] {name}: {text}")
            else:
                parts.append(f"[slack: {ch_name} {t}] {text}")
        if extra:
            ch_name = await _resolve_slack_channel(channel)
            name = await _resolve_slack_user(obs_user_id)
            t = datetime.now().strftime("%H:%M")
            parts.append(f"[slack: {ch_name} {t}] {name}: {extra}")
        if parts:
            payload = "\n".join(parts)
            pid = _resolve_pid(tab_id)
            await _route_to_pane(pid, tab_id, payload, "slack")
            _slack_reply_channel = channel
            print(f"[slack] flushed {len(parts)} input messages to pane ({channel})")


async def _process_slack_direct():
    while _slack_direct_queue:
        text, user_id, channel = _slack_direct_queue.pop(0)
        tab_pattern = SLACK_DIRECT.get(channel)
        if not tab_pattern:
            continue
        tab_id = _find_tab([tab_pattern])
        if tab_id is None:
            print(f"[slack-direct] no tab matching '{tab_pattern}', dropped: '{text[:50]}'")
            continue
        name = await _resolve_slack_user(user_id)
        ts = f" [{_now_ts()}]" if MSG_TIMESTAMPS else ""
        prefixed = f"{name}{ts} says: {text}"
        pid = _resolve_pid(tab_id)
        _set_tab_last_source(tab_id, "slack")
        _slack_direct_tab[tab_id] = channel
        await _route_to_pane(pid, tab_id, prefixed, "slack-direct")


async def on_message(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.text:
        return

    # === debate chat (open to all members) ===
    if DEBATE_ENABLED and m.chat_id == DEBATE_CHAT:
        u = update.effective_user
        if u:
            print(f"[debate] from {u.first_name} (uid={u.id})")
        sender = u.first_name if u else "?"
        await _handle_debate_message(m, sender)
        return

    # === forum topics (existing flow, requires owner + thread_id) ===
    if not _is_owner(update) or not m.message_thread_id:
        return

    reply_pid = msg_pane.get(m.reply_to_message.message_id) if m.reply_to_message else None
    tab_id = topic_tab.get(m.message_thread_id) if reply_pid is None else pane_tab.get(reply_pid)
    pid = _resolve_pid(tab_id, reply_pid) if tab_id is not None else reply_pid
    if tab_id is not None:
        _set_tab_last_source(tab_id, "tg")
    await _route_to_pane(pid, tab_id, m.text, "tg")



async def on_collab(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.message_thread_id or not _is_owner(update):
        return
    tab_id = topic_tab.get(m.message_thread_id)
    if tab_id is None:
        return

    if tab_id in collab_tabs:
        del collab_tabs[tab_id]
        collab_signoffs.pop(tab_id, None)
        _persist()
        await m.reply_text("collab off")
    else:
        # parse: /collab [rounds] [msg...]
        rounds = 0
        msg = ""
        rest = (m.text or "").split(None, 1)[1] if len((m.text or "").split()) > 1 else ""
        if rest:
            first_word = rest.split()[0]
            try:
                rounds = int(first_word)
                msg = rest.split(None, 1)[1] if len(rest.split()) > 1 else ""
            except ValueError:
                msg = rest
        collab_tabs[tab_id] = rounds
        collab_signoffs.pop(tab_id, None)
        _persist()
        label = f"collab on ({rounds} rounds)" if rounds else "collab on"
        await m.reply_text(label)
        if msg:
            tab = tab_topic_name.get(tab_id, "?")
            targets = [p for p, t in pane_tab.items() if t == tab_id and p in pane_harness]
            for p in targets:
                h = pane_harness.get(p, "?")
                status = await send_and_verify(p, msg)
                print(f"[collab>{tab}/{h}] '{msg[:50]}' {status}")


async def on_list(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    matched, unmatched = await discover()
    if not matched and not unmatched:
        await update.message.reply_text("no panes")
        return
    collab_tab_ids = set(collab_tabs.keys())
    lines = []
    for p, h_name in matched:
        tab = html.escape(p.get("tab_title", ""))
        title = html.escape(p.get("title", "?"))
        flag = " 🤝" if p["tab_id"] in collab_tab_ids else ""
        lines.append(f"<code>{h_name:6}</code> <b>{tab}</b> {title}{flag}")
    for p, _ in unmatched:
        tab = html.escape(p.get("tab_title", ""))
        title = html.escape(p.get("title", "?"))
        flag = " 🤝" if p["tab_id"] in collab_tab_ids else ""
        lines.append(f"<code>{'--':6}</code> <b>{tab}</b> {title}{flag}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def on_clear(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.message_thread_id or not _is_owner(update):
        return
    tid = m.message_thread_id
    tab_id = topic_tab.get(tid)
    if tab_id is None:
        return
    # get tab title for recreation
    matched, unmatched = await discover()
    name = f"tab-{tab_id}"
    for p, _ in matched + unmatched:
        if p["tab_id"] == tab_id:
            name = p.get("tab_title", name)[:128]
            break
    try:
        await _recreate_telegram_topic(tab_id, tid, name)
        # clear tracked messages and signoffs for this topic
        for mid in [k for k, v in msg_pane.items()
                    if pane_tab.get(v) == tab_id]:
            msg_pane.pop(mid, None)
        collab_signoffs.pop(tab_id, None)
        # re-seek so we don't replay old output
        next_cursors = dict(_source_cursors)
        for pid, tab in pane_tab.items():
            if tab == tab_id:
                _seek_to_end(pid, next_cursors)
        _replace_delivery_state(_pending_sends, next_cursors)
        print(f"[clear] recreated topic for tab {tab_id}")
    except Exception as e:
        print(f"clear: {e}")


# --- signal handlers -------------------------------------------------------


def _format_signal_input(text, data, sender, envelope_timestamp=None):
    quote = data.get("quote", {})
    short_ts = _ts_from_millis(envelope_timestamp) or _now_ts()
    ts = f" [{short_ts}]" if short_ts else ""
    quote_text = quote.get("text", "") if quote else ""
    reply_ctx = f" (replying to: {quote_text[:100]})" if quote_text else ""
    return f"{sender}{ts} says{reply_ctx}: {text}"


async def _flush_signal_history(group_id, tab_id, reply_pid=None):
    """Route all undelivered archived messages. Returns (message_count, routed)."""
    pending = _signal_db_pending(group_id)
    message_count = len(pending)
    if not pending:
        return 0, True

    messages = [row["formatted_text"] for row in pending]
    if len(messages) > 1:
        payload = "\n\n".join([
            "Signal chat history since the last delivery:",
            *messages,
        ])
    else:
        payload = messages[0]

    pid = _resolve_pid(tab_id, reply_pid)
    if tab_id is not None:
        _set_tab_last_source(tab_id, "sig")
    routed = await _route_to_pane(pid, tab_id, payload, "sig")
    if routed:
        _signal_db_mark_delivered([row["id"] for row in pending])
    return message_count, routed


async def _flush_ready_signal_history():
    """Replay archived input that arrived before its group mapping was ready."""
    for group_id, tab_id in sig_group_tab.items():
        pending = _signal_db_pending(group_id)
        if not pending:
            continue
        if (
            group_id in clod_off_groups
            and not any(row["is_mention"] for row in pending)
        ):
            continue
        await _flush_signal_history(group_id, tab_id)


async def _signal_handle_command(text, group_id, tab_id):
    cmd = text.strip().split()[0].lower()
    if cmd == "/list":
        matched, unmatched = await discover()
        if not matched and not unmatched:
            await _signal_client.send_message(group_id, "no panes")
            return
        collab_tab_ids = set(collab_tabs.keys())
        lines = []
        for p, h_name in matched:
            tab = p.get("tab_title", "")
            title = p.get("title", "?")
            flag = " collab" if p["tab_id"] in collab_tab_ids else ""
            lines.append(f"[{h_name}] {tab} — {title}{flag}")
        for p, _ in unmatched:
            tab = p.get("tab_title", "")
            title = p.get("title", "?")
            flag = " collab" if p["tab_id"] in collab_tab_ids else ""
            lines.append(f"[--] {tab} — {title}{flag}")
        await _signal_client.send_message(group_id, "\n".join(lines))

    elif cmd == "/collab":
        if tab_id is None:
            return
        if tab_id in collab_tabs:
            del collab_tabs[tab_id]
            collab_signoffs.pop(tab_id, None)
            _persist()
            await _signal_client.send_message(group_id, "collab off")
        else:
            rounds = 0
            msg = ""
            rest = text.strip().split(None, 1)[1] if len(text.strip().split()) > 1 else ""
            if rest:
                first_word = rest.split()[0]
                try:
                    rounds = int(first_word)
                    msg = rest.split(None, 1)[1] if len(rest.split()) > 1 else ""
                except ValueError:
                    msg = rest
            collab_tabs[tab_id] = rounds
            collab_signoffs.pop(tab_id, None)
            _persist()
            label = f"collab on ({rounds} rounds)" if rounds else "collab on"
            await _signal_client.send_message(group_id, label)
            if msg:
                tab = tab_topic_name.get(tab_id, "?")
                targets = [p for p, t in pane_tab.items() if t == tab_id and p in pane_harness]
                for p in targets:
                    h = pane_harness.get(p, "?")
                    status = await send_and_verify(p, msg)
                    print(f"[collab>{tab}/{h}] '{msg[:50]}' {status}")

    elif cmd == "/crosspost":
        global debate_crosspost
        debate_crosspost = not debate_crosspost
        state = "on" if debate_crosspost else "off"
        await _signal_client.send_message(group_id, f"crosspost {state}")

    elif cmd == "/clodoff":
        clod_off_groups.add(_normalize_signal_group_id(group_id))
        _persist()
        await _signal_client.send_message(
            group_id,
            "clod off — buffering messages; @clod or /clodon will deliver them",
        )

    elif cmd == "/clodon":
        normalized_group_id = _normalize_signal_group_id(group_id)
        clod_off_groups.discard(normalized_group_id)
        _persist()
        count, routed = await _flush_signal_history(normalized_group_id, tab_id)
        if count and routed:
            status = f"clod on — delivered {count} buffered message(s)"
        elif count:
            status = f"clod on — {count} buffered message(s) waiting for an agent pane"
        else:
            status = "clod on — listening again"
        await _signal_client.send_message(group_id, status)

    elif cmd == "/invite":
        args = text.strip().split()[1:]
        if not args:
            await _signal_client.send_message(group_id, "usage: /invite +1234567890")
            return
        try:
            result = await _signal_client.add_members(group_id, args)
            await _signal_client.send_message(group_id, f"invited {', '.join(args)}")
        except Exception as e:
            await _signal_client.send_message(group_id, f"invite error: {e}")

    elif cmd == "/kick":
        args = text.strip().split()[1:]
        if not args:
            await _signal_client.send_message(group_id, "usage: /kick +1234567890")
            return
        try:
            await _signal_client.remove_members(group_id, args)
            await _signal_client.send_message(group_id, f"removed {', '.join(args)}")
        except Exception as e:
            await _signal_client.send_message(group_id, f"kick error: {e}")

    elif cmd == "/newgroup":
        args = text.strip().split()[1:]
        if len(args) < 2:
            await _signal_client.send_message(group_id, "usage: /newgroup <name> <+number> [+number...]")
            return
        name = args[0]
        members = args[1:]
        try:
            result = await _signal_client.create_group(name, members)
            gid = result.get("groupId") if isinstance(result, dict) else result
            await _signal_client.send_message(group_id, f"created '{name}' ({gid})")
        except Exception as e:
            await _signal_client.send_message(group_id, f"newgroup error: {e}")

    elif cmd == "/refresh":
        if tab_id is None:
            return
        old_gid = sig_tab_group.get(tab_id)
        if not old_gid:
            return
        matched, unmatched = await discover()
        name = f"tab-{tab_id}"
        for p, _ in matched + unmatched:
            if p["tab_id"] == tab_id:
                name = p.get("tab_title", name)[:128]
                break
        try:
            # create new group first, then try to leave old (best-effort)
            result = await _signal_client.create_group(name, _sig_members_for(name))
            new_gid = result.get("groupId") if isinstance(result, dict) else result
            if not new_gid:
                raise RuntimeError(f"refresh create_group returned no group id: {result}")
            old_gid_normalized = _normalize_signal_group_id(old_gid)
            new_gid_normalized = _normalize_signal_group_id(new_gid)
            was_muted = old_gid_normalized in clod_off_groups
            if was_muted:
                clod_off_groups.discard(old_gid_normalized)
                clod_off_groups.add(new_gid_normalized)
            _signal_db_move_pending(old_gid_normalized, new_gid_normalized)
            sig_name_group[name.strip().lower()] = new_gid
            sig_tab_group[tab_id] = new_gid
            sig_tab_name[tab_id] = name
            _rebuild()
            _persist()
            await _signal_client.send_message(new_gid, "refreshed")
            _sig_greeted.add(new_gid)
            # clear tracked signal messages for this tab
            for ts in [k for k, v in sig_msg_pane.items()
                       if pane_tab.get(v) == tab_id]:
                sig_msg_pane.pop(ts, None)
            collab_signoffs.pop(tab_id, None)
            next_cursors = dict(_source_cursors)
            for pid, tab in pane_tab.items():
                if tab == tab_id:
                    _seek_to_end(pid, next_cursors)
            _replace_delivery_state(_pending_sends, next_cursors)
            print(f"[signal] refreshed group for tab {tab_id} -> {new_gid}")
            # leave old group (best-effort — fails if bot is last admin)
            try:
                await _signal_client.leave_group(old_gid)
            except Exception:
                pass
        except Exception as e:
            print(f"[signal] refresh error: {e}")


_sig_seen_ts = set()  # dedup: envelope timestamps we've already processed


async def _on_signal_message(notification):
    params = notification.get("params", {})
    # notifications come in two formats:
    # direct: {"params": {"envelope": {...}}}
    # wrapped: {"params": {"subscription": N, "result": {"envelope": {...}}}}
    envelope = params.get("envelope") or params.get("result", {}).get("envelope", {})

    source = (envelope.get("sourceNumber")
              or envelope.get("sourceUuid")
              or envelope.get("source") or "")

    # dedup by sender + envelope timestamp (signal-cli may deliver duplicates)
    env_ts = envelope.get("timestamp")
    if env_ts:
        seen_key = (source, env_ts)
        if seen_key in _sig_seen_ts:
            return
        _sig_seen_ts.add(seen_key)
        # keep set bounded
        if len(_sig_seen_ts) > 200:
            _sig_seen_ts.clear()

    data = envelope.get("dataMessage", {})
    if not data:
        return
    msg_text = data.get("message") or ""
    text = msg_text
    atts = data.get("attachments") or []
    if atts:
        print(f"[signal] attachments: {atts}")
    for att in atts:
        att_id = att.get("id", "")
        if att_id:
            att_path = Path.home() / ".local/share/signal-cli/attachments" / att_id
            ct = att.get("contentType", "")
            text = text + f"\n[attached {ct}: {att_path}]"
        else:
            print(f"[signal] attachment without id: {att}")
    text = text.strip()
    group_info = data.get("groupInfo", {})
    group_id = _normalize_signal_group_id(group_info.get("groupId", ""))

    source_name = envelope.get("sourceName") or source[:12]
    source_number = envelope.get("sourceNumber", "")
    all_members = set(SIGNAL_MEMBERS)
    for m in SIGNAL_TAB_MEMBERS.values():
        all_members.update(m)
    if not source_number:
        print(f"[signal] no phone number for {source_name} ({source})")
    accepted = not (
        source != SIGNAL_OWNER
        and source not in SIGNAL_ALLOWED
        and source_number not in all_members
    )
    is_command = msg_text.strip().startswith("/")
    is_mention = _is_clod_mention(text, data)
    formatted = (
        _format_signal_input(text, data, source_name, env_ts) if text else ""
    )
    conversation_id = group_id or f"direct:{source or 'unknown'}"
    _row_id, inserted, should_process = _signal_db_archive(
        group_id=conversation_id,
        envelope_timestamp=env_ts,
        sender_id=source,
        sender_number=source_number,
        sender_name=source_name,
        text=text,
        formatted_text=formatted,
        data=data,
        accepted=accepted,
        is_command=is_command,
        is_mention=is_mention,
    )

    if not accepted:
        print(f"[signal] ignoring message from {source_name} ({source})")
        return
    if not group_id:
        print(f"[signal] archived direct message from {source_name}")
        return

    tab_id = sig_group_tab.get(group_id)
    if tab_id is None:
        print(f"[signal] unknown group from {source_name}, id={group_id[:20]}...")

    # queue for processing in poll_loop (avoid _call deadlock)
    if is_command and inserted:
        _signal_cmd_queue.append((msg_text, group_id, tab_id))
    elif should_process and tab_id is not None:
        _signal_input_queue.append((data, tab_id, group_id, is_mention))


def _is_clod_mention(text, data):
    """True if the message directly @-mentions Clod, so it wakes me even when muted.
    Matches a literal '@clod' in the text, or a Signal @-mention (rendered as the
    ￼ placeholder) whose target resolves to this bridge's own account."""
    if re.search(r"(?<!\w)@clod\b", text or "", re.IGNORECASE):
        return True
    for men in (data.get("mentions") or []):
        if SIGNAL_ACCOUNT and SIGNAL_ACCOUNT in (
            str(men.get("number") or ""),
            str(men.get("uuid") or ""),
            str(men.get("name") or ""),
        ):
            return True
    return False


async def _process_signal_queues():
    """Process queued Signal commands and input (called from poll_loop)."""
    # commands
    while _signal_cmd_queue:
        text, group_id, tab_id = _signal_cmd_queue.pop(0)
        await _signal_handle_command(text, group_id, tab_id)

    # input routing
    while _signal_input_queue:
        data, tab_id, group_id, is_mention = _signal_input_queue.pop(0)
        is_muted = group_id in clod_off_groups
        if is_muted and not is_mention:
            continue

        quote = data.get("quote", {})
        reply_pid = None
        if quote:
            quote_ts = quote.get("id")
            if quote_ts:
                reply_pid = sig_msg_pane.get(quote_ts)
        await _flush_signal_history(group_id, tab_id, reply_pid=reply_pid)


async def _signal_receive_task():
    while True:
        try:
            global _signal_client
            if not _signal_client:
                _signal_client = SignalClient(SIGNAL_SOCKET)
            await _signal_client.connect()
            print(f"[signal] connected to {SIGNAL_SOCKET}")
            await _signal_client.receive_loop(_on_signal_message)
        except Exception as e:
            print(f"[signal] disconnected: {e}, reconnecting...")
            try:
                await _signal_client.close()
            except Exception:
                pass
            await asyncio.sleep(5)


# --- local control interface ----------------------------------------------


def _control_route(name):
    key = _telegram_topic_key(name)
    matches = [
        (tab_id, title)
        for tab_id, title in tab_topic_name.items()
        if _telegram_topic_key(title) == key
    ]
    if not matches:
        raise RequestFailure(
            "route_not_found", f"no live Panetone route matches {name!r}"
        )
    if len(matches) != 1:
        raise RequestFailure(
            "route_ambiguous", f"more than one live Panetone route matches {name!r}"
        )
    tab_id, title = matches[0]
    pid = _resolve_pid(tab_id)
    h_name = pane_harness.get(pid)
    topic_id = tab_topic.get(tab_id)
    if pid is None or not h_name:
        raise RequestFailure(
            "route_unavailable", f"route {title!r} has no live agent pane"
        )
    if not topic_id:
        raise RequestFailure(
            "route_unavailable", f"route {title!r} has no live Telegram topic"
        )
    return {
        "title": title,
        "tab_id": tab_id,
        "pane_id": pid,
        "harness": h_name,
        "topic_id": topic_id,
    }


def _utf16_chunks(text, limit=3900):
    """Split text below Telegram's UTF-16 message limit."""
    chunk = []
    units = 0
    for char in text:
        char_units = 2 if ord(char) > 0xFFFF else 1
        if chunk and units + char_units > limit:
            yield "".join(chunk)
            chunk = []
            units = 0
        chunk.append(char)
        units += char_units
    if chunk:
        yield "".join(chunk)


async def _control_failure_annotation(
    bot, topic_id, first_message_id, first_text, request_id, reason, target_pid
):
    marker = (
        "DELIVERY FAILED\n"
        f"Request: {request_id}\n"
        f"{reason}\n"
        "Panetone did not retry the target prompt."
    )
    try:
        sent = await bot.send_message(
            CHAT,
            marker,
            message_thread_id=topic_id,
            reply_to_message_id=first_message_id,
        )
        msg_pane[sent.message_id] = target_pid
        return {"kind": "reply", "message_id": sent.message_id}
    except Exception as reply_error:
        replacement = next(_utf16_chunks(f"{marker}\n\n{first_text}"))
        try:
            await bot.edit_message_text(
                replacement,
                chat_id=CHAT,
                message_id=first_message_id,
            )
            return {"kind": "edited", "message_id": first_message_id}
        except Exception as edit_error:
            return {
                "kind": "failed",
                "reply_error": str(reply_error),
                "edit_error": str(edit_error),
            }


async def _control_success_annotation(
    bot, topic_id, first_message_id, first_text, request_id, target_pid
):
    submitted_text = first_text.replace("[pending]", "[submitted]", 1)
    try:
        await bot.edit_message_text(
            submitted_text,
            chat_id=CHAT,
            message_id=first_message_id,
        )
        return {"kind": "edited", "message_id": first_message_id}
    except Exception as edit_error:
        marker = f"DELIVERY SUBMITTED\nRequest: {request_id}"
        try:
            sent = await bot.send_message(
                CHAT,
                marker,
                message_thread_id=topic_id,
                reply_to_message_id=first_message_id,
            )
            msg_pane[sent.message_id] = target_pid
            return {"kind": "reply", "message_id": sent.message_id}
        except Exception as reply_error:
            return {
                "kind": "failed",
                "edit_error": str(edit_error),
                "reply_error": str(reply_error),
            }


def _control_prompt_envelope(request_id, source, target, message, return_final):
    reply_mode = "asynchronous final callback" if return_final else "one-way"
    return (
        "[Panetone cross-agent message]\n"
        f"From: {source['title']} ({source['harness']})\n"
        f"To: {target['title']} ({target['harness']})\n"
        f"Request ID: {request_id}\n"
        f"Reply mode: {reply_mode}\n\n"
        f"{message}"
    )


async def _handle_control_send(request, transition, journal=None):
    params = request["params"]
    request_id = request["id"]
    return_final = params.get("return_final", False)
    if return_final and not _wakterm_return_supported:
        raise RequestFailure(
            "return_final_unavailable",
            "Wakterm does not support durable return requests; no delivery was attempted",
            details={"reason": _wakterm_return_unavailable_reason},
        )
    await _refresh_telegram_routes()
    source = _control_route(params["from"])
    target = _control_route(params["to"])
    if return_final:
        try:
            agents = await asyncio.to_thread(_wakterm_agent_catalog_sync)
            source = _bind_route_agent(source, agents)
            target = _bind_route_agent(target, agents)
        except Exception as error:
            raise RequestFailure(
                "return_final_unavailable",
                "Wakterm could not bind the return request to exact live agents; "
                "no delivery was attempted",
                details={"reason": str(error)},
            ) from error
    delivered_prompt = _control_prompt_envelope(
        request_id,
        source,
        target,
        params["message"],
        return_final,
    )
    source_harness = harnesses.get(source["harness"])
    bot = source_harness.bot if source_harness else _primary_bot
    if not bot:
        raise RequestFailure(
            "telegram_unavailable", "no Telegram bot is available for the source route"
        )

    audit_text = (
        f"{source['title']} \u2192 {target['title']}:\n"
        f"Request: {request_id} [pending]\n"
        f"{params['message']}"
    )
    audit_ids = []
    first_text = ""
    for index, chunk in enumerate(_utf16_chunks(audit_text)):
        if index == 0:
            first_text = chunk
        try:
            kwargs = {}
            if audit_ids:
                kwargs["reply_to_message_id"] = audit_ids[0]
            sent = await bot.send_message(
                CHAT,
                chunk,
                message_thread_id=target["topic_id"],
                **kwargs,
            )
        except Exception as error:
            annotation = None
            if audit_ids:
                annotation = await _control_failure_annotation(
                    bot,
                    target["topic_id"],
                    audit_ids[0],
                    first_text,
                    request_id,
                    "The Telegram audit message was incomplete, so the target prompt was not sent.",
                    target["pane_id"],
                )
            raise RequestFailure(
                "telegram_audit_failed",
                "Telegram audit delivery failed; the target prompt was not sent",
                details={
                    "error": str(error),
                    "message_ids": audit_ids,
                    "failure_annotation": annotation,
                },
            ) from error
        audit_ids.append(sent.message_id)
        msg_pane[sent.message_id] = target["pane_id"]
        try:
            await transition(
                "audit_posted",
                {
                    "source": source["title"],
                    "target": target["title"],
                    "topic_id": target["topic_id"],
                    "message_ids": audit_ids,
                },
            )
        except Exception as error:
            annotation = await _control_failure_annotation(
                bot,
                target["topic_id"],
                audit_ids[0],
                first_text,
                request_id,
                "Panetone could not durably record the audit, so the target prompt was not sent.",
                target["pane_id"],
            )
            raise RequestFailure(
                "journal_update_failed",
                "the audit succeeded but its durable state could not be recorded",
                details={"failure_annotation": annotation, "error": str(error)},
                indeterminate=True,
            ) from error

    try:
        if return_final:
            if journal is None:
                raise RuntimeError("durable return journal is unavailable")
            await asyncio.to_thread(
                journal.register_return_route, request_id, source, target
            )
        _set_tab_last_source(target["tab_id"], "tg")
        await transition(
            "delivering",
            {
                "source": source["title"],
                "target": target["title"],
                "target_pane_id": target["pane_id"],
                "topic_id": target["topic_id"],
                "message_ids": audit_ids,
            },
        )
    except Exception as error:
        annotation = await _control_failure_annotation(
            bot,
            target["topic_id"],
            audit_ids[0],
            first_text,
            request_id,
            "Panetone could not prepare durable delivery state, so the target prompt was not sent.",
            target["pane_id"],
        )
        raise RequestFailure(
            "delivery_state_failed",
            "the audit succeeded but delivery state could not be prepared",
            details={"failure_annotation": annotation, "error": str(error)},
            indeterminate=True,
        ) from error

    try:
        if return_final:
            wakterm_receipt = await agent_send(
                target["pane_id"],
                delivered_prompt,
                return_final=True,
                request_id=request_id,
                timeout_ms=params.get("timeout_ms", 0),
            )
        else:
            wakterm_receipt = await agent_send(target["pane_id"], delivered_prompt)
    except Exception as error:
        annotation = await _control_failure_annotation(
            bot,
            target["topic_id"],
            audit_ids[0],
            first_text,
            request_id,
            "Wakterm delivery failed or became indeterminate.",
            target["pane_id"],
        )
        if return_final and journal is not None:
            terminal = {
                "request_id": request_id,
                "state": "delivery_failed",
                "final_message": None,
                "detail": str(error),
                "terminal_event_sequence": None,
            }
            await asyncio.to_thread(
                journal.record_return_result, request_id, terminal
            )
            await _deliver_return(request_id, journal)
        raise RequestFailure(
            "wakterm_delivery_indeterminate",
            "the audit succeeded but Wakterm delivery failed or became indeterminate",
            details={
                "error": str(error),
                "telegram": {
                    "chat_id": CHAT,
                    "topic_id": target["topic_id"],
                    "message_ids": audit_ids,
                    "failure_annotation": annotation,
                },
            },
            indeterminate=True,
        ) from error

    status_annotation = await _control_success_annotation(
        bot,
        target["topic_id"],
        audit_ids[0],
        first_text,
        request_id,
        target["pane_id"],
    )
    print(
        f"[control>{source['title']}/{target['title']}] "
        f"request {request_id} submitted via Wakterm to pane {target['pane_id']}"
    )
    return {
        "source": source,
        "target": target,
        "telegram": {
            "chat_id": CHAT,
            "topic_id": target["topic_id"],
            "message_ids": audit_ids,
            "status_annotation": status_annotation,
        },
        "wakterm": wakterm_receipt,
        "reply_mode": "return_final" if return_final else "one_way",
        "reply_pending": return_final,
    }


def _return_callback_text(result, source, target):
    request_id = result["request_id"]
    state = result.get("state", "indeterminate")
    message = result.get("final_message")
    header = (
        f"Panetone return for request {request_id}\n"
        f"From: {target['title']}\n"
        f"Status: {state}"
    )
    if message:
        return f"{header}\n\n{message}"
    detail = result.get("detail") or "No final assistant message was available."
    return f"{header}\n\n{detail}"


async def _deliver_return(request_id, journal):
    async with _return_delivery_lock:
        row = await asyncio.to_thread(journal.get_return, request_id)
        if not row or not row["result_json"]:
            return
        result = json.loads(row["result_json"])
        source = json.loads(row["source_json"])
        target = json.loads(row["target_json"])
        callback = _return_callback_text(result, source, target)

        if row["agent_state"] == "pending":
            callback_request_id = _return_callback_request_id(request_id)
            await asyncio.to_thread(
                journal.set_return_destination,
                request_id,
                "agent",
                "delivering",
            )
            try:
                if not source.get("agent_id") or not source.get("incarnation_id"):
                    raise RuntimeError(
                        "the persisted source route lacks an exact Wakterm agent incarnation"
                    )
                receipt = await agent_admit(
                    source,
                    callback,
                    request_id=callback_request_id,
                )
            except Exception as error:
                await asyncio.to_thread(
                    journal.set_return_destination,
                    request_id,
                    "agent",
                    "indeterminate",
                    str(error),
                )
                print(f"[control>{source['title']}] agent callback indeterminate: {error}")
            else:
                status = receipt["status"]
                if status == "accepted":
                    await asyncio.to_thread(
                        journal.set_return_destination,
                        request_id,
                        "agent",
                        "delivered",
                    )
                elif status == "busy":
                    await asyncio.to_thread(
                        journal.set_return_destination,
                        request_id,
                        "agent",
                        "pending",
                    )
                    print(
                        f"[control>{source['title']}] source busy; callback remains queued"
                    )
                elif status == "indeterminate":
                    await asyncio.to_thread(
                        journal.set_return_destination,
                        request_id,
                        "agent",
                        "indeterminate",
                        receipt.get("detail"),
                    )
                else:
                    await asyncio.to_thread(
                        journal.set_return_destination,
                        request_id,
                        "agent",
                        "failed",
                        receipt.get("detail") or f"Wakterm admission returned {status}",
                    )

        row = await asyncio.to_thread(journal.get_return, request_id)
        if row["telegram_state"] == "pending":
            await asyncio.to_thread(
                journal.set_return_destination,
                request_id,
                "telegram",
                "delivering",
            )
            try:
                source_harness = harnesses.get(source["harness"])
                bot = source_harness.bot if source_harness else _primary_bot
                if not bot:
                    raise RuntimeError("no Telegram bot is available for the source route")
                first_message_id = None
                for chunk in _utf16_chunks(callback):
                    kwargs = {}
                    if first_message_id:
                        kwargs["reply_to_message_id"] = first_message_id
                    sent = await bot.send_message(
                        CHAT,
                        chunk,
                        message_thread_id=source["topic_id"],
                        **kwargs,
                    )
                    first_message_id = first_message_id or sent.message_id
                    msg_pane[sent.message_id] = source["pane_id"]
            except Exception as error:
                await asyncio.to_thread(
                    journal.set_return_destination,
                    request_id,
                    "telegram",
                    "indeterminate",
                    str(error),
                )
                print(f"[control>{source['title']}] Telegram callback indeterminate: {error}")
            else:
                await asyncio.to_thread(
                    journal.set_return_destination,
                    request_id,
                    "telegram",
                    "delivered",
                )


async def _handle_wakterm_return_event(result, journal):
    sequence = result.get("terminal_event_sequence")
    request_id = result.get("request_id")
    if not isinstance(sequence, int) or not isinstance(request_id, str):
        raise RuntimeError("Wakterm returned an invalid terminal request event")
    route = await asyncio.to_thread(journal.get_return, request_id)
    if route:
        await asyncio.to_thread(journal.record_return_result, request_id, result)
        await _deliver_return(request_id, journal)
    await asyncio.to_thread(journal.set_event_cursor, sequence)


async def _wakterm_return_watch_loop(journal):
    retry_delay = 2
    while True:
        cursor = await asyncio.to_thread(journal.event_cursor)
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                WAKTERM_BIN,
                "cli",
                "agent",
                "request",
                "watch",
                "--after",
                str(cursor),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            while line := await process.stdout.readline():
                result = json.loads(line)
                await _handle_wakterm_return_event(result, journal)
                retry_delay = 2
            stderr = (await process.stderr.read()).decode(errors="replace").strip()
            await process.wait()
            supported, reason = await asyncio.to_thread(
                _wakterm_return_capability_sync
            )
            if not supported:
                print(
                    "[control] Wakterm return capability became unavailable; "
                    f"watcher disabled until restart: {reason}"
                )
                return
            detail = stderr or f"exit status {process.returncode}"
            print(
                f"[control] Wakterm return stream closed: {detail}; "
                f"retrying in {retry_delay}s"
            )
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                process.terminate()
                await process.wait()
            raise
        except Exception as error:
            print(
                f"[control] Wakterm return stream error: {error}; "
                f"retrying in {retry_delay}s"
            )
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 300)


async def _pending_return_delivery_loop(journal):
    while True:
        try:
            rows = await asyncio.to_thread(journal.pending_returns)
            for row in rows:
                await _deliver_return(row["request_id"], journal)
        except Exception as error:
            print(f"[control] pending return delivery error: {error}")
        await asyncio.sleep(5)


# --- lifecycle -------------------------------------------------------------

_return_delivery_lock = None
_wakterm_return_supported = False
_wakterm_return_unavailable_reason = "capability has not been checked"
_background_tasks = set()


def _start_background_task(coro, name):
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def finished(done):
        _background_tasks.discard(done)
        if not done.cancelled() and (error := done.exception()) is not None:
            print(f"[task] {done.get_name()} stopped unexpectedly: {error}")

    task.add_done_callback(finished)
    return task


async def poll_loop():
    while True:
        try:
            matched, unmatched = await _refresh_telegram_routes()
            if SIGNAL_ENABLED:
                await sync_signal_groups(matched, unmatched)
                await _process_signal_queues()
            if SLACK_ENABLED:
                await _process_slack_queue()
                await _process_slack_direct()
            await check_output()
        except Exception as e:
            print(f"tick: {e}")
        await asyncio.sleep(POLL)


async def startup(app: Application):
    global _primary_bot, _control_server, _return_delivery_lock
    global _wakterm_return_supported, _wakterm_return_unavailable_reason
    _init_harnesses()
    _signal_db_init()
    _primary_bot = harnesses["claude"].bot

    # fetch bot display names
    for h in harnesses.values():
        try:
            me = await h.bot.get_me()
            h.display_name = me.first_name
        except Exception:
            pass

    saved = _load()
    for title, topic_id in saved.get("telegram_topics", {}).items():
        key = _telegram_topic_key(title)
        if key and topic_id:
            tg_name_topic[key] = int(topic_id)
    for title, source in saved.get("last_sources", {}).items():
        key = _telegram_topic_key(title)
        if key and source in {"tg", "sig", "debate", "slack"}:
            last_source_name[key] = source
    for k, v in saved.get("collab", {}).items():
        collab_tabs[int(k)] = v
    for gid in saved.get("clod_off_groups", []):
        normalized = _normalize_signal_group_id(gid)
        if normalized:
            clod_off_groups.add(normalized)
    legacy_history = saved.get("clod_history", {})
    if legacy_history:
        _signal_db_migrate_legacy_history(legacy_history)
    for k in saved.get("clod_off", []):
        _legacy_clod_off_tabs.add(int(k))
    _sig_names = saved.get("signal_group_names", {})  # legacy: tab_id -> title
    for k, v in saved.get("signal_groups", {}).items():
        gid = _normalize_signal_group_id(v)
        if not gid:
            continue
        # new format keys by title; legacy format keys by tab_id (numeric),
        # in which case recover the title from the old signal_group_names map.
        if str(k).lstrip("-").isdigit():
            key = str(_sig_names.get(str(k), "")).strip().lower()
        else:
            key = str(k).strip().lower()
        if key and not re.match(r"^tab-\d+$", key):
            sig_name_group.setdefault(key, gid)
    resumed_delivery_state = _load_pending()
    _rebuild()
    if tg_name_topic:
        print(f"[tg] loaded {len(tg_name_topic)} title-keyed topic(s)")
    if sig_name_group:
        print(f"[signal] loaded {len(sig_name_group)} group(s) from state: "
              f"{sorted(sig_name_group)}")
    if legacy_history:
        _persist()
        print("[signal] migrated JSON mute backlog to Signal database")

    # start signal
    if SIGNAL_ENABLED:
        # set profile name via a one-shot connection
        try:
            _r, _w = await asyncio.open_unix_connection(SIGNAL_SOCKET)
            req = json.dumps({"jsonrpc": "2.0", "id": 1,
                              "method": "updateProfile",
                              "params": {"givenName": "Debater",
                                         "account": SIGNAL_ACCOUNT}}) + "\n"
            _w.write(req.encode())
            await _w.drain()
            await asyncio.wait_for(_r.readline(), timeout=5)
            _w.close()
            await _w.wait_closed()
            print("[signal] profile set to Debater")
        except Exception as e:
            print(f"[signal] profile set error: {e}")
        _start_background_task(_signal_receive_task(), "signal-receive")

    if SLACK_ENABLED:
        from slack_sdk.web.async_client import AsyncWebClient
        global _slack_web_client, _slack_bot_user_id
        _slack_web_client = AsyncWebClient(token=SLACK_BOT_TOKEN)
        try:
            auth = await _slack_web_client.auth_test()
            _slack_bot_user_id = auth["bot_id"]
        except Exception as e:
            print(f"[slack] auth_test failed: {e}")
        _start_background_task(_slack_receive_task(), "slack-receive")

    matched, unmatched = await _refresh_telegram_routes()
    # Resolve routes before reading sources. Durable cursors resume exactly;
    # a genuinely new session starts at record zero so its first reply cannot
    # be skipped if it was completed between discovery polls.
    if not resumed_delivery_state:
        initial_cursors = {}
        for p, _h_name in matched:
            _seek_to_end(p["pane_id"], initial_cursors)
        _replace_delivery_state([], initial_cursors)

    # print tab summary
    tabs = {}
    for p, h_name in matched:
        tabs.setdefault(p["tab_id"], []).append((p, h_name))
    for p, _ in unmatched:
        tabs.setdefault(p["tab_id"], []).append((p, "--"))
    for tab_id in sorted(tabs):
        panes = tabs[tab_id]
        title = panes[0][0].get("tab_title", f"tab-{tab_id}")
        parts = " ".join(f"{h}:{p['pane_id']}" for p, h in panes)
        print(f"  {title}: {parts}")

    journal = ControlJournal(CONTROL_JOURNAL)
    await asyncio.to_thread(journal.initialize)
    _return_delivery_lock = asyncio.Lock()
    dispatcher = DurableDispatcher(
        journal, lambda request, transition: _handle_control_send(
            request, transition, journal
        )
    )
    _control_server = ControlServer(CONTROL_SOCKET, dispatcher)
    await _control_server.start()
    print(f"[control] listening on {CONTROL_SOCKET}")

    (
        _wakterm_return_supported,
        _wakterm_return_unavailable_reason,
    ) = await asyncio.to_thread(_wakterm_return_capability_sync)
    if _wakterm_return_supported:
        print("[control] Wakterm durable return requests enabled")
        _start_background_task(
            _wakterm_return_watch_loop(journal), "wakterm-return-watch"
        )
    else:
        print(
            "[control] Wakterm durable return requests unavailable; "
            f"--return-final disabled: {_wakterm_return_unavailable_reason}"
        )
    _start_background_task(
        _pending_return_delivery_loop(journal), "pending-return-delivery"
    )
    _start_background_task(poll_loop(), "poll")


async def shutdown(_app: Application):
    global _control_server
    if _control_server:
        await _control_server.close()
        _control_server = None
    tasks = list(_background_tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _background_tasks.clear()


def main():
    _init_harnesses()
    app = (
        Application.builder()
        .token(CLAUDE_TOKEN)
        .post_init(startup)
        .post_shutdown(shutdown)
        .build()
    )
    app.add_handler(CommandHandler("list", on_list))
    app.add_handler(CommandHandler("collab", on_collab))
    app.add_handler(CommandHandler("refresh", on_clear))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    names = ", ".join(harnesses.keys())
    sig = f" +signal({SIGNAL_ACCOUNT})" if SIGNAL_ENABLED else ""
    deb = f" +debate({DEBATE_CHAT})" if DEBATE_ENABLED else ""
    slk = f" +slack({','.join(SLACK_CHANNELS)})" if SLACK_ENABLED else ""
    print(f"panetone: [{names}] polling chat {CHAT} every {POLL}s{sig}{deb}{slk}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
