#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0", "slack-sdk>=3.0", "aiohttp"]
# ///
"""panetone: wezterm <> telegram/signal bridge for multiple AI coding agents

Each wezterm tab gets one Telegram forum topic (by tab_title) and/or
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
"""

import asyncio
import html
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from telegram import Bot, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
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
SLACK_ENABLED = bool(SLACK_BOT_TOKEN and SLACK_APP_TOKEN and SLACK_CHANNELS)

MSG_TZ = os.environ.get("WEZ_MSG_TZ", "America/New_York")
MSG_TIMESTAMPS = os.environ.get("WEZ_MSG_TIMESTAMPS", "1") != "0"

# --- wezterm cli -----------------------------------------------------------

import shutil

def _find_wezterm():
    """Resolve wezterm binary: WEZTERM_BIN > known paths > PATH lookup."""
    from_env = os.environ.get("WEZTERM_BIN", "")
    if from_env:
        return from_env
    for p in (Path.home() / ".local/bin/wezterm", Path("/usr/local/bin/wezterm")):
        if p.exists():
            return str(p)
    return shutil.which("wezterm") or "wezterm"

WEZTERM_BIN = _find_wezterm()
print(f"[wezterm] binary: {WEZTERM_BIN}, socket: {os.environ.get('WEZTERM_UNIX_SOCKET', '(auto)')}")


def _wez_sync(*args):
    try:
        r = subprocess.run(
            [WEZTERM_BIN, "cli", *args], capture_output=True, text=True, timeout=5
        )
        if r.returncode != 0:
            print(f"[wezterm] cli {' '.join(args)} failed (rc={r.returncode}): {r.stderr.strip()}")
            return ""
        return r.stdout
    except subprocess.TimeoutExpired:
        print(f"[wezterm] cli {' '.join(args)} timed out")
        return ""
    except FileNotFoundError:
        print(f"[wezterm] binary not found: {WEZTERM_BIN}")
        return ""


def _all_panes_sync():
    out = _wez_sync("list", "--format", "json")
    if not out:
        print("[wezterm] cli list returned empty")
    return json.loads(out) if out else []


def _send_text_sync(pid, text):
    try:
        pane = [WEZTERM_BIN, "cli", "send-text", "--pane-id", str(pid)]
        subprocess.run(pane, input=text.encode(), capture_output=True, timeout=5)
        time.sleep(0.2)
        subprocess.run(
            pane + ["--no-paste"], input=b"\x0d", capture_output=True, timeout=5
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass


def _send_enter_sync(pid):
    try:
        pane = [WEZTERM_BIN, "cli", "send-text", "--pane-id", str(pid), "--no-paste"]
        subprocess.run(pane, input=b"\x0d", capture_output=True, timeout=5)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass


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
    h_name = pane_harness.get(pid, "?")
    print(f"[verify] {h_name}/{pid}: >4s watching {path} (baseline mtime={baseline})")
    return "\u2717 >4s"


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
        for days_ago in range(2):
            t = time.gmtime(now - days_ago * 86400)
            day = CODEX_DIR / f"{t.tm_year:04d}/{t.tm_mon:02d}/{t.tm_mday:02d}"
            if not day.is_dir():
                continue
            for f in day.glob("rollout-*.jsonl"):
                ps = str(f)
                if ps in _codex_cache:
                    continue
                try:
                    with open(f) as fh:
                        meta = json.loads(fh.readline())
                    _codex_cache[ps] = meta.get("payload", {}).get("cwd", "")
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


# opencode state: session_id -> last seen part rowid
_oc_cursors = {}  # session_id -> max_rowid


def _opencode_seek_end(session_info):
    """Set cursor to max rowid so we skip existing messages."""
    db_path, session_id = session_info
    try:
        con = sqlite3.connect(str(db_path), timeout=2)
        row = con.execute(
            "SELECT MAX(rowid) FROM part WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        con.close()
        if row and row[0]:
            _oc_cursors[session_id] = row[0]
    except (sqlite3.Error, OSError):
        pass


def _opencode_read_new(session_info):
    """Read new assistant text parts from opencode DB. Returns list of strings."""
    db_path, session_id = session_info
    cursor = _oc_cursors.get(session_id, 0)
    messages = []
    try:
        con = sqlite3.connect(str(db_path), timeout=2)
        # ensure we see latest WAL writes
        con.execute("PRAGMA wal_checkpoint(PASSIVE)")
        rows = con.execute(
            "SELECT p.rowid, p.data, m.data FROM part p "
            "JOIN message m ON p.message_id = m.id "
            "WHERE p.session_id = ? AND p.rowid > ? "
            "ORDER BY p.rowid",
            (session_id, cursor),
        ).fetchall()
        con.close()
    except (sqlite3.Error, OSError) as e:
        print(f"[opencode] read error: {e}")
        return []

    max_rowid = cursor
    for rowid, pdata_str, mdata_str in rows:
        max_rowid = max(max_rowid, rowid)
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
    if max_rowid > cursor:
        _oc_cursors[session_id] = max_rowid
        pass
    return messages


# gemini state: session_path_str -> message count already seen
_gemini_cursors = {}


def _gemini_find_session(cwd):
    """Find most recent Gemini session file for a project cwd."""
    project_name = Path(cwd).name
    chats_dir = GEMINI_DIR / project_name / "chats"
    if not chats_dir.is_dir():
        return None
    files = list(chats_dir.glob("session-*.json"))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None


def _gemini_seek_end(session):
    """Set cursor to current message count so we skip existing messages."""
    try:
        data = json.loads(session.read_text())
        _gemini_cursors[str(session)] = len(data.get("messages", []))
    except (json.JSONDecodeError, OSError):
        pass


def _gemini_read_new(session):
    """Read new gemini-type messages. Returns list of strings."""
    key = str(session)
    cursor = _gemini_cursors.get(key, 0)
    try:
        data = json.loads(session.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    messages = data.get("messages", [])
    if len(messages) <= cursor:
        return []
    new_msgs = messages[cursor:]
    _gemini_cursors[key] = len(messages)
    results = []
    for msg in new_msgs:
        if msg.get("type") == "gemini":
            text = (msg.get("content") or "").strip()
            if text:
                results.append(text)
    return results


# --- harness: message formatters ------------------------------------------


def _claude_format(record):
    if record.get("type") != "assistant":
        return None
    content = record.get("message", {}).get("content", [])
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
        return await self._call("send", {
            "groupId": group_id,
            "message": text,
            "account": SIGNAL_ACCOUNT,
        })

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
                    print(f"[signal] callback error: {e}")


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


def _save(data):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(data))


# --- bridge state ----------------------------------------------------------

tab_topic = {}  # tab_id -> topic_id
tab_topic_name = {}  # tab_id -> last known topic name
topic_tab = {}  # topic_id -> tab_id

# per pane
pane_harness = {}  # pane_id -> harness name
pane_tab = {}  # pane_id -> tab_id
pane_cwds = {}  # pane_id -> cwd
file_pos = {}  # pane_id -> (path_str, offset)

# reply routing
msg_pane = {}  # telegram_msg_id -> pane_id
tab_last_pid = {}  # tab_id -> pane_id of last agent to send a message


# signal bridge state
sig_tab_group = {}    # tab_id -> signal group_id
sig_group_tab = {}    # group_id -> tab_id
sig_tab_name = {}     # tab_id -> last known group name
sig_msg_pane = {}     # signal_timestamp -> pane_id
sig_tab_last_pid = {} # tab_id -> pane_id
_signal_client = None
_signal_cmd_queue = []  # [(text, group_id, tab_id), ...]
_signal_input_queue = []  # [(text, tab_id), ...]

# slack observer state
_slack_msg_buffer = {}     # channel_id -> [(ts, user_id_or_None, text), ...]
_slack_obs_queue = []      # [(extra_text, user_id, channel_id), ...] !obs triggers
_slack_reply_channel = None  # set by !obs, cleared after agent responds
_slack_web_client = None   # AsyncWebClient, set in startup
_slack_bot_user_id = None  # our own bot user id, to skip self-echo


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
        "topics": {str(k): v for k, v in tab_topic.items()},
        "collab": {str(k): v for k, v in collab_tabs.items()},
    }
    if sig_tab_group:
        data["signal_groups"] = {str(k): v for k, v in sig_tab_group.items()}
        data["signal_group_names"] = {str(k): v for k, v in sig_tab_name.items()}
    _save(data)


_seeked = set()  # pids that have been seeked


def _seek_to_end(pid):
    """Position at end of session so we don't replay."""
    cwd = pane_cwds.get(pid)
    h = harnesses.get(pane_harness.get(pid, ""))
    if not cwd or not h:
        return
    session = h.find_session(cwd)
    if not session:
        return
    if h.read_new:
        if h.name == "opencode":
            _opencode_seek_end(session)
        elif h.name == "gemini":
            _gemini_seek_end(session)
    else:
        file_pos[pid] = (str(session), session.stat().st_size)


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


async def sync_topics(matched, unmatched):
    active_pids = set()
    active_tabs = set()
    dirty = False

    # track harness-matched panes (have session files, output works)
    for p, h_name in matched:
        pid = p["pane_id"]
        tab_id = p["tab_id"]
        cwd = _parse_cwd(p.get("cwd", ""))
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"

        active_pids.add(pid)
        active_tabs.add(tab_id)
        pane_harness[pid] = h_name
        pane_tab[pid] = tab_id
        pane_cwds[pid] = cwd

        if tab_id not in tab_topic:
            t = await _primary_bot.create_forum_topic(CHAT, title)
            tab_topic[tab_id] = t.message_thread_id
            tab_topic_name[tab_id] = title
            dirty = True

        if pid not in _seeked:
            _seek_to_end(pid)
            _seeked.add(pid)

    # also create topics + track panes that don't have sessions yet (input only)
    for p, _ in unmatched:
        pid = p["pane_id"]
        tab_id = p["tab_id"]
        cwd = _parse_cwd(p.get("cwd", ""))
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"

        active_pids.add(pid)
        active_tabs.add(tab_id)
        pane_tab[pid] = tab_id
        pane_cwds[pid] = cwd

        if tab_id not in tab_topic:
            t = await _primary_bot.create_forum_topic(CHAT, title)
            tab_topic[tab_id] = t.message_thread_id
            tab_topic_name[tab_id] = title
            dirty = True

    # rename topics whose tab title changed
    for p, _ in matched + unmatched:
        tab_id = p["tab_id"]
        if tab_id not in tab_topic:
            continue
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"
        if tab_topic_name.get(tab_id) != title:
            try:
                await _primary_bot.edit_forum_topic(CHAT, tab_topic[tab_id], name=title)
                print(f"[tg] renamed topic {tab_id} -> '{title}'")
            except Exception:
                pass  # Topic_not_modified or other non-fatal error
            tab_topic_name[tab_id] = title

    for pid in [p for p in list(pane_tab) if p not in active_pids]:
        pane_harness.pop(pid, None)
        pane_tab.pop(pid, None)
        pane_cwds.pop(pid, None)
        file_pos.pop(pid, None)

    # keep topics for tabs that disappeared (mux restart, etc.)

    if dirty:
        _rebuild()
        _persist()


def _tab_match(title, patterns, empty_means_all=False):
    """Check if a tab title matches any pattern in the list."""
    if not patterns:
        return empty_means_all
    t = title.lower()
    return any(pat in t for pat in patterns)


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
    if not _sig_groups_verified and sig_tab_group:
        _sig_groups_verified = True
        try:
            groups = await _signal_client.list_groups()
            valid_ids = {
                _normalize_signal_group_id(g.get("id"))
                for g in (groups or [])
                if g.get("isMember")
            }
            valid_ids.discard("")
            stale = [tid for tid, gid in list(sig_tab_group.items())
                     if _normalize_signal_group_id(gid) not in valid_ids]
            if stale:
                for tid in stale:
                    sig_tab_group.pop(tid)
                    sig_tab_last_pid.pop(tid, None)
                _rebuild()
                _persist()
                print(f"[signal] removed {len(stale)} stale group(s)")
        except Exception as e:
            print(f"[signal] group verification error: {e}")

    active_tabs = set()
    dirty = False
    # build reverse map: title -> existing tab_id (for reassigning after tab_id changes)
    _title_to_old_tab = {}
    for tid, name in sig_tab_name.items():
        if tid in sig_tab_group:
            _title_to_old_tab[name.lower()] = tid

    for p, h_name in matched:
        tab_id = p["tab_id"]
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"
        if not _tab_match(title, SIGNAL_TABS, empty_means_all=True):
            continue
        active_tabs.add(tab_id)
        # reassign group if tab_id changed but title matches
        if tab_id not in sig_tab_group:
            old_tid = _title_to_old_tab.get(title.lower())
            if old_tid and old_tid != tab_id and old_tid in sig_tab_group:
                sig_tab_group[tab_id] = sig_tab_group.pop(old_tid)
                sig_tab_name[tab_id] = sig_tab_name.pop(old_tid, title)
                sig_tab_last_pid.pop(old_tid, None)
                active_tabs.discard(old_tid)
                dirty = True
                print(f"[signal] reassigned group '{title}' from tab {old_tid} -> {tab_id}")
        if tab_id not in sig_tab_group:
            try:
                result = await _signal_client.create_group(
                    title, _sig_members_for(title)
                )
                gid = result.get("groupId") if isinstance(result, dict) else result
                if gid:
                    sig_tab_group[tab_id] = gid
                    sig_tab_name[tab_id] = title
                    dirty = True
                    print(f"[signal] created group '{title}' -> {gid}")
                else:
                    print(f"[signal] create group returned no group id: {result}")
            except Exception as e:
                print(f"[signal] create group error: {e}")

    for p, _ in unmatched:
        tab_id = p["tab_id"]
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"
        if not _tab_match(title, SIGNAL_TABS, empty_means_all=True):
            continue
        active_tabs.add(tab_id)
        # reassign group if tab_id changed but title matches
        if tab_id not in sig_tab_group:
            old_tid = _title_to_old_tab.get(title.lower())
            if old_tid and old_tid != tab_id and old_tid in sig_tab_group:
                sig_tab_group[tab_id] = sig_tab_group.pop(old_tid)
                sig_tab_name[tab_id] = sig_tab_name.pop(old_tid, title)
                sig_tab_last_pid.pop(old_tid, None)
                active_tabs.discard(old_tid)
                dirty = True
                print(f"[signal] reassigned group '{title}' from tab {old_tid} -> {tab_id}")
        if tab_id not in sig_tab_group:
            try:
                result = await _signal_client.create_group(
                    title, _sig_members_for(title)
                )
                gid = result.get("groupId") if isinstance(result, dict) else result
                if gid:
                    sig_tab_group[tab_id] = gid
                    sig_tab_name[tab_id] = title
                    dirty = True
                    print(f"[signal] created group '{title}' -> {gid}")
                else:
                    print(f"[signal] create group returned no group id: {result}")
            except Exception as e:
                print(f"[signal] create group error: {e}")

    # rename groups whose tab title changed (only for signal-matched tabs)
    for p, _ in matched + [(p, None) for p, _ in unmatched]:
        tab_id = p["tab_id"]
        if tab_id not in sig_tab_group:
            continue
        title = (p.get("tab_title") or f"tab-{tab_id}").strip()[:128] or f"tab-{tab_id}"
        if not _tab_match(title, SIGNAL_TABS, empty_means_all=True):
            # tab_id was reused by a non-signal tab — detach the group
            old_name = sig_tab_name.get(tab_id, "?")
            print(f"[signal] tab_id {tab_id} reused (was '{old_name}', now '{title}'), detaching group")
            sig_tab_group.pop(tab_id, None)
            sig_tab_name.pop(tab_id, None)
            dirty = True
            continue
        if sig_tab_name.get(tab_id) != title:
            try:
                await _signal_client.rename_group(sig_tab_group[tab_id], title)
                print(f"[signal] renamed group {tab_id} -> '{title}'")
            except Exception:
                pass
            sig_tab_name[tab_id] = title

    # track greeted groups (no greeting message sent)
    for tab_id in active_tabs:
        gid = sig_tab_group.get(tab_id)
        if gid:
            _sig_greeted.add(gid)

    # note: we don't auto-leave groups for missing tabs — discover() can
    # return partial results and we'd lose groups permanently. Use /refresh
    # to explicitly recreate a group.

    if dirty:
        _rebuild()
        _persist()


def _read_new_sync(pid):
    h = harnesses.get(pane_harness.get(pid, ""))
    cwd = pane_cwds.get(pid)
    if not h or not cwd:
        return []

    session = h.find_session(cwd)
    if not session:
        return []

    # first time seeing a session for this pane? seek to end
    if pid not in _seeked:
        _seek_to_end(pid)
        _seeked.add(pid)

    # opencode (and other DB-based harnesses) handle their own reading
    if h.read_new:
        msgs = h.read_new(session)
        if msgs:
            tab = tab_topic_name.get(pane_tab.get(pid, -1), "?")
            print(f"[{tab}/{h.name}] db read: {len(msgs)} messages")
        return msgs

    session_str = str(session)
    prev_path, prev_pos = file_pos.get(pid, (None, 0))

    if prev_path != session_str:
        file_pos[pid] = (session_str, session.stat().st_size)
        return []

    cur_size = session.stat().st_size
    if cur_size <= prev_pos:
        return []

    with open(session, "r") as f:
        f.seek(prev_pos)
        new_data = f.read()

    file_pos[pid] = (session_str, cur_size)

    messages = []
    lines = [l for l in new_data.strip().split("\n") if l.strip()]
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        formatted = h.format_record(record)
        if formatted:
            messages.append(formatted)
    if lines:
        tab = tab_topic_name.get(pane_tab.get(pid, -1), "?")
        print(f"[{tab}/{h.name}] read {len(lines)} records, {len(messages)} messages")
    return messages


async def check_output():
    global _slack_reply_channel
    pids = list(pane_harness.keys())
    results = await asyncio.gather(
        *(asyncio.to_thread(_read_new_sync, pid) for pid in pids)
    )
    for pid, messages in zip(pids, results):
        h = harnesses.get(pane_harness.get(pid, ""))
        if not h or not messages:
            continue
        tab_id = pane_tab.get(pid)
        if tab_id:
            tab_last_pid[tab_id] = pid
            sig_tab_last_pid[tab_id] = pid
        tid = tab_topic.get(tab_id)
        gid = sig_tab_group.get(tab_id) if SIGNAL_ENABLED else None
        source = tab_last_source.get(tab_id)
        if source is None:
            source = "sig" if gid else "tg"
        for msg in messages:
            # telegram topic (default output channel)
            if tid and source == "tg":
                for chunk in _chunkify(msg):
                    try:
                        sent = await h.bot.send_message(
                            CHAT, chunk, message_thread_id=tid
                        )
                        msg_pane[sent.message_id] = pid
                    except Exception as e:
                        print(f"send err [{h.name}/{pid}]: {e}")
            # signal
            if gid and _signal_client and source == "sig":
                tab = tab_topic_name.get(tab_id, "?")
                print(f"[check_output] sending to signal: {tab}/{h.name} msg={msg[:60]}")
                for chunk in _chunkify(f"{h.display_name}: {msg}"):
                    try:
                        result = await _signal_client.send_message(gid, chunk)
                        ts = result.get("timestamp") if isinstance(result, dict) else None
                        if ts:
                            sig_msg_pane[ts] = pid
                    except Exception as e:
                        print(f"signal send err [{h.name}/{pid}]: {e}")
            # debate chat (each bot posts as itself, no prefix)
            if DEBATE_ENABLED and source == "debate" and _tab_match(tab_topic_name.get(tab_id, ""), DEBATE_TABS):
                for chunk in _chunkify(msg):
                    try:
                        sent = await h.bot.send_message(DEBATE_CHAT, chunk)
                        debate_msg_pane[sent.message_id] = pid
                    except Exception as e:
                        print(f"debate send err [{h.name}/{pid}]: {e}")
            # slack observer (reply to !obs, then clear)
            if SLACK_ENABLED and _slack_reply_channel and _tab_match(tab_topic_name.get(tab_id, ""), SLACK_TABS):
                slack_msg = _md_tables_to_slack(msg)
                for chunk in _chunkify(slack_msg):
                    try:
                        await _slack_web_client.chat_postMessage(channel=_slack_reply_channel, text=chunk)
                    except Exception as e:
                        print(f"slack send err [{h.name}/{pid}]: {e}")
                _slack_reply_channel = None

            # collab: forward to other harness panes in this tab
            tab_id = pane_tab.get(pid)
            if tab_id and tab_id in collab_tabs:
                # check for /signoff
                if "/signoff" in msg.lower():
                    signoffs = collab_signoffs.setdefault(tab_id, set())
                    signoffs.add(pid)
                    # check if all harness panes in this tab signed off
                    tab_panes = {p for p, t in pane_tab.items()
                                 if t == tab_id and p in pane_harness}
                    if tab_panes and signoffs >= tab_panes:
                        del collab_tabs[tab_id]
                        collab_signoffs.pop(tab_id, None)
                        _persist()
                        try:
                            await _primary_bot.send_message(
                                CHAT, "all agents signed off, collab done",
                                message_thread_id=tid,
                            )
                        except Exception:
                            pass
                        continue
                else:
                    # non-signoff message resets that pane's signoff
                    signoffs = collab_signoffs.get(tab_id)
                    if signoffs:
                        signoffs.discard(pid)

                ts = f" [{_now_ts()}]" if MSG_TIMESTAMPS else ""
                prefixed = f"{h.display_name}{ts} says: {msg}"
                for target_pid in _other_panes(tab_id, pid):
                    await send_and_verify(target_pid, prefixed)
                # decrement rounds if limited
                rounds = collab_tabs.get(tab_id)
                if rounds and rounds > 0:
                    collab_tabs[tab_id] = rounds - 1
                    if rounds - 1 <= 0:
                        del collab_tabs[tab_id]
                        collab_signoffs.pop(tab_id, None)
                        _persist()
                        try:
                            await _primary_bot.send_message(
                                CHAT, "collab done", message_thread_id=tid
                            )
                        except Exception:
                            pass


# --- collab mode -----------------------------------------------------------

collab_tabs = {}  # tab_id -> rounds_remaining (0 = infinite)
collab_signoffs = {}  # tab_id -> set of pane_ids that signed off
debate_msg_pane = {}  # msg_id -> pane_id (reply routing for debate chat)
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




async def _handle_debate_message(m):
    """Route incoming debate chat message to pane(s)."""
    text = m.text
    if text.strip().startswith("/"):
        await _debate_handle_command(m)
        return

    tab_id = _find_tab(DEBATE_TABS)
    if not tab_id:
        print(f"[debate] no matching tab, dropped: '{text[:50]}'")
        return

    reply_pid = debate_msg_pane.get(m.reply_to_message.message_id) if m.reply_to_message else None
    pid = _resolve_pid(tab_id, reply_pid)
    tab_last_source[tab_id] = "debate"
    await _route_to_pane(pid, tab_id, text, "debate")


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
        if not tab_id:
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


async def on_message(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.text:
        return

    # === debate chat (open to all members) ===
    if DEBATE_ENABLED and m.chat_id == DEBATE_CHAT:
        u = update.effective_user
        if u:
            print(f"[debate] from {u.first_name} (uid={u.id})")
        await _handle_debate_message(m)
        return

    # === forum topics (existing flow, requires owner + thread_id) ===
    if not _is_owner(update) or not m.message_thread_id:
        return

    reply_pid = msg_pane.get(m.reply_to_message.message_id) if m.reply_to_message else None
    tab_id = topic_tab.get(m.message_thread_id) if reply_pid is None else pane_tab.get(reply_pid)
    pid = _resolve_pid(tab_id, reply_pid) if tab_id else reply_pid
    if tab_id:
        tab_last_source[tab_id] = "tg"
    await _route_to_pane(pid, tab_id, m.text, "tg")



async def on_collab(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.message_thread_id or not _is_owner(update):
        return
    tab_id = topic_tab.get(m.message_thread_id)
    if not tab_id:
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
    if not tab_id:
        return
    # get tab title for recreation
    matched, unmatched = await discover()
    name = f"tab-{tab_id}"
    for p, _ in matched + unmatched:
        if p["tab_id"] == tab_id:
            name = p.get("tab_title", name)[:128]
            break
    try:
        await _primary_bot.delete_forum_topic(CHAT, tid)
        t = await _primary_bot.create_forum_topic(CHAT, name)
        tab_topic[tab_id] = t.message_thread_id
        _rebuild()
        _persist()
        # clear tracked messages and signoffs for this topic
        for mid in [k for k, v in msg_pane.items()
                    if pane_tab.get(v) == tab_id]:
            msg_pane.pop(mid, None)
        collab_signoffs.pop(tab_id, None)
        # re-seek so we don't replay old output
        for pid, tab in pane_tab.items():
            if tab == tab_id:
                _seek_to_end(pid)
        print(f"[clear] recreated topic for tab {tab_id}")
    except Exception as e:
        print(f"clear: {e}")


# --- signal handlers -------------------------------------------------------


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
        if not tab_id:
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
        if not tab_id:
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
            sig_tab_group[tab_id] = new_gid
            _rebuild()
            _persist()
            await _signal_client.send_message(new_gid, "refreshed")
            _sig_greeted.add(new_gid)
            # clear tracked signal messages for this tab
            for ts in [k for k, v in sig_msg_pane.items()
                       if pane_tab.get(v) == tab_id]:
                sig_msg_pane.pop(ts, None)
            collab_signoffs.pop(tab_id, None)
            for pid, tab in pane_tab.items():
                if tab == tab_id:
                    _seek_to_end(pid)
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

    # dedup by envelope timestamp (signal-cli may deliver same msg twice)
    env_ts = envelope.get("timestamp")
    if env_ts:
        if env_ts in _sig_seen_ts:
            return
        _sig_seen_ts.add(env_ts)
        # keep set bounded
        if len(_sig_seen_ts) > 200:
            _sig_seen_ts.clear()

    source = (envelope.get("sourceNumber")
              or envelope.get("sourceUuid")
              or envelope.get("source", ""))
    data = envelope.get("dataMessage", {})
    msg_text = data.get("message") or ""
    text = msg_text
    for att in data.get("attachments") or []:
        att_id = att.get("id", "")
        if att_id:
            text = text + " " + str(Path.home() / ".local/share/signal-cli/attachments" / att_id)
    text = text.strip()
    group_info = data.get("groupInfo", {})
    group_id = _normalize_signal_group_id(group_info.get("groupId", ""))

    source_name = envelope.get("sourceName") or source[:12]
    if not text or not group_id:
        return
    source_number = envelope.get("sourceNumber", "")
    all_members = set(SIGNAL_MEMBERS)
    for m in SIGNAL_TAB_MEMBERS.values():
        all_members.update(m)
    if not source_number:
        print(f"[signal] no phone number for {source_name} ({source})")
    if (source != SIGNAL_OWNER
            and source not in SIGNAL_ALLOWED
            and source_number not in all_members):
        print(f"[signal] ignoring message from {source_name} ({source})")
        return

    tab_id = sig_group_tab.get(group_id)

    # queue for processing in poll_loop (avoid _call deadlock)
    if msg_text.strip().startswith("/"):
        _signal_cmd_queue.append((msg_text, group_id, tab_id))
    elif tab_id:
        _signal_input_queue.append((text, data, tab_id, source_name))


async def _process_signal_queues():
    """Process queued Signal commands and input (called from poll_loop)."""
    # commands
    while _signal_cmd_queue:
        text, group_id, tab_id = _signal_cmd_queue.pop(0)
        await _signal_handle_command(text, group_id, tab_id)

    # input routing
    while _signal_input_queue:
        text, data, tab_id, sender = _signal_input_queue.pop(0)
        quote = data.get("quote", {})
        reply_pid = None
        if quote:
            quote_ts = quote.get("id")
            if quote_ts:
                reply_pid = sig_msg_pane.get(quote_ts)
        pid = _resolve_pid(tab_id, reply_pid)
        tab_last_source[tab_id] = "sig"
        ts = f" [{_now_ts()}]" if MSG_TIMESTAMPS else ""
        prefixed = f"{sender}{ts} says: {text}"
        await _route_to_pane(pid, tab_id, prefixed, "sig")


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


# --- lifecycle -------------------------------------------------------------

_script_path = Path(__file__).resolve()
_script_mtime = _script_path.stat().st_mtime


async def poll_loop():
    global _script_mtime
    while True:
        try:
            # hot reload: re-exec if bridge.py changed on disk
            mt = _script_path.stat().st_mtime
            if mt != _script_mtime:
                print("bridge.py changed, reloading...")
                _persist()
                os.execv(sys.executable, [sys.executable, str(_script_path)])

            matched, unmatched = await discover()
            await sync_topics(matched, unmatched)
            if SIGNAL_ENABLED:
                await sync_signal_groups(matched, unmatched)
                await _process_signal_queues()
            if SLACK_ENABLED:
                await _process_slack_queue()
            await check_output()
        except Exception as e:
            print(f"tick: {e}")
        await asyncio.sleep(POLL)


async def startup(app: Application):
    global _primary_bot
    _init_harnesses()
    _primary_bot = harnesses["claude"].bot

    # fetch bot display names
    for h in harnesses.values():
        try:
            me = await h.bot.get_me()
            h.display_name = me.first_name
        except Exception:
            pass

    saved = _load()
    # migrate flat format (old) to nested format
    topics = saved.get("topics", saved) if "topics" in saved else saved
    for k, v in topics.items():
        tab_topic[int(k)] = v
    for k, v in saved.get("collab", {}).items():
        collab_tabs[int(k)] = v
    for k, v in saved.get("signal_groups", {}).items():
        gid = _normalize_signal_group_id(v)
        if gid:
            sig_tab_group[int(k)] = gid
    for k, v in saved.get("signal_group_names", {}).items():
        sig_tab_name[int(k)] = v
    _rebuild()
    if sig_tab_group:
        print(f"[signal] loaded {len(sig_tab_group)} group(s) from state")

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
        asyncio.create_task(_signal_receive_task())

    if SLACK_ENABLED:
        from slack_sdk.web.async_client import AsyncWebClient
        global _slack_web_client, _slack_bot_user_id
        _slack_web_client = AsyncWebClient(token=SLACK_BOT_TOKEN)
        try:
            auth = await _slack_web_client.auth_test()
            _slack_bot_user_id = auth["bot_id"]
        except Exception as e:
            print(f"[slack] auth_test failed: {e}")
        asyncio.create_task(_slack_receive_task())

    matched, unmatched = await discover()
    for p, h_name in matched:
        pid = p["pane_id"]
        pane_harness[pid] = h_name
        pane_tab[pid] = p["tab_id"]
        pane_cwds[pid] = _parse_cwd(p.get("cwd", ""))
        _seek_to_end(pid)
        _seeked.add(pid)
    for p, _ in unmatched:
        pane_tab[p["pane_id"]] = p["tab_id"]
        pane_cwds[p["pane_id"]] = _parse_cwd(p.get("cwd", ""))

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

    asyncio.create_task(poll_loop())


def main():
    _init_harnesses()
    app = Application.builder().token(CLAUDE_TOKEN).post_init(startup).build()
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
