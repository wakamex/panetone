#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["python-telegram-bot>=22.0"]
# ///
"""panetone: wezterm <> telegram bridge for multiple AI coding agents

Each wezterm tab gets one Telegram forum topic (by tab_title).
Multiple harnesses (Claude, Codex, ...) share the topic but post
via their own bot identity. Replies route to the right pane.

Env:
  WEZ_TG_TOKEN_CLAUDE  - bot token for Claude messages (required)
  WEZ_TG_TOKEN_CODEX   - bot token for Codex messages (optional)
  WEZ_TG_CHAT          - forum group chat id
  WEZ_TG_OWNER         - your telegram user id (optional lock)
  WEZ_TG_POLL          - poll interval seconds (default 2)
"""

import asyncio
import html
import json
import os
import re
import subprocess
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
CLAUDE_DIR = Path.home() / ".claude" / "projects"
CODEX_DIR = Path.home() / ".codex" / "sessions"

# --- wezterm cli -----------------------------------------------------------


def _wez_sync(*args):
    try:
        r = subprocess.run(
            ["wezterm", "cli", *args], capture_output=True, text=True, timeout=5
        )
        return r.stdout if r.returncode == 0 else ""
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return ""


def _all_panes_sync():
    out = _wez_sync("list", "--format", "json")
    return json.loads(out) if out else []


def _send_text_sync(pid, text):
    try:
        pane = ["wezterm", "cli", "send-text", "--pane-id", str(pid)]
        subprocess.run(pane, input=text.encode(), capture_output=True, timeout=5)
        time.sleep(0.1)
        subprocess.run(
            pane + ["--no-paste"], input=b"\x0d", capture_output=True, timeout=5
        )
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass


async def send_text(pid, text):
    await asyncio.to_thread(_send_text_sync, pid, text)


def _parse_cwd(cwd_url):
    if cwd_url.startswith("file://"):
        return urlparse(cwd_url).path
    return cwd_url


# --- harness: session finders ---------------------------------------------


def _claude_find_session(cwd):
    proj_dir = CLAUDE_DIR / cwd.replace("/", "-")
    if not proj_dir.is_dir():
        return None
    files = list(proj_dir.glob("*.jsonl"))
    return max(files, key=lambda f: f.stat().st_mtime) if files else None


_codex_cache = {}  # path_str -> cwd
_codex_cache_t = 0.0


def _codex_find_session(cwd):
    global _codex_cache, _codex_cache_t
    now = time.time()

    if now - _codex_cache_t > 30:
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


# --- harness: message formatters ------------------------------------------


def _claude_format(record):
    if record.get("type") != "assistant":
        return None
    content = record.get("message", {}).get("content", [])
    parts = []
    for block in content:
        btype = block.get("type")
        if btype == "text":
            t = block.get("text", "").strip()
            if t:
                parts.append(t)
        elif btype == "tool_use":
            name = block.get("name", "?")
            inp = block.get("input", {})
            if name in ("Read", "Glob", "Grep"):
                target = (
                    inp.get("file_path") or inp.get("pattern") or inp.get("path", "")
                )
                parts.append(f"[{name}: {target}]")
            elif name in ("Edit", "Write"):
                parts.append(f"[{name}: {inp.get('file_path', '?')}]")
            elif name == "Bash":
                parts.append(f"[$ {inp.get('command', '')[:80]}]")
            elif name == "Task":
                parts.append(f"[Task: {inp.get('description', '?')}]")
            else:
                parts.append(f"[{name}]")
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

    if p.get("type") == "function_call":
        name = p.get("name", "?")
        try:
            args = json.loads(p.get("arguments", "{}"))
            if name == "exec_command":
                return f"[$ {args.get('cmd', '')[:80]}]"
        except json.JSONDecodeError:
            pass
        return f"[{name}]"

    if p.get("type") == "custom_tool_call":
        return f"[{p.get('name', '?')}]"

    return None


# --- harness registry ------------------------------------------------------


class Harness:
    def __init__(self, name, token, find_session, format_record):
        self.name = name
        self.bot = Bot(token)
        self.find_session = find_session
        self.format_record = format_record


harnesses = {}  # name -> Harness


def _init_harnesses():
    harnesses["claude"] = Harness(
        "claude", CLAUDE_TOKEN, _claude_find_session, _claude_format
    )
    if CODEX_TOKEN:
        harnesses["codex"] = Harness(
            "codex", CODEX_TOKEN, _codex_find_session, _codex_format
        )


# --- text helpers ----------------------------------------------------------


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
topic_tab = {}  # topic_id -> tab_id

# per pane
pane_harness = {}  # pane_id -> harness name
pane_tab = {}  # pane_id -> tab_id
pane_cwds = {}  # pane_id -> cwd
file_pos = {}  # pane_id -> (path_str, offset)

# reply routing
msg_pane = {}  # telegram_msg_id -> pane_id


def _rebuild():
    global topic_tab
    topic_tab = {tid: tab for tab, tid in tab_topic.items()}


def _persist():
    _save({str(k): v for k, v in tab_topic.items()})


def _seek_to_end(pid):
    """Position at end of session file so we don't replay."""
    cwd = pane_cwds.get(pid)
    h = harnesses.get(pane_harness.get(pid, ""))
    if not cwd or not h:
        return
    session = h.find_session(cwd)
    if session:
        file_pos[pid] = (str(session), session.stat().st_size)


# --- pane discovery --------------------------------------------------------


_SHELLS = frozenset({
    "zsh", "bash", "fish", "sh", "dash",
    "node", "uv", "python", "python3", "ruby",
    "nvim", "vim", "nano", "htop", "top", "less", "man",
})


def _discover_sync():
    """Find all panes, classify by harness where possible."""
    all_panes = _all_panes_sync()
    by_tab = {}
    for p in all_panes:
        by_tab.setdefault(p["tab_id"], []).append(p)

    matched = []  # (pane, harness_name) — have a session file
    unmatched = []  # (pane, None) — no session but not a shell

    for panes in by_tab.values():
        claimed = set()
        for h_name, h in harnesses.items():
            best_pane, best_mt = None, 0.0
            for p in panes:
                if p["pane_id"] in claimed:
                    continue
                cwd = _parse_cwd(p.get("cwd", ""))
                session = h.find_session(cwd)
                if session:
                    mt = session.stat().st_mtime
                    if mt > best_mt:
                        best_pane, best_mt = p, mt
            if best_pane:
                matched.append((best_pane, h_name))
                claimed.add(best_pane["pane_id"])
        # also track non-shell panes without sessions (for topic creation + input)
        for p in panes:
            if p["pane_id"] not in claimed:
                if p["title"].strip().lower() not in _SHELLS:
                    unmatched.append((p, None))
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
        title = p.get("tab_title", f"tab-{tab_id}")[:128]

        active_pids.add(pid)
        active_tabs.add(tab_id)
        pane_harness[pid] = h_name
        pane_tab[pid] = tab_id
        pane_cwds[pid] = cwd

        if tab_id not in tab_topic:
            t = await _primary_bot.create_forum_topic(CHAT, title)
            tab_topic[tab_id] = t.message_thread_id
            dirty = True

        if pid not in file_pos:
            _seek_to_end(pid)

    # also create topics + track panes that don't have sessions yet (input only)
    for p, _ in unmatched:
        pid = p["pane_id"]
        tab_id = p["tab_id"]
        cwd = _parse_cwd(p.get("cwd", ""))
        title = p.get("tab_title", f"tab-{tab_id}")[:128]

        active_pids.add(pid)
        active_tabs.add(tab_id)
        pane_tab[pid] = tab_id
        pane_cwds[pid] = cwd

        if tab_id not in tab_topic:
            t = await _primary_bot.create_forum_topic(CHAT, title)
            tab_topic[tab_id] = t.message_thread_id
            dirty = True

    for pid in [p for p in list(pane_tab) if p not in active_pids]:
        pane_harness.pop(pid, None)
        pane_tab.pop(pid, None)
        pane_cwds.pop(pid, None)
        file_pos.pop(pid, None)

    for tab_id in [t for t in list(tab_topic) if t not in active_tabs]:
        try:
            await _primary_bot.close_forum_topic(CHAT, tab_topic[tab_id])
        except Exception:
            pass
        tab_topic.pop(tab_id, None)
        dirty = True

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
        print(f"[{h.name}/{pid}] read {len(lines)} records, {len(messages)} messages")
    return messages


async def check_output():
    pids = list(pane_harness.keys())
    results = await asyncio.gather(
        *(asyncio.to_thread(_read_new_sync, pid) for pid in pids)
    )
    for pid, messages in zip(pids, results):
        h = harnesses.get(pane_harness.get(pid, ""))
        tid = tab_topic.get(pane_tab.get(pid))
        if not h or not tid or not messages:
            continue
        for msg in messages:
            for chunk in _chunkify(msg):
                try:
                    sent = await h.bot.send_message(
                        CHAT, chunk, message_thread_id=tid
                    )
                    msg_pane[sent.message_id] = pid
                except Exception as e:
                    print(f"send err [{h.name}/{pid}]: {e}")

            # collab: forward to other harness panes in this tab
            tab_id = pane_tab.get(pid)
            if tab_id and tab_id in collab_tabs:
                for target_pid in _other_panes(tab_id, pid):
                    await send_text(target_pid, msg)
                # decrement rounds if limited
                rounds = collab_tabs[tab_id]
                if rounds > 0:
                    collab_tabs[tab_id] = rounds - 1
                    if rounds - 1 <= 0:
                        del collab_tabs[tab_id]
                        try:
                            await _primary_bot.send_message(
                                CHAT, "collab done", message_thread_id=tid
                            )
                        except Exception:
                            pass


# --- collab mode -----------------------------------------------------------

collab_tabs = {}  # tab_id -> rounds_remaining (0 = infinite)


def _other_panes(tab_id, src_pid):
    """Find panes in the same tab belonging to different harnesses."""
    src_h = pane_harness.get(src_pid)
    return [
        p for p, h in pane_harness.items()
        if pane_tab.get(p) == tab_id and h != src_h
    ]


# --- telegram handlers -----------------------------------------------------


def _is_owner(update):
    return not OWNER or (update.effective_user and update.effective_user.id == OWNER)


async def on_message(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.message_thread_id or not m.text or not _is_owner(update):
        return

    pid = None

    # reply to a bot message -> route to that pane
    if m.reply_to_message:
        pid = msg_pane.get(m.reply_to_message.message_id)

    # otherwise -> first pane in this tab (prefer claude, then any tracked pane)
    if pid is None:
        tab_id = topic_tab.get(m.message_thread_id)
        if tab_id:
            # prefer harness-matched panes (claude first)
            for p, h in sorted(
                pane_harness.items(), key=lambda x: x[1] != "claude"
            ):
                if pane_tab.get(p) == tab_id:
                    pid = p
                    break
            # fall back to any pane in this tab
            if pid is None:
                for p, t in pane_tab.items():
                    if t == tab_id:
                        pid = p
                        break

    if pid is not None:
        tab_id = pane_tab.get(pid)
        if tab_id and tab_id in collab_tabs:
            # send to all harness panes in this tab
            seen = set()
            for p, h in pane_harness.items():
                if pane_tab.get(p) == tab_id and p not in seen:
                    await send_text(p, m.text)
                    seen.add(p)
        else:
            await send_text(pid, m.text)


async def on_collab(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    m = update.message
    if not m or not m.message_thread_id or not _is_owner(update):
        return
    tab_id = topic_tab.get(m.message_thread_id)
    if not tab_id:
        return

    if tab_id in collab_tabs:
        del collab_tabs[tab_id]
        await m.reply_text("collab off")
    else:
        # parse optional rounds: /collab 10
        rounds = 0
        args = (m.text or "").split()
        if len(args) > 1:
            try:
                rounds = int(args[1])
            except ValueError:
                pass
        collab_tabs[tab_id] = rounds
        label = f"collab on ({rounds} rounds)" if rounds else "collab on"
        await m.reply_text(label)


async def on_list(update: Update, _ctx: ContextTypes.DEFAULT_TYPE):
    matched, unmatched = await discover()
    if not matched and not unmatched:
        await update.message.reply_text("no panes")
        return
    lines = []
    for p, h_name in matched:
        tab = html.escape(p.get("tab_title", ""))
        title = html.escape(p.get("title", "?"))
        lines.append(f"<code>{h_name:6}</code> <b>{tab}</b> {title}")
    for p, _ in unmatched:
        tab = html.escape(p.get("tab_title", ""))
        title = html.escape(p.get("title", "?"))
        lines.append(f"<code>{'--':6}</code> <b>{tab}</b> {title}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# --- lifecycle -------------------------------------------------------------


async def poll_loop():
    while True:
        try:
            matched, unmatched = await discover()
            await sync_topics(matched, unmatched)
            await check_output()
        except Exception as e:
            print(f"tick: {e}")
        await asyncio.sleep(POLL)


async def startup(app: Application):
    global _primary_bot
    _init_harnesses()
    _primary_bot = harnesses["claude"].bot

    saved = _load()
    for k, v in saved.items():
        tab_topic[int(k)] = v
    _rebuild()

    matched, unmatched = await discover()
    for p, h_name in matched:
        pid = p["pane_id"]
        pane_harness[pid] = h_name
        pane_tab[pid] = p["tab_id"]
        pane_cwds[pid] = _parse_cwd(p.get("cwd", ""))
        _seek_to_end(pid)
    for p, _ in unmatched:
        pane_tab[p["pane_id"]] = p["tab_id"]
        pane_cwds[p["pane_id"]] = _parse_cwd(p.get("cwd", ""))

    asyncio.create_task(poll_loop())


def main():
    _init_harnesses()
    app = Application.builder().token(CLAUDE_TOKEN).post_init(startup).build()
    app.add_handler(CommandHandler("list", on_list))
    app.add_handler(CommandHandler("collab", on_collab))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))
    names = ", ".join(harnesses.keys())
    print(f"panetone: [{names}] polling chat {CHAT} every {POLL}s")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
