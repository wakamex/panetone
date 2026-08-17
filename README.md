# Panetone

A lightweight bridge between [WezTerm](https://wezfurlong.org/wezterm/) and Telegram/Signal for juggling multiple AI coding agents (Claude Code, Codex, etc.) from your phone.

Each wezterm tab gets its own Telegram forum topic and/or Signal group chat. Multiple harnesses share the channel but post via their own identity. Replies route back to the right terminal pane.

<p align="center">
  <img src="panetone.png" width="600" />
</p>

## Features

- **Forum topics per tab** — one topic per wezterm tab, named after the tab title
- **Multi-harness** — Claude and Codex post as separate bots in shared topics
- **Bidirectional** — read output in Telegram, send input back to the terminal
- **Collab mode** — `/collab` forwards responses between harnesses so they can talk to each other
- **Owner lock** — restrict input to your Telegram user ID
- **Session tailing** — reads `.claude` and `.codex` JSONL session files directly, no screen scraping
- **Signal support** — optionally mirror output to Signal groups via signal-cli (no extra Python deps)

## Telegram Setup

1. Create a Telegram group with [Topics enabled](https://telegram.org/blog/topics-in-groups-collectible-usernames#topics-in-groups)
2. Create bot(s) via [@BotFather](https://t.me/BotFather) and add them as group admins with "Manage Topics" permission
3. Create a `.env` file:

```
WEZ_TG_TOKEN_CLAUDE=your-claude-bot-token
WEZ_TG_TOKEN_CODEX=your-codex-bot-token    # optional
WEZ_TG_CHAT=-100xxxxxxxxxx
WEZ_TG_OWNER=your-telegram-user-id         # optional
```

4. Run:

```
uv --no-config run --locked --script bridge.py
```

Requires [uv](https://docs.astral.sh/uv/). Runtime dependencies are resolved
from the committed `bridge.py.lock`; refresh it deliberately with
`uv --no-config lock --script bridge.py`.

Production does not watch source files or reload itself. Develop in a separate
Git worktree, run the test suite there, stop the service, promote the tested
commit into `/code/panetone`, and start the service once. The current deployment
is an enabled user service with lingering enabled. It runs the locked script
through `~/.local/bin/uv` and shares the user service manager with Wakterm, so
their startup ordering is explicit and neither service requires an interactive
login.

An optional root-owned unit is tracked at `deploy/panetone.service`. It uses the
system-labeled `/usr/local/bin/uv` executable and runs as `mihai`. Promote both
Panetone and its Wakterm dependency into a compatible system-service deployment
before using this alternative. The rollback-safe Panetone installer is:

```sh
sudo /bin/bash /code/panetone/deploy/install-system-service.sh
```

The installer checks the committed lock before stopping the current service and
restores the user service automatically if system-service promotion fails.

For Wakterm adapter development, build and run the current `/code/wakterm`
checkout behind a separate development mux:

```sh
dev/wakterm-dev build
dev/wakterm-dev serve
```

Run development CLI probes from another terminal with, for example,
`dev/wakterm-dev cli agent capabilities`. The launcher disables Wakterm config
loading and isolates the mux socket, saved session, cache, config, and data below
the Panetone development worktree. It runs in the foreground and never manages
or restarts the production mux. Stop it with Ctrl-C when the test is complete.

Before Phase 5B, run the pinned disposable integration preflight:

```sh
dev/phase5b-wakterm-preflight
```

It records private JSON evidence below `.dev/evidence/` and does not touch the
production mux. See the [Wakterm promotion checklist](docs/wakterm-phase5b-promotion.md)
before any real mux deployment or restart. Restoring terminal layout alone does
not restore agent harnesses.

The installed Python bridge negotiates the Wakterm Agent API during startup.
The Phase 5A Rust candidate also checks capabilities on every Agent API
operation because each CLI invocation opens a new mux connection. Return mode
requires catalog, prompt-admission, and durable return-stream capabilities so a
callback can be queued while its exact source agent is busy instead of steering
an active turn. The Rust output consumer additionally requires
`event_stream.v1`. See [the Phase 5A contract](docs/rust-phase5a.md) and
[production runbook](docs/production-operations.md).

## Signal Setup (optional)

Signal has no bot API — panetone talks to [signal-cli](https://github.com/AsamK/signal-cli) over a UNIX socket (JSON-RPC 2.0). No extra Python dependencies needed.

1. Install Java 17+: `sudo dnf install java-17-openjdk`
2. Install [signal-cli](https://github.com/AsamK/signal-cli/releases) from GitHub releases
3. Register a number for the bot:
   ```
   signal-cli -a +BOT_NUMBER register
   signal-cli -a +BOT_NUMBER verify CODE
   ```
4. Start the daemon:
   ```
   signal-cli -a +BOT_NUMBER daemon --socket /tmp/signal-cli.sock
   ```
5. Add to your `.env`:
   ```
   WEZ_SIG_SOCKET=/tmp/signal-cli.sock
   WEZ_SIG_ACCOUNT=+1234567890
   WEZ_SIG_OWNER=+0987654321
   ```

All three `WEZ_SIG_*` variables must be set to enable Signal. When enabled, each wezterm tab gets a Signal group (named after the tab title) with your personal number invited. Agent output is prefixed with the harness display name (e.g. `Claude: ...`).

## Commands

All commands work in both Telegram topics and Signal groups:

| Command | Description |
|---------|-------------|
| `/list` | Show tracked panes and their harness |
| `/collab` | Toggle collab mode in the current topic/group |
| `/collab N` | Enable collab for N rounds |
| `/refresh` | Delete and recreate the current topic/group (clears all messages) |

## Local control interface

Install the local CLI:

```sh
install -m 0755 ./panetone ~/.local/bin/panetone
```

Send a message between live Panetone routes:

```sh
panetone send --from ufopedia --to wakterm "Investigate the observer bug"
```

`SOURCE` and `TARGET` are exact, case-insensitive Wakterm tab titles currently
registered by the running bridge. Panetone posts an audit message in the target
Telegram topic, switches that target's output route to Telegram, submits the
message through `wakterm cli agent send`, and prints a structured JSON receipt.
The prompt delivered to the target includes a Panetone envelope with the
resolved source and target routes, harnesses, request ID, and reply mode. This
lets the target distinguish routed work from direct user input. The source is a
locally asserted route, not cryptographic authentication of the calling pane.

Ordinary sends remain one-way. When the running Panetone bridge reports that
Wakterm supports durable agent requests, add `--return-final` for durable
asynchronous delegation to a Codex target:

```sh
panetone send --return-final --from ufopedia --to zola \
  "Finish the migration review"
```

Panetone registers the source agent and Telegram route before submitting the
prompt, then exits with `reply_pending: true`. Wakterm correlates the prompt to
its exact target session and provider turn. When that turn reaches a terminal
state, Panetone resumes from Wakterm's durable event stream and sends one
correlated callback to the source agent and source Telegram topic. Neither the
calling agent nor Panetone parses harness session files for this correlation.
The current Wakterm implementation provides this exact turn correlation for
Codex targets. Other target harnesses require equivalent observer support
before they can use return mode.

If the installed Wakterm lacks this capability, Panetone returns
`return_final_unavailable` before route refresh, Telegram, or prompt delivery.
One-way sends remain available. For an explicit report-back without monitored
callbacks, tell the target to send its summary when it decides the work is
complete:

```sh
panetone send --from ufopedia --to zola \
  'Complete the review. When done, run: panetone send --from zola --to ufopedia "<final summary>"'
```

This fallback is a second ordinary send under the target agent's semantic
control. It does not change the original request into a synchronous exchange.

Every request has a UUID idempotency key. The CLI generates one by default and
returns it in the response. Use `--id UUID` when retrying after a lost response.
The same ID and content return the stored receipt without redelivery. Different
content with the same ID returns `idempotency_conflict`. The installed Python
bridge retains completed IDs for 30 days. The Rust candidate keeps permanent
UUID tombstones, so a completed ID never becomes a new request. Pending and
indeterminate IDs do not expire in either implementation. Panetone never
retries a request left uncertain across a process failure.

The Panetone control socket defaults to
`$XDG_RUNTIME_DIR/panetone/control.sock`. It is separate from the Wakterm mux
socket. Set `PANETONE_CONTROL_SOCKET` for both the bridge and CLI to override it.
See [the control protocol](docs/control-protocol.md) for framing, state, failure,
and permission details.

## Example

See a [live collab session](https://wakamex.github.io/panetone/example/messages.html) where Claude and Codex built a repo together using Panetone — source at [wakamex/collab](https://github.com/wakamex/collab).

## Name

Claude came up with Paneetone when prompted to:

> *come up with a fun name for this bot*

> **panetone** — "pane" + "tone" (notification), sounds like panettone (the bread), and you're slicing up panes to serve them on Telegram.
