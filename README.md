# Panetone

Panetone routes messages between Wakterm workspaces and Telegram or Signal.
Each durable route stores a workspace title and its Telegram topic, Signal
group, or both. Panetone resolves the current live agent through Wakterm before
each admission instead of persisting a process identity or writing directly to
terminal panes or provider session files.

The current implementation is Rust. The Python bridge is retained only in Git
history, and its stopped state snapshot is a cold historical artifact rather
than a rollback target.

## Current scope

- Observe agent lifecycle and output through Wakterm Agent API v1.
- Deliver output to Telegram and Signal.
- Establish and inspect fresh title-to-Telegram routes through the supported
  control API.
- Expose whether exact Wakterm assistant output was durably projected or left
  unrouted.
- Resolve routes from Wakterm's current effective tab titles.
- In a multi-agent tab, route replies to the agent that produced the quoted
  message, otherwise the last agent that produced output, otherwise the first
  live agent pane.
- Treat Debate group chats as ordinary routes with Signal bindings.
- Send one-way local messages between routes.
- Support durable `--return-final` callbacks when Wakterm advertises the
  required capability.
- Preserve accepted input, explicit pending output, event cursors, request
  UUIDs, and return state in one SQLite database.

Slack is removed.

## Build and verify

```sh
cargo build --locked --release
cargo test --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
```

The production user unit runs `target/release/panetone` from this checkout and
uses the database at
`~/.local/state/panetone-rust/migration/panetone.sqlite3`. Inspect the exact
loaded unit before starting it:

```sh
systemctl --user cat panetone.service
systemctl --user start panetone.service
systemctl --user status panetone.service
journalctl --user -u panetone.service -f
```

The user unit is enabled and starts with the lingering user systemd manager.

## Configuration

The user service reads `/code/panetone/.env`. Telegram requires the chat,
Claude token, and authorized owner's numeric user ID together. Missing owner
configuration fails startup. Additional harness tokens select the sending
identity for output from that harness.

```text
WAK_TG_CHAT=-1000000000000
WAK_TG_TOKEN_CLAUDE=...
WAK_TG_TOKEN_CODEX=...
WAK_TG_TOKEN_GEMINI=...
WAK_TG_TOKEN_OPENCODE=...
WAK_TG_OWNER=123456789
```

Passive agent output produced while Panetone is stopped is skipped on the next
startup. Accepted channel input, explicit workflows, and final returns remain
durable and replay normally. Set `PANETONE_REPLAY_OFFLINE_OUTPUT=true` before a
deliberate catch-up start. Telegram output is paced per bot token at one message
every 3.1 seconds and honors longer server `retry_after` responses.

Signal is optional. All three variables are required when it is enabled:

```text
WAK_SIG_SOCKET=/run/signal-cli/socket
WAK_SIG_ACCOUNT=+15550000000
WAK_SIG_OWNER=+15551111111
```

After authorization and routing, Panetone admits the message body unchanged.
Channel, topic, sender, update, and reply metadata remain internal and do not
alter what the harness sees.

## Commands

Inspect the running daemon:

```sh
target/release/panetone status \
  --socket /run/user/1000/panetone/control.sock
```

Ensure a fresh live workspace has a durable route and Telegram topic:

```sh
target/release/panetone route ensure infobase \
  --socket /run/user/1000/panetone/control.sock
```

The result includes the exact live agent and incarnation plus an event-cursor
baseline. After sending a bootstrap prompt through Wakterm, wait until its
first assistant output is durably projected:

```sh
target/release/panetone output wait \
  --route infobase \
  --agent-id detected-pane-14 \
  --incarnation-id INCARNATION_ID \
  --after EVENT_CURSOR \
  --expect-text READY \
  --socket /run/user/1000/panetone/control.sock
```

The wait exits successfully only for `projected`. It fails immediately for
`unrouted` or `misrouted` output. This reads Panetone's existing durable event
disposition and does not use the SQLite file or retired Python state.

Send a one-way message between exact, case-insensitive route titles:

```sh
target/release/panetone send \
  --from ufopedia \
  --to wakterm \
  --socket /run/user/1000/panetone/control.sock \
  "Investigate the observer bug"
```

Add `--return-final` when a correlated completion callback is wanted. A stable
`--id UUID` makes a retry idempotent. Panetone permanently reserves completed
UUIDs and never automatically retries a prompt whose admission became
uncertain.

Run a side-effect-free local check against the loaded Wakterm service:

```sh
target/release/panetone doctor \
  --socket /run/user/1000/panetone/control.sock \
  --journal ~/.local/state/panetone-rust/migration/panetone.sqlite3 \
  --wakterm-bin ~/.local/bin/wakterm \
  --wakterm-socket /run/user/1000/wakterm/sock
```

## Storage

Schema version 7 contains eight tables:

- `routes`
- `idempotency_tombstones`
- `workflows`
- `return_deliveries`
- `outbox`
- `inbox`
- `metadata`
- `agent_events`

The Python migration bundle remains outside the runtime database as a cold
archive. Panetone has no Python migration command, promotion hold, per-route
promotion policy, legacy disposition workflow, or operator replay journal.

## Development Wakterm

Run adapter experiments against an isolated development mux:

```sh
dev/wakterm-dev build
dev/wakterm-dev serve
```

Probe it from another terminal with `dev/wakterm-dev cli agent capabilities`.
The development launcher does not manage or restart the production mux.

## Name

Panetone combines pane and tone and sounds like panettone.
