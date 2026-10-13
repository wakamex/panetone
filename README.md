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
- Attach workspace files to normal agent output through the same selected Telegram or Signal route.
- Create a durable route and Telegram topic when a live Wakterm agent title is
  first discovered, including agents started manually in a shell.
- Establish or inspect routes synchronously through the supported control API.
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

Passive agent output produced while Panetone is stopped is skipped on the next startup unless it was observed in the last 10 minutes; a question an agent is still waiting on is always delivered. Output already captured in the outbox, accepted channel input, explicit workflows, and final returns remain durable and replay normally. Set `PANETONE_REPLAY_OFFLINE_OUTPUT=true` before a deliberate catch-up start. Telegram output is paced per bot token at one message every 3.1 seconds and honors longer server `retry_after` responses. Telegram polling retries rate limits and temporary transport or upstream errors inside the inbound worker, so those failures do not restart Panetone.

Managed Codex command approvals and blocking single-choice questions, plus observer-backed Claude single-choice questions, appear in the route's Telegram topic with Wakterm's advertised choices as inline buttons. Only `WAK_TG_OWNER` can resolve them. Panetone recovers the exact agent and incarnation from the durable event and delegates resolution to Wakterm. Codex is answered through its native app-server request. Wakterm validates a Claude question against the exact live transcript and submits the selected label to that pane's native question UI. Repeated buttons, replaced agents, and provider-resolved prompts are rejected as stale. The primary Telegram bot owns these interactive messages so the same bot also receives their callback updates.

A Claude question form with several questions or multi-select answers appears as one message listing every question, with a button per option. Tapping records an answer and updates the message, and a reply to the message such as `1: your answer` records a typed answer. Submit sends all answers through Wakterm, which checks Claude's review screen before submitting; "Chat about this" and Cancel close the form the other ways. This needs Wakterm's `question_form_answers.v1`; without it, a form is posted as text to answer in the agent's pane.

Signal is optional. All three variables are required when it is enabled:

```text
WAK_SIG_SOCKET=/run/signal-cli/socket
WAK_SIG_ACCOUNT=+15550000000
WAK_SIG_OWNER=+15551111111
```

Further Signal accounts, each with its own signal-cli daemon, let different routes appear in their groups under different bot names:

```text
WAK_SIG_EXTRA_ACCOUNTS=+15552222222=/run/user/1000/signal-cli-second.sock
```

The value holds comma-separated `ACCOUNT=SOCKET` pairs. `WAK_SIG_ACCOUNT` remains the default account and serves every Signal group whose route names no account. `panetone route ensure TITLE --signal-group-id ID --signal-account +15552222222` moves a group to another configured account: messages for the group are then accepted only when they arrive on that account, and replies are sent through it. A group that two accounts have joined is therefore read and answered once, by the account its route names. To move a group between accounts, add the new account to the group, run `route ensure` with `--signal-account`, then remove the old account. Messages keep flowing through the old account until the `route ensure` and through the new one after it.

Signal subscription timeouts and transport disconnects reconnect inside the inbound worker, so a signal-cli restart does not take down Panetone's control, Telegram, or outbound workers.

After authorization and routing, Panetone sends the message body unchanged except for sender labels on member-enabled Signal groups and attachment paths. An idle agent receives a normal admitted prompt. A busy agent receives immediate active-turn steering after Wakterm definitively confirms that admission did not write the prompt.
Channel, topic, update, and reply metadata remain internal and do not alter what the harness sees. Local `panetone send` delivers into a busy target's active turn the same way by default; `--queue` waits for the target to become idle instead. Signal attachments are downloaded by signal-cli; Panetone appends their absolute local paths to the message so the harness can inspect them.
Telegram documents up to 20 MB are downloaded into an `attachments/telegram`
directory beside the database before the update cursor advances. Telegram
photos use the largest available image size and the same durable download path.
Their absolute paths are appended to the message. Attachment-only Signal and
Telegram messages are supported.

To attach files to an outbound response, the harness places one standalone line per file anywhere in its normal assistant message:

```text
[panetone:attach /absolute/path/to/image.png]
[panetone:attach /absolute/path/to/another-image.jpg]
```

Panetone removes attachment directives outside fenced or indented code blocks, captures the files into the durable outbox, and sends them through the route's already selected Signal or Telegram binding. The harness does not name or inspect the transport. Up to 10 attachment directives are accepted per assistant message. Any regular file the agent's user can read may be attached; each file may be at most 10 MiB, and the combined payload may be at most 50 MiB. Multiple JPEG, PNG, and WebP files become one Telegram media group; a single image uses Telegram's photo presentation; non-image files use its document presentation. Signal receives the files as native attachments on one message. A missing, unreadable, oversized, or out-of-workspace file makes the whole attachment set a visible `Attachment unavailable:` notice and does not stall later output.

Signal routes are owner-only by default. A route bound with `panetone route ensure TITLE --signal-group-id ID --signal-allow-members` accepts every member of that exact Signal group and gives the harness the minimal sender context `<first name> says: <message>`. Unknown groups remain ignored.
On every route, an assistant response consisting exactly of `<panetone:no-reply>` after trimming whitespace is durably recorded as suppressed and is not sent to Signal or Telegram. Normal responses and failure notices remain visible.

## Commands

Client commands use `$XDG_RUNTIME_DIR/panetone/control.sock` by default. `PANETONE_CONTROL_SOCKET` overrides that path, and an explicit `--socket` takes precedence over both.

Inspect the running daemon:

```sh
target/release/panetone status
```

List every configured send target and its current availability:

```sh
target/release/panetone route list
```

The JSON result is sorted by title. An available entry includes every live agent currently sharing that route, with its Wakterm `name`. Channel bindings are omitted from the listing. When a route has several live agents, `panetone send --to` takes one agent's name instead of the route title.

Manual Wakterm agents are discovered automatically from their effective title.
Use `route ensure` when a launcher needs synchronous confirmation that the
route exists before it sends a bootstrap prompt:

```sh
target/release/panetone route ensure infobase
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
  --expect-text READY
```

The wait exits successfully only for `projected`. It fails immediately for
`unrouted` or `misrouted` output. This reads Panetone's existing durable event
disposition and does not use the SQLite file or retired Python state.

Send a one-way message from a Wakterm agent. The CLI asks `wakterm agent caller` for the exact calling agent, which works for pane harnesses and for managed Codex tool commands that run outside the pane, and derives the route from that agent:

```sh
target/release/panetone send \
  --to wakterm \
  "Investigate the observer bug"
```

An idle target starts a normal new turn. A busy target receives the message in its active turn, so a notice is not held until a long turn ends. Wait for the target to become idle instead when the message should start its own turn after the current work:

```sh
target/release/panetone send --queue \
  --to wow-sol \
  "When you finish, rerun the benchmark on the corrected data"
```

Add `--return-final` when a reply is wanted. The send command exits after target admission. For an idle target, Panetone mirrors the turn's final response to the source route's channel when it arrives and delivers the agent callback when the source agent is idle. A busy target is instead asked, in the message header, to reply with its own `panetone send`, and the acknowledgement says so with `reply_mode: "message"`. A stable `--id UUID` makes a retry idempotent. Panetone permanently reserves completed UUIDs and never automatically retries a prompt whose admission became uncertain.

A sender outside Wakterm, such as a remote assistant with shell access, names itself with `--as NAME`, for example `panetone send --as orch --to wakterm "..."`. The target sees `From: orch (external)`, and such a send is one-way, since there is no agent to return a final to. Use `--from ROUTE` to override the calling pane's route for the shared channel mirror and audit identity. A final callback still returns to the exact calling agent while it remains live. If that agent has exited, Panetone falls back to the selected source route's current agent.

Run a side-effect-free local check against the loaded Wakterm service:

```sh
target/release/panetone doctor \
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
