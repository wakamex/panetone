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
- Let a request ask for a reply that the target sends by tagging a message
  `[panetone:reply]`, with reminders until it does.
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
WAK_SIG_EXTRA_ACCOUNTS=+15552222222=/run/user/1000/signal-cli-second.sock=/home/user/.local/share/signal-cli-second
```

The value holds comma-separated `ACCOUNT=SOCKET=DATA_DIR` entries, where `DATA_DIR` is the `--config` directory of that account's signal-cli daemon. Panetone gives agents the paths of received attachments under its `attachments` directory; without `DATA_DIR` it uses the default account's `signal-cli` data directory, which is wrong for a daemon that has its own. `WAK_SIG_ACCOUNT` remains the default account and serves every Signal group whose route names no account. `panetone route ensure TITLE --signal-group-id ID --signal-account +15552222222` moves a group to another configured account: messages for the group are then accepted only when they arrive on that account, and replies are sent through it. A group that two accounts have joined is therefore read and answered once, by the account its route names. To move a group between accounts, add the new account to the group, run `route ensure` with `--signal-account`, then remove the old account. Messages keep flowing through the old account until the `route ensure` and through the new one after it.

A route bound with `--signal-direct` also takes the direct messages that `WAK_SIG_OWNER` sends to the account serving its group. Such a message reaches the agent with `[direct message]` before it, and the agent's output goes back to the owner as a direct message from that account until a group or Telegram message arrives for the route. Direct messages from anyone else are dropped, and direct messages are left out of chat archives.

A route can be served by an [endchat](https://github.com/wakamex/endchat) profile instead of a Wakterm agent: `panetone route ensure TITLE --endchat-profile /absolute/profile/dir` (or `none` to undo it). Panetone then hands the route's channel messages and other agents' sends to `endchat serve` through the profile's control socket, and posts each agent message the profile appends to its `main/` log, honoring `[panetone:attach PATH]`, `[panetone:reply]` tags and `<panetone:no-reply>` as for a Wakterm agent. Reading starts at the end of the log when the route is switched, so earlier history is not posted. Each message the profile logs from a user starts or joins a turn, and each line in the profile's `turns.jsonl` ends one, so an endchat route gets reply reminders like a Wakterm agent.

Signal subscription timeouts and transport disconnects reconnect inside the inbound worker, so a signal-cli restart does not take down Panetone's control, Telegram, or outbound workers.

After authorization and routing, Panetone sends the message body unchanged except for sender labels on member-enabled Signal groups and attachment paths. An idle agent receives a normal admitted prompt. A busy agent receives immediate active-turn steering after Wakterm definitively confirms that admission did not write the prompt.
Channel, topic, update, and reply metadata remain internal and do not alter what the harness sees. Local `panetone send` delivers into a busy target's active turn the same way by default; `--queue` waits for the target to become idle instead. Signal attachments are downloaded by signal-cli; Panetone appends their absolute local paths to the message so the harness can inspect them.
Telegram documents, audio files, voice notes, videos and video notes up to 20 MB are downloaded into an `attachments/telegram`
directory beside the database before the update cursor advances. A voice note is saved as `voice.ogg` (Ogg Opus) and a video note as `video-note.mp4`, since Telegram gives them no file name; agents transcribe them themselves. Telegram
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

Add `--reply` (or its older name `--return-final`) when a reply is wanted. The target then owes the sender a reply: it answers in any turn by writing the answer as normal output with `[panetone:reply]` on its own line, and Panetone delivers that message to the sender as a cross-agent message. `[panetone:progress]` sends an update and keeps the request open. After each turn of the target that ends with a reply still owed, Panetone reminds it once, except after a turn the reminder itself started. `panetone cancel REQUEST_ID` withdraws the request, and `panetone status` lists replies still owed. A tagged reply is never posted to a Signal group that admits other members. [ADR 0006](docs/architecture/0006-tagged-replies.md) gives the design. A stable `--id UUID` makes a retry idempotent. Panetone permanently reserves completed UUIDs and never automatically retries a prompt whose admission became uncertain.

Repeat `--to` to send one message to several targets, for example `panetone send --reply --to con-argon --to re-opus "..."`. Each target gets its own request, so each owes its own reply. Instead of the full acknowledgement, the command prints one JSON line per target, in order, with `target`, `request_id`, `accepted`, `delivery_state` and `reply_pending`, or `accepted: false` and an `error`, and it fails if any target was not accepted. With `--id`, each target's request ID is a version 5 UUID of that ID and the target name, so rerunning the same command with the same `--id` delivers nothing twice.

A sender outside Wakterm, such as a remote assistant with shell access, names itself with `--as NAME`, for example `panetone send --as orch --to wakterm "..."`. The target sees `From: orch (external)`, and such a send is one-way, since there is no agent to return a final to. A script that sends notifications this way should follow the [retry rules](docs/control-protocol.md#idempotency-and-retries): stop on exit status 0, and reuse one `--id` per event so a retry never delivers twice. Use `--from ROUTE` to override the calling pane's route for the shared channel mirror and audit identity. A final callback still returns to the exact calling agent while it remains live. If that agent has exited, Panetone falls back to the selected source route's current agent.

Chat with every agent on a route at once from a terminal:

```sh
target/release/panetone chat evilcorp
```

This makes the route a room. Everyone in a room sees every message: what you type goes to each of the route's agents, and each agent's output goes to the other agents, the terminal and the route's Telegram or Signal channel. Messages from the route's channel join the room too. The first message an agent gets explains the room and tells it to answer with exactly `<panetone:no-reply>` when it has nothing to add, which keeps the room quiet. A route's only agent, such as an endchat profile, gets plain messages with no explanation, and speakers are named only once a second person has spoken. Your name defaults to your login name; `--as NAME` sets another. In the chat, `/pause` stops passing agents' messages to each other while yours still reach everyone, `/resume` restarts that, `/off` turns the room back into an ordinary route, and `/quit` leaves with the room still open. In Wakterm each agent is marked with its harness icon from the Wakterm tab bar, printed as the private-use character Wakterm draws as that icon; elsewhere, or with `PANETONE_CHAT_PLAIN` set, a coloured dot.

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
