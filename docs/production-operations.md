# Panetone production operations

Panetone uses an enabled systemd user service and starts with the lingering user
manager.

## Loaded paths

- checkout: `/code/panetone`
- binary: `/code/panetone/target/release/panetone`
- unit: `~/.config/systemd/user/panetone.service`
- environment: `/code/panetone/.env`
- database: `~/.local/state/panetone-rust/migration/panetone.sqlite3`
- control socket: `/run/user/1000/panetone/control.sock`
- Wakterm socket: `/run/user/1000/wakterm/sock`

Panetone client commands default to `$XDG_RUNTIME_DIR/panetone/control.sock`. `PANETONE_CONTROL_SOCKET` overrides that path, and an explicit `--socket` takes precedence. The service keeps an explicit socket path so its loaded configuration is self-contained.

The service and database are user-owned. An agent can inspect and restart them
without root.

## Build and inspect

Build a committed candidate and run the locked tests:

```sh
cargo build --locked --release
cargo test --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Verify what systemd will actually load:

```sh
systemctl --user daemon-reload
systemctl --user cat panetone.service
systemctl --user show panetone.service \
  -p FragmentPath -p ExecStart -p EnvironmentFiles -p UnitFileState
sha256sum /code/panetone/target/release/panetone
```

Verify Wakterm separately. A source commit does not prove that the running mux
contains the fix:

```sh
~/.local/bin/wakterm --version
systemctl --user status wakterm-mux-server.service
```

Do not restart Wakterm merely to start Panetone. Its live panes are separate
state and need their own maintenance decision.

## Start and stop

```sh
systemctl --user start panetone.service
systemctl --user status panetone.service
journalctl --user -u panetone.service -f
```

Stop it without starting another delivery owner:

```sh
systemctl --user stop panetone.service
```

The retired `panetone.service` Python unit has been removed.

## Health

```sh
target/release/panetone status
```

Status reports current workflows, returns, inbox and outbox state, UUID
tombstones, Wakterm event state, channel availability, and supervised worker
health. It has no promotion or legacy-migration section.

Run the read-only doctor when the daemon is stopped:

```sh
target/release/panetone doctor \
  --journal ~/.local/state/panetone-rust/migration/panetone.sqlite3 \
  --wakterm-bin ~/.local/bin/wakterm \
  --wakterm-socket /run/user/1000/wakterm/sock
```

## Route resolution

Routes persist only the workspace title and channel bindings. Panetone resolves
the current effective title and live agent panes through Wakterm before each
admission, retry, and callback. Closing and recreating a workspace therefore
requires no Panetone repair or reconciliation command.

`panetone route list` returns all configured route titles with current availability and exact live agent identities and names. Use it for target discovery instead of joining Wakterm pane and catalog output. The read-only listing omits channel bindings.

At startup Panetone creates a durable route and Telegram topic for each live Wakterm title that contains an agent. Every agent with the same effective title shares that route even when the agents are in different tabs. Panetone repeats reconciliation when Wakterm reports an agent lifecycle change or visible output. An agent started manually in a shell therefore does not require `route ensure`. Empty tabs are ignored.

When a route has multiple agent panes, a quoted channel reply targets the pane that produced the quoted message. Otherwise the most recent pane to produce visible output wins, followed by the lowest live pane ID.

A Telegram or Signal message sent as a reply to an earlier message reaches the agent with a first line naming who and what it answers, such as `[replying to Melissa Young: "If it attracts me I will read the paper"]`, quoting at most 100 characters of the replied-to text's first line. A reply to the agent's own post names "you". Signal identifies a quote's author only by number or UUID, so Panetone uses the name from that author's latest message it received, or "someone". Telegram's implicit reply to a forum topic's first message is not a quote. Chat archives keep the original text without this line.

Local `panetone send` calls identify their caller with `wakterm agent caller`, so `--from` is unnecessary inside Wakterm, including tool commands of managed Codex threads, which do not inherit `WAKTERM_PANE`. Panetone derives the source route from that agent's pane and returns asynchronous finals to the exact calling agent. `--from ROUTE` overrides only the shared channel mirror and audit route while preserving the exact caller for the agent callback. Callers outside Wakterm must provide it. If the original agent exits before a final arrives, Panetone falls back to the source route's current agent.

Assistant messages and plans are forwarded to the route's selected output channel. Each standalone `[panetone:attach /absolute/path]` line outside fenced or indented code adds a file from inside the emitting harness's Wakterm-reported working directory. Panetone removes up to 10 directives, captures files of at most 10 MiB each and 50 MiB combined into the same durable outbox record, and lets the existing route preference choose Signal or Telegram. Invalid attachment requests become visible failure notices without blocking the event cursor. Memory citation blocks, `<memory-used>...</memory-used>`, are removed from forwarded output on every route, including blocks that span lines; blocks inside fenced code are kept, an unterminated block is removed to the end of the message, and a message left empty is not posted. The agent's transcript keeps them. When Wakterm reports an aborted turn with a safe nonempty detail, Panetone forwards that detail as a `Turn failed:` notice, except for reason `no_reply`, a turn that ended without a reply: a Panetone message that never started work already produces an unconfirmed-delivery notice. Completed `turn_final` events are not forwarded because their assistant message was already projected. Event identity provides the same durable deduplication as other visible output.

Wakterm `approval_requested` events are projected to the route's Telegram topic through the durable outbox. These cover managed Codex command approvals and single-choice questions, plus observer-backed Claude single-choice questions. The primary Telegram bot sends all interactive messages because it owns the inbound update cursor. A callback is accepted only from `WAK_TG_OWNER`, in the topic bound to the stored route, for the exact stored request, agent, incarnation, and advertised choice. Wakterm performs the final live-state check. It answers Codex through its app-server protocol and submits Claude selections only while the exact question remains pending in the exact session. A failure is shown as stale or unavailable.

A Claude question form whose event carries structured `questions` is posted as one message with a button per option plus Submit, "Chat about this" and Cancel. Answers are stored as `form:<request_id>` metadata as they arrive, so they survive restarts, and the message is edited to show them. An owner reply to the form message that starts with a question number records a typed answer and is not delivered to the agent. Submit resolves the form with `wakterm agent approval --choice submit --answers`; if Wakterm stops before submitting, the form stays open and the reason is shown on the tapped button.

## Launcher contract

An external launcher must use the control CLI rather than the database or the
retired `~/.config/wez-tg` files.

After Wakterm has registered the fresh agent, synchronously verify its route:

```sh
route_json=$(target/release/panetone route ensure infobase)
```

Require `result.live.status` to be `available`. Save these exact fields from
the response:

- `result.route.channels[].topic_id`
- the `result.live.agents[]` entry whose `pane_id` equals the pane just launched,
  including its `agent_id` and `incarnation_id`
- `result.event_cursor`

Send the bootstrap prompt through Wakterm only after that response. Then prove
that the resulting assistant output was durably routed:

```sh
target/release/panetone output wait \
  --route infobase \
  --agent-id "$agent_id" \
  --incarnation-id "$incarnation_id" \
  --after "$event_cursor" \
  --expect-text READY \
  --timeout-ms 90000
```

Send the real startup prompt only after this command exits zero and its JSON
result has `disposition: "projected"`. The result identifies the exact event by
sequence and event ID and includes its text. `unrouted` and `misrouted` exit
nonzero immediately. A timeout exits nonzero while the result remains
`pending`.

`projected` means Panetone assigned the event to that durable route and
committed the event, outbox effect, and cursor together. It does not mean
Telegram has already delivered the effect. The launcher needs durable capture
before the real prompt, not remote delivery confirmation.

## Offline output and Telegram pacing

Normal startup preserves already captured outbox effects and skips stale agent output first observed while Panetone was stopped. It resumes the Wakterm event cursor just before the first offline event observed in the last 10 minutes or the first question an agent is still waiting on, so a restart for a deploy or crash loses nothing and an unanswered question still reaches its channel. Older offline output is skipped. It also preserves accepted Telegram or Signal input, explicit workflow effects, busy work, and pending final returns.

For a deliberate catch-up start, set
`PANETONE_REPLAY_OFFLINE_OUTPUT=true` in `/code/panetone/.env`, restart the
service, then remove the setting after the backlog drains. Catch-up is off by
default because a long-lived passive backlog can be noisy rather than useful.

Telegram sends through each bot token are serialized at one message every 3.1
seconds, below Telegram's 20-per-minute group limit. A Telegram 429 response
extends the pause by the returned `retry_after` interval. Each outbox worker
pass attempts one item, so a flood cannot monopolize shutdown or other workers.
Visible agent output is split before persistence using Telegram's UTF-16 limit
or Signal's text limit. Every chunk has a deterministic effect ID and is
committed with the source event cursor, so restart resumes at the first unsent
chunk.

Outbound posts are retried according to whether the failed attempt may have reached the chat. Neither Telegram nor Signal lets Panetone check whether a post appeared, so a retry after such an attempt starts with `[resent] ` and a duplicate is recognizable. Unreachable endpoints, rate limits, and signal-cli's `ChatServiceInactiveException`, which it reports when its own connection to the Signal server is already closed, mean nothing was posted, so those retries are unlabeled and unlimited. Every failed attempt is logged with whether it may have posted. Timeouts, dropped connections, malformed responses, and Telegram 5xx responses may have posted; after three such attempts the item becomes indeterminate instead of risking further duplicates. Other rejections fail the item at once.

Telegram inbound polling retries timeouts, transport failures, rate limits, and
upstream 5xx responses in place. Backoff starts at one second, caps at 30
seconds, and honors a longer Telegram `retry_after`. Other polling errors still
terminate the critical worker and fail the daemon.

Signal inbound reconnects after subscription timeouts and transport disconnects with the same one-to-30-second bounded backoff. A signal-cli restart therefore does not restart Panetone or interrupt its other workers.

The event, outbox, inbox, busy-target and return workers retry a failed pass in place with the same one-to-30-second backoff instead of stopping the daemon, so a broken Wakterm event stream leaves channel input, agent sends and channel output running. While a worker fails, `panetone status` shows it as `retrying` with its last error, and it returns to `running` after a successful pass.

## Durable failure rules

- Never replay an indeterminate prompt merely because the daemon restarted.
- A delivering outbox item returns to pending after restart and counts as an
  attempt that may have posted, so its retry is labeled `[resent]`.
- An admission prepared without a definitive receipt becomes indeterminate.
- Preserve the database before manual repair.
- Attribute Wakterm client, mux, Telegram, Signal, and Panetone failures to one
  layer before changing code.

## Cold archive

The Python migration bundle is retained only as historical evidence. It is not
accepted by the current CLI and is not a rollback target. After Rust processed
real channel and Wakterm effects, restoring the Python snapshot would lose or
duplicate post-cutover state.

## Chat archives

`PANETONE_CHAT_ARCHIVES` lists routes whose Signal messages are appended to a chat archive, as comma-separated `TITLE=PATH_STEM` pairs, such as `debate=/code/debate/archive/debate`. Each message is appended to `STEM.jsonl` as one JSON object and to `STEM.txt` as one `YYYY-MM-DD HH:MM Sender: text` line, in the formats of the debate archive's `build_archive.py`. Inbound messages are appended once when first stored, with the sender's Signal name and attachment file names. An agent's post is appended as `Clod` once Signal confirms it, with the text actually sent; Panetone's own notices and suppressed no-reply responses are not. Each line is written in one write under an exclusive file lock, and a failed append is logged without affecting delivery. Rewriting the archive files while Panetone runs can drop lines appended during the rewrite.

## Route health

The `route-health` worker checks every 60 seconds for live Claude or Codex agents in a routed tab that Panetone cannot fully use: an agent Wakterm detected but did not register, so messages in that route's channel are not delivered to it, and a registered agent whose output Wakterm cannot read, so its replies are not forwarded. A problem that persists across two checks is posted once in that route's channel as `[Agent problem]`, and as `[Resolved]` when it clears. Reported problems survive restarts in store metadata, and `panetone status` lists them under `degraded_routes`.

## Watch for silent failures

`scripts/watch.py` runs every 5 minutes from the user units `panetone-watch.service` and `panetone-watch.timer` in `~/.config/systemd/user/`. It reads the store read-only and Wakterm's agent list, and sends one Panetone message to the `panetone` route listing problems it has not reported before:

- channel posts that failed, or that stopped retrying after attempts that may have posted
- inbound messages that are unconfirmed or still undelivered after 10 minutes
- agent-to-agent sends and final returns that are unconfirmed
- Panetone workers that are not running, once per failure
Reported problems are recorded in `~/.local/state/panetone-rust/watch.json` before the message is sent, so an unconfirmed report is never repeated, and the watcher ignores its own reports. Deleting that file makes the next run record a silent baseline. Run the tests with `python3 -m unittest test_watch` from `scripts/`.
