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
target/release/panetone status \
  --socket /run/user/1000/panetone/control.sock
```

Status reports current workflows, returns, inbox and outbox state, UUID
tombstones, Wakterm event state, channel availability, and supervised worker
health. It has no promotion or legacy-migration section.

Run the read-only doctor when the daemon is stopped:

```sh
target/release/panetone doctor \
  --socket /run/user/1000/panetone/control.sock \
  --journal ~/.local/state/panetone-rust/migration/panetone.sqlite3 \
  --wakterm-bin ~/.local/bin/wakterm \
  --wakterm-socket /run/user/1000/wakterm/sock
```

## Route resolution

Routes persist only the workspace title and channel bindings. Panetone resolves
the current effective title and live agent panes through Wakterm before each
admission, retry, and callback. Closing and recreating a workspace therefore
requires no Panetone repair or reconciliation command.

When a tab has multiple agent panes, a quoted channel reply targets the pane
that produced the quoted message. Otherwise the most recent pane to produce
visible output wins, followed by the lowest live pane ID.

## Launcher contract

An external launcher must use the control CLI rather than the database or the
retired `~/.config/wez-tg` files.

After Wakterm has registered the fresh agent, establish or verify its route:

```sh
route_json=$(target/release/panetone route ensure infobase \
  --socket /run/user/1000/panetone/control.sock)
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
  --timeout-ms 90000 \
  --socket /run/user/1000/panetone/control.sock
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

Normal startup discards unsent passive agent-output notifications and advances
the Wakterm event cursor to the current catalog head. It does not discard
accepted Telegram or Signal input, explicit workflow effects, busy work, or
pending final returns.

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

Telegram inbound polling retries timeouts, transport failures, rate limits, and
upstream 5xx responses in place. Backoff starts at one second, caps at 30
seconds, and honors a longer Telegram `retry_after`. Other polling errors still
terminate the critical worker and fail the daemon.

## Durable failure rules

- Never replay an indeterminate prompt merely because the daemon restarted.
- A delivering outbox item returns to pending after restart because channel
  sends use stable effect IDs and channel-level deduplication where available.
- An admission prepared without a definitive receipt becomes indeterminate.
- Preserve the database before manual repair.
- Attribute Wakterm client, mux, Telegram, Signal, and Panetone failures to one
  layer before changing code.

## Cold archive

The Python migration bundle is retained only as historical evidence. It is not
accepted by the current CLI and is not a rollback target. After Rust processed
real channel and Wakterm effects, restoring the Python snapshot would lose or
duplicate post-cutover state.
