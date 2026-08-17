# Panetone production operations

This runbook applies to the Rust system service. Phase 5A prepares and rehearses
it. Phase 5B is the separate live cutover.

## Files and ownership

- binary: `/usr/local/bin/panetone`, root-owned mode `0755`
- unit: `/etc/systemd/system/panetone.service`, root-owned mode `0644`
- credentials: `/etc/panetone/panetone.env`, root-owned mode `0600`
- database: `/var/lib/panetone/panetone.sqlite3`, `mihai:mihai` mode `0600`
- control socket: `/run/panetone/control.sock`, mode `0600` in a mode `0700`
  runtime directory
- Wakterm socket: `/run/wakterm/sock`, separate from the Panetone socket
- installation evidence: `/var/lib/panetone/install-evidence.json`

The unit runs as `mihai`, has no source checkout or hot reload in `ExecStart`,
and uses the system-labelled `/usr/local/bin/panetone`. It requires the Wakterm
mux system service under the same system manager. Move Wakterm separately and
restore its agents before scheduling Panetone promotion.

## Configuration

The environment file can contain:

```text
WEZ_TG_CHAT=-1000000000000
WEZ_TG_TOKEN_CLAUDE=...
WEZ_TG_TOKEN_CODEX=...
WEZ_TG_TOKEN_GEMINI=...
WEZ_TG_TOKEN_OPENCODE=...
WEZ_TG_OWNER=123456789
WEZ_SIG_SOCKET=/run/signal-cli/socket
WEZ_SIG_ACCOUNT=+15550000000
WEZ_SIG_OWNER=+15551111111
WEZ_SLACK_BOT_TOKEN=xoxb-...
WEZ_SLACK_APP_TOKEN=xapp-...
```

Telegram requires a chat and Claude token together. Other harness tokens select
the visible output identity. Signal is enabled only when its socket, account,
and owner are all present. Slack output uses the bot token and Socket Mode uses
the app token. `PANETONE_SLACK_SOCKET_URL` is a test-only alternative to the app
token. Debate needs no additional variable because it is a Signal group route.

## Side-effect-free candidate check

Build and check an exact committed candidate without sudo:

```sh
cargo build --locked --release
deploy/install-system-service.sh \
  --binary target/release/panetone \
  --database /path/to/rehearsal/panetone.sqlite3
```

For the full permanent test and release gate:

```sh
dev/phase5a-rehearsal /path/to/rehearsal/panetone.sqlite3
```

Neither command installs, starts, stops, polls, admits, or sends.

## Held install

Only during the labeled Phase 5B window, after Python is stopped and the final
migration bundle and rollback snapshots are verified:

```sh
sudo /bin/bash deploy/install-system-service.sh \
  --binary target/release/panetone \
  --database /path/to/final-bundle/panetone.sqlite3 \
  --environment /path/to/panetone.env \
  --start-held
```

The installer refuses an active user Panetone service, a missing system Wakterm
service, an existing Rust state database, a non-0600 environment file, or an
unsafe partial installation. A failed held start restores the previous binary
and unit before any route is released.

## Promotion commands

Use a stable `--id UUID` for every command and save each structured response.
Run them in this order while the global hold remains active:

```sh
panetone operator --id UUID --socket /run/panetone/control.sock \
  baseline-event

panetone operator --id UUID --socket /run/panetone/control.sock \
  baseline-telegram

panetone operator --id UUID --socket /run/panetone/control.sock \
  reconcile-route ROUTE

panetone operator --id UUID --socket /run/panetone/control.sock \
  enable-route ROUTE
```

If an existing route identity changes, the first reconciliation leaves it in
`reconciliation_required`. Review the old and new identities, then repeat with
a new operation UUID and `--replace-identity` only when the replacement is
intentional.

Resolve every staged legacy record before release. Examples:

```sh
panetone operator --id UUID --socket /run/panetone/control.sock \
  dispose-legacy control REQUEST_ID no-replay \
  --evidence 'inspected stopped Python journal; delivery remains uncertain'

panetone operator --id UUID --socket /run/panetone/control.sock \
  dispose-legacy debate EFFECT_ID map-debate-to-signal \
  --route ROUTE --expected-legacy-destination OLD_CHAT_ID \
  --evidence 'old Debate destination matched this exact Signal group'
```

Release only the reviewed canary:

```sh
panetone operator --id UUID --socket /run/panetone/control.sock release
```

If status reports `event_cursor_gap`, keep the service held. Review the missing
sequence interval, fresh catalog, channel state, and any output that may need a
manual disposition. Then acknowledge that exact gap and release separately:

```sh
panetone operator --id UUID --socket /run/panetone/control.sock \
  acknowledge-event-gap REQUESTED_AFTER_SEQUENCE \
  --evidence 'reviewed retained interval, catalog snapshot, and channel state'
```

Then run the Phase 5B live gates before enabling another route. Ordinary send
is one-way. For explicit report-back, the original prompt tells the target to
run a second `panetone send` when it decides the work is complete.

## Rollback boundary

Before the first Rust channel acknowledgement, prompt admission, accepted
inbound message, or event-cursor advance, stop Rust and restore Python from the
verified legacy snapshots. The installer automates rollback only for a failed
held start.

After any Rust-owned effect, do not blindly restore the old snapshots. Preserve
the Rust database and reconcile every post-cutover inbox, outbox, control,
callback, and cursor delta. Prefer forward recovery.
