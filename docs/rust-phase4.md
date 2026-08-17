# Rust Phase 4 offline migration

Phase 4 converts a stopped Python deployment into one private, atomic migration
bundle. It never edits, renames, opens for writing, or prunes a legacy source.
It does not install the Rust binary, connect a channel, submit a prompt, stop a
service, or promote the resulting database.

## Command

Stop Python Panetone deliberately, then provide every source path explicitly:

```sh
panetone migrate \
  --state /path/to/state.json \
  --pending /path/to/pending_sends.json \
  --control-journal /path/to/control-journal.sqlite3 \
  --signal-database /path/to/signal.sqlite3 \
  --legacy-control-socket /run/user/1000/panetone/control.sock \
  --output /private/path/panetone-migration
```

Omit `--signal-database` only when the Python deployment never created one.
The output parent must already be a real private directory. Migration refuses
symlink sources, a live legacy control socket, malformed state, conflicting
case-folded routes, an incomplete existing output, or changed source hashes.
It checks the legacy socket and all source hashes again after creating the
snapshot, so a service that starts or state that changes during the copy fails
the run.

The command creates the complete result under a private temporary sibling and
renames that directory into place only after every database transaction, hash,
file sync, and directory sync succeeds. A failed run removes only its unique
temporary directory. It never replaces an existing output.

## Bundle

```text
panetone-migration/
  panetone.sqlite3
  migration-manifest.json
  legacy/
    state.json
    pending_sends.json
    control-journal.sqlite3
    signal.sqlite3
```

The bundle directory and `legacy` directory are mode `0700`. Every file is mode
`0600`. JSON snapshots preserve the exact source bytes. SQLite snapshots use
SQLite's online backup API against read-only connections, so they contain a
consistent logical copy even when the source previously used WAL.

The deterministic manifest records source logical hashes, snapshot file
hashes, migrated row counts, the target database logical hash, warnings, and
the target schema version. Repeating the command with the same sources and
output verifies all three artifacts and returns `reused: true`. Repeating into
a second output produces an identical manifest. An already completed UUID that
Python expired before the snapshot is unknowable; the manifest states this
boundary explicitly.

## Mapping and refusal rules

- Telegram and Signal title bindings become stable UUIDv5 Panetone routes.
  Routes retain channel bindings but start unavailable with no agent binding.
  Phase 5 must reconcile each route through a fresh Wakterm catalog. Pane and
  tab IDs never become durable identity.
- Pending Telegram, Signal, Slack, and Debate chunks become canonical pending
  outbox work with deterministic effect IDs. A route ID is attached only when
  the title or channel destination identifies exactly one migrated route.
- Every Signal row remains in the canonical Signal archive. Only accepted,
  undelivered, non-command incoming text becomes pending inbox work. The older
  JSON mute history remains archived, matching Python's non-mention behavior.
- Provider paths, file identities, offsets, and parser cursors are retained as
  rollback-only metadata. Migration never creates `wakterm_event_cursor`.
- Every Python control UUID becomes a permanent tombstone with
  `python_control_v1` hash provenance. Since Python did not retain the request
  message, same-content replay compares the incoming command against only the
  finite omitted or explicit default forms accepted by the old protocol.
  Any message, route, callback, or non-default timeout difference conflicts.
- Python `in_progress`, `audit_posted`, and `delivering` control requests become
  indeterminate. They are never submitted automatically. Terminal control rows
  retain their terminal state and response in a provenance table.
- Legacy return rows retain result, source, target, per-destination state, and
  the shared legacy error. A delivering destination becomes indeterminate.
  Returns remain staged as reconciliation-required rather than receiving an
  invented live source identity.
- Stable last-channel and Signal mute decisions are retained by route ID.
  Collaboration and mute state keyed only by ephemeral tab IDs remains
  rollback-only and appears as a manifest warning.

The migrated database exposes legacy indeterminate controls and unresolved
returns in both `status` and `doctor`. `doctor` remains degraded until those
records receive explicit operator resolution during promotion.

## Restore drill

Do not run Python directly against the only bundle. Copy `legacy/` into a fresh
private restore directory, preserve mode `0600`, point the Python environment at
those files, and start the old binary and lockfile. The permanent test performs
this copy, opens the restored control journal through Python's current
`ControlJournal`, reads both JSON formats, and reads the Signal archive. The
source files are compared again afterward to prove that migration did not
modify them.

Phase 5 owns the deliberate production stop, final snapshot, migration,
installation, reconciliation, canary, and rollback. Phase 4 evidence cannot be
used as live delivery evidence.

## Validation

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
uv --no-config run --locked --script tests/run_python_suite.py
```

Dependency license and RustSec advisory checks remain required.
