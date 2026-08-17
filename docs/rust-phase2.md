# Rust Phase 2 offline core

Phase 2 is an offline implementation. It has no production Telegram, Signal,
Slack, Debate, or Wakterm connection code and it was not installed or started as
a service. The fakes execute the same typed workflow boundaries and record every
attempt so restart and effect-count assertions remain meaningful.

## Package and commands

Panetone remains one Cargo package and one binary:

```text
panetone daemon --socket PATH --journal PATH --effect-log PATH \
  --wakterm-fixture PATH --profile current
panetone send --socket PATH --from SOURCE --to TARGET [--id UUID] MESSAGE
panetone status --socket PATH --json
panetone doctor --socket PATH --journal PATH --wakterm-fixture PATH
panetone migrate --help
```

`daemon` is deliberately limited to offline fake operation in Phase 2. The
`current` Wakterm profile supports catalog, exact prompt admission, and the
return-request terminal stream, but does not start a general event consumer.
`future-events` is fixture-only and exercises ordered event resumption,
`cursor_too_old`, lifecycle changes, observer failures, and compatibility
rejection. It is validation input, not a claim that live Wakterm advertises the
capability.

Return admission also consumes the golden observer-readiness classification. A
missing baseline cursor is `observer_failure`, `definitive: true`, and
`prompt_written: false`. It is not target busy and it is not an uncertain pane
write. Choosing a one-way fallback remains a caller decision.

The hidden `conformance-backend` command implements the replaceable backend
interface used by `tests/run_control_conformance.py`. It is not an operator
interface.

The Phase 2 `migrate` placeholder has been replaced by the copy-only Phase 4
bundle command. See [the Phase 4 migration interface](rust-phase4.md) for its
required source paths, refusal rules, output layout, and restore procedure.

## Durable behavior

The store is owned by one blocking thread behind a bounded Tokio channel. It
uses mode `0600`, WAL, full synchronous commits, foreign keys, explicit schema
versioning, and refuses newer schemas and symlink database paths. The control
runtime directory is mode `0700`, the socket is mode `0600`, Linux peers must
have the daemon UID, and cleanup is tied to the inode created by that daemon.

Request UUIDs have permanent compact tombstones. Pruning can remove large
terminal workflow and fully delivered return payloads, but it cannot remove
queued, failed, pending, or indeterminate work. The 64 MiB journal value is a
pruning trigger rather than a SQLite hard limit, because a hard limit could
prevent Panetone from recording new uncertain or pending state. Completed UUID
idempotency therefore does not expire in the Rust store even after its payload
is pruned. Phase 2 validates the pruning selection offline; scheduling it with
the live service remains part of adapter promotion.

Target prompts and source-agent callbacks are at-most-once. The workflow writes
an in-flight state before admission, and a restart in an uncertain window makes
that effect indeterminate without automatic redelivery. A definitive busy and
no-write receipt is the only admission result that queues a retry. It retains
the same effect ID and exact fields. Before a target retry, Panetone resolves the
same durable route again and uses its fresh exact agent and incarnation IDs.

Messaging mirrors are at-least-once and carry stable effect IDs. A visible
target audit precedes prompt admission. Linked queued, submitted, or delivery
failed annotations prevent a pending arrow from being mistaken for confirmed
delivery. Async final results are durably registered before fanout, mirrored to
the source channel, and admitted to the persisted exact source identity. A busy
source queues the same callback ID. A missing source leaves the result durable
and visible in its channel.

## Validation

The focused Rust suites cover:

- pure workflow, route, callback, channel, formatting, and retry state machines
- schema creation, permissions, WAL reopen, unsupported versions, compare-and-
  swap transitions, permanent tombstones, and safe pruning
- malformed, oversized, stale, live, symlink, permission, and inode-replacement
  control socket cases
- current and fixture-only future Wakterm capabilities and golden events
- all four recording channel adapters
- busy target and callback queues, source disappearance, unrelated finals,
  audit failure, and linked audit states
- injected crashes after prompt and callback effects with exact effect counts
- supervisor failure propagation, process startup, status, doctor, graceful
  SIGTERM, and the target control-v1 black-box conformance profile

Run the offline gates with:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
python3 tests/run_control_conformance.py \
  --profile target \
  --backend 'target/debug/panetone conformance-backend'
```
