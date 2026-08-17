# ADR 0004: Durable storage, migrations, and shutdown

Status: accepted and implemented through Phase 4

## Context

The Python service spreads durable truth across two JSON files and two SQLite databases, while some queues and receipts exist only in memory. Blocking persistence and subprocess work also share the event-loop path with network adapters.

## Decision

Rust Panetone owns one SQLite database with an explicit monotonic schema version and one dedicated blocking writer task. Domain and adapter tasks submit typed storage commands. Reads may gain a small pool only after measurement shows the single owner is insufficient.

Startup applies migrations before accepting control requests or inbound channel work. Each migration is transactional and idempotent when possible. The daemon refuses a database with a newer unsupported schema. Migration logs and manifests contain schema versions, row counts, and hashes but no message bodies or credentials.

Legacy migration is offline and copy-first. It reads the JSON state, pending
output, Signal database, and control journal without modifying them. One atomic
private output bundle contains exact JSON copies, consistent SQLite backups,
the new database written in one transaction, and a deterministic manifest.
Rerunning against the same output verifies source, snapshot, and target hashes.
A conflicting canonical route is an error. State that has only ephemeral legacy
identity is retained as explicitly rollback-only or reconciliation-required
data rather than guessed into a live route, agent, cursor, or callback.

Imported Python control hashes retain a distinct provenance marker. The target
accepts only the finite omitted or explicit default encodings that were
semantically equivalent in the Python protocol. It never converts a provider
file cursor into a Wakterm event cursor, and every nonterminal external control
operation is migrated as indeterminate.

One supervisor owns every long-running task. It records handles, propagates fatal failures into health, and coordinates cancellation. Shutdown follows this order:

1. stop accepting new control and inbound work
2. cancel or quiesce adapter receivers
3. let durable queues finish only work whose deadline fits the shutdown budget
4. persist safe checkpoints and classify uncertain in-flight effects as indeterminate
5. flush and close the store
6. remove the control socket only after verifying its inode

No async reactor task performs blocking SQLite or subprocess waits directly. Queues are bounded. Work that must survive pressure is persisted before it enters an in-memory queue.

## Consequences

The Python formats remain production truth until Phase 4 migration. Their inventory and restart behavior are frozen in the Phase 1 parity ledger. The Rust implementation must pass crash-boundary tests before it may read production state.
