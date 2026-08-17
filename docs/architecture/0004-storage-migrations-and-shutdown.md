# ADR 0004: Durable storage, migrations, and shutdown

Status: accepted for Phase 1

## Context

The Python service spreads durable truth across two JSON files and two SQLite databases, while some queues and receipts exist only in memory. Blocking persistence and subprocess work also share the event-loop path with network adapters.

## Decision

Rust Panetone owns one SQLite database with an explicit monotonic schema version and one dedicated blocking writer task. Domain and adapter tasks submit typed storage commands. Reads may gain a small pool only after measurement shows the single owner is insufficient.

Startup applies migrations before accepting control requests or inbound channel work. Each migration is transactional and idempotent when possible. The daemon refuses a database with a newer unsupported schema. Migration logs and manifests contain schema versions, row counts, and hashes but no message bodies or credentials.

Legacy migration is offline and copy-first. It reads the JSON state, pending output, Signal database, and control journal without modifying them, writes the new database in one transaction, and validates counts, identities, cursors, payload hashes, and delivery states. Ambiguity is an error, not a guessed mapping.

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
