# ADR 0004: Durable storage, schema upgrades, and shutdown

Status: accepted and implemented through schema version 7

## Context

The Python service spreads durable truth across two JSON files and two SQLite databases, while some queues and receipts exist only in memory. Blocking persistence and subprocess work also share the event-loop path with network adapters.

## Decision

Rust Panetone owns one SQLite database with an explicit monotonic schema version and one dedicated blocking writer task. Domain and adapter tasks submit typed storage commands. Reads may gain a small pool only after measurement shows the single owner is insufficient.

Startup validates the schema before accepting control requests or inbound
channel work. Fresh databases are created at schema version 7. Schema 6
upgrades transactionally by removing persisted route agent and status fields.
Older nonzero schemas are rejected.

Schema version 7 is the current post-cutover boundary. Routes contain only
their UUID, effective workspace title, and channel bindings. Debate is a named
route with a Signal binding, not a separate transport.

The original copy-first Python migration bundle remains a cold archive outside
the runtime database. Its conversion implementation and CLI were removed after
the owner permanently retired Python. Future upgrades migrate only supported
Rust schemas.

One supervisor owns every long-running task. It records handles, propagates fatal failures into health, and coordinates cancellation. Shutdown follows this order:

1. stop accepting new control and inbound work
2. cancel or quiesce adapter receivers
3. let durable queues finish only work whose deadline fits the shutdown budget
4. persist safe checkpoints and classify uncertain in-flight effects as indeterminate
5. flush and close the store
6. remove the control socket only after verifying its inode

No async reactor task performs blocking SQLite or subprocess waits directly. Queues are bounded. Work that must survive pressure is persisted before it enters an in-memory queue.

## Consequences

The Rust database is the only production truth. Restoring the old Python
snapshot after Rust-owned effects would lose or duplicate state. Old schema
copies remain readable as cold archives but cannot be opened by Panetone.
