# ADR 0003: Idempotency and delivery semantics

Status: accepted and implemented

## Context

Panetone crosses local durable state, Wakterm, and remote messaging APIs. Those systems cannot be committed in one transaction. Reliability therefore requires an explicit semantic for each side effect rather than a general claim of exactly-once delivery.

## Decision

Every control request ID is an idempotency key. Panetone hashes the normalized semantic request, including default `return_final: false` and `timeout_ms: 0` whether those fields were omitted or explicit.

Reusing an ID with the same normalized request returns its durable result without repeating a side effect. Reusing it with different semantic content returns `id_conflict`. A request is recorded as in progress before prompt delivery. A crash after possible Wakterm acceptance but before a durable receipt makes the prompt indeterminate and Panetone never retries it automatically.

The current store retains completed workflow records and compact request-ID and
semantic-hash tombstones. It has no production pruning policy. If pruning is
added after measured growth, tombstones remain permanent so an expired retry
cannot become a new prompt. Pending and indeterminate records must not be
pruned.

Delivery policies are:

| Effect | Policy | Required behavior after uncertainty |
| --- | --- | --- |
| Target Telegram audit | At least once, but required before prompt | Fail closed if visibility cannot be established |
| Wakterm target prompt | At most once | Persist indeterminate and require manual resolution |
| Audit failure annotation | At least once | Retry safely using the audit identity or add a linked failure |
| Source-agent final callback | At most once | Persist per-destination indeterminate state |
| Telegram or other final mirror | At least once | Retry durable chunks and tolerate recognizable duplicates |
| Ordinary agent output | Durable after capture | Retry captured outbox effects across daemon restarts; skip only Wakterm output first observed while Panetone was stopped unless an explicit catch-up start requests replay |
| Topic or group reconciliation | Idempotent | Recreate or rebind through the durable route |

Every durable destination has its own status, attempts, last error, timestamp, and external receipt where available. One destination cannot overwrite another destination's diagnostic.

For launcher synchronization, a visible Wakterm event has a durable
disposition in `agent_events`. `projected` means the event row, deterministic
outbox effect, and consumed cursor were committed in one transaction.
`unrouted` is stored explicitly. The supported disposition query reads this
existing state; it does not create a second delivery receipt or cursor.

A definitively busy target is not a failed or indeterminate prompt. It enters the durable `awaiting_target_idle` state described in ADR 0005. Panetone submits it only after authoritative idle admission and route revalidation.

## Consequences

Imported tombstones retain their hash provenance so old request UUIDs remain
reserved. New requests use the normalized Rust hash.

One-way `send` remains the default. `--return-final` registers an asynchronous callback and returns immediately. An explicit second `panetone send` back to the source remains the reliable semantic report-back pattern when the target should decide when work is complete.
