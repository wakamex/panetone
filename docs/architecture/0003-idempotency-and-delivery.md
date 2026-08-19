# ADR 0003: Idempotency and delivery semantics

Status: accepted for Phase 1

## Context

Panetone crosses local durable state, Wakterm, and remote messaging APIs. Those systems cannot be committed in one transaction. Reliability therefore requires an explicit semantic for each side effect rather than a general claim of exactly-once delivery.

## Decision

Every control request ID is an idempotency key. Panetone hashes the normalized semantic request, including default `return_final: false` and `timeout_ms: 0` whether those fields were omitted or explicit.

Reusing an ID with the same normalized request returns its durable result without repeating a side effect. Reusing it with different semantic content returns `id_conflict`. A request is recorded as in progress before prompt delivery. A crash after possible Wakterm acceptance but before a durable receipt makes the prompt indeterminate and Panetone never retries it automatically.

Large completed records may be pruned after 30 days in the current Python journal. Pending and indeterminate records are never removed to meet the size target. The Rust store will retain compact request-ID and semantic-hash tombstones permanently after pruning payloads and response detail, so an expired retry can never become a new prompt. Until that migration, Python's completed UUID idempotency expires after 30 days.

Delivery policies are:

| Effect | Policy | Required behavior after uncertainty |
| --- | --- | --- |
| Target Telegram audit | At least once, but required before prompt | Fail closed if visibility cannot be established |
| Wakterm target prompt | At most once | Persist indeterminate and require operator resolution |
| Audit failure annotation | At least once | Retry safely using the audit identity or add a linked failure |
| Source-agent final callback | At most once | Persist per-destination indeterminate state |
| Telegram or other final mirror | At least once | Retry durable chunks and tolerate recognizable duplicates |
| Ordinary agent output | At least once | Retry from durable output and cursor state |
| Topic or group reconciliation | Idempotent | Recreate or rebind through the durable route |

Every durable destination has its own status, attempts, last error, timestamp, and external receipt where available. One destination cannot overwrite another destination's diagnostic.

A definitively busy target is not a failed or indeterminate prompt. It enters the durable `awaiting_target_idle` state described in ADR 0005. Panetone submits it only after authoritative idle admission and route revalidation.

## Consequences

The existing Python hash distinguishes omitted defaults from explicit defaults. The conformance suite records both current behavior and the intended normalized behavior. Production compatibility must be handled deliberately before changing hashes for journal entries that already exist.

One-way `send` remains the default. `--return-final` registers an asynchronous callback and returns immediately. An explicit second `panetone send` back to the source remains the reliable semantic report-back pattern when the target should decide when work is complete.
