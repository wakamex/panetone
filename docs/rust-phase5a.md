# Rust Phase 5A completion contract

Phase 5A makes the Rust candidate deployable and rehearses promotion without
changing the production service. It does not authorize the Phase 5B cutover.

The candidate is one `panetone` binary. Its production `daemon` uses only the
live Wakterm Agent API and configured Telegram, Signal, and Slack adapters. The
old fixture-backed daemon remains available only as the hidden
`conformance-backend` test command. Debate has no transport or Telegram bot. A
Debate route is an ordinary Signal group binding.

## Wakterm boundary

Production startup requires the live capabilities `catalog.v1`,
`prompt_admission.v1`, `return_request_terminal_stream.v1`, and
`event_stream.v1`. Panetone reads the versioned Wakterm CLI boundary and does
not inspect provider stores, TTY processes, or transcript files.

The producer boundary is Wakterm commit
`41e1ca00062bb51dd212e2254688ec50854ecd28` with codec v62. The deployed
Wakterm CLI and mux must match that wire codec. Panetone negotiates capabilities
on every CLI connection and fails closed if the required live capability is
absent.

The event consumer drains pages to the advertised head. It records each
complete page and advances its cursor in one SQLite transaction. It deduplicates
by event ID plus process incarnation. Plans and assistant messages create
durable outbox work.
`turn_final` records state but is not mirrored because it repeats the last
assistant message. Lifecycle events update only the availability of an exact
agent and incarnation binding. They do not erase the durable identity needed
for a structured unavailable or stale-incarnation result. Unknown identities
remain visibly unrouted.

A retention gap cannot be made lossless. Panetone takes the required fresh
catalog snapshot, reconciles availability only for exact persisted identities,
records the missing interval, advances to the fresh conservative lower bound,
and enters the global hold. Release is refused until an operator acknowledges
the exact gap with review evidence.

Every enabled harness must have live shadow evidence before Phase 5B. Wakterm
implements Codex, Claude, Gemini, and OpenCode projections, but only Codex has a
currently live production harness result. The Wakterm agent owns the remaining
three side-effect-free shadow runs. Any harness without passing evidence must be
removed from the Phase 5B enabled configuration.

## Held startup and promotion state

A new or migrated schema-v5 database starts with a global delivery hold. While
held, the daemon can preflight Wakterm, open its store, serve status and
operator requests, and report supervised task health. It does not call
`agent events`, watch return terminals, poll a channel, drain inbox or outbox
work, admit a prompt, or advance a cursor.

The only promotion operations are:

- global hold or release
- route enable or disable
- exact Wakterm event cursor initialization
- fresh-catalog event cursor baseline
- explicit cursor-gap acknowledgement with evidence
- Telegram update baseline or exact offset initialization
- fresh route reconciliation through pane ID and two catalog snapshots
- explicit legacy no-replay, externally-verified, or Debate-to-Signal
  disposition

Every operation has a caller UUID, durable audit row, semantic replay response,
and different-content conflict. Release requires an event cursor and at least
one enabled, authoritatively available route. The effect gate waits for any
in-flight operation before a new hold succeeds, then prevents new effects.

The Telegram baseline command is the only operator action that polls a channel
while held. It requests the current tail, discards the returned message body,
and durably records only the next update offset. Ordinary held startup never
does this automatically.

## Durable effects and restart behavior

Control sends keep audit-before-prompt ordering. A definitive busy target is
queued with the same Wakterm request ID and exact bytes until admission is
safe. An uncertain admission becomes indeterminate and is never silently
retried.

General assistant output, migrated pending output, and callback mirrors use the
same durable outbox. Remote channel delivery is at least once across the narrow
acknowledgement-to-checkpoint crash window. Prompt and callback admission are
at most once when uncertain. Return terminal results are persisted and their
Wakterm sequence is checkpointed before a held source route is considered for
mirror or callback delivery.

Inbound Telegram, Signal, and Slack messages are durable before routing.
Telegram and Signal preserve an explicit sender identity for authorization.
An inbound prompt is marked `admission_prepared` before Wakterm. Restarting at
that boundary makes it indeterminate instead of redelivering it.

Completed control payloads may be compacted, but their schema-v5 UUID
tombstones do not expire. The same UUID can never become a new prompt later.
Pending, failed, and indeterminate workflows, returns, inbox work, and outbox
work remain durable.

## Permanent evidence

The process tests cover:

- held production startup with no event, return, channel, prompt, or cursor
  effect
- pane-title to catalog reconciliation when display names differ
- cursor and Telegram baseline initialization
- canary enablement and hold release
- Telegram audit and authoritative Wakterm prompt admission
- structured one-way acknowledgement
- explicit second-send report-back
- same-content UUID replay without another prompt or Telegram send
- different-content UUID conflict
- daemon restart with cursor, route policy, outbox, and idempotency state intact

The local promotion rehearsal uses only a fake Wakterm executable and a loopback
Telegram endpoint. It is promotion-logic evidence, not live promotion evidence.

## Remaining Phase 5B gate

Do not stop Python or install this unit until the exact Wakterm producer commit
is deployed, all four configured provider projections pass shadow comparison,
Wakterm has a system service under the same manager, a final stopped-service
migration bundle is created, and the operator has reviewed every staged legacy
record. No Wakterm restart may be bundled into the Panetone cutover without a
separate agent restoration plan.
