# ADR 0005: Busy target admission

Status: implemented

## Context

Wakterm definitively refuses a new prompt when the target agent already has an
active turn. Local control sends need a safe choice between rejection and
waiting for the next turn.

Steering an active turn is a different operation. It can be appropriate for a
Telegram reply but cannot preserve new-turn return correlation.

## Decision

The local control `send` method waits durably for a busy target to become idle.
It never steers an active turn.

Authorized Telegram and Signal input has different semantics. Panetone first
uses authoritative admission against the exact current agent identity. An idle
agent accepts the message as a new prompt. A definitive `busy` receipt proves
that no prompt was written, so Panetone immediately sends the unchanged body
through Wakterm's active-turn steering command instead of queueing it.

The local control admission sequence is:

1. claim and persist the idempotency key
2. resolve the workspace title to its current live agents and select one agent incarnation
3. establish a visible Telegram audit
4. if the target is busy, persist `awaiting_target_idle` and return immediately
5. after an authoritative idle observation, resolve the workspace again,
   persist the delivery attempt boundary, and submit once through Wakterm
6. edit the audit from queued to submitted, or visibly mark a definitive or indeterminate failure

The structured queued acknowledgement is successful registration, not delivery confirmation. It includes `delivery_state: "queued"` and `submitted: false`. `reply_pending` continues to describe the optional final callback, not whether target delivery is pending.

The Telegram audit uses `[queued]` while waiting and `[submitted]` only after Wakterm acceptance. A same-content duplicate UUID returns the same durable queued acknowledgement without another audit or prompt. A different request with the same UUID remains a conflict.

Queued work survives Panetone and Wakterm restarts. It has no automatic expiry in control v1. It remains visible through future status and cancellation operations until submitted or explicitly cancelled. The worker uses bounded backoff or lifecycle notification rather than a tight poll.

Before submission, Panetone resolves the persisted workspace title again. A new agent incarnation may receive the work when the title still has a live agent. Panetone records both the originally observed and submitted incarnation in the workflow audit. Missing live routes are not guessed.

A target with no live agent pane is not the same as a busy target. Control v1 continues to return `route_unavailable` before external side effects for a genuinely agentless route. Waiting for a route that does not exist would require a separate bounded workflow and is not part of this decision.

## Failure semantics

Wakterm must classify `busy` as a definitive non-acceptance result. Panetone may queue only when Wakterm guarantees that no prompt write occurred. Any possible acceptance without a receipt remains indeterminate and is never automatically retried.

Audit failure still fails closed. A crash after a queued audit but before its durable queued checkpoint is indeterminate and receives a visible failure annotation. A crash after the queued checkpoint resumes the queue without adding another audit.

Before either channel path, Panetone persists `admission_prepared`. A steering
command or receipt failure changes the inbox item to `indeterminate` and is not
retried automatically. This avoids duplicate steering when it is unknown
whether the pane write occurred. Local queued work continues to use the durable
workflow and audit states described above.
