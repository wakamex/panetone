# ADR 0005: Busy target admission

Status: accepted for Phase 1 target semantics

## Context

Wakterm correctly refuses a return-correlated request when the target agent already has an active turn. Panetone currently treats that refusal as an indeterminate delivery failure after posting the Telegram audit. The visible failure and callback can make the source agent diagnose routing even though the route is healthy and only temporarily busy.

Writing a new prompt into the active turn would be worse. It can steer unrelated work, invalidate turn correlation, and make a later final belong to the wrong request.

## Decision

Default `send` waits durably for a busy target to become idle. It never steers an active turn.

The admission sequence is:

1. claim and persist the idempotency key
2. resolve one stable live target route and current Wakterm agent incarnation
3. establish a visible Telegram audit
4. if the target is busy, persist `awaiting_target_idle` and return immediately
5. after an authoritative idle observation, revalidate the route and incarnation, persist the delivery attempt boundary, and submit once through Wakterm
6. edit the audit from queued to submitted, or visibly mark a definitive or indeterminate failure

The structured queued acknowledgement is successful registration, not delivery confirmation. It includes `delivery_state: "queued"` and `submitted: false`. `reply_pending` continues to describe the optional final callback, not whether target delivery is pending.

The Telegram audit uses `[queued]` while waiting and `[submitted]` only after Wakterm acceptance. A same-content duplicate UUID returns the same durable queued acknowledgement without another audit or prompt. A different request with the same UUID remains a conflict.

Queued work survives Panetone and Wakterm restarts. It has no automatic expiry in control v1. It remains visible through future status and cancellation operations until submitted or explicitly cancelled. The worker uses bounded backoff or lifecycle notification rather than a tight poll.

Before submission, Panetone resolves the persisted stable route again. A new agent incarnation may receive the work only when it is the unambiguous current binding of that same durable route. Panetone records both the originally observed and submitted incarnation. Missing, ambiguous, or conflicting reconciliation is explicit and never guessed.

A target with no live agent pane is not the same as a busy target. Control v1 continues to return `route_unavailable` before external side effects for a genuinely agentless route. Waiting for a route that does not exist would require a separate bounded workflow and is not part of this decision.

## Failure semantics

Wakterm must classify `busy` as a definitive non-acceptance result. Panetone may queue only when Wakterm guarantees that no prompt write occurred. Any possible acceptance without a receipt remains indeterminate and is never automatically retried.

Audit failure still fails closed. A crash after a queued audit but before its durable queued checkpoint is indeterminate and receives a visible failure annotation. A crash after the queued checkpoint resumes the queue without adding another audit.

An explicit active-turn steering or interrupt operation may be designed later, but it must use a different command or flag and an unmistakable audit label.
