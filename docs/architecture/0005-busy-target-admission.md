# ADR 0005: Busy target admission

Status: implemented

## Context

Wakterm definitively refuses a new prompt when the target agent already has an active turn. A message for a busy target can either wait for the next turn or be steered into the active one.

Waiting keeps each message in a turn of its own, but its delay is the length of the target's turn, which the sender cannot know. A target in a ten-hour turn received thirteen notices only after the turn ended, when most were out of date, and each then started a separate turn.

Steering delivers at once but gives the message no turn of its own, so the turn's final response cannot be correlated with it.

## Decision

Messages are delivered when they are sent. Local control `send` and authorized Telegram and Signal input use the same admit-then-steer behavior: Panetone first uses authoritative admission against the exact current agent identity, and an idle agent accepts the message as a new prompt. A definitive `busy` receipt proves that no prompt was written, so Panetone immediately sends the message through Wakterm's active-turn steering command instead of queueing it.

A local sender that wants its message to start a turn after the current work sets `queue`, which waits durably for the target to become idle. A message from a sender outside Wakterm always waits, because that sender reads the answer from the turn its message starts.

A request that wants a reply is delivered the same way, idle or busy; the target answers with a tagged message, as [ADR 0006](0006-tagged-replies.md) describes.

The local control admission sequence is:

1. claim and persist the idempotency key
2. resolve the workspace title to its current live agents and select one agent incarnation
3. establish a visible Telegram audit
4. if the target is busy, steer the message into its active turn; when the request sets `queue` or the steer is refused, persist `awaiting_target_idle` and return immediately
5. after an authoritative idle observation, resolve the workspace again,
   persist the delivery attempt boundary, and submit once through Wakterm
6. edit the audit from queued to submitted, or visibly mark a definitive or indeterminate failure

The structured queued acknowledgement is successful registration, not delivery confirmation. It includes `delivery_state: "queued"` and `submitted: false`. `reply_pending` says whether the sender is owed a reply, not whether target delivery is pending.

The structured steering acknowledgement uses `delivery_state: "steered"`, `submitted: true`, and `steering_acknowledged` to report whether Wakterm observed the submitted input. A same-content, same-mode UUID retry returns the stored acknowledgement without another pane write. Reusing the UUID with a different delivery mode is an idempotency conflict.

The Telegram audit uses `[queued]` while waiting and `[submitted]` only after Wakterm acceptance. A same-content duplicate UUID returns the same durable queued acknowledgement without another audit or prompt. A different request with the same UUID remains a conflict.

Queued work survives Panetone and Wakterm restarts. Each agent is offered its oldest queued message first, and prompts for one agent are written one at a time. It has no automatic expiry in control v1. It remains visible through future status and cancellation operations until submitted or explicitly cancelled. The worker uses bounded backoff or lifecycle notification rather than a tight poll.

Before submission, Panetone resolves the persisted workspace title again and selects the agent chosen when the work was sent. A new incarnation of that agent may receive the work after a restart. Panetone records both the originally observed and submitted incarnation in the workflow audit. Missing live routes are not guessed.

A target with no live agent pane is not the same as a busy target. Control v1 continues to return `route_unavailable` before external side effects for a genuinely agentless route. Waiting for a route that does not exist would require a separate bounded workflow and is not part of this decision.

## Failure semantics

Wakterm must classify `busy` as a definitive non-acceptance result. Panetone may queue only when Wakterm guarantees that no prompt write occurred. Any possible acceptance without a receipt remains indeterminate and is never automatically retried.

Audit failure still fails closed. A crash after a queued audit but before its durable queued checkpoint is indeterminate and receives a visible failure annotation. A crash after the queued checkpoint resumes the queue without adding another audit.

Before either steering path, Panetone persists `admission_prepared`. When Wakterm refuses to steer because a dialog, shell mode, startup prompt or a draft the user is typing holds the target's keyboard, nothing was written: channel input stays pending and local work waits as queued. Any other steering command or receipt failure, or an unacknowledged steer of channel input, is not retried automatically. Channel input then waits for proof of delivery as described in the production operations guide, and a local workflow becomes `indeterminate`. This avoids duplicate steering when it is unknown whether the pane write occurred. Local queued work continues to use the durable workflow and audit states described above.

The return-terminal stream may race the admission receipt. A terminal for a workflow still in `admission_prepared` remains unread until that workflow records whether submission occurred. If the workflow then ends without a submitted target, Panetone consumes the redundant terminal without manufacturing a callback. One request-level inconsistency therefore cannot stop unrelated routes or channel workers.
