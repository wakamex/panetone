# ADR 0006: Replies as tagged messages

Status: implemented

## Context

A cross-agent request that wanted an answer used `return_final`: Wakterm matched the request to the provider turn its prompt started, and Panetone returned that turn's final response to the sender. One piece of text then served two readers, and the turn was the wrong unit for an answer:

- The answer was whatever the target said when the turn ended. An agent that started work in the background and ended its turn returned a progress note, and the sender received it as the result.
- Work that finished in a later turn, after a background task, a user's approval or a captcha, never reached the sender.
- Matching a request to a turn failed when the conversation was compacted mid-turn, when the request arrived as the previous turn ended, and when the prompt held tab characters.
- The final response was also posted to the target's chat, so an answer meant for another agent appeared in a Signal group in front of other people.

## Decision

A request states that it wants a reply. The target answers when it has the answer, in any turn, by tagging an ordinary message. Panetone delivers the tagged message to the sender, reminds a target that owes a reply, and never matches requests to turns.

### Asking

`panetone send --to TARGET --reply MESSAGE` records a reply debt: TARGET owes the sender a reply to the request ID. `--return-final` and the control field `return_final: true` mean the same thing. The message header names the reply mode and says how to answer. The acknowledgement has `reply_mode: "tagged_reply"` and `reply_pending: true`. A sender outside Wakterm cannot ask for a reply, since nothing can deliver one to it.

The debt opens when the message reaches the target: at once for an idle or busy target, or later for a message that waits in the busy queue.

### Answering

The target writes its answer as normal output and puts `[panetone:reply]` on its own line, outside fenced or indented code, in the message that holds the answer. `[panetone:reply ID]` names the request when the agent owes several replies; an unambiguous prefix of the ID is enough. Panetone removes the tag and delivers that whole message to the sender as a final reply, which closes the debt. A reply that says the target cannot do the work is a final reply like any other.

`[panetone:progress]` or `[panetone:progress ID]` delivers the message as an update and keeps the debt open.

One message can answer several requests: with more than one tag, each tag takes the text since the previous tag, and the last also takes any text after it.

The reply reaches the sender as a cross-agent message from the target, opening with the request ID and the first line of the request, so a sender that asked several agents, or whose context was compacted, knows what the answer is for. It is delivered like any other send: into the sender's active turn when it is busy, or as a new turn.

A tag that names no open debt, or a bare tag while several are open, delivers nothing; the next reminder reports it.

### Visibility

A tagged message is also posted to the target's route, with the tag removed, except to a shared group: a Signal group whose route accepts members other than the owner. When the route's output channel is a shared group, the message goes to the route's Telegram topic instead, or nowhere when it has none.

### Reminders

When a turn of the target ends while it owes a reply, and nothing has started another turn within 30 seconds, Panetone sends it one reminder that lists every open debt with the first line of each request and how to answer. A turn that a reminder itself started is never followed by another reminder. Reminders therefore follow only turns that something else started: the user, another agent, or a background task finishing. An agent that never replies costs at most one extra turn each time it is woken anyway, and is never woken by reminders alone.

### Closing

A debt closes only when the target sends a final reply or the sender withdraws it with `panetone cancel REQUEST_ID`. Debts do not expire. `panetone status` lists open debts with their sender, target, age and the first line of the request.

### Migration

Requests submitted with Wakterm's return correlation before this decision finish through it. Panetone no longer asks Wakterm to correlate a request with a turn, so that path receives no new work and can be removed once those requests have finished.

## Consequences

- Agents learn one tag. Its form matches the existing `[panetone:attach PATH]` tag.
- A target that forgets to reply is reminded instead of returning whatever it said last. A sender sees an open debt in `panetone status` rather than a misleading answer.
- Reminders start turns whose output goes to the target's chat, so the user sees the target's response to a reminder.
- An obsolete debt keeps producing reminders after the target's turns until the sender cancels it or the target replies.
- A failed or aborted turn is no longer reported to the sender automatically; the sender learns of it from the target's reply or from `status`.
- Panetone sees the tag only in output Wakterm observes. A harness without observed output cannot answer this way.
