# Panetone control protocol

Panetone exposes a local Unix socket for status, route setup, durable output
disposition, and cross-agent sends. The Rust CLI is the supported client.

## Transport

Each connection carries one newline-terminated UTF-8 JSON request and one
newline-terminated JSON response. Requests are limited to 256 KiB and each
connection has a five-second server deadline.

The socket normally lives at:

```text
$XDG_RUNTIME_DIR/panetone/control.sock
```

The CLI uses this path when neither `--socket` nor `PANETONE_CONTROL_SOCKET` is supplied. The explicit flag has highest precedence, followed by the environment variable and then the runtime-directory default. Commands fail with a direct configuration error when no override is supplied and `XDG_RUNTIME_DIR` is missing or invalid.

Its parent is a real mode `0700` directory and the socket is mode `0600`. On Linux, the server accepts only the same UID and classifies the peer from its Unix credentials, pidfd, and user, mount, PID, and IPC namespaces before parsing the request. A same-UID peer in different namespaces may call the read-only `status`, `route.list`, `route.inspect`, and `output.disposition` methods. `send`, `route.ensure`, and unknown future methods are denied before any workflow, channel, route, or Wakterm side effect. Client-supplied route names and request IDs never establish authority.

The namespace check detects a known Linux sandbox boundary but does not prove that a same-namespace process is unrestricted. Inherited descriptors, proxies, readable credentials, confinement without distinct namespaces, and non-Linux systems require enforcement by the sandbox or credential owner. Sending to a more privileged agent is privilege delegation, so a restricted caller cannot use the host Panetone daemon as an authority broker.

Startup refuses symlinks, regular files, and active sockets. It removes a stale socket only after a failed connection probe and an inode recheck.

## Envelope

Requests use:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "status",
  "params": null
}
```

Successful responses use:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "ok": true,
  "result": {}
}
```

Errors use:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "ok": false,
  "error": {
    "code": "invalid_request",
    "message": "send requires non-empty from, to, and message fields"
  }
}
```

`id` is the correlation identifier for every method and the idempotency key for
`send`.

## Route inspection and establishment

`route.list` returns every configured route title with its current send availability and exact live agents. The result is sorted case-insensitively by title and omits channel bindings:

```json
{
  "routes": [
    {
      "title": "infobase",
      "available": true,
      "agents": [{
        "agent_id": "detected-pane-14",
        "incarnation_id": "01K...",
        "harness": "codex",
        "pane_id": 14
      }]
    },
    {
      "title": "offline-project",
      "available": false,
      "agents": []
    }
  ]
}
```

`route.inspect` reads one exact case-insensitive durable title and its current
Wakterm resolution:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "route.inspect",
  "params": {"title": "infobase"}
}
```

`route.ensure` establishes the requested durable channel bindings, creating a Telegram forum topic and route when needed:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "route.ensure",
  "params": {
    "title": "infobase",
    "signal_group_id": "base64-signal-group-id",
    "signal_allow_members": true,
    "signal_account": "+15552222222",
    "signal_direct": true
  }
}
```

The title must contain 1 to 128 characters without surrounding whitespace. A new route requires a live Wakterm agent with that effective title. The configured primary Telegram bot creates its topic. A caller may instead pass `telegram_topic_id` to bind a known positive topic ID. `signal_group_id` adds one exact Signal group binding. Signal input remains owner-only unless `signal_allow_members` is true, in which case authenticated group members are accepted with a minimal first-name label. An existing conflicting binding returns `route_binding_conflict` and is not changed. Repeating a member-enabled binding does not downgrade it.

`signal_account` names the Signal account that serves the group, either the daemon's default account or one listed in `WAK_SIG_EXTRA_ACCOUNTS`; an account the daemon is not configured with is rejected as `invalid_params`. Omitted, an existing binding keeps its account and a new binding uses the default account. Messages for the group are accepted only from the bound account's subscription, and posts to the group go through it. `signal_direct` gives the route the owner's direct messages to that account; it stays set once given.

The result is:

```json
{
  "created": true,
  "binding_created": true,
  "route": {
    "id": "f708a8f3-78cb-47d8-8a09-21aa6ddac774",
    "title": "infobase",
    "channels": [{"kind": "telegram", "topic_id": 777}]
  },
  "live": {
    "status": "available",
    "agents": [{
      "agent_id": "detected-pane-14",
      "incarnation_id": "01K...",
      "harness": "codex",
      "pane_id": 14
    }]
  },
  "event_cursor": 27317
}
```

`created` reports creation of the route. `binding_created` reports addition or policy upgrade of a channel binding. Repeating `route.ensure` returns both as false and does not create another topic. `event_cursor` is Panetone's durable baseline for an output sent after this response. A launcher that knows its new pane ID selects the matching entry from `live.agents`; a tab may legitimately contain more than one agent pane.

## Durable output disposition

`output.disposition` finds a stored `assistant_message` with the exact expected
text after a sequence for one exact Wakterm agent and incarnation:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "output.disposition",
  "params": {
    "route": "infobase",
    "agent_id": "detected-pane-14",
    "incarnation_id": "01K...",
    "after_sequence": 27317,
    "expected_text": "READY"
  }
}
```

The result disposition is one of:

- `pending`: no matching assistant output is stored yet
- `projected`: the event was assigned to the requested route
- `unrouted`: the event was stored without a usable route or destination
- `misrouted`: the event was projected to another durable route

A non-pending response includes the complete Wakterm event, expected route,
and actual route when one exists. `projected` is durable proof of routing:
Panetone inserts the event and its deterministic outbox effect and advances the
event cursor in one SQLite transaction. Delivery may still be pending. The
global event cursor alone is not proof because it also advances over unrouted
events.

The CLI `output wait` repeatedly calls this read-only method. It exits zero
only for `projected`, exits nonzero immediately for another terminal
disposition, and exits nonzero after its local timeout while still pending.

## Send

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "send",
  "params": {
    "to": "wakterm",
    "message": "Investigate the observer bug",
    "source_agent": {
      "agent_id": "8a6c0e8e-0d5f-4b52-9b63-2f0b4a6c9d10",
      "incarnation_id": "c3f1d2a4-7b9e-4e1a-8f60-5d2c7b1e9a34"
    },
    "reply": false,
    "queue": false,
    "timeout_ms": 0
  }
}
```

`to` and `message` must be non-empty. A caller must provide `source_agent`, `source_pane_id`, or `from`, and at most one of `source_agent` and `source_pane_id`. Without `--source-pane-id`, the CLI runs `wakterm agent caller` in its inherited environment and supplies the resulting `agent_id` and `incarnation_id` as `source_agent`, so normal calls from a Wakterm agent omit `--from`. Wakterm resolves pane harnesses through `WAKTERM_PANE` and managed Codex tool commands, which run in a shared app server without a pane, through `CODEX_THREAD_ID`. If Wakterm cannot identify exactly one caller, the CLI fails unless `--from` is given. Panetone validates that the exact agent incarnation, or the agent in `source_pane_id`, is live and derives the source route from its pane's current effective title. A stale source fails with `source_agent_unavailable` or `source_pane_unavailable` unless `from` is also given. `from` remains an optional route override for the shared channel mirror and audit identity. A caller outside Wakterm instead sets `sender` to its name, which cannot be combined with `from`, `source_pane_id`, `source_agent` or `return_final`; the message is recorded on the target's route under the identity `external:NAME` with harness `external`, and the target sees `From: NAME (external)`. Route titles resolve by exact case-insensitive match and fail when missing. Every live agent with the same effective title belongs to the route, including agents in separate tabs. `to` is a route title or a live agent's Wakterm name, also matched case-insensitively, and a title takes precedence over an agent name. A title whose route has several live agents fails with `target_route_has_several_agents` and lists their names in `details.agents`, so the caller resends to one by name. A message queued for a busy target is later admitted to the same agent, or to its replacement if that agent has restarted. `return_final` and `queue` default to false, and `timeout_ms` defaults to zero. A nonzero timeout is rejected because asynchronous final callbacks do not expire.

The normal sequence is:

1. resolve the exact calling pane or explicit source route, then resolve the target route to its current live Wakterm agent and incarnation
2. claim the request UUID and semantic hash
3. durably enqueue and deliver the visible target-channel audit
4. persist the admission boundary
5. submit through exact Wakterm agent and incarnation identity
6. save the definitive receipt or an indeterminate state

A successful acknowledgement is:

```json
{
  "accepted": true,
  "delivery_state": "submitted",
  "submitted": true,
  "reply_pending": false,
  "reply_mode": "none"
}
```

Panetone always attempts exact atomic admission first. An idle target accepts the message as a normal new turn and returns `delivery_state: "submitted"`.

A definitively busy target receives the complete cross-agent message through Wakterm's steering path, so a message is not held until the end of a long turn. The acknowledgement has `delivery_state: "steered"`, `submitted: true`, a `delivery_detail` saying the message went into the active turn, and `steering_acknowledged: true` or `false`, which distinguishes observer confirmation from submission.

Set `queue: true` (`--queue`) to wait for the target to become idle instead, for a message that should start its own turn after the current work. A message from an external `sender` always waits, because that sender reads the answer from the turn its message starts. A waiting message returns a successful durable registration with `delivery_state: "queued"`, `submitted: false`, and `reply_pending: false`, with a `delivery_detail` saying the message is delivered when the target is idle and must not be sent again. The busy worker resolves the workspace route again before admission and offers each agent its oldest queued message first.

When Wakterm refuses to steer because a dialog, shell mode, startup prompt or a draft the user is typing holds the target's keyboard, nothing is written and the message waits in the busy queue with `delivery_state: "queued"`. It is delivered, by steering if the target is still busy, once the target can take input. A channel message to such a target stays pending and is retried the same way.

Requests that carry the retired `steer` field are accepted and the field is ignored.

If Wakterm may have accepted a prompt but Panetone did not receive or persist a
definitive receipt, the workflow becomes indeterminate and is never retried
automatically.

A definitive Wakterm rejection stores the complete target admission receipt in `workflow.target_admission_receipt`. The control error and linked channel failure annotation include its status and detail, so an observer, identity, availability, or contract failure is distinguishable without reading Wakterm's private store.

### Idempotency and retries

Reusing a UUID with the same semantic request returns its stored
acknowledgement without repeating effects. Reusing it with different content
returns `idempotency_conflict`. Request UUIDs are reserved in durable
tombstones from the initial claim.

A successful response (`ok: true`, which the CLI reports with exit status 0) always carries `accepted: true` and means Panetone holds the message durably and will deliver it or report its failure in the target's channel; a caller stops retrying on it, whatever `delivery_state` and `submitted` say. Any other response means the message may not be held. A caller that retries with the same request UUID, such as `panetone send --id`, can never cause a second delivery, so a notifier can retry until it succeeds without keeping its own record of what it sent; a version 5 UUID of its own event ID makes a suitable request ID. A send to a title with no route, or to a route with no live agent, is rejected and is not held for a session that starts later.

## Replies

`reply: true` (or `return_final: true`, its older name) asks for a reply. When the message reaches the target, Panetone records that the target owes the sender a reply, and the message header tells the target how to answer. The acknowledgement has `reply_mode: "tagged_reply"`, `reply_pending: true` and a `reply_detail` describing the path. A sender outside Wakterm cannot ask for a reply.

The target answers in any turn by writing the answer as normal output with a standalone `[panetone:reply]` line outside fenced or indented code, or `[panetone:reply ID]` with the request ID or an unambiguous prefix of it when it owes several replies. Panetone removes the tag and sends that message to the sender as a cross-agent message that opens with the request ID and the first line of the request; this closes the debt. `[panetone:progress]` sends the message the same way as an update and keeps the debt open. A tag that matches no open debt delivers nothing and is reported in the next reminder. A tagged message is posted to the target's route without the tag, except to a Signal group whose route admits members other than the owner; it then goes to the route's Telegram topic, or nowhere.

When a turn of the target ends while it owes a reply, and nothing starts another turn within 30 seconds, Panetone admits one reminder listing every reply it owes. No reminder follows a turn that a reminder started, so reminders never wake an agent on their own.

`reply.cancel` takes `{"request_id": UUID}` and withdraws an open debt; the CLI form is `panetone cancel REQUEST_ID`. Its result reports `cancelled` and `previous_state`, which is `open`, `replied` or `cancelled`. A request that did not ask for a reply fails with `reply_not_found`. Debts do not expire.

Requests that Wakterm correlated with a provider turn before replies were tagged still return that turn's final response through the terminal stream. The terminal watcher calls Wakterm only while such a request has no stored terminal result.

## Status

The `status` method takes null parameters. Its result includes:

- package version, production mode, and uptime
- durable workflow, inbox, outbox, return, tombstone, and cursor counts
- `replies_owed`: each open reply debt with its request ID, sender, target, age and the first line of the request
- Wakterm socket and startup capabilities
- configured channel availability
- control socket path
- named supervisor task health

Every production task is critical. An unexpected task exit shuts down the
daemon with a failure so the systemd user unit can restart the complete durable
process. An idle Signal receive deadline is handled as a normal empty poll.
