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
    "signal_allow_members": true
  }
}
```

The title must contain 1 to 128 characters without surrounding whitespace. A new route requires a live Wakterm agent with that effective title. The configured primary Telegram bot creates its topic. A caller may instead pass `telegram_topic_id` to bind a known positive topic ID. `signal_group_id` adds one exact Signal group binding. Signal input remains owner-only unless `signal_allow_members` is true, in which case authenticated group members are accepted with a minimal first-name label. An existing conflicting binding returns `route_binding_conflict` and is not changed. Repeating a member-enabled binding does not downgrade it.

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
    "return_final": false,
    "steer": false,
    "timeout_ms": 0
  }
}
```

`to` and `message` must be non-empty. A caller must provide `source_agent`, `source_pane_id`, or `from`, and at most one of `source_agent` and `source_pane_id`. Without `--source-pane-id`, the CLI runs `wakterm agent caller` in its inherited environment and supplies the resulting `agent_id` and `incarnation_id` as `source_agent`, so normal calls from a Wakterm agent omit `--from`. Wakterm resolves pane harnesses through `WAKTERM_PANE` and managed Codex tool commands, which run in a shared app server without a pane, through `CODEX_THREAD_ID`. If Wakterm cannot identify exactly one caller, the CLI fails unless `--from` is given. Panetone validates that the exact agent incarnation, or the agent in `source_pane_id`, is live and derives the source route from its pane's current effective title. A stale source fails with `source_agent_unavailable` or `source_pane_unavailable` unless `from` is also given. `from` remains an optional route override for the shared channel mirror and audit identity, and is required for callers outside Wakterm. Route titles resolve by exact case-insensitive match and fail when missing. Every live agent with the same effective title belongs to the route, including agents in separate tabs. `return_final` and `steer` default to false, and `timeout_ms` defaults to zero. A nonzero timeout is rejected because asynchronous final callbacks do not expire.

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

A definitively busy target returns a successful durable registration with
`delivery_state: "queued"`, `submitted: false`, and `reply_pending: false`.
The busy worker resolves the workspace route again before admission. It does
not steer an active turn.

Set `steer: true` to allow immediate active-turn steering instead of queueing. Panetone still attempts exact atomic admission first. An idle target accepts the message as a normal new turn and returns `delivery_state: "submitted"`. A definitively busy target receives the same complete cross-agent message through Wakterm's steering path and returns `delivery_state: "steered"`, `submitted: true`, and `steering_acknowledged: true` or `false`. The acknowledgement field distinguishes observer confirmation from submission. `steer: true` cannot be combined with `return_final: true` because steering does not create a new provider turn with separate final-response correlation.

If Wakterm may have accepted a prompt but Panetone did not receive or persist a
definitive receipt, the workflow becomes indeterminate and is never retried
automatically.

A definitive Wakterm rejection stores the complete target admission receipt in `workflow.target_admission_receipt`. The control error and linked channel failure annotation include its status and detail, so an observer, identity, availability, or contract failure is distinguishable without reading Wakterm's private store.

Reusing a UUID with the same semantic request returns its stored
acknowledgement without repeating effects. Reusing it with different content
returns `idempotency_conflict`. Request UUIDs are reserved in durable
tombstones from the initial claim.

## Asynchronous final return

`return_final: true` requests a correlated Wakterm terminal result. Panetone stores the exact source and target incarnations, then returns immediately after target admission with `reply_pending: true`, `reply_mode: "asynchronous_final_callback"`, and a `reply_detail` explaining the delivery path. The control response is an admission acknowledgement, not the target's final response.

When Wakterm emits the terminal result, Panetone verifies it against the exact
submitted target and persists it before:

- mirroring it to the source route's channel
- admitting a structured callback to the exact calling agent when it remains live, otherwise resolving the explicit or derived source route's current agent

Each destination has independent durable state. A possibly accepted callback
becomes indeterminate and is not retried. A definitive busy callback remains
pending until the source is idle.

The terminal watcher calls Wakterm only while a `return-final` workflow has a
submitted target and no stored terminal result. Ordinary operation therefore
does not run an idle terminal poll.

## Status

The `status` method takes null parameters. Its result includes:

- package version, production mode, and uptime
- durable workflow, inbox, outbox, return, tombstone, and cursor counts
- Wakterm socket and startup capabilities
- configured channel availability
- control socket path
- named supervisor task health

Every production task is critical. An unexpected task exit shuts down the
daemon with a failure so the systemd user unit can restart the complete durable
process. An idle Signal receive deadline is handled as a normal empty poll.
