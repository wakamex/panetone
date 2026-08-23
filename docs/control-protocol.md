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

Its parent is a real mode `0700` directory and the socket is mode `0600`. On
Linux, the server accepts only the same UID. Startup refuses symlinks, regular
files, and active sockets. It removes a stale socket only after a failed
connection probe and an inode recheck.

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

`route.ensure` returns an existing Telegram-bound route unchanged or creates a
Telegram forum topic and persists a new route:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "route.ensure",
  "params": {"title": "infobase"}
}
```

The title must contain 1 to 128 characters without surrounding whitespace. A
new route requires exactly one live Wakterm tab with that effective title. The
configured primary Telegram bot creates its topic. A caller may instead pass
`telegram_topic_id` to bind a known positive topic ID. An existing conflicting
binding returns `route_binding_conflict` and is not changed.

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

`created` reports creation of the route. `binding_created` reports addition of
its Telegram binding. Repeating `route.ensure` returns both as false and does
not create another topic. `event_cursor` is Panetone's durable baseline for an
output sent after this response. A launcher that knows its new pane ID selects
the matching entry from `live.agents`; a tab may legitimately contain more than
one agent pane.

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
    "from": "ufopedia",
    "to": "wakterm",
    "message": "Investigate the observer bug",
    "return_final": false,
    "timeout_ms": 0
  }
}
```

`from`, `to`, and `message` must be non-empty. Route titles resolve by exact
case-insensitive match and fail when missing or ambiguous. `return_final` and
`timeout_ms` default to false and zero. A nonzero timeout is rejected because
asynchronous final callbacks do not expire.

The normal sequence is:

1. resolve source and target workspace titles to the current live Wakterm
   agent and incarnation
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
  "reply_pending": false
}
```

A definitively busy target returns a successful durable registration with
`delivery_state: "queued"`, `submitted: false`, and `reply_pending: false`.
The busy worker resolves the workspace route again before admission. It does
not steer an active turn.

If Wakterm may have accepted a prompt but Panetone did not receive or persist a
definitive receipt, the workflow becomes indeterminate and is never retried
automatically.

Reusing a UUID with the same semantic request returns its stored
acknowledgement without repeating effects. Reusing it with different content
returns `idempotency_conflict`. Request UUIDs are reserved in durable
tombstones from the initial claim.

## Asynchronous final return

`return_final: true` requests a correlated Wakterm terminal result. Panetone
stores the exact source and target incarnations, then returns immediately after
target admission with `reply_pending: true`.

When Wakterm emits the terminal result, Panetone verifies it against the exact
submitted target and persists it before:

- mirroring it to the source route's channel
- resolving the source workspace again and admitting a structured callback to
  its current agent

Each destination has independent durable state. A possibly accepted callback
becomes indeterminate and is not retried. A definitive busy callback remains
pending until the source is idle.

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
