# Panetone control protocol

The local control interface lets a client ask the running Panetone bridge to
send a cross-agent message, optionally with an asynchronous final response.
The bridge remains authoritative for live routes, Telegram topics, bot
identities, reply routing, and Wakterm delivery. The CLI does not read or copy
any of that state.

## Transport and permissions

The bridge listens on a Panetone-owned Unix stream socket. The default is:

```text
$XDG_RUNTIME_DIR/panetone/control.sock
```

`PANETONE_CONTROL_SOCKET` overrides the path for the bridge and CLI. The parent
directory is mode `0700` and the socket is mode `0600`. On Linux, the server also
checks that the peer UID matches the bridge UID.

The Wakterm mux socket remains separate. Panetone invokes `wakterm cli agent
send` as a client of that interface after it has resolved its own route and
Telegram state.

At startup, Panetone refuses to replace a symlink, regular file, or active
socket. It removes a socket only after a connection probe establishes that the
recorded inode is stale. Shutdown removes only the inode created by that server
instance. This also makes recovery safe after an unclean service restart.

## Framing and request

Each connection carries one UTF-8 JSON request terminated by a newline and one
newline-terminated JSON response. Requests are limited to 256 KiB.

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "method": "send",
  "params": {
    "from": "ufopedia",
    "to": "wakterm",
    "message": "Investigate the observer bug",
    "return_final": true,
    "timeout_ms": 3600000
  }
}
```

The ID is both the correlation identifier and the idempotency key. Omitting
`return_final`, or setting it to false, preserves one-way delivery. With
`return_final: true`, Panetone durably registers the source agent and Telegram
route before submitting the same ID to Wakterm. `timeout_ms` is optional and
applies asynchronously. Zero disables the deadline.

## Successful response

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "ok": true,
  "result": {
    "source": {
      "title": "ufopedia",
      "tab_id": 1,
      "pane_id": 1,
      "harness": "codex",
      "topic_id": 164000
    },
    "target": {
      "title": "wakterm",
      "tab_id": 7,
      "pane_id": 8,
      "harness": "codex",
      "topic_id": 164481
    },
    "telegram": {
      "chat_id": -1000000000000,
      "topic_id": 164481,
      "message_ids": [9001],
      "status_annotation": {
        "kind": "edited",
        "message_id": 9001
      }
    },
    "wakterm": {
      "agent_id": "detected-pane-8",
      "agent_name": "wakterm",
      "pane_id": 8,
      "submitted": true,
      "acknowledgement": {
        "kind": "session_observer",
        "acknowledged": true,
        "latency_ms": 41
      }
    },
    "reply_mode": "return_final",
    "reply_pending": true
  }
}
```

The successful response is a durable registration receipt, not the final agent
message. The CLI exits while the delegated turn is still running. For one-way
delivery, the older `submitted` and observer acknowledgement receipt is
unchanged.

Return mode is capability-dependent. Panetone probes for Wakterm durable agent
request support at startup and enables the return watcher only when that probe
succeeds. Installing a compatible Wakterm takes effect after a deliberate
Panetone restart. The current Wakterm implementation supports correlated return
requests for Codex targets. Other harnesses require equivalent observer
support. The startup probe detects the command-level capability, not support for
a particular target harness. If Wakterm rejects a target during submission, the
normal audit-linked Wakterm failure and indeterminate-delivery policy applies.

## Delivery order and failures

Panetone performs these steps:

1. Claim the UUID durably as `in_progress`.
2. Refresh and resolve the live source and target routes.
3. Post `SOURCE → TARGET`, the UUID marked `[pending]`, and the message in the
   target Telegram topic.
4. Persist the Telegram receipt as `audit_posted`.
5. Switch the target's response route to Telegram.
6. Persist `delivering` before invoking Wakterm.
7. For return mode, persist the source return route and run `wakterm cli agent
   send --return-final --request-id UUID` outside the asyncio event loop.
8. Edit the audit to `[submitted]`, or add a linked submitted marker if editing
   fails.
9. Store the registration response.
10. Consume Wakterm's resumable terminal request stream in one background
    subscription.
11. Persist the terminal result, then deliver one correlated callback to the
    source agent and its Telegram topic.

If Telegram fails before any audit is visible, Panetone does not invoke
Wakterm. If an audit is partially visible, Panetone adds or edits a linked
`DELIVERY FAILED` marker and still does not invoke Wakterm.

If Wakterm fails after the audit, Panetone adds a reply-linked `DELIVERY FAILED`
message. The request becomes `indeterminate` because a PTY write and a database
commit cannot be one transaction. Panetone never retries it automatically.
An unclean restart can leave an audit marked `[pending]`, but it cannot leave an
unconfirmed arrow presented as a successful submission.

Errors use this shape:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "ok": false,
  "error": {
    "code": "request_indeterminate",
    "message": "the service restarted or lost contact during this request; it will not be retried automatically"
  }
}
```

If `return_final` is true but the running bridge did not negotiate durable
agent request support, the request fails with:

```json
{
  "schema": "panetone.control.v1",
  "id": "fe57dc90-994e-4e73-b09c-fac483d9f05b",
  "ok": false,
  "error": {
    "code": "return_final_unavailable",
    "message": "Wakterm does not support durable return requests; no delivery was attempted"
  }
}
```

This capability failure occurs before live route refresh, Telegram audit, or
Wakterm prompt submission. It is a determinate failure and ordinary one-way
sends remain available.

## Durable idempotency journal

The journal defaults to `control-journal.sqlite3` beside Panetone's existing
state file. `PANETONE_CONTROL_JOURNAL` overrides it. The database is mode `0600`,
uses full synchronous commits, and refuses to grow beyond 64 MiB. If it reaches
the bound, new IDs fail with `journal_full` before any external action.

Reusing an ID with the same request returns its stored response without posting
to Telegram or Wakterm again. Reusing it with different content returns
`idempotency_conflict`. Finding a nonterminal request after restart persists and
returns `request_indeterminate`; it is not replayed.

Completed `succeeded` and `failed` UUIDs expire 30 days after their last update.
After expiry, reusing that UUID is a new request and can deliver again. Pruning
never removes `in_progress`, `audit_posted`, `delivering`, or `indeterminate`
records. These safety records remain durable even if their retention causes the
journal to reach its size bound.

The CLI does not retry automatically. If the connection is lost, use the UUID
shown by the CLI with `--id` and the exact same request. A cached result is safe
to return, while an uncertain request remains explicitly indeterminate.

Return routes, terminal results, per-destination delivery states, and the last
processed Wakterm event sequence live in the same journal. A restart resumes
pending terminal deliveries and restarts the Wakterm stream after the durable
cursor. If Panetone restarts while a destination call is in flight, that
destination becomes `indeterminate` and is not repeated. This at-most-once
boundary avoids duplicate agent prompts or Telegram messages when an external
API accepted a call but its receipt was lost.

## Final-response correlation

Panetone never reads Codex or Claude session formats for return requests. It
registers routes, submits the request ID, and consumes Wakterm's terminal event
stream. Wakterm binds the result to the exact target process incarnation,
observer session, submitted prompt hash, provider turn ID, and armed output
cursor. A stale session, reused pane, intervening prompt, extra user input, or
skipped provider turn produces an asynchronous `indeterminate` callback instead
of returning an unrelated final message.

When return mode is unavailable, the source can include a plain-language
instruction asking the target to run a second ordinary `panetone send` with its
final summary. That explicit report-back is initiated by the target and is not
correlated automatically with the original request.
