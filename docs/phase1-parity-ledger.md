# Phase 1 parity and state ledger

Date: 2026-08-16

This ledger freezes observable Python behavior for conformance work. It separates behavior that must remain compatible from known implementation details and deliberate future changes. A fixture or test reference is added as each row becomes executable.

## Compatibility labels

- Preserve means Rust must reproduce the observable contract before cutover.
- Replace means Rust must provide the stated stronger contract and migration evidence.
- Retire means the behavior belongs to Wakterm and must not be copied into Rust Panetone.
- Discovery means a shadow result can expose differences but cannot authorize production delivery.

## Local control and Wakterm workflow

| Behavior | Current Python contract | Target | Evidence |
| --- | --- | --- | --- |
| Framing | One UTF-8 JSON object followed by newline on a Unix socket, maximum 256 KiB | Preserve | `tests/test_control.py` and control fixtures |
| Socket boundary | Separate from Wakterm, parent `0700`, socket `0600`, same-UID peer check on Linux, symlink refusal, stale-socket probe, inode-safe cleanup | Preserve | `tests/test_control.py` |
| Request validation | Schema `panetone.control.v1`, method `send`, UUID ID, non-empty `from`, `to`, and `message`, route names at most 128 characters | Preserve | `tests/test_control.py` and control fixtures |
| Semantic defaults | Omitted and explicit false or zero are documented as equivalent but currently hash differently | Replace with one normalized semantic hash, with compatibility handling for existing rows | Default-normalization fixture |
| UUID reuse | Same hash returns the durable response, different hash returns `idempotency_conflict`, unfinished rows after restart become indeterminate | Preserve | `tests/test_control.py` and restart fixtures |
| Completed retention | Large succeeded and failed rows expire after 30 days; pending and indeterminate rows survive size pruning | Replace large rows after 30 days but retain compact UUID tombstones permanently | Journal fixtures |
| Route resolution | Refresh live routes, then exact case-insensitive title match; missing, ambiguous, agentless, or topicless routes fail before delivery | Preserve, then bind the result to a stable route UUID | Route fixtures |
| Audit ordering | Write a visible target-topic audit and durably checkpoint its IDs before attempting the Wakterm prompt | Preserve | `tests/test_bridge.py` |
| Audit failure | Fail closed. If an audit is partial or a later prompt step fails, reply to it or edit it with an unmistakable failure marker | Preserve | `tests/test_bridge.py` |
| Prompt delivery | Wakterm agent submission is async-safe. Possible acceptance without a receipt is indeterminate and is not retried | Preserve through an authoritative Wakterm receipt | `tests/test_bridge.py` and Agent API fixtures |
| Success result | Structured acknowledgement includes resolved source and target, Telegram audit, Wakterm receipt, reply mode, and pending flag | Preserve control v1 fields | Control fixtures |
| One-way result | Default send does not route the target's response back to the source | Preserve | Protocol documentation |
| Explicit report-back | Target may run a second one-way send to the original source when it decides work is complete | Preserve as a documented workflow, not a special protocol | Protocol documentation |
| Async final callback | `--return-final` returns immediately with `reply_pending: true`; Wakterm later correlates one terminal result, which Panetone durably delivers to the source agent and source Telegram topic | Preserve while the current request stream remains supported | Callback tests and Agent API fixtures |
| Capability failure | Unsupported installed Wakterm returns `return_final_unavailable` before Telegram or prompt side effects | Preserve | `tests/test_bridge.py` |

## Agent observation and routing

| Behavior | Current Python contract | Target | Evidence |
| --- | --- | --- | --- |
| Pane discovery | Panetone walks process trees by TTY and associates Claude, Codex, Gemini, and OpenCode with panes | Retire after provider cutover | Sanitized discovery fixtures and shadow logs |
| Provider session selection | Panetone searches provider stores by cwd, timestamps, and provider-specific metadata | Retire | Sanitized provider fixtures |
| Provider parsing | Panetone reads Claude and Codex JSONL, Gemini JSON, and OpenCode SQLite parts into normalized text | Retire | Provider-output fixtures |
| First observation | An existing source is baselined at its current tail on fresh startup; a genuinely new session discovered later begins at its first record | Replace with an explicit Wakterm cursor and lifecycle event | Cursor fixtures |
| Cursor checkpoint | All output chunks and the next source cursor are written in one atomic JSON replacement before any remote send | Preserve invariant in one SQLite transaction | Restart fixtures |
| Missing route during read | Do not advance the source cursor when messages exist but no main route or active Slack observer can receive them | Preserve | Routing fixtures |
| Main output route | Use the stable last source for the route, falling back through configured Telegram, Signal, debate, or Slack bindings | Preserve decisions | Routing fixtures |
| Collab forwarding | Agent output can be prefixed and forwarded to other panes, with finite rounds and signoff completion | Preserve until policy is deliberately revised | Collab fixtures |
| Authoritative future stream | Wakterm emits normalized messages, plans, turns, finals, lifecycle, and observer failures with durable sequence and incarnation identity | New boundary | Wakterm Agent API fixtures |
| Codex shadow | Compare Wakterm events with the Python reader through recording sinks only | Discovery | Shadow comparison report |

## Messaging adapters

| Surface | Current behavior and restart boundary | Target |
| --- | --- | --- |
| Telegram topics | Title-keyed topic IDs persist. Deleted topics are recreated and pending chunks retargeted. Inbound startup currently uses `drop_pending_updates=True`. | Preserve recreation and remove intentional inbound dropping. Store durable route bindings. |
| Telegram output | Chunks are durably queued before send. A crash after remote acceptance and before dequeue can duplicate a chunk. | Preserve at-least-once policy with a stable delivery ID. |
| Telegram replies | Message-to-pane and last-pane reply maps are process-local and rebuilt only by new activity. | Persist enough inbound work and receipts to make restart behavior uniform. |
| Signal groups | Title-keyed group IDs persist and padding is normalized. Legacy numeric tab keys and muted backlog are migrated on startup. | Migrate once offline into route and channel binding tables. |
| Signal inbox | Accepted incoming messages are archived in SQLite and remain pending until submitted. Duplicate envelopes are ignored by group, sender, and timestamp. | Preserve durable inbox and deduplication in the unified store. |
| Signal queues | Command and decoded-input queues are process-local lists. | Persist accepted work before queueing and use bounded channels. |
| Slack observer | Message buffers, `!obs` work, direct input, and the one active observer reply destination are process-local. | Persist accepted work and use bounded per-adapter commands. |
| Debate routing | Reply maps and crosspost toggle are process-local; configured chat and tab patterns determine participation. | Preserve user-visible policy, but make required workflow state explicit. |

## Durable state inventory

### `state.json`

Default path: `~/.config/wez-tg/state.json` or `WEZ_TG_STATE`.

| Key | Shape | Meaning and migration rule |
| --- | --- | --- |
| `telegram_topics` | object from lowercase title to integer topic ID | Stable Telegram binding. Preserve every valid entry, including one whose route is temporarily absent. |
| `collab` | object from decimal tab ID string to remaining rounds | Current persistence uses ephemeral tab IDs. Migration must resolve from the stopped layout snapshot or reject ambiguity. |
| `clod_off_groups` | array of normalized Signal group IDs | Muted groups. Preserve exactly. |
| `signal_groups` | object from lowercase title to Signal group ID | Stable Signal binding. Preserve normalized IDs. |
| `last_sources` | object from lowercase title to `tg`, `sig`, `debate`, or `slack` | Preferred output channel. Preserve only recognized values. |
| `clod_off` | legacy array of tab IDs | Resolve through legacy state or reject ambiguity, then remove. |
| `clod_history` | legacy object from group ID to message list | Import into Signal inbox without marking delivered, then remove. |
| `signal_group_names` | legacy object from tab ID to title | Used only to recover legacy numeric `signal_groups` keys. |

The file has no schema field. It is atomically replaced and fsynced, but its default write mode follows the process umask rather than forcing `0600`. Migration reads it as legacy input and never modifies it.

### `pending_sends.json`

Default path: beside `state.json` or `WEZ_TG_PENDING`. Schema is `panetone.delivery-state.v2`.

| Field | Shape | Meaning and migration rule |
| --- | --- | --- |
| `saved_at` | Unix milliseconds | Diagnostic timestamp, not ordering authority. |
| `cursors` | object from stable source key to provider-specific cursor object | Import for rollback evidence only. Rust never reads provider stores. A provider cutover maps the last safe cursor to a Wakterm event watermark. |
| `items[].id` | string UUID | Durable output item identity. Preserve. |
| `items[].kind` | `tg`, `sig`, `debate`, or `slack` | Destination adapter. Preserve. |
| `items[].target` | adapter-specific topic, group, or channel ID | Resolve into a channel binding without silently changing destination. |
| `items[].chunk` | string | Pending body. Preserve byte-for-byte after JSON decoding. |
| `items[].pane_id` | integer | Historical origin only. Do not use as durable identity. |
| `items[].harness` | provider name | Historical formatting metadata. Map to an opaque Wakterm agent when unambiguous. |
| `items[].route_title` | lowercase title or empty | Resolve to a route UUID. Reject ambiguous non-empty titles. |

The outbox and cursors form one atomic checkpoint. Delivery is at least once, and successful remote acceptance may duplicate after a crash before dequeue.

### `signal.db`

The `signal_messages` table contains message identity, group, receive and envelope timestamps, sender fields, original and formatted text, raw JSON, direction, accepted, command and mention flags, and delivered timestamp. Its deduplication key is `(group_id, sender_id, envelope_timestamp)`. The `signal_history` view is diagnostic.

Migration preserves every row, primary key, deduplication identity, direction, acceptance flags, body, raw JSON, and delivery state. The current database has no explicit schema version and performs an in-place legacy table rewrite at startup. Rust migration operates on a copy while Python is stopped.

### `control-journal.sqlite3`

`control_request` stores request ID, semantic hash, source, target, state, progress JSON, response JSON, and timestamps. `return_delivery` stores source and target snapshots, terminal result, independent agent and Telegram states, one shared last error, and timestamps. `control_meta` stores the Wakterm request-terminal event cursor.

Migration preserves all requests, including pending and indeterminate rows, response bodies, progress, return results, destination states, timestamps, and the cursor. The shared callback error is copied to each destination that is not delivered, with the migration manifest recording that the original destination attribution was unavailable.

## Process-local state that must be classified

| State | Current owner | Restart result | Target disposition |
| --- | --- | --- | --- |
| Pane, tab, harness, cwd, topic, and group indexes | Discovery loop | Rebuilt | Wakterm catalog plus Panetone route bindings |
| Telegram, Signal, and debate reply maps | Channel handlers | Lost | Durable inbound work or explicitly best-effort UI context |
| Last active pane per route | Channel and output handlers | Lost | Opaque live agent selection recorded with an incarnation |
| Signal command and input queues | Signal callback | Lost | Durable inbox plus bounded worker queue |
| Slack message, observer, and direct queues | Slack callback | Lost | Durable inbox plus bounded worker queue |
| Collab signoffs and debate crosspost toggle | Workflow handlers | Lost | Persist if retained as supported behavior |
| Background task handles and health | Startup supervisor set | Cancelled at clean shutdown, failures logged | One typed supervisor and explicit degraded state |

## Deliberate future differences

The following are not parity failures when backed by their migration or compatibility test:

- explicit semantic defaults hash identically
- stable route UUIDs replace durable reliance on titles and tab IDs
- permanent compact UUID tombstones replace expiration of completed idempotency
- provider discovery, parsing, and file cursors move entirely to Wakterm
- each callback destination receives its own error field
- accepted Telegram, Signal, and Slack input is not intentionally dropped on restart
- all durable truth moves into one versioned SQLite database
- task failure and backlog become visible through `status --json` and `doctor`

## Phase 1 evidence checklist

- [x] Architecture authority, route, delivery, storage, and shutdown decisions are recorded.
- [x] Every production state source is inventoried.
- [x] Control v1 golden fixtures reproduce Python behavior.
- [x] Provider and routing fixtures reproduce current normalized observations.
- [x] A black-box harness runs the same control cases against a replaceable backend command.
- [ ] Wakterm Agent API fixtures cover capability, catalog, receipt, events, cursor gaps, retention, lifecycle, and classified errors.
- [ ] Independent protocol review has no unresolved material finding.
- [ ] Codex recording-sink shadow comparison has zero unexplained normalized differences.

The shadow comparison is discovery evidence. It cannot be cited as production cutover evidence.
