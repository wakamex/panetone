# Python and Rust Panetone architecture review

Date: 2026-08-21

Status: architecture review for an external reviewer. This document evaluates
the implementations against the current single-user requirement. It is not a
cutover plan and does not authorize a service restart or state change.

Post-review outcome: the owner permanently retired Python and authorized the
cutover cleanup recommended here. The current working tree removes the Python
migration module and CLI, promotion authority, per-route promotion policy,
operator replay journal, and imported legacy tables. Schema version 6 preserves
ordinary routes, including Debate Signal bindings, and runtime cursors. The
quantitative inventory below describes the pre-cleanup Rust snapshot that was
reviewed and should not be mistaken for the smaller post-review tree.

## Verdict

The current Rust implementation is over-engineered for the present use case,
but its core architecture is not.

The following changes solve real problems and should remain:

- Wakterm, rather than Panetone, owns provider discovery, session selection,
  parsing, turn boundaries, and live agent incarnation identity.
- Panetone stores channel bindings and delivery state in one versioned SQLite
  database.
- Remote output and accepted inbound messages are durable before processing.
- A control request that may have reached an agent is not silently retried.
- Panetone runs as a systemd user service that an agent can inspect and restart
  without root.

The excess is concentrated in machinery created for the Python-to-Rust
transition and for a stricter deployment model than the user requires:

- global promotion hold and per-route release policy
- idempotent operator actions for one-time cutover commands
- legacy disposition tables and permanent runtime diagnostics for migration
  artifacts
- a 1,580-line migration implementation compiled into the main product
- offline and conformance service implementations alongside production
- fake adapters in the production source tree
- six independent one-second production workers with fail-permanently behavior
- capability negotiation before almost every Wakterm operation
- a durable busy-target queue whose semantics conflict with the user's stated
  expectation that Telegram messages can steer an active turn

The recommended direction is a smaller Rust daemon, not a return to provider
parsing in Python. Retain the Wakterm boundary and the minimum durable store,
then remove transition-only subsystems, consolidate workers, and make retry and
steering semantics match actual use.

## Review question

The question is not whether the Rust implementation is more structured than a
single Python script. It necessarily has more explicit types and modules. The
question is whether each additional authority, durable state machine, queue,
table, worker, command, and compatibility layer pays for an observed
requirement for this deployment.

This review uses the following test:

1. What concrete failure or desired workflow does the component address?
2. Did the Python implementation already address it adequately?
3. Does Wakterm or a remote channel already own the same durable truth?
4. Is the component permanent product behavior or one-time transition work?
5. Is there a smaller implementation with the same user-visible result?
6. Has the component itself caused or amplified an observed failure?

## User engineering preferences are evaluation criteria

This review applies the owner's standing engineering preferences as explicit
evaluation criteria. They come from the global agent guidance in
`~/.codex/AGENTS.md`, the decisions recorded in this repository, and the
conversation that led to this review. There is no Panetone-specific
`AGENTS.md`. The `/code/AGENTS.md` confidentiality rule does not affect this
architecture comparison.

These preferences are requirements for a design that serves this user. They
are not evidence that any particular Rust component is unnecessary. The
component-level findings below still require source, runtime, or workflow
evidence.

| Standing preference | Consequence for this review |
| --- | --- |
| Prefer the smallest clean design that solves the real requirement. | Every subsystem, durable authority, worker, and compatibility layer needs a concrete current benefit. Rust by itself is not a reason for more machinery. |
| Prefer general solutions only when they simplify the design. | An abstraction does not earn its cost merely by supporting hypothetical providers, deployments, or future users. |
| Start from the ideal long-term architecture and work backward. | The durable Wakterm boundary can be retained, but transition scaffolding needs an expiry condition and removal path. |
| Require evidence before paying speculative complexity or migration cost. | New journals, queues, recovery systems, policy layers, and compatibility frameworks need an observed failure or realistic workflow that a smaller change cannot address. |
| Allow a local preventive fix when the code directly proves a realistic failure. | Wakterm's bounded `NotificationQueue` is justified by the observed `cat <binary>` output storm and OOM path. Its accidental subscriber-lifetime bug should be fixed locally rather than used as evidence that all queues are bad. |
| State the benefit, simpler alternatives, and proportionality before making machinery a roadmap gate. | The reviewer should compare each retained mechanism with deletion, a direct call, an in-memory state transition, or a simpler retry before accepting it. |
| Remove or rewrite plans when evidence invalidates their premise. | Old phase, promotion, and parity gates should not survive merely as historical residue after the deployment goal changed. |
| Falsify a fix with the cheapest existing counterfactual and change one causal layer at a time. | Cross-layer failures must be attributed before redesign: the Wakterm subscriber leak belonged to Wakterm, while Panetone's permanent worker death after one timeout belongs to Panetone. |
| After two failed interventions at one gate, audit the premise and form a new hypothesis. | Repeated restart, preflight, or promotion failures are a reason to question the gate, not automatically to add another recovery layer. |
| Verify effective runtime state, not only source state. | A committed Wakterm fix is distinct from the binary and service actually loaded. Reviews should record executable hashes, units, configuration, and observed behavior where relevant. |
| Prefer agent-manageable systemd user services for unprivileged long-lived processes. | Panetone should not gain a system service, privileged operator path, or root-owned deployment machinery without a concrete privilege requirement. |

The conversation adds several Panetone-specific decisions to that standing
rubric:

- Five months of generally reliable Python operation is relevant production
  evidence. A Rust rewrite must identify the concrete benefit of each added
  mechanism rather than treating the old implementation as an untrusted toy.
- Direct observation of the wanted workflows is an acceptable completion
  test. Full uptime, feature-parity, and promotion matrices are not goals for
  this single-user deployment.
- Panetone may be intentionally stopped, so continuous availability and
  unattended failover are not requirements.
- Telegram messages sent during an active turn should be delivered as steering
  instructions rather than held until the agent becomes idle.
- Slack is gone. Preserving or recreating Slack-related generality has no value.
- Signal and Debate are optional. Their untested parity should not block the
  Telegram workflow.
- Agent restartability is a real operational requirement, which supports a
  systemd user service and inspectable durable state without implying a larger
  deployment control plane.

An external reviewer should not:

- infer that Rust requires multiple workers, policy objects, or durable state
  machines;
- count one-time migration safety as a permanent runtime requirement;
- reject all added structure merely because Python had fewer lines;
- charge Panetone for a Wakterm defect, or excuse a Panetone defect because it
  was exposed by Wakterm;
- preserve an obsolete roadmap gate after its premise has been rejected; or
- promote an optional, unobserved feature into the definition of completion.

## User and operating context

An external review should evaluate the code against these constraints rather
than against a hypothetical hosted service:

- There is one user and one Fedora host.
- The host is headless.
- Panetone may be stopped. There is no continuous-uptime target.
- Feature completeness is judged by direct observation of wanted workflows.
- Telegram is required.
- Signal and the Debate Signal route are optional and should exist only while
  they are used.
- Slack has been removed and must not return.
- A systemd user service with lingering is preferred over a system service.
- Wrong-agent routing and duplicate prompts are high-cost failures.
- Exhaustive parity for unused historical features is not required.
- Telegram messages sent during an active agent turn are expected to be usable
  as steering instructions. The current Rust behavior instead waits for idle.
- The user has operated the Python version for about five months with generally
  reliable behavior.

The current roadmap records most of these constraints in
[`panetone-rust-roadmap.md`](panetone-rust-roadmap.md), especially its
observation-based completion rule and triggers for additional machinery.

## Compared snapshots

### Python production baseline

The primary Python baseline is commit
`b5b75ef1ec6de303f80a65e82075647bc62bc66d` from 2026-08-17, titled
`Use active Wakterm socket and archive legacy Signal inbox`.

This is the final Python production shape before the Rust trial. Rust source
already existed in the repository by then, but the running product was still
`bridge.py` plus `panetone_control.py` at that commit. This snapshot is
preferable to
the last repository commit before any Rust source because Slack had already
been removed from the Python runtime.

Those two Python files are unchanged between `b5b75ef` and the current working
tree, so the reviewed baseline can be inspected directly without reconstructing
an old worktree.

For historical context, commit `a607eb5` was the last snapshot before the first
Rust implementation commit. Its Python runtime was 4,585 lines and still
contained Slack. The reviewed final Python runtime is 4,384 lines after Slack
removal.

### Current Rust candidate

The Rust source baseline is commit
`45bfc33f6c5f425ed7e46ac725aed73f5cba986e` from 2026-08-19, titled
`Simplify Rust replacement roadmap`, plus the current uncommitted retirement
edits visible on 2026-08-21. Those edits remove old Phase 5B scripts and reframe
the system-service runbook, but they do not remove the promotion runtime,
operator commands, migration implementation, or legacy tables discussed here.

The current Rust trial database and service observations are included as
operational evidence. They are not treated as benchmark results.

### Evidence limits

This review reads the source, architecture records, migration manifest, live
trial database, systemd state, and logs from the failed Telegram response. It
does not have five months of normalized Python telemetry, and it does not claim
a controlled throughput or latency benchmark. Line counts describe maintenance
surface, not runtime quality. Signal, Debate, collaboration, and topic
recreation were not exercised during the Rust trial, so their current value is
left as a question rather than inferred from code.

## Quantitative inventory

The line counts below include comments and blank lines. Generated lockfiles are
excluded. Rust's source count includes 331 lines under `#[cfg(test)]`, so the
approximate non-test Rust source count is 10,169 lines.

| Measure | Final Python runtime | Current Rust implementation |
| --- | ---: | ---: |
| Production implementation lines | 4,384 | 10,500 total, about 10,169 excluding embedded tests |
| Main implementation files | 2 | 28 |
| Direct runtime dependencies | 2 Python packages | 12 Rust crates |
| Baseline or current integration test lines | 3,138 Python | 4,977 Rust |
| Explicit long-running application loops | about 4, plus Telegram framework polling | 9 supervised tasks, plus the store owner |
| Durable storage locations | 2 JSON files and 2 SQLite databases | 1 SQLite database |
| Durable tables | 4 principal tables across the SQLite files | 16 tables |
| Default Wakterm discovery or event cadence | one CLI list about every 2 seconds | at least capabilities plus events every 1 second |

The Rust implementation is about 2.3 times the size of the final Python
runtime after excluding its embedded unit tests. Size alone is not a verdict,
but it establishes where the burden of proof lies.

Five phase-specific Rust modules account for 2,979 source lines before any
cross-cutting operator, store, CLI, or test code is counted:

| Module | Lines | Current role |
| --- | ---: | --- |
| `src/migration.rs` | 1,580 | One-time Python state conversion and verification |
| `src/service/offline.rs` | 838 | Offline workflow implementation and fault injection |
| `src/service/conformance.rs` | 350 | Hidden replaceable conformance backend |
| `src/wakterm/fake.rs` | 140 | Fixture-backed Wakterm implementation |
| `src/channels/fake.rs` | 71 | Recording channel implementation |

These modules were useful while building and validating the replacement. Their
continued inclusion in the permanent product requires a separate argument.

## Python runtime architecture

The Python implementation is described as a single-file bridge, but its
production control journal is a second module. Its architecture is compact but
not stateless or simplistic.

```text
Wakterm CLI list + ps + provider stores
                  |
                  v
       process-local pane/session maps
                  |
                  v
 provider-specific readers and cursors
                  |
                  v
 pending_sends.json: output chunks + source cursors
                  |
                  v
          Telegram and Signal

Telegram handlers ----------------------> pane input
Signal subscription -> signal.db ------> pane input

local control socket -> control journal -> Telegram audit -> Wakterm admission
                                            |
                                            v
                                  durable return watcher
```

### Authority and discovery

Every two seconds, `poll_loop` calls `_refresh_telegram_routes`, which invokes
`wakterm cli list --format json`. Python also inspects processes by TTY and
searches provider-specific state:

- Claude and Codex JSONL sessions
- Gemini state
- OpenCode SQLite state
- provider-specific cwd, timestamps, and cursor formats

Python therefore owns both messaging policy and a second interpretation of
agent truth. That duplication produced known stale-session and identity risks
and is the strongest architectural reason to replace this part.

### Output durability

Python reads provider output into normalized messages. It atomically replaces
`pending_sends.json` with both:

- every newly derived remote output chunk
- the next cursor for each provider source

It does this before remote delivery. A crash cannot advance a provider cursor
without also retaining the corresponding output. Delivery is at least once,
so a crash after remote acknowledgement but before local dequeue can duplicate
a message.

This is a sound and relatively small durability design. Rust did not invent
durable output delivery. It generalized and centralized it.

### Inbound behavior

Telegram is handled by `python-telegram-bot`. Process-local maps connect topic,
reply message, tab, pane, and harness. Replying to a particular bot message can
select the corresponding pane within a shared topic.

Signal input is archived in `signal.db` before routing. Signal's subscription
loop catches disconnections, waits five seconds, and reconnects. Accepted
messages remain undelivered in the database until routed.

Some Telegram reply maps, Signal command queues, collaboration state, and
debate state are process-local. Their loss on restart is a real limitation,
although five months of use did not establish that every one requires durable
replacement.

### Control and callback durability

The local Unix control server is separate from the Wakterm socket and uses
same-UID and filesystem protections. `control-journal.sqlite3` records request
identity, semantic hash, progress, terminal response, callback destinations,
and the Wakterm return cursor.

The control flow already enforces important ordering:

1. validate and resolve routes
2. post the Telegram audit
3. durably record audit progress
4. mark delivery in progress
5. submit through Wakterm
6. record success, rejection, or indeterminate delivery

Python therefore already had durable idempotency and at-most-once handling for
uncertain agent submissions. Rust makes these rules more explicit and extends
them, but the entire concept is not new complexity created by Rust.

### Failure behavior

The main Python `poll_loop` catches an exception for one tick, logs it, sleeps,
and tries again. The Signal receive loop reconnects after errors. The durable
return watcher retries with exponential backoff up to five minutes.

This coarse retry behavior is one reason the Python service felt reliable. A
single transient adapter error generally did not permanently disable a
subsystem.

### Python liabilities

The compact implementation also has real costs:

- 3,669 lines in one bridge file with extensive global mutable state
- duplicate ownership of provider and session truth
- four durable formats with different migration and permission behavior
- process-local routing and queue state with inconsistent restart semantics
- direct blocking SQLite and subprocess calls moved through `to_thread` on a
  case-by-case basis
- title and ephemeral tab or pane identity mixed into routing state
- no unified health view for failed background tasks
- automatic Telegram topic creation and recreation coupled to discovery
- optional collaboration and debate behaviors intertwined with core routing

The Python design is smaller, but not a clean long-term boundary.

## Rust runtime architecture

Rust removes provider parsing from Panetone. It consumes Wakterm's versioned
Agent API and treats Wakterm as authoritative for live agents, incarnations,
prompt admission, turns, and durable event order.

```text
Wakterm Agent API
      |
      v
wakterm-events worker
      |
      v
agent_events + consumed cursor + outbox in one SQLite transaction
                                     |
                                     v
                                outbox worker
                                     |
                                     v
                            Telegram and Signal

Telegram long poll -> inbox table -> inbox worker -> Wakterm admission
Signal subscription -> inbox table -> inbox worker -> Wakterm admission

control socket -> workflow state -> audit -> busy queue or admission
                                  -> return state -> mirror or callback

operator socket -> promotion hold, route policy, cursor baselines,
                   reconciliation, and legacy dispositions
```

All store operations pass through one bounded Tokio channel to one blocking
SQLite owner. The production supervisor starts:

- one critical control task
- Wakterm event polling
- outbox delivery
- busy-target retries
- return-terminal polling
- pending-return delivery
- inbox delivery
- Telegram inbound polling when configured
- Signal inbound subscription when configured

The six middle workers run on the same default one-second cadence.

### Unified durable store

The current schema contains 16 tables:

| Permanent-looking core | Transition and policy |
| --- | --- |
| `routes` | `promotion_state` |
| `idempotency_tombstones` | `route_delivery_policy` |
| `workflows` | `operator_actions` |
| `return_deliveries` | `legacy_control_requests` |
| `outbox` | `legacy_return_deliveries` |
| `inbox` | `legacy_debate_outbox` |
| `metadata` | `legacy_dispositions` |
| `signal_messages` |  |
| `agent_events` |  |

One versioned database is easier to inspect, back up, permission, and migrate
than four independent state locations. Atomic event-to-outbox and
inbound-before-admission transactions are concrete improvements.

The current schema nevertheless embeds the cutover in the permanent runtime.
Seven of the 16 tables exist for promotion policy or legacy reconciliation.
They should not survive indefinitely merely because they were useful during
one migration.

### Stable route and agent identity

Rust assigns a UUID to each Panetone route, preserves channel bindings when an
agent disappears, and binds the route to Wakterm's opaque agent and incarnation
identity. This prevents a reused pane ID or title from silently receiving an
old request or callback.

The protection is valuable for wrong-agent routing and asynchronous callbacks.
The present implementation is incomplete as a daily-use feature because it
has no simple command to create a new route and channel binding. The first live
trial therefore required the full legacy migration just to populate routes.
That reverses the desired dependency: a permanent route model should make
fresh setup simpler than migration, not require migration.

### Event consumption and duplicated durability

Rust reads Wakterm's durable event stream, stores every event in
`agent_events`, projects visible messages into `outbox`, and advances its
consumed cursor in the same transaction.

The atomic projection is justified. Retaining a second full event history is
less clearly justified because Wakterm already owns the durable ordered stream.
Panetone needs a cursor, event deduplication, projection state, and durable
outbox effects. It does not necessarily need an indefinitely retained copy of
every processed lifecycle and turn event. A compact deduplication record or
bounded processed-event retention may provide the same safety.

### Inbound durability and admission

Rust persists Telegram and Signal input in `inbox` before attempting Wakterm
admission. This makes accepted Telegram input restart-safe, unlike Python's
process-local Telegram handler state.

Before admission, an inbox item moves from `pending` to
`admission_prepared`. If the Wakterm result is lost, the item becomes
`indeterminate` and is not submitted again. This is appropriate for avoiding a
duplicate prompt.

The semantic choice around an active turn is not appropriate for the current
expectation. Wakterm's admission API reports `busy` without writing, and Rust
leaves the message pending until the target becomes idle. The user observed
that Telegram messages waited until turn end instead of steering the current
turn. This is deliberate behavior from the busy-target architecture, not a
timing bug.

Steering and queued new work are different operations. The architecture should
expose that difference instead of forcing every Telegram message through the
queued-new-turn path.

### Supervision and retries

The typed supervisor makes task state visible through status. That is useful.
Its current degraded-task policy is not resilient:

- a worker method returns an error
- `production_worker_loop` propagates it with `?`
- the supervisor records the degraded worker as failed
- the worker is never restarted
- the daemon remains active, so systemd does not restart it

This exact behavior caused Telegram replies to disappear during the trial. A
single Wakterm CLI timeout permanently stopped `wakterm-events` while inbound
Telegram continued running.

Python's smaller loop caught a tick error and retried. Rust added a supervisor
and health model but regressed the basic recovery behavior. The fix does not
require another subsystem. Each long-running adapter loop should classify
errors, record the latest error, back off, and retry until shutdown. A truly
fatal invariant or schema error should still stop the process.

### Wakterm subprocess use

Production startup calls version, capabilities, and catalog concurrently, but
`catalog()` calls capabilities again. Each event poll calls capabilities and
then events. Admission, catalog, and terminal operations also repeat capability
checks.

At the default cadence, the event path alone creates at least two short-lived
Wakterm CLI clients per second. Python normally created one discovery client
every two seconds and kept its return watcher alive.

This did not create Wakterm's notification subscriber lifetime bug. That bug
belonged to Wakterm and has been fixed there. Rust's redundant connection rate
exposed and amplified it. A startup capability check plus revalidation after a
classified compatibility or reconnect failure is sufficient for one local
daemon and one local mux.

## Direct behavior comparison

| Concern | Python production | Current Rust | Review |
| --- | --- | --- | --- |
| Provider truth | Panetone parses provider state and process trees | Wakterm supplies normalized events and identities | Rust boundary is materially better |
| Route identity | Titles plus ephemeral tab and pane maps | Stable route UUID plus opaque agent incarnation | Keep, but add simple route creation |
| Telegram output durability | Atomic pending chunks plus provider cursors | Atomic event projection, cursor, and outbox | Both are durable; Rust centralizes it |
| Telegram inbound durability | Framework handler and process-local reply maps | Durable inbox before admission | Rust improves restart safety |
| Active-turn Telegram input | Can write into the current pane and steer | Waits for authoritative idle | Rust does not match current expectation |
| Multi-harness shared topic | Reply message can select a pane or harness | One current agent binding per route | Python is more capable here |
| Signal input | Archived before routing and reconnects after failure | Durable unified inbox and supervised subscriber | Rust storage is cleaner; retry policy is worse |
| Local control idempotency | SQLite journal with 30-day completed retention | Unified workflow plus permanent tombstone | Rust is stronger, but permanence has low marginal user value |
| Uncertain prompt result | Recorded indeterminate and not retried | Recorded indeterminate and not retried | Required invariant in both |
| Busy local-control target | Definitive rejection or failure path | Durable wait-until-idle queue | Added behavior is useful only if the user wants queued new turns |
| Return-final | Durable watcher and per-destination delivery state | Unified workflow and return tables | Same core requirement, more explicit in Rust |
| Adapter retry | Main loops catch, reconnect, and retry | One error permanently fails most degraded workers | Python behavior is better |
| Health reporting | Logs and task completion callback | Structured supervisor status and doctor | Rust is better if workers also recover |
| Fresh setup | Discovers tabs and creates topics automatically | Requires migrated routes; no fresh route creation command | Rust is operationally worse |
| Topic recreation | `/refresh` and automatic retargeting | Deferred | Acceptable only while unused |
| Collaboration | Implemented | Deferred | Acceptable only while unused |
| Slack | Removed from final baseline | Removed and rejected | Equivalent |
| Service ownership | User service was available and reliable | User service trial | Correct direction |

## Trial evidence from 2026-08-21

The first Rust trial provides more useful evidence than the earlier phase
documents because it exercised the real user, mux, Telegram channel, and
migrated state.

### State and setup

- Migration produced 40 durable routes from the stopped Python state.
- Fourteen unambiguous live routes were enabled.
- The database contained 79 imported control tombstones, 668 Signal messages,
  11 legacy return deliveries, and no migrated pending outbox work.
- Fresh route creation was unavailable, so migration was required even though
  most historical state was not important to the user.

### Successful observations

- Telegram input was durably accepted and reached the intended Panetone agent.
- The corrected Wakterm continuation-session selection allowed the agent reply
  to be observed under the right incarnation.
- Missing assistant responses remained intact in Wakterm's durable event
  stream.
- After recovery, Rust ingested the retained events and delivered the missing
  responses exactly once from its outbox.
- The consolidated database made cursor, inbox, outbox, and worker-state
  diagnosis possible.

These observations support the Wakterm authority boundary and the durable
inbox, cursor, and outbox core.

### Failed observations

1. One Wakterm CLI timeout permanently failed the `wakterm-events` worker.
2. The daemon stayed active, so systemd did not repair the failed worker.
3. Telegram input continued, which made the service appear partially healthy.
4. Assistant replies existed in Wakterm but were never projected into the
   Telegram outbox.
5. Restarting Panetone entered repeated startup failures because every preflight
   had a fixed ten-second Wakterm deadline.
6. Wakterm was overloaded by a separate disconnected-client subscriber leak.
   Rust's high CLI connection rate accelerated it.
7. After the Wakterm backlog was cleared, Panetone caught up and delivered 38
   queued messages in a burst. Telegram rate-limited three additional
   diagnostic messages for 30 seconds.
8. Telegram input sent during an active turn waited for turn completion rather
   than steering it.

The Wakterm subscriber leak was not caused by Panetone and is not evidence
against Wakterm's bounded notification queue. It was a client-lifetime bug in
Wakterm. The permanent worker death, redundant capability calls, fixed startup
failure loop, catch-up pacing, and steering mismatch are Panetone concerns.

At the time of this review, both Python and Rust Panetone services are stopped.
The Rust service was stopped deliberately to avoid recreating pressure before
the Wakterm fix is installed and active. Wakterm source commit `bba995d8e`
contains the subscriber lifetime fix, while the running installed mux still
identified itself as the earlier `d2a36c05` build. Activating that fix requires
a separately planned mux replacement because the service owns the live panes.

## Complexity assessment by subsystem

| Subsystem | Concrete benefit | Simpler alternative | Verdict |
| --- | --- | --- | --- |
| Wakterm Agent API authority | Eliminates duplicated provider parsing and supplies exact incarnation identity | None inside Panetone | Retain |
| Stable route UUID | Prevents title and pane reuse from retargeting durable work | Title-only routes are simpler but unsafe for callbacks | Retain, simplify reconciliation |
| Unified SQLite owner | One transaction boundary and one backup surface | Direct pooled connections are unnecessary at this scale | Retain one owner |
| Durable inbox and outbox | Survives restart and prevents cursor-only advancement | Python already proves a small durable queue works | Retain with fewer states |
| Control workflow state | Avoids duplicate prompts after uncertain acceptance | Python journal had the same essential invariant | Retain, reduce response detail where unused |
| Permanent tombstones | Prevents an old UUID from becoming new work | Thirty-day expiry was adequate for five months | Low-cost optional protection |
| Full `agent_events` archive | Diagnostics and event deduplication | Cursor plus compact processed-event IDs and outbox rows | Bound or remove full history |
| Typed supervisor | Visible health and coordinated shutdown | A few named retrying tasks are sufficient | Retain health, replace fail-permanently policy |
| Six one-second effect workers | Separates workflow categories | One ordered effect-drain loop plus event and channel loops | Consolidate |
| Capability check per operation | Detects a changed API on every subprocess | Cache startup result and recheck after compatibility failure | Remove repeated checks |
| Durable busy-target queue | Protects an active turn from unrelated new work | Explicit `queue` operation and separate `steer` operation | Do not use as universal inbound behavior |
| Global delivery hold | Safe one-time cutover | Start disabled once, or stop the service during setup | Remove after trial |
| Per-route delivery policy | Canary cutover control | Enable routes in ordinary route configuration | Merge into route enabled state |
| Audited operator action replay | Idempotent scripted promotion | Manual single-user commands with clear output | Remove after cutover |
| Legacy disposition tables | Prevented uncertain migration replay | Preserve immutable migration bundle outside runtime | Archive and drop after acceptance window |
| Offline migration bundle | Safe copy-first conversion | Fresh route binding would avoid migration for discarded history | Extract from permanent daemon after cutover |
| Offline and conformance services | Enabled phase construction and cross-language comparison | Focused domain, store, and adapter tests | Remove hidden runtime backend after transition |
| System-service installer and runbook | Supports root-owned deployment | User service already meets ownership needs | Archive unless a privilege need appears |

## Proposed smaller Rust architecture

The target can keep Rust's useful boundaries with fewer permanent concepts.

### Runtime tasks

Use five named retrying loops plus the SQLite owner:

1. Control server.
2. Wakterm event consumer with bounded backoff and capability refresh after a
   classified reconnect or compatibility error.
3. One effect drain for inbox, outbox, queued control work, and pending returns.
4. Telegram long poll with bounded backoff.
5. Signal subscription with bounded backoff when Signal is enabled.

The effect drain can preserve ordering with one small priority rule:

1. checkpoint newly accepted inbound work
2. classify or submit prepared agent effects
3. project and send durable channel effects
4. retry definitive busy work only when Wakterm reports idle

Separate workers are warranted only when measurement shows one category blocks
another despite bounded adapter calls.

### Startup

Startup should:

1. open and migrate the current Rust schema
2. negotiate Wakterm version and capabilities once
3. load routes and channel configuration
4. bind the control socket
5. start retrying loops

A transient Wakterm failure should leave Panetone running and visibly degraded,
then retry with backoff. A schema incompatibility, unsafe database path, or
invalid credentials configuration may still fail startup.

This avoids a systemd restart loop when Wakterm is temporarily slow.

### Persistent state

A reviewer should attempt to reduce the permanent schema toward:

- schema metadata and consumed channel or Wakterm cursors
- routes with enabled state and channel bindings
- inbox effects
- outbox effects
- control workflows and optional return state
- compact idempotency records

Processed Wakterm events can be pruned after their projection and cursor are
durable. Promotion and legacy tables can move to the immutable migration bundle
or an archived database once the acceptance window closes.

### Route management

Add a small explicit daily-use surface before preserving migration machinery:

```text
panetone route bind TITLE --telegram-topic ID [--signal-group ID]
panetone route reconcile TITLE
panetone route enable TITLE
panetone route disable TITLE
panetone route list
```

Automatic discovery may be added only if repeated manual binding is a real
burden. A fresh empty database must be usable without importing Python history.

### Steering and queued work

Define two operations with different audit labels and Wakterm contracts:

- steer the current active turn now
- queue a new prompt for the next idle turn

Telegram's default should follow the user's chosen behavior. Local control can
keep queue-until-idle for `--return-final` if exact turn correlation requires a
new turn. The architecture should not silently translate steering intent into
delayed work.

## Recommended simplification sequence

This ordering removes observed pain before deleting historical scaffolding.

1. Make every recoverable production loop retry with bounded backoff and report
   its latest error and retry time.
2. Cache Wakterm capabilities after startup. Remove the capability subprocess
   from each event, catalog, admission, and terminal operation.
3. Define and test steering versus queued-new-turn behavior.
4. Add fresh route binding so migration is no longer required for normal use.
5. Pace outbox catch-up according to Telegram retry information and avoid
   turning internal diagnostic chatter into a channel flood.
6. Complete the short Rust acceptance window and preserve one immutable legacy
   migration bundle.
7. Remove global promotion hold, operator replay, route delivery policy, and
   live legacy disposition machinery.
8. Extract or archive the Python migration command after rollback is no longer
   wanted.
9. Remove the hidden offline and conformance runtime backends. Keep focused
   tests for routing identity, atomic projection, uncertain admission, adapter
   retries, and migration fixtures only while migration remains supported.
10. Consolidate the six effect workers unless a measured blocking interaction
    requires separation.

No new subsystem is needed for this sequence.

## Questions for an external reviewer

The reviewer should answer these questions against the source and the actual
single-user workflows:

1. Can `agent_events` be reduced to bounded deduplication after atomic
   projection, given Wakterm's durable event ownership?
2. Can return delivery state be represented inside the control workflow rather
   than in a second state machine and table?
3. Which imported legacy rows still have operational value after the user has
   explicitly dispositioned the seven uncertain requests?
4. Is permanent UUID reservation worth its code path, or is a long finite
   retention adequate for one user?
5. Does stable route identity require UUIDs plus a separate delivery-policy
   table, or can enabled state live on the route?
6. Can a single effect drain preserve the needed ordering without six polling
   workers?
7. Which failures are genuinely fatal, and which should be logged and retried?
8. What exact Wakterm operation should implement steering without weakening
   new-turn admission and return correlation?
9. Does the user still exercise multi-harness shared topics, collaboration,
   topic recreation, Signal mute commands, or Debate? If not, should their old
   Python behavior remain retired rather than shape Rust abstractions?
10. After fresh route binding exists, what continuing user workflow requires
    migration or promotion code in the main binary?

## Reproduction and source references

The main source entry points are:

- Python runtime: `bridge.py` at commit `b5b75ef`
- Python control journal: `panetone_control.py` at commit `b5b75ef`
- Rust startup and worker creation: [`src/main.rs`](../src/main.rs)
- Rust production workflows: [`src/service/production.rs`](../src/service/production.rs)
- Rust offline workflows: [`src/service/offline.rs`](../src/service/offline.rs)
- Rust store and schema: [`src/store/mod.rs`](../src/store/mod.rs)
- Rust Wakterm subprocess adapter: [`src/wakterm/cli.rs`](../src/wakterm/cli.rs)
- Rust supervisor: [`src/supervisor.rs`](../src/supervisor.rs)
- Migration implementation: `src/migration.rs` at commit `45bfc33`
- Original authority decisions: [`docs/architecture/0001-authority-boundaries.md`](architecture/0001-authority-boundaries.md)
- Original storage decisions: [`docs/architecture/0004-storage-migrations-and-shutdown.md`](architecture/0004-storage-migrations-and-shutdown.md)
- Current single-user scope: [`docs/panetone-rust-roadmap.md`](panetone-rust-roadmap.md)

The quantitative counts can be reproduced with:

```sh
git show b5b75ef:bridge.py | wc -l
git show b5b75ef:panetone_control.py | wc -l
find src -name '*.rs' -print0 | sort -z | xargs -0 wc -l
find tests -maxdepth 1 -name '*.rs' -print0 | xargs -0 cat | wc -l
```

## Final assessment

The Python implementation earned trust through months of use and had a good
small durability pattern for output plus a real control journal. Its main
architectural defect was duplicating Wakterm's provider and session authority,
not its language.

The Rust replacement correctly removes that duplicate authority and provides a
cleaner durable core. The live trial proved that retained Wakterm events could
be projected and delivered after interruption. It also proved that the current
worker and preflight design is less resilient than Python's simple retry loops.

The present Rust codebase combines three products:

1. the permanent local message router
2. a Python-to-Rust migration and promotion system
3. an offline conformance and fault-testing implementation

Only the first is a continuing user requirement. The second should expire
after the transition, and the third should be reduced to focused tests rather
than remain a parallel runtime architecture.

The appropriate decision is to simplify the Rust implementation around its
useful core. Keeping all current machinery would be over-engineered for this
deployment. Reverting to Python provider parsing would discard the strongest
architectural improvement and is not recommended.
