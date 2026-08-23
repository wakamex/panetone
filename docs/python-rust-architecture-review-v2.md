# Python and Rust Panetone architecture review v2

Date: 2026-08-21

Status: post-cutover architecture review for an external reviewer, updated
after the third cleanup on 2026-08-21. The owner permanently retired Python,
removed cutover machinery, and then removed the remaining transition-only
runtime, deployment, tests, and phase records. It does not authorize a service
start or database rewrite.

The original review remains at
[`python-rust-architecture-review.md`](python-rust-architecture-review.md). It
is the pre-cleanup record. This v2 is self-contained but uses the original for
the longer reconstruction of the Python implementation and the first Rust
trial.

## Verdict

The three cleanup passes removed the largest sources of over-engineering. The
active tree now contains one Rust router plus focused test adapters. Migration,
promotion, the cross-language conformance daemon, the Python product, the root
deployment alternative, and old phase machinery are gone. Git plus the cold
migration bundle preserve the history.

The current durable core is defensible for this user's requirements. The third
cleanup removed 364 Rust CLOC from `src`, including about 329 non-test CLOC, by
deleting expired compatibility, dead parity helpers, unused commands, and
test-oriented CLI surface. The remaining excess is concentrated in worker
topology, repeated Wakterm capability subprocesses, a production workflow type
coupled to test fakes, and a full local event archive whose retention and
diagnostic value have not yet been decided.

The production router still has two concrete design defects:

- one transient error permanently kills a degraded worker while the process
  stays active
- Wakterm capabilities are re-read through additional CLI processes during
  routine operations after a successful startup negotiation

Both defects were identified in the first review and remain in the current
source. They need local fixes, not another recovery or deployment subsystem.

The next work should fix retry and subprocess behavior. Consolidating queues,
tables, workers, or return state should wait until a smaller design has been
tested against the exact workflows that still matter.

## User requirements and review standard

This is a one-user local service on one headless Fedora host. The relevant
requirements are:

- Telegram is the primary exercised channel.
- Signal and Debate remain supported when configured. Debate groups are
  ordinary Signal-bound routes, not legacy migration machinery.
- Slack is removed and must not return.
- Panetone may be intentionally stopped. There is no continuous-uptime target.
- Direct observation of wanted workflows is an acceptable completion test.
- Wrong-agent delivery and duplicate prompts after an uncertain result are
  high-cost failures.
- Telegram input during an active turn is expected to steer when Wakterm
  supports steering. Silently waiting until idle does not meet that intent.
- A systemd user service is preferred because an agent can inspect and restart
  it without root.
- Python is permanently retired. Rollback to Python and ongoing Python parity
  are not requirements.

The owner's standing engineering preferences are also design criteria:

| Preference | Review consequence |
| --- | --- |
| Use the smallest clean design that solves a real requirement. | Every durable authority, queue, worker, compatibility path, and abstraction needs a current benefit. |
| Prefer generality only when it simplifies the system. | Hypothetical deployments, users, and providers do not justify permanent interfaces. |
| Work backward from the desired long-term architecture. | One-time transition code needs removal once it no longer advances that architecture. |
| Require stronger evidence for a subsystem, redundant state, journal, queue, or recovery mechanism. | A possible future failure is not enough when a direct local solution exists. |
| Remove an invalidated plan instead of accumulating both plans. | Phase gates, promotion documents, and parity backends should expire after cutover. |
| Attribute a failure to one layer before editing it. | Wakterm's notification subscriber bug stays a Wakterm issue. Panetone's fail-permanently worker policy stays a Panetone issue. |
| Prefer systemd user services for unprivileged processes. | A root-owned installer and system unit need a privilege requirement that Panetone does not have. |

The five months of generally reliable Python operation are relevant evidence.
Rust structure is justified where it removes duplicate provider authority or
protects a concrete durability invariant. Rust is not itself evidence that
more machinery is needed.

## Compared snapshots

### Final Python production baseline

The final Python baseline is commit
`b5b75ef1ec6de303f80a65e82075647bc62bc66d` from 2026-08-17, titled `Use
active Wakterm socket and archive legacy Signal inbox`.

Its production runtime was:

- `bridge.py`, 3,669 lines at the baseline commit
- `panetone_control.py`, 715 lines at the baseline commit
- 4,384 lines total
- two direct Python packages, `aiohttp` and `python-telegram-bot`

Python owned provider-specific session parsing, route discovery, channel
delivery, channel input, control idempotency, returns, and several pieces of
process-local routing state. It used two JSON files and two SQLite databases.
Its compact output durability scheme wrote derived output and the corresponding
provider cursor together before delivery.

The Python control journal already protected the essential uncertain-admission
invariant. It did not blindly retry a prompt that might have reached an agent.
The Python poll and Signal loops generally caught transient failures and tried
again, which is one reason the service behaved reliably despite its less typed
architecture.

Python's main architectural problem was duplicated authority. It interpreted
provider files, process trees, panes, sessions, and turn boundaries even though
Wakterm was becoming the right owner of that information. It also mixed
ephemeral tab and pane identity with routing and spread persistence over four
formats.

### Current Rust working tree

The current tree is based on commit
`45bfc33f6c5f425ed7e46ac725aed73f5cba986e` from 2026-08-19, titled `Simplify
Rust replacement roadmap`, plus the uncommitted post-cutover cleanup present on
2026-08-21.

The three cleanup passes removed:

- the 1,580-line migration module and `migrate` command
- the promotion module, global hold, and route promotion policy
- the operator command and replay journal
- imported legacy control, return, Debate, Signal, and disposition tables
- Phase 4, Phase 5A, and Phase 5B promotion documents and preflight script
- the hidden conformance daemon and cross-language parity runner
- the Python runtime, client, dependency locks, and Python-only tests
- the root-owned system-service installer and rehearsal
- the remaining Phase 1 through Phase 3 reports
- the unused store compaction command and tests
- the schema-v5 cutover upgrader and Python request-hash interpreter
- frozen route and channel parity helpers and fixtures
- the unused standalone event-cursor command
- fixture-mode doctor and the removed-Slack environment guard
- zero-caller public methods and Tokio's unused `fs` feature

Schema version 6 performed the one remaining in-place transition. It preserved
normal routes, including Debate routes and Signal bindings, current workflows,
returns, inbox and outbox state, agent events, output preferences, and cursors.
It dropped the cutover-only tables.

Both `panetone.service` and `panetone-rust.service` were inactive during this
review. Both user units were disabled. This is an intentional and supported
state, not a failed availability target.

## Quantitative change

Counts include comments and blank lines. The Rust production estimate excludes
274 physical lines and 253 CLOC under four source-tree `#[cfg(test)]` blocks.
Generated lockfiles are excluded.

| Measure | Final Python runtime | Rust before cutover cleanup | Current Rust tree |
| --- | ---: | ---: | ---: |
| Physical implementation lines | 4,384 | 10,500 total, about 10,169 excluding embedded tests | 6,662 total, about 6,388 excluding embedded tests |
| CLOC | 3,548 | not recorded | 6,235 total, about 5,982 excluding embedded unit tests |
| Main implementation files | 2 | 28 | 25 |
| Direct runtime dependencies | 2 packages | 12 crates | 12 crates |
| Rust integration test lines | n/a | 4,977 | 3,367 physical, 3,189 CLOC |
| Durable tables | 4 principal tables across two databases | 16 | 8 |
| Production tasks with Telegram and Signal | about 4 loops plus framework polling | 9 supervised tasks plus the store owner | 9 supervised tasks plus the store owner |
| Release binary size | n/a | not used for this comparison | 11,770,056 bytes |

The current Rust production source is about 1.69 times the final Python CLOC.
That ratio is not itself a defect. Rust excludes provider parsers, owns one
durable store, models exact identity explicitly, and includes test adapters in
the source tree. The remaining difference needs to be judged by subsystem
rather than by line count alone.

The largest current modules are:

| Module | Lines | Role |
| --- | ---: | --- |
| `src/store/mod.rs` | 1,669 | schema, single SQLite owner, commands, projections, queues, and status |
| `src/service/offline.rs` | 838 | actual workflow engine plus fake backends and fault injection |
| `src/main.rs` | 610 | production wiring, six worker scheduler, channel loops, CLI, and doctor |
| `src/service/production.rs` | 645 | event, inbox, outbox, return, route, status, and control orchestration |
| `src/wakterm/cli.rs` | 493 | bounded Wakterm CLI adapter and wire validation |
| `src/domain/workflow.rs` | 367 | workflow state, receipts, callbacks, hashes, and tests |

## Current production architecture

Rust correctly moved provider and agent authority into Wakterm:

```text
Wakterm Agent API
      |
      v
events worker -> one SQLite transaction
                 - record event
                 - update route lifecycle
                 - enqueue visible output
                 - advance consumed cursor
                           |
                           v
                      outbox worker
                           |
                           v
                  Telegram or Signal

Telegram long poll -> durable inbox -> inbox worker -> Wakterm admission
Signal subscription -> durable inbox -> inbox worker -> Wakterm admission

local control socket -> durable workflow -> channel audit -> Wakterm admission
                                          -> optional return terminal
                                          -> channel mirror and agent callback
```

All SQLite access crosses one bounded Tokio channel to one blocking database
owner. The production supervisor runs:

- one critical control server
- six degraded one-second workers for events, outbox, busy targets, return
  terminals, pending returns, and inbox
- one degraded Telegram loop when configured
- one degraded Signal loop when configured
- the database owner outside the supervisor

The eight schema v6 tables are:

| Table | Continuing purpose |
| --- | --- |
| `routes` | stable Wakterm identity and Telegram or Signal bindings |
| `workflows` | local send ordering, admission state, response, and return intent |
| `idempotency_tombstones` | permanent reservation of completed request UUIDs |
| `outbox` | durable channel effects |
| `inbox` | durable remote input before agent admission |
| `return_deliveries` | asynchronous final result delivery to channel and source agent |
| `agent_events` | local copy, deduplication, diagnostics, and projection state for Wakterm events |
| `metadata` | Wakterm and Telegram cursors, route output preferences, and gap records |

This is a coherent durable core. The route, cursor, inbox, outbox, workflow,
and uncertain-admission concepts address exercised or high-cost failures. The
fact that the schema has eight tables is not evidence of over-engineering by
itself.

## Current production state

The authoritative schema v6 database passed `PRAGMA integrity_check` during
this review. Its contents were:

| State | Count |
| --- | ---: |
| Routes | 40 total, 14 available and 26 unavailable |
| Workflows | 0 |
| Outbox | 38, all delivered |
| Inbox | 14 total, 12 delivered and 2 archived |
| Current return deliveries | 0 |
| Agent events | 236 total, 41 projected, 169 recorded, and 26 unrouted |
| UUID tombstones | 79 total, all imported with an uninterpreted retired hash kind |

The metadata contains the Wakterm event cursor at 13,119, the Wakterm return
cursor at 4, the Telegram update offset, and route output preferences. No
promotion, migration-manifest, or legacy metadata remains in the runtime
database.

This state supports several cleanup conclusions:

- no current workflow or return row depends on Python rollback behavior
- all current channel effects are terminal
- the 79 imported records still reserve their UUIDs, but the application no
  longer interprets their retired hash format
- the local event archive is currently small, but it has no production
  retention call

## What the first cleanup resolved

| Pre-cleanup concern | Current result |
| --- | --- |
| Migration implementation compiled into the product | Removed |
| Global promotion authority | Removed |
| Per-route promotion policy | Removed |
| Operator replay journal and command | Removed |
| Legacy runtime tables | Removed |
| Debate confused with migration residue | Corrected. Debate is an ordinary Signal route. |
| Python rollback treated as a goal | Removed from current operations and roadmap |
| Fresh startup required migration to create a database | Removed. Missing cursors initialize from current Wakterm and Telegram heads. |
| Route identity replacement required promotion tooling | Replaced with direct `route reconcile` |
| Six fail-permanently workers | Unchanged |
| Repeated Wakterm capability subprocesses | Unchanged |
| Hidden conformance runtime | Removed |
| Fake adapters mixed into workflow production code | Unchanged |
| Root-owned deployment alternative | Removed |
| Retired Python runtime and active parity suite | Removed from the active tree |

The cleanup converged on the intended architecture instead of adding a second
post-cutover compatibility path. It is a material simplification, not only a
documentation rewrite.

## Remaining subsystem assessment

| Subsystem | Concrete benefit | Smaller alternative | Assessment |
| --- | --- | --- | --- |
| Wakterm Agent API authority | removes duplicate provider parsing and supplies exact incarnation identity | none inside Panetone | Keep |
| Stable routes | prevents title or pane reuse from silently retargeting durable work | title-only routing | Keep |
| One SQLite owner | supplies one transaction boundary and avoids async blocking | direct connections would not simplify this workload much | Keep |
| Durable inbox and outbox | preserves accepted input and output across restart | process memory | Keep |
| Workflow admission states | prevent duplicate prompts after uncertain acceptance | blind retry or loss | Keep |
| Return state | implements an explicitly supported `--return-final` workflow | remove only if the user retires that workflow after exercising it | Keep for now |
| Permanent tombstones | reserve old and new UUIDs after workflow compaction | finite retention | Keep. Retired hash kinds are conflict-only reservations. |
| Full `agent_events` rows | diagnostics and exact replay checks | cursor plus deterministic outbox IDs, or bounded recent diagnostics | Review after retry fixes |
| Typed supervisor | named health and coordinated shutdown | plain task joins | Keep, change retry policy |
| Six periodic workers | isolates effect classes | fewer retrying drains with explicit priority | Likely simplify after local retry fix |
| Capability read per operation | detects API change immediately | cache startup capabilities and refresh after a compatibility failure | Remove repeated reads |
| Busy-target queue | supports queue-until-idle local work | explicit steering and explicit queued-new-turn operations | Do not use as universal Telegram behavior |
| Fake variants in production workflow type | deterministic failure-boundary tests | test-only adapters behind a small trait or dedicated test support feature | Separate only if the result is smaller |
| Hidden conformance daemon | cross-language cutover comparison | focused Rust protocol and process tests | Removed |
| Root-owned system deployment | alternate installation model | existing user unit | Removed |
| Python runtime and parity suite | historical reconstruction | Git history, v1 review, and selected neutral fixtures | Removed |

## Second cleanup completed without new architecture

These items had no current production consumer and were removed as one focused
cleanup series.

### Hidden conformance product removed

Removed:

- `src/service/conformance.rs`, 350 lines
- the hidden `conformance-backend` command and its fake daemon arguments
- `tests/run_control_conformance.py`, 207 lines
- `tests/rust_control_conformance.rs`, 39 lines
- `tests/test_control_conformance.py`, 31 lines
- `docs/conformance.md`

Keep direct Rust control protocol, process, store, and uncertain-admission
tests. The cross-language backend proved its cutover premise and now maintains
a second behavior implementation solely to compare with a product that will
never run again.

### Root-owned deployment path removed

Removed:

- `deploy/install-system-service.sh`, 193 lines
- `deploy/panetone.service`, 37 lines
- system-service assertions in `tests/test_deployment.py`

The current systemd user unit already meets the ownership and restartability
requirement. Panetone does not need privileged ports, devices, users, or
machine-wide ownership. Keeping a tested root installer creates an unsupported
second deployment model.

### Python retired from the active tree

The Python runtime, its dependency locks, Python-only runners, and tests were
removed after mapping the neutral fixtures still used by Rust. Preserved:

- Git commit `b5b75ef1ec6de303f80a65e82075647bc62bc66d`
- the cold migration bundle outside the runtime database
- the original architecture review
- protocol or routing fixtures directly consumed by retained Rust tests

Do not preserve a Python test gate after deleting the Python product. A static
fixture remains useful only when it states a current Rust contract rather than
historical parity.

### Stale phase documents removed

The remaining Phase 1, Phase 2, and Phase 3 reports described migration,
provider shadows, Python parity, and the hidden conformance daemon. Git
preserves them without presenting obsolete gates as supported workflows.

Retain current ADRs only where they still describe a live invariant. Update or
delete paragraphs whose sole subject is migration or promotion.

### Local environment backups ignored

`.gitignore` now ignores `.env.*` without reading or committing `.env.bak`.
This is repository hygiene, not runtime architecture.

### Dead compaction API removed

`StoreHandle::compact` was called only by Rust store tests. It and its tests
were removed. Add retention later only after actual database growth or a
specific privacy requirement justifies it.

## Third cleanup removed expired compatibility and dead surface

The deletion-only pass changed no exercised message path. It removed 388
physical lines and 364 CLOC from `src`. Excluding embedded unit tests, it
removed about 350 physical lines and 329 CLOC. Integration tests and fixtures
fell by 255 physical lines and 249 CLOC.

| Removed item | Result |
| --- | --- |
| Schema-v5 upgrader | Fresh and schema-6 databases remain supported. Schema 5 is now rejected explicitly, and its promotion-table upgrade fixture is gone. |
| Python request-hash compatibility | Imported UUIDs remain permanently reserved. Their retired hash kind never matches, so retries return `idempotency_conflict` and no old prompt can replay. |
| Frozen route and channel parity helpers | Deleted unused route lookup, chunking, Signal normalization, retry-selection helpers, and two JSON fixtures. Existing lifecycle tests remain, and direct event projection tests cover explicit Telegram and default Signal output. |
| Standalone event-cursor command | Deleted the zero-caller handle method, command variant, and SQLite function. Event ingestion still advances its cursor transactionally. |
| Fixture-mode doctor | Doctor now requires a Wakterm binary and socket and always checks the selected live adapter. Golden contract fixtures remain in adapter tests. |
| Removed-Slack environment guard | Slack has no adapter, dependency, configuration behavior, or special process test. |
| Zero-caller public methods | Deleted the control-server identity getter and unused fake-Wakterm terminal and capability methods. |
| Unused Tokio feature | Removed the unused `fs` feature. |

The production channel-selection abstraction was also inlined into event
projection because the direct logic was smaller. Durable inbox, outbox,
workflow, return, and agent-event behavior were not changed.

The full Rust suite passed 68 tests. Clippy with warnings denied and the locked
release build also passed.

## Local production fixes before structural consolidation

### Retry transient worker failures

Every periodic worker propagates one method error with `?`. Telegram and
Signal loops also return on most store or adapter errors. The supervisor marks
a degraded task failed but does not restart it, and the daemon remains active.

This exact shape already caused a partial service failure during the Rust
trial. The local fix is:

1. classify schema, invariant, identity, and invalid-configuration errors as
   fatal
2. record a transient adapter or channel error in task health
3. retry it with bounded backoff until shutdown
4. clear the degraded state after recovery

This does not require a new queue, watchdog, process manager, or journal.
Systemd cannot repair a degraded worker while the main process remains alive.

### Cache Wakterm capability negotiation

Startup already negotiates version and capabilities. `catalog`, `admit`,
`terminal_events`, and `event_page` perform additional capability reads. The
event path can therefore create at least two short-lived Wakterm CLI clients
per second at the default cadence.

Keep the startup result in `ProductionService`. Re-read capabilities after a
classified compatibility or reconnect error. This reduces load and source
complexity without weakening the contract for one local daemon and one local
mux.

### Make steering explicit

Busy admission currently means durable queue-until-idle. That is a valid local
send mode, but it conflicts with the stated Telegram steering expectation.

The interface should distinguish:

- steer the active turn now
- submit a new prompt, optionally waiting for idle

Do not delete the busy queue until the local-send behavior is decided. Do not
use the busy queue as the implementation of steering.

## Cleanup that needs a counterfactual first

### Reduce the six periodic workers only after retry is correct

The six workers all tick every second and mostly drain the same store. A single
effect drain or two grouped drains could reduce task state, queries, and
partial-failure modes. But consolidation changes ordering and can let a slow
channel operation block agent events or inbound admission.

First make each loop retry correctly. Then run the existing workflow tests
with one grouped drain. Keep separate workers only where a measured blocking
interaction or required ordering proves useful.

### Split test fakes from the workflow engine only if the code gets smaller

`OfflineService` is misleadingly named. It is the production workflow engine,
but it also contains:

- real and fake backend enums
- a fault injector with ten crash points
- fake-only accessors
- production workflow logic

The crash-boundary tests protect the duplicate-prompt invariant and should not
be discarded casually. A small internal adapter trait could move fake types and
fault injection into test support, but an elaborate trait hierarchy would be
worse than the current enums.

The target is a clearly named `WorkflowService` whose production path does not
expose fake-only methods. Accept the split only if the resulting production
code and public API are smaller.

### Decide whether the local event archive is useful

Wakterm owns the durable ordered event stream. Panetone already advances its
cursor in the same transaction that creates deterministic outbox effects and
updates route lifecycle state. The `agent_events` table retains a second full
copy indefinitely and accounts for a substantial part of the 1,669-line store.

The current table also exposes 26 unrouted events and observer failures, which
are useful diagnostics during early testing. Before deleting it, falsify the
simpler design:

1. project an event and crash before and after commit
2. replay the same page
3. recover from a Wakterm cursor gap
4. diagnose an intentionally unrouted assistant message

If deterministic outbox IDs, cursor compare-and-swap, and bounded logs cover
those cases, remove the table. If the diagnostics are useful, retain only a
bounded recent window. Do not add another archival subsystem.

### Keep return machinery until it is exercised

The current database has no return rows and the user has not yet exercised the
Rust `--return-final` path in normal use. Return handling spans the workflow,
one table, two workers, Wakterm terminal polling, mirror delivery, and agent
callback delivery.

The feature has a stated use, so absence of current rows is not enough to
delete it. Exercise one real return. If the user does not want the behavior
after that test, remove the entire feature rather than simplifying it into a
different unrequested callback system.

## Recommended cleanup order

1. Add bounded retry and recovery state to existing production loops.
2. Cache Wakterm capabilities after startup.
3. Verify and implement explicit steering versus queued-new-turn behavior.
4. Exercise Signal, Debate, and `--return-final` only when wanted.
5. Test a grouped effect drain and retain separate workers only where blocking
    evidence requires them.
6. Decide whether `agent_events` diagnostics justify a bounded table.

Items 1 through 3 fix observed behavior. Items 4 through 6 are evidence-driven
simplification and should not become a new phase or acceptance program.

## What should not be added

The current evidence does not justify:

- a new promotion, rollback, or deployment control plane
- a system service
- a second database or event journal
- automatic failover to Python
- a generic plugin framework for channels or Wakterm
- a permanent Python parity matrix
- a soak-period or uptime gate
- automatic route creation before manual reconciliation becomes burdensome
- a scheduler whose only purpose is to make an unused compaction API active

## Questions for an external reviewer

An external reviewer should answer these against the current source and user
requirements:

1. Which public APIs now remain used only by tests?
2. Can transient retry be added inside the existing loops without changing
   store states or adding another supervisor?
3. Can startup capabilities be passed through the Wakterm adapter without a
   larger transport abstraction?
4. Does grouping effect drains reduce code and failure modes after slow calls
   are bounded?
5. Can `agent_events` be removed while retaining atomic cursor and outbox
   projection and enough diagnosis for wrong-route failures?
6. Does `--return-final` remain useful after one real test, or should the whole
   feature and its two workers be retired?
7. Do both retained fixtures state a live Rust contract, or can their cases
   move into smaller direct tests?
8. Which remaining public APIs are used only by integration tests, and can
   they be removed without adding a larger test-support abstraction?

## Reproduction and source references

Primary source entry points:

- final Python runtime: `bridge.py` and `panetone_control.py` at commit
  `b5b75ef1ec6de303f80a65e82075647bc62bc66d`
- Rust startup and worker policy: [`src/main.rs`](../src/main.rs)
- Rust production orchestration:
  [`src/service/production.rs`](../src/service/production.rs)
- shared production and fake workflow engine:
  [`src/service/offline.rs`](../src/service/offline.rs)
- store and schema: [`src/store/mod.rs`](../src/store/mod.rs)
- Wakterm CLI adapter: [`src/wakterm/cli.rs`](../src/wakterm/cli.rs)
- task health: [`src/supervisor.rs`](../src/supervisor.rs)
- current scope: [`panetone-rust-roadmap.md`](panetone-rust-roadmap.md)
- current operations: [`production-operations.md`](production-operations.md)

The principal counts can be reproduced with:

```sh
git show b5b75ef:bridge.py | wc -l
git show b5b75ef:panetone_control.py | wc -l
find src -name '*.rs' -type f -print0 | xargs -0 wc -l
find tests -maxdepth 1 -name 'rust_*.rs' -type f -print0 | xargs -0 wc -l
find src -name '*.rs' -type f | wc -l
```

The production schema and state can be checked without starting Panetone:

```sh
sqlite3 ~/.local/state/panetone-rust/migration/panetone.sqlite3 \
  'PRAGMA user_version; PRAGMA integrity_check; SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;'
systemctl --user is-active panetone.service panetone-rust.service
systemctl --user is-enabled panetone.service panetone-rust.service
```

## Final assessment

The Rust replacement retains one major architectural improvement that should
not be undone: Wakterm is authoritative for providers, live agents,
incarnations, turns, and output events. Panetone is now a channel router and
durability boundary instead of a second agent runtime interpreter.

The three cleanups removed the transition systems and cut physical Rust source
by about 37 percent. The eight-table durable core is proportionate to
exact routing, crash-safe channel effects, and uncertain prompt admission.

The remaining over-engineering is concentrated in the active Rust design. Fix
the already-observed retry and Wakterm subprocess problems locally. Further
consolidation should be accepted only when it reduces code while preserving
exact route identity, atomic output projection, durable input, and no duplicate
prompt after an uncertain admission.
