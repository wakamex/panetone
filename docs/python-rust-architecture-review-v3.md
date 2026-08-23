# Python and Rust Panetone infrastructure review v3

Date: 2026-08-21

Purpose: compare the Python reference implementation with the current Rust
implementation as two static infrastructure designs. This document contains
the operating context needed for an external reviewer.

## Verdict

The current Rust design has a sound core:

- Wakterm owns agent discovery, provider interpretation, live identity, and
  output events.
- Panetone owns routes, channel ingress and egress, and the durability boundary
  around message admission.
- Workspace titles locate current agents, and exact agent and incarnation
  identity protects each individual admission from a replacement race.
- One SQLite database transactionally connects cursors, inbox work, outbox
  work, workflows, and uncertain admission.
- A systemd user service lets an agent inspect and restart Panetone without
  root.

That core explains part of the size increase from about 3,548 Python CLOC to
about 5,982 non-test Rust CLOC. The increase is not fully justified. The main
remaining costs are:

- six polling workers that permanently stop after one error
- repeated Wakterm capability subprocesses during normal operations
- a complete local copy of Wakterm events with no retention policy
- production workflow code coupled to fake backends and ten fault-injection
  points
- two workers and a table for `--return-final`, which has not yet been
  exercised in normal Rust use

The immediate work should fix retry, cache capability negotiation, and make
active-turn steering explicit. Larger deletion decisions should focus on the
event archive, return-final, and test infrastructure. No new supervisor,
journal, queue, plugin framework, deployment layer, or migration system is
needed.

## Operating requirements

Panetone is a local service for one user on one headless Fedora host.

- Telegram is the required channel.
- Signal and the Debate Signal route are supported when wanted.
- Slack is absent.
- Panetone may remain stopped. Continuous uptime is not a requirement.
- Direct observation of wanted workflows is an acceptable completion test.
- A systemd user service is preferred for restartability and agent access.
- Wrong-agent delivery and duplicate prompts after an uncertain result are
  high-cost failures.
- Telegram input during an active turn should steer that turn.
- One-way local sends are supported.
- `--return-final` is optional and should remain only if it proves useful.
- Additional users, hosted operation, automatic failover, and exhaustive
  provider parity are outside the requirement.

The design standard is the smallest clean system that meets these
requirements. New durable state, queues, recovery mechanisms, compatibility
paths, and abstractions need a concrete current benefit.

## Compared source

The Python design is `bridge.py` plus `panetone_control.py` at Git commit
`b5b75ef1ec6de303f80a65e82075647bc62bc66d`. It is the final Python shape with
Telegram, Signal, Debate, local control, and return handling but without Slack.

The Rust design is the current working tree. Its entry points are:

- `src/main.rs` for CLI, startup, workers, channel loops, and supervision
- `src/service/production.rs` for production orchestration
- `src/service/offline.rs` for the workflow engine and fault injection
- `src/store/mod.rs` for schema, transactions, projections, and queues
- `src/wakterm/cli.rs` for the live Wakterm boundary

Counts exclude generated lockfiles. Rust production estimates exclude embedded
`#[cfg(test)]` blocks.

| Measure | Python | Rust |
| --- | ---: | ---: |
| Physical implementation lines | 4,384 | 6,662 total, about 6,388 production |
| CLOC | 3,548 | 6,235 total, about 5,982 production |
| Main implementation files | 2 | 25 |
| Direct runtime dependencies | 2 packages | 12 crates |
| Integration test lines | 3,138 | 3,367 |
| Durable storage locations | 2 JSON files and 2 SQLite databases | 1 SQLite database |
| Principal durable tables | 4 across two databases | 8 |
| Long-running tasks with both channels | about 4 loops plus Telegram polling | 9 supervised tasks plus the store owner |

Rust production CLOC is about 1.69 times Python. Line count is maintenance
evidence, not a quality score. The subsystem comparison below identifies which
additional code protects a requirement.

## Python infrastructure

Provider output flows from Wakterm discovery and direct provider-session
readers into JSON-backed pending sends, then to Telegram or Signal. Telegram
and Signal input route directly to pane input. Local control requests pass
through a SQLite journal, a visible channel audit, and Wakterm admission.

### Authority and routing

Python discovers panes through Wakterm and the process tree, then locates
Claude, Codex, Gemini, and OpenCode sessions directly. It parses provider files
and databases to detect output. Route identity is primarily title, tab, pane,
and process state held in memory and persisted as JSON where needed.

This is compact and operationally flexible. It can discover tabs, create or
recreate Telegram topics, and route input directly into the active pane. It
also makes Panetone a second interpreter of provider and terminal state.
Ephemeral pane and process identity can leak into durable routing decisions.

### Durability

Python uses:

- `state.json` for route and cursor state
- `pending_sends.json` for output effects and their source cursor updates
- `signal.db` for Signal messages
- `control-journal.sqlite3` for request idempotency and final returns

Output discovery writes new channel effects and corresponding provider cursor
updates together before delivery. The control journal records admission
boundaries and does not retry a prompt whose result may be indeterminate.
These are the important durability properties and are not unique to Rust.

The cost is four state locations with different schemas, locking behavior, and
backup boundaries. Some Telegram routing state remains process-local.

### Tasks and failure behavior

The main process runs Telegram framework polling, provider output polling,
Signal receive and queue processing, Wakterm return watching, and pending
return delivery. Its loops generally catch transient errors, reconnect, and
retry.

Health is mostly visible through logs and task completion callbacks. There is
no unified status model. Direct provider readers and route maps make individual
failure paths harder to attribute, but basic retry behavior is simple and
reliable.

## Rust infrastructure

Wakterm events consumed while Panetone is running enter one SQLite transaction
that stores the event, creates visible outbox work, and advances the event
cursor. Normal startup skips passive output accumulated while Panetone was
stopped; an explicit catch-up setting preserves the old cursor instead.
Telegram and Signal input enter the durable inbox before Wakterm admission.
Local control requests create a durable workflow before channel audit and
agent admission.

### Authority and identity

Rust does not parse provider session stores or reconstruct agents from process
trees. Wakterm supplies normalized catalogs, exact agent IDs, incarnation IDs,
turn state, terminal returns, and ordered output events.

Each Panetone route has a stable UUID, effective workspace title, and channel
bindings. Panetone resolves the current live agents before admission and does
not persist an agent, incarnation, pane, tab, or route availability status.
The exact pair returned by Wakterm is used for that admission only. Closing a
workspace and later recreating it needs no reconciliation.

When one tab contains several agents, a quoted channel reply targets the agent
that produced that output. Otherwise the last agent that produced visible
output wins, followed by the lowest live pane ID. Zero live matches are
unavailable and multiple matching tabs are ambiguous.

### Store

One dedicated blocking owner serializes all SQLite access across a bounded
Tokio channel. Schema version 7 has eight tables:

| Table | Purpose |
| --- | --- |
| `routes` | Stable workspace title and channel bindings |
| `idempotency_tombstones` | Permanent reservation of request UUIDs |
| `workflows` | Send ordering, admission state, response, and return intent |
| `outbox` | Durable Telegram and Signal effects |
| `inbox` | Durable remote input before agent admission |
| `return_deliveries` | Final result delivery to channel and source agent |
| `agent_events` | Local event copy, deduplication, projection state, and diagnostics |
| `metadata` | Event and channel cursors, preferences, and cursor gaps |

The owner and unified transaction boundary are appropriate. Replacing them
with a pool, mutex-wrapped connection, or second database is unlikely to make
this workload smaller.

### Admission safety

Inbound channel messages are stored before Wakterm admission. A workflow
records the exact source and target incarnations and moves through an explicit
admission boundary. A possibly accepted prompt becomes indeterminate and is
never retried automatically. Request UUIDs remain reserved after workflow
completion.

These states directly protect the duplicate-prompt and wrong-agent invariants.
They are justified even for one user.

### Tasks and health

The supervisor owns:

- one critical control server
- six one-second workers for events, outbox, busy targets, return terminals,
  pending returns, and inbox
- one Telegram loop when configured
- one Signal loop when configured

The database owner runs separately. Status exposes named task health.

A worker currently propagates one method error, is marked failed, and is never
restarted. The daemon remains active, so systemd cannot repair that partial
failure. Python's retrying loops are better for this requirement. The local fix
is classified retry with bounded backoff inside the existing tasks.

### Wakterm process boundary

Startup checks Wakterm version, capabilities, and catalog. Normal catalog,
admission, terminal, and event operations call capabilities again. At the
default event cadence, routine polling can create at least two Wakterm CLI
clients per second.

For one daemon and one local mux, startup negotiation can be cached. A
classified compatibility or reconnect error can trigger revalidation. A
persistent transport abstraction is unnecessary unless measurement proves the
CLI boundary itself inadequate.

### Test infrastructure in production source

`OfflineService` is the production workflow engine despite its name. It
contains real and fake backend variants plus a fault injector with ten crash
points. `src/wakterm/contract.rs`, `src/wakterm/fake.rs`, and
`src/channels/fake.rs` also compile as library source.

The crash-boundary tests protect real invariants. Moving fakes behind traits or
features is useful only if the production API and total code become smaller.
A generic adapter framework would add abstraction without a second production
implementation.

## Direct comparison

| Concern | Python | Rust | Assessment |
| --- | --- | --- | --- |
| Provider authority | Parses provider and process state itself | Delegates normalized state to Wakterm | Keep Rust boundary |
| Route identity | Titles plus ephemeral tabs and panes | Route UUID plus exact agent incarnation | Keep Rust model |
| Output durability | Atomic pending output and source cursor update | Atomic event projection, cursor, and outbox | Both satisfy the requirement |
| Inbound durability | Signal is archived; Telegram relies more on process state | Both channels enter one durable inbox | Rust is stronger |
| Uncertain prompt result | Journaled and not retried | Workflow state and not retried | Required in both |
| Active-turn Telegram input | Direct pane input can steer | Busy admission waits for idle | Rust needs explicit steering |
| Transient errors | Main loops generally retry | Most workers permanently exit | Python behavior is better |
| Health | Logs and callbacks | Structured task and store status | Rust is better when retry is fixed |
| Persistence surface | Four locations | One database | Rust is easier to inspect and back up |
| Event history | Provider files remain authoritative | Wakterm plus a full Panetone copy | Rust may be redundant |
| Return-final | Journal plus watcher and delivery states | Workflow, table, and two workers | Keep only if used |
| Deployment | Unprivileged local process | systemd user service | Rust deployment fits the requirement |
| Maintenance size | 3,548 CLOC | About 5,982 production CLOC | Rust needs continued simplification |

## Remaining infrastructure assessment

| Subsystem | Current benefit | Smaller option | Recommendation |
| --- | --- | --- | --- |
| Wakterm authority | Removes provider parsing and supplies exact identity | None inside Panetone | Keep |
| Stable routes | Prevents silent retargeting across restarts | Title-only routes | Keep |
| SQLite owner | Supplies one transaction boundary without async blocking | Direct connections | Keep |
| Inbox and outbox | Preserve accepted input and channel effects | Process memory | Keep |
| Workflow admission states | Prevent duplicate prompts after uncertainty | Blind retry or loss | Keep |
| Permanent UUID tombstones | Prevent old IDs from becoming new work | Finite retention | Keep unless growth becomes real |
| Typed supervisor health | Makes partial failures visible | Logs only | Keep, add retry |
| Six periodic workers | Isolate effect categories | One or two ordered drains | Test consolidation after retry |
| Repeated capability reads | Recheck compatibility every operation | Cache startup result | Remove |
| Full `agent_events` rows | Diagnostics and replay inspection | Cursor, deterministic effect IDs, and bounded logs | Falsify and then remove or bound |
| Return-final machinery | Correlated asynchronous results | Delete the feature | Exercise once, then decide |
| Fake backends in `src` | Deterministic crash tests | Smaller test-support boundary | Change only if net code falls |

The target is one user-owned daemon with Wakterm authority, one SQLite owner,
stable routes, durable channel effects, and only the workflow states needed
for exact identity and uncertain admission. It should have explicit steering,
retrying loops, and cached Wakterm capabilities.

Before removing `agent_events`, test projection crashes, page replay, cursor-gap
recovery, and diagnosis of an unrouted output. Before consolidating workers,
verify that slow channel delivery cannot block event ingestion or admission.
Before separating fakes, compare total CLOC and public API size.

## Questions for an external reviewer

1. Which Rust states and tables are necessary for the two high-cost failures:
   wrong-agent delivery and duplicate prompts?
2. Can `agent_events` be removed while preserving atomic cursor and outbox
   projection and adequate diagnosis?
3. Can the six workers become fewer retrying drains without introducing
   head-of-line blocking?
4. Is `--return-final` useful enough to justify its table, two workers, terminal
   polling, channel mirror, and source callback?
5. Can fake backends leave the production API with a net reduction in code?
6. After those decisions, what concrete requirement explains each remaining
   difference from the Python implementation?

## Source references

- Python snapshot:
  `git show b5b75ef:bridge.py` and
  `git show b5b75ef:panetone_control.py`
- Rust runtime: [`src/main.rs`](../src/main.rs)
- Rust orchestration:
  [`src/service/production.rs`](../src/service/production.rs)
- Rust workflow engine:
  [`src/service/offline.rs`](../src/service/offline.rs)
- Rust store: [`src/store/mod.rs`](../src/store/mod.rs)
- Wakterm adapter: [`src/wakterm/cli.rs`](../src/wakterm/cli.rs)
- Control protocol: [`control-protocol.md`](control-protocol.md)
- Operations: [`production-operations.md`](production-operations.md)

Reproduce the principal counts with:

```sh
git show b5b75ef:bridge.py | cloc --stdin-name=bridge.py -
git show b5b75ef:panetone_control.py | cloc --stdin-name=panetone_control.py -
cloc --include-lang=Rust src
find src -name '*.rs' -type f -print0 | xargs -0 wc -l
find tests -maxdepth 1 -name 'rust_*.rs' -type f -print0 | xargs -0 wc -l
```
