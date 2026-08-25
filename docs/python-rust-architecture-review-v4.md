# Python and Rust Panetone architecture review v4

Date: 2026-08-22

This is a current-state review for an external reviewer. The Python service is
retired and serves only as a compact reference. Rust is the sole production
implementation.

## Verdict

Rust has the right ownership boundary and too much machinery around it.

Keep:

- Wakterm as the authority for agent discovery, effective workspace titles,
  exact live identity, prompt admission, and normalized output events.
- Panetone as the owner of durable routes and Telegram or Signal transport.
- Stable title-based routes, with exact agent and incarnation identity used
  only as a transient compare-and-submit token for one admission.
- One SQLite owner for transactional inbox, outbox, cursor, and uncertain
  admission state.
- A systemd user service that an agent can restart without root.

Change, in order:

1. Deploy the persistent Wakterm event reader now implemented in source.
2. Make unexpected worker death restart the daemon.
3. Restore durable outbound message chunking lost in the Rust rewrite.
4. Keep `--return-final` through a real end-to-end test, but stop paying its
   idle polling cost when no return is outstanding.
5. Remove or bound the redundant Wakterm event archive.
6. Separate durable route data from transient agent bindings, then reorganize
   the remaining large modules along existing ownership boundaries.

No new queue, journal, supervisor framework, pool, plugin system, deployment
layer, or compatibility layer is warranted.

## Scope and measured state

Panetone serves one user on one headless Fedora host. Telegram is the normal
remote interface. Signal is optional, Slack is absent, and Debate is an
ordinary Signal route. Continuous uptime and automatic failover are not
requirements. Passive output accumulated while Panetone was stopped may be
skipped by default, but accepted input and explicit outbound work must survive
a restart. An uncertain prompt must never be submitted again automatically.

A durable route represents a workspace title such as a direct child of
`/code`. Closing a tab and recreating a different tab for the same folder days
later must work. Multiple agent panes in one tab are valid. A quoted reply
prefers its producing agent; otherwise recent output and pane order give a
deterministic choice.

The Python reference is `bridge.py` plus `panetone_control.py` at commit
`b5b75ef1ec6de303f80a65e82075647bc62bc66d`. No Python runtime remains in the
working tree.

| Measure | Python reference | Current Rust |
| --- | ---: | ---: |
| Implementation CLOC | 3,548 | 6,564 |
| Main implementation files | 2 | 25 |
| Integration test CLOC | 3,138 | 3,359 |
| Direct runtime dependencies | 2 packages | 12 crates |
| Durable storage locations | 2 JSON and 2 SQLite files | 1 SQLite database |
| Principal durable tables | 4 across two databases | 8 |

Rust CLOC is 1.85 times Python. All 12 Rust dependencies have current source
uses, so dependency pruning should follow feature deletion.

| Largest Rust module | CLOC | Responsibility |
| --- | ---: | --- |
| `src/store/mod.rs` | 1,619 | Actor, schema, recovery, all queries and transactions |
| `src/service/offline.rs` | 815 | Workflow, returns, fake backends, fault injection |
| `src/service/production.rs` | 765 | Workers, routing, control handling |
| `src/main.rs` | 616 | CLI, composition, channel loops, supervision, doctor |
| `src/wakterm/cli.rs` | 553 | Process execution, contracts, catalog and route joins |

The live database was 3.4 MB with 40 durable routes, 2,134 agent events, 480
delivered outbox items, one pending inbox item, and zero Rust workflows or
returns. Eight Wakterm route groups were live. Forty durable routes are not
automatically stale because dormant mappings are how a later workspace finds
its existing channel.

## Python and Rust design comparison

Python discovered panes and interpreted provider session stores itself. It
used four persistence locations, but its ordinary loops retried transient
errors and it durably chunked long channel messages. Its compactness came with
duplicated Wakterm/provider authority and more ephemeral identity in routing.

Rust delegates provider interpretation to Wakterm, resolves a title to the
current live agent, and uses that exact agent and incarnation for admission.
One SQLite transaction can store an input or output effect with its cursor.
This is the better long-term boundary. The exact identity pair is not a second
route identity and is not over-engineering; it prevents the selected agent
from silently changing between lookup and prompt write.

Remote input is owner-filtered, stored in `inbox`, resolved by current
effective title, and admitted to an exact agent. Wakterm output is read from an
ordered event page, joined to a current route snapshot, and stored with its
outbox effect and cursor update. An explicit `panetone send` adds UUID claim,
channel audit, busy handling, uncertain admission, and optional final-return
machinery. Only the channel input and output path has observed production use.

Fresh setup no longer depends on migrated routes. The supported control API
can ensure a live effective title has a durable Telegram binding and can return
its exact live agents with an event-cursor baseline. A launcher can then query
the first assistant output from that agent and incarnation after the baseline.
The response distinguishes `projected`, `unrouted`, and `misrouted` using the
existing durable event row. No second receipt or launcher cursor is stored.

## Finding 1: idle Wakterm polling is the largest immediate problem

Each one-second event pass runs two `catalog()` calls, each preceded by
`capabilities()`, then `wakterm list`, then another capabilities and event-page
pair. That is seven Wakterm CLI processes before finding an event. The idle
return worker adds two. The current unavailable inbox item adds five route
snapshot processes every second.

The Wakterm journal showed 420 codec negotiations in 30 seconds, exactly 14
per second in this state. Panetone consumed about a quarter of a CPU core while
otherwise idle and used about 12 MB of memory.

Apply the smallest fixes first:

- Validate and retain capabilities at startup.
- Read the event page before resolving routes. Skip the route scan when no
  visible output needs projection.
- Poll terminals only while a workflow awaits a final result, or remove the
  feature.
- After collapsing route lookup to one command, keep the one-second retry if it
  is cheap; add backoff only if measurement still justifies it.

The event path now runs `wakterm agent events --follow` and retains that child
across event passes. A controlled 20-poll comparison reduced Wakterm launches
and mux connections from 20 to 1. Client CPU fell from 3.36 seconds to 0.59
seconds even though follow mode returned 22 pages in the comparison window.
Catalog, admission, and terminal commands remain one-shot because they are
demand-driven after the earlier polling reductions.

The cleaner Wakterm boundary is one catalog response containing each agent's
server-computed effective title, window, tab, and pane. Panetone could replace
the racy `catalog/list/catalog` join with one authoritative response. The live
churn now provides evidence for that Wakterm change, but it is not required for
the first Panetone fixes.

## Finding 2: degraded workers fail permanently

The control server is critical, but six workers and both channel receivers are
degraded tasks. One propagated error exits a task permanently while the daemon
stays active, so the unit's `Restart=on-failure` policy cannot repair it.
Signal has a direct case: an idle receive can hit its 35-second deadline and
permanently stop the Signal task.

Treat expected receive timeouts as idle polls and reconnect after ordinary
transport loss with one bounded backoff. Treat every unexpected worker exit as
fatal, then let the existing systemd user unit restart the whole durable
daemon after five seconds. This may allow `TaskPolicy::Degraded` and related
policy code to be deleted. Do not build worker-specific retry machinery unless
whole-process restarts prove disruptive.

## Finding 3: Rust lost outbound message chunking

Python split ordinary output into 4,000-character chunks and used UTF-16-aware
3,900-unit chunks for Telegram audits and callbacks. Rust sends one complete
`OutboxItem`. The largest delivered Rust message is 2,991 characters, so the
failure has not occurred yet, but a normal long agent response can exceed the
single-message limit and become a failed outbox item.

Chunk before inserting outbox work. Derive each chunk's effect ID from the
source event or workflow effect plus its index, and insert every chunk with the
source cursor in one transaction. Splitting inside the HTTP sender would make
a crash between chunks ambiguous. This restores a real Python behavior and
the durable-chunk semantics already stated in ADR 0003 without adding a queue.

## Finding 4: return-final is untested and should remain bounded

`--return-final` requires terminal capability negotiation, a terminal worker,
`return_deliveries`, callback admission states, source and channel delivery,
a pending-return worker, and many workflow crash tests. The live database has
zero workflows and zero returns, so the machinery is untested rather than
proven unnecessary.

Retain it until one real two-workspace workflow completes. Add a store query
for work awaiting a terminal event and skip `terminal_events()` when that query
is false. Likewise, query pending returns before resolving live routes. This
keeps the feature without two permanently active polling paths.

The current Wakterm terminal payload contains `target_agent_id` but not the
target incarnation. Panetone therefore cannot enforce the exact pair recorded
at admission even though ADR 0002 says terminal results are validated against
it. Add `target_incarnation_id` to the Wakterm terminal contract and reject a
terminal result unless both values match the submitted target.

The real test should establish all of these outcomes:

- `panetone send --return-final` acknowledges without blocking for completion.
- The target receives one prompt with the resolved agent and incarnation.
- Wakterm emits one correlated terminal result.
- Panetone delivers the final text to the source agent and configured channel.
- Restarting Panetone after target admission does not resubmit the prompt.
- If the source workspace is absent, the final result remains pending and is
  delivered after that workspace returns.

After that test, keep the feature if the workflow is useful. If it is not,
remove it as one vertical slice rather than polishing its abstractions.

## Finding 5: the complete event archive is redundant

`agent_events` and its indexes occupy about 2.9 MB of the 3.4 MB database. The
2,134 rows include 598 projected assistant messages, 479 unrouted messages,
eight observer failures, and all lifecycle and turn events. Delivered outbox
rows also retain visible message bodies.

Atomic projection does not require a permanent event copy. One transaction
can check the expected cursor, insert deterministic outbox effects, and advance
the cursor. Replaying an uncommitted page recreates the same effect IDs.

First inspect the 479 unrouted records while route projection is still being
tested. Then retain only diagnostics that are used, such as unrouted and
observer failures, or keep a bounded recent window. Do not add a general
compaction API. Delivered outbox growth is currently small and can wait for a
measured size trigger.

## Finding 6: types and modules obscure the remaining design

`Route` contains `agent: Option<AgentBinding>` with `#[serde(skip)]`, and
`Route::with_agent` clones durable data to attach a transient target. Keep
`Route` purely durable and pass `AgentBinding` explicitly. A small borrowed
`ResolvedRoute` is reasonable only if it reduces argument noise.

After feature deletion:

- Rename retained `OfflineService` to `WorkflowService`.
- Move the production prompt builder out of `FakeWakterm` and beside its
  workflow.
- Split `store/mod.rs` into core/schema, channels, events, and workflows while
  retaining one connection owner and keeping transactions with their owning
  operation.
- Move daemon composition and channel loops out of `main.rs`; leave Clap and a
  thin entry point there.

Do not split files solely to reduce line counts.

## Local Rust and repository cleanup

| Current code or state | Smaller change |
| --- | --- |
| Runtime methods repeatedly convert errors to `String` | Preserve typed or `anyhow` errors until the supervisor boundary |
| Closed inbox, acknowledgement, and task states are strings | Use local enums where the state set is closed |
| Enum database names serialize through JSON, unwrap, and allocate | Add exhaustive `as_str()` matches without another crate |
| Four paths repeat outbox delivery transitions, and the worker loads every pending row | Add atomic claim-one operations and reuse one finalization path |
| Tokio enables `test-util` in runtime features for one test | Enable it only as a dev feature or rewrite the test |
| Telegram pacing depends on the API base being one exact URL | Pace every production client and expose an unpaced test path only |
| Metadata keys and clock helpers repeat | Consolidate them inside existing owning modules when touched |

The store's two-dozen-variant `Command` enum is boilerplate, but a boxed
closure mailbox trades it for dynamic dispatch and less visible operations.
Try that only as a contained diff and keep it only if code and readability
clearly shrink. Do not add macros or an ORM.

Fake channels, fake Wakterm, its contract fixture, and fault injection account
for about 1,243 CLOC in production source. Integration tests require them from
the compiled library, so `#[cfg(test)]` alone is not a solution. Reduce
test-only public methods first. A feature-gated support crate is justified only
if total source and public API decrease.

Ignored Python residue remains locally: `.pytest_cache`, `.ruff_cache`, two
`__pycache__` trees, and `.env.bak`. Remove the caches. Delete `.env.bak` after
confirming the current `.env` has every needed value because the backup holds
credentials.

The current program does not read `WAK_SIG_MEMBERS_DEBATE`,
`WAK_SIG_MEMBERS_PG13`, `WAK_SIG_ALLOWED`, `WAK_TG_DEBATE_CHAT`,
`WAK_TG_DEBATE_TABS`, or the Twilio credentials still in `.env`. Remove unused
keys and move credentials used elsewhere to that service's environment.

The live unit is correctly a user service. Do not restore root deployment
machinery. After v4 is accepted, keep this as the current review and either
remove older versions using Git history or place them under `docs/archive/`.

## Specific fixes and acceptance checks

Implement these as separate causal changes:

| Order | Files | Change | Acceptance check |
| ---: | --- | --- | --- |
| 1 | `src/wakterm/cli.rs` | Store capabilities in a clone-shared `tokio::sync::OnceCell`; make every existing capability check use it | Preflight followed by idle workers runs capabilities once per process |
| 2 | `src/service/production.rs` | Fetch the event page first and resolve live routes only when it contains `assistant_message` or `plan` output | An empty page runs no catalog or list command |
| 3 | `src/store/mod.rs`, `src/service/production.rs` | Add `has_unresolved_terminal()`: a return-final workflow has a submitted target and no `return_deliveries` row; return early from `terminal_once()` when false | The watcher is idle with no return work and starts on the next pass after a return-final admission |
| 4 | Wakterm terminal API, `src/wakterm/cli.rs`, `src/service/production.rs` | Add and validate `target_incarnation_id`; add a real return-final process test rather than another fake-only state test | A mismatched incarnation never produces a callback, while one valid terminal reaches source and channel |
| 5 | `src/main.rs`, `src/channels/inbound.rs`, `src/supervisor.rs` | Treat a Signal receive deadline as an idle poll; make every unexpected task exit critical | A forced receiver failure exits Panetone and systemd restores a healthy process; 60 seconds of idle Signal does not restart it |
| 6 | `src/store/mod.rs`, `src/service/offline.rs`, `src/service/production.rs` | Replace load-then-save with atomic `claim_outbox(id)` and `claim_oldest_outbox()` operations; reuse one result-finalization path across the four send sites | Concurrent workflow and worker drains send one effect once; a backlog deserializes one row per pass |
| 7 | `src/store/mod.rs`, `src/channels/real.rs`, workflow projection | Split outbound text before persistence using channel-aware limits and deterministic chunk IDs | A long Telegram response delivers every chunk in order; restart after chunk one causes no lost chunk |
| 8 | Wakterm catalog and `src/wakterm/cli.rs` | Include effective title and tab coordinates in each catalog agent, then delete `catalog/list/catalog` | One live-route resolution uses one Wakterm command and still rejects ambiguous titles |
| 9 | `src/domain/route.rs`, workflow calls | Remove transient `Route.agent` and pass the selected `AgentBinding` explicitly | Route JSON and route values have identical fields; stale-incarnation tests still pass |

After fixes 1 through 3, the current state should fall from 14 Wakterm codec
negotiations per second to at most the deliberately retried unavailable inbox
lookup. Fix 8 should make that lookup one command instead of five. Record the
same 30-second journal count and systemd CPU usage before and after.

Run the real return-final test after fixes 1 through 6 and before deleting or
reorganizing workflow code. Use two live workspaces and preserve the request
UUID. Test the normal completion first, then restart Panetone after target
admission and confirm the same UUID is not admitted twice. Finally close the
source workspace before completion, reopen it, and confirm the pending result
reaches the new live incarnation and its channel mirror.

Only after that test should the event archive, route type, store modules, and
test-support boundary change. This keeps cleanup failures from being confused
with an untested return path.

The target is one user-owned daemon with one SQLite owner, durable channel
effects, stable effective-title routes, and exact identity only at admission.
Wakterm compatibility is checked once at startup. Idle operation does almost
no work. Unexpected task death exits the process for systemd to restart.

Questions for an external reviewer:

1. Does the real `--return-final` test pass, and is the resulting workflow
   useful enough to keep?
2. If local send remains, what is the minimum state that preserves
   at-most-once admission after uncertainty?
3. Should Wakterm include effective title and tab coordinates in its catalog?
4. Which event diagnostics are needed after projection?
5. Do proposed module splits reduce navigation and public API after deletions?

Relevant sources:

- [`src/main.rs`](../src/main.rs)
- [`src/service/production.rs`](../src/service/production.rs)
- [`src/service/offline.rs`](../src/service/offline.rs)
- [`src/store/mod.rs`](../src/store/mod.rs)
- [`src/wakterm/cli.rs`](../src/wakterm/cli.rs)
- [`src/channels/real.rs`](../src/channels/real.rs)
- [`docs/production-operations.md`](production-operations.md)
- [`docs/architecture/0003-idempotency-and-delivery.md`](architecture/0003-idempotency-and-delivery.md)
