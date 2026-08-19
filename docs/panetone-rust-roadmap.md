# Panetone Rust roadmap

Date: 2026-08-16

Last reviewed: 2026-08-19

Status: Phase 0 completed on 2026-08-16. Wakterm has since implemented the
Agent API boundary that Phase 1 expected to design. Phase 1 now begins with a
consumer review of that implemented contract, a hermetic Panetone conformance
suite, and a side-effect-free shadow comparison. This document retains the
initial audit findings as historical context and records their current status
separately.

## Decision

Replace the Python service with one Rust daemon through a contract-led, staged
replacement. Do not rewrite behavior from memory, do not split Panetone into
microservices, and do not run a permanent Python and Rust hybrid.

Rust is justified here, but not because the present code is Python. The
justification is that Panetone has become a long-lived local message router with
durable workflow state, several external transports, security-sensitive local
control, crash recovery, and operational coupling to Wakterm. A single compiled
binary, typed state machines, explicit task ownership, bounded concurrency, and
reproducible deployment are useful for that job. Merely translating the current
global dictionaries and control flow into Rust would preserve the important
problems and produce a worse codebase.

The recommended end state is one `panetone` package and binary with `daemon`,
`send`, `status`, and migration subcommands. It owns one SQLite database and one
Unix control socket. It talks to Wakterm and the configured messaging services
through narrow adapters. Python remains the production implementation while
the Rust replacement is built and tested offline, then there is one controlled
cutover. The old implementation remains available only as a rollback artifact
for a bounded period.

## Audit basis

The review covered `bridge.py`, `panetone_control.py`, the `panetone` CLI,
`README.md`, `docs/control-protocol.md`, both test files, Git state and history,
the installed CLI, the active service unit, process state, and recent service
logs.

At the start of this review the Git working tree was clean. The Unix control
socket and CLI work described as uncommitted had already been committed as
`7c36292 Add durable Panetone agent return routing`. HEAD was six commits ahead
of `origin/main`. There was therefore no current `git diff` to review. The
relevant control change was reviewed from that commit, which added 2,240 lines
across the control module, CLI, bridge integration, documentation, and tests.

The tests available during the initial audit were useful. On this host, 18
control tests and 18 bridge tests passed. They cover idempotent dispatch,
uncertain restart states, socket replacement safety, file permissions, durable
output-before-send ordering, cursor checkpointing, route retargeting,
audit-before-prompt ordering, and return callback deduplication.

The implementation also has several strengths worth preserving:

- The Panetone control socket is separate from the Wakterm mux socket.
- Socket directory and inode handling is careful, and Linux peer credentials
  restrict calls to the service user.
- The protocol is small, versioned, local, and easy to inspect.
- The audit is made visible before the target prompt is sent.
- Uncertain prompt delivery is not retried automatically.
- Source cursors and pending output are checkpointed before network delivery.
- The repository Wakterm implementation, rather than Panetone, owns exact
  provider-turn correlation for `--return-final`.
- Route titles are treated as stable user-facing names while ephemeral tab and
  pane identifiers are rebuilt.

These are the behavioral contracts for the replacement. They are not reasons
to retain the current internal structure.

## Phase 0 completion evidence

Phase 0 corrected the immediate production failures without changing the
agreed callback architecture:

- Commit `4ad1ce4` removed production hot reload, added a startup Wakterm
  capability probe, rejected unsupported return requests before any external
  side effect, added capped transient watcher backoff, and made background task
  shutdown explicit.
- Commit `e385dc7` committed the Python runtime lock and a hardened deployment
  unit. Commit `6609f97` added permanent deployment regression gates, and
  commit `2650e1c` added a rollback-safe optional system-service handoff.
- The repository and installed Panetone CLI hashes match. Wakterm was promoted
  to `12e7b47f`, which includes durable asynchronous return commit `1ac79325d`,
  and Panetone now negotiates that capability successfully at startup.
- The enabled Panetone user service has lingering enabled and shares the user
  service manager with Wakterm. Its cgroup contains one `uv` launcher and one
  Python child, with no nested reload chain and no unsupported-command loop.
- The credential file is mode `0600`, the control directory is mode `0700`, and
  the control socket is mode `0600`.
- All 43 focused control, bridge, and deployment tests pass.
- Live promotion evidence covered route resolution, Telegram audit, Wakterm
  prompt delivery, structured acknowledgement, explicit second-send
  report-back, duplicate UUID handling without redelivery, and an asynchronous
  Codex callback delivered to both the source agent and source Telegram topic.

The explicit second-send workflow is the current fallback and live Phase 0
gate. It does not replace the non-blocking asynchronous callback agreed and
implemented in Panetone commit `7c36292` and Wakterm commit `1ac79325d`.

## Phase 1 review evidence

The 2026-08-19 review changed several planning premises:

- The installed Wakterm binary is revision `2549048e`. Its live capability
  response advertises `wakterm.agent-api.v1`, `catalog.v1`,
  `prompt_admission.v1`, `return_request_terminal_stream.v1`, and
  `event_stream.v1`.
- Wakterm commit `af748ffc5` implemented the durable Agent API event stream,
  bounded retention, catalog ordering, explicit cursor gaps, and Wakterm-owned
  golden fixtures. Commits `d3c496cd6` and `088c606a5` hardened the four
  provider projections and derive agent turns from provider events. At the
  installed revision, `docs/agent-api/v1/golden-fixtures.json` has SHA-256
  `f845ed1943ed20c47292d0b925a90389580bd272ae528417f185e16dd2ebb8f8`.
- Codex, Claude, Gemini, and OpenCode projections are already live in Wakterm.
  Their existence is implementation evidence, not Panetone compatibility or
  promotion evidence. Panetone has not yet consumed the event stream or run the
  required shadow comparison.
- The Wakterm checkout contains work newer than the installed binary. Phase 1
  evidence must record the installed binary revision and negotiated response,
  not infer deployed behavior from checkout HEAD.
- The Panetone Python entry point treats extra arguments as normal service
  startup and loads configured production adapters. A test or fixture command
  that obtains dependencies by executing `bridge.py` can therefore contact
  real services and mutate durable cursors. Phase 1 needs a dedicated hermetic
  test entry point, temporary XDG directories, synthetic configuration, and
  recording adapters before any black-box harness is run.
- The Panetone user unit is enabled but was inactive during this review, with a
  successful prior exit. Enabled state is not current health evidence. Any live
  discovery or promotion run must record active state, loaded unit, binary
  revision, negotiated capabilities, state paths, and queue counts before it
  begins. No cause should be inferred from an intentionally stopped unit.

These findings remove Wakterm API design and provider-projection implementation
from Phase 1. They add consumer verification and test isolation as explicit
gates. They do not authorize a production provider cutover.

## Problems found in the initial audit

The most important findings were architectural and operational, not
performance problems. The Phase 0 items below are retained to explain the
roadmap decisions even where the immediate production symptom is now fixed.

### Deployment drift caused failures

At audit time, the repository CLI and `~/.local/bin/panetone` had different
hashes. The installed CLI lacked the repository version of `--return-final`.
The live Wakterm binary did not implement `agent request`, but Panetone launched
`wakterm cli agent request watch` every two seconds. This produced a continuous
failure loop and log noise instead of a stable degraded state.

The service performed in-process hot reload by executing `uv` again. The active
systemd cgroup contained a chain of 17 nested `uv` processes plus the Python
process, used about 175 MiB, and left systemd supervising the oldest wrapper.
This was a deployment bug, not evidence that Python itself was too slow. A Rust
service must not reproduce hot reload.

The active unit launched a mutable source checkout through a user-installed
`uv`. The README documented only `uv run bridge.py`, not a reproducible
production install, migration, health check, or rollback. Inline Python
dependencies used open minimum versions and there was no committed application
lock or release artifact.

### Durable truth is fragmented

Panetone currently uses:

- a JSON file for topic, group, collab, mute, and last-source state
- another atomically replaced JSON file for source cursors and output backlog
- a Signal SQLite database
- a control journal SQLite database
- many process-local dictionaries and queues

This makes cross-feature invariants difficult to state or test. The pending
output file is rewritten as a whole after every enqueue or dequeue. Reply
receipts and several inbound queues exist only in memory. Restart behavior
therefore varies by feature.

The control journal opens a connection and applies pragmas for each operation,
has no explicit schema version or migration framework, and stores important
progress as JSON. Indeterminate records never expire, while the database has a
hard 64 MiB ceiling. The eventual failure mode is refusing all new requests,
with no documented operator resolution. Completed request IDs expire after 30
days and can then execute again. A compact permanent idempotency tombstone is
safer than allowing an old retry to become a new prompt.

### One module owns too much mutable behavior

`bridge.py` is about 3,500 lines. It combines configuration, process discovery,
provider session parsing, route reconciliation, Telegram, Signal, Slack,
collaboration policy, durable output, local control, callback fanout, hot
reload, and application startup. Its many module globals are implicitly owned
by one event loop, but blocking SQLite and subprocess work is mixed into that
model. Background tasks are created without a supervisor retaining their
handles, so a task can die without bringing the service down or setting an
explicit degraded health state. Shutdown closes the control socket but does not
coordinate every adapter, worker, watcher, and durable queue.

The one global return-delivery lock also means a slow callback can block every
other return. Fixed retry intervals have no capability gate, exponential
backoff, or circuit state.

### Some protocol and reliability claims need tightening

The local control protocol is the correct transport, but the implementation
has details that should not become compatibility commitments:

- Omitting `return_final: false` and `timeout_ms: 0` is documented as
  equivalent to sending them, but the normalized request only includes fields
  that were originally present. The semantically identical requests therefore
  hash differently for idempotency.
- The current response waits for route refresh, one or more Telegram calls,
  Wakterm submission, and audit editing behind a fixed 15-second client
  timeout. A timeout can leave the caller uncertain even while the service is
  still handling the request.
- The docs call the Wakterm watcher a stream, while the present Wakterm command
  polls internally and the installed binary does not support it at all.
- Telegram output is effectively at-least-once, while prompt and callback
  delivery are treated as at-most-once. Those choices are reasonable in places,
  but they are encoded by procedure rather than declared per side effect.
- `last_error` is shared by both return destinations, so one destination can
  overwrite the diagnostic for the other.
- Telegram starts with `drop_pending_updates=True`, which can discard inbound
  messages during a restart. Slack and some Signal work queues are also
  process-local.

No implementation can make a remote API call and a local SQLite commit exactly
once. The design should state the chosen failure semantic for every side effect
instead of implying a stronger guarantee.

### Security is good at the socket boundary but inconsistent elsewhere

The socket protections should be retained. The state and credential boundary
needs improvement. The current repository `.env` is ignored by Git but was
mode `0644` during this review and contains configured service credentials.
Secrets should not live in a broadly readable project file. SQLite sidecars
must be born with restrictive permissions, not corrected only after database
initialization. Logs currently include message snippets, paths, route details,
and raw exception strings. The future default should log identifiers and
outcomes, not message bodies or credentials.

## Ownership in the ideal system

Clear ownership is more important than the language.

### Wakterm owns terminal truth

Wakterm owns live panes, adopted agents, process incarnations, harness and
provider session identity, prompt submission, interruption, exact turn
correlation, final response state, and the durable event sequence for agent
requests.

Panetone must not infer prompt acceptance from session file modification or
retry a prompt by writing another carriage return. All normal input should use
a Wakterm agent operation with an explicit receipt. If a harness lacks that
capability, the fallback must be labeled legacy and at-most-once rather than
silently using weaker semantics.

The intended output path now exists as Wakterm's durable Agent API event
interface. Panetone currently parses Claude, Codex, OpenCode, and Gemini stores
and rediscovers processes by TTY, cwd, and modification time. That duplicates
Wakterm authority and caused the class of stale-session problem fixed there.
Do not add provider readers to Rust Panetone. Existing Python readers remain
only until each provider has passed shadow comparison and a controlled cutover
to authoritative Wakterm events.

### Panetone owns messaging and workflow truth

Panetone owns:

- stable Panetone route IDs and user-facing title aliases
- bindings from a route to the current Wakterm agent and ephemeral pane
- Telegram topics, Signal groups, Slack channels, and channel identity
- authorization of inbound channel users
- the audit-before-prompt policy
- control request idempotency and registration receipts
- durable inbound work, notification outboxes, and per-destination delivery
  state
- source return routes and consumption of the Wakterm event cursor
- collaboration and cross-channel routing policy

A title remains the CLI locator, with exact case-insensitive matching and an
ambiguity error. Internally it resolves to a stable route UUID before any side
effect. The route record stores current display name and Wakterm binding.
Ephemeral pane and tab IDs never become durable identity. Until Wakterm exposes
a stable route or agent identity for every supported case, title reconciliation
remains explicit and conservative.

### Wakterm Agent API is the shared boundary

Do not extract provider parsing into a shared runtime library. The duplication
is conflicting authority, not reusable application logic. Wakterm privately
owns provider paths, parser cursors, process matching, observer state, and turn
reconciliation. Panetone consumes normalized events and never imports those
internals.

The shared artifact is the implemented Wakterm-owned v1 wire contract and its
golden compatibility fixture file. Panetone must consume the fixture from a
recorded Wakterm revision and verify its hash rather than maintaining rewritten
examples. The contract covers:

- a stable agent catalog with opaque agent and process-incarnation identities
- authoritative prompt submission and durable receipts
- normalized assistant messages, plans, turn transitions, finals, observer
  failures, and lifecycle events
- monotonic durable event sequences and resumable `after_sequence` reads
- bounded retention with an explicit `cursor_too_old` response
- capability and schema-version negotiation across independently deployed
  Wakterm and Panetone versions

The current `wakterm-client` crate is too broad for Panetone because it carries
mux, terminal, SSH, PTY, configuration, and transport internals. Panetone
should initially consume the stable CLI or wire protocol. If Rust Panetone
later creates genuine duplicated DTO and compatibility code, Wakterm may own a
tiny `wakterm-agent-protocol` crate. That crate must contain only wire DTOs and
compatibility logic, with no mux, panes, SQLite, provider formats, executors, or
Panetone workflow policy.

Wakterm owns the Agent API architecture, event journal, golden fixtures, and
provider projections. Panetone owns consumer requirements, its durable consumed
cursor, compatibility harness, shadow comparison, provider cutover, and removal
of duplicate readers. One bounded independent protocol and failure-semantics
review is useful before promotion, but it must not become a third implementation
authority.

### Timing decision

Validate the implemented boundary before starting the Rust core, but do not
make any provider production cutover a prerequisite for Rust. The cheapest
reliable sequence is:

1. Pin the installed Wakterm revision, negotiated capabilities, and hash of its
   v1 golden fixture as the review baseline.
2. Panetone records sanitized fixtures for the normalized outputs, ordering,
   cursors, restart behavior, and routing decisions it currently depends on.
3. Panetone reviews the implemented catalog, admission receipt, events,
   retention gap, lifecycle ordering, and error contract as a consumer. An
   independent reviewer then challenges compatibility and failure semantics
   once.
4. Build a hermetic Panetone fixture and black-box harness that cannot load
   production credentials, sockets, state paths, or network adapters.
5. Panetone runs a side-effect-free Codex comparison against the durable event
   stream. This is discovery evidence and cannot become production promotion
   evidence.
6. Once that boundary is validated, Rust core work proceeds against fake
   Wakterm events while remaining provider shadow runs and live cutovers happen
   during Phase 3.

Doing the consumer validation later would let temporary Python ownership shape
the Rust design and create a foreseeable rewrite. Wakterm's existing four
projections do not require Panetone to validate or promote them together. The
Codex comparison remains the representative Phase 1 gate.

### SQLite owns durable state

Use one SQLite database with one schema migration history and one dedicated
blocking writer task. WAL and full synchronous commits are appropriate at this
traffic level. The async runtime sends typed commands to the store task rather
than calling SQLite on reactor threads. A small number of read connections can
be added only if measurement shows the single owner is a bottleneck.

The database should contain separate tables for routes and channel bindings,
source cursors, durable inbound messages, outbound items and chunks, control
requests, compact idempotency tombstones, return routes, per-destination return
delivery, external receipts, and metadata such as the Wakterm event cursor.
Signal history can live in the same database as a separate table. This is not
event sourcing. Current state plus durable inbox and outbox records are enough.

Every external operation follows one state machine and records:

- stable operation and request IDs
- normalized semantic input hash
- destination and delivery policy
- prepared, in-flight, delivered, failed, or indeterminate state
- bounded attempt count and next retry time where retry is safe
- external receipt when one exists
- separate last error and timestamp per destination

Keep permanent compact request-ID tombstones after pruning large payloads and
responses. Reusing an old UUID must never silently become a new prompt.

### Channels own remote acceptance

The delivery policy must be explicit:

- Agent prompts are at-most-once. If the local receipt is lost after possible
  acceptance, mark the prompt indeterminate and never resend automatically.
- Audit messages must become visible before a prompt is attempted. Failure to
  establish a visible audit fails closed.
- Ordinary agent-output notifications are at-least-once. A crash after remote
  acceptance may produce a duplicate, which is preferable to losing output.
  Include a stable Panetone delivery ID so duplicates are recognizable.
- A return sent as a new prompt to the source agent is at-most-once.
- A return mirrored to Telegram, Signal, or Slack is at-least-once, chunked into
  independently recorded items. Duplicate notification is safer than a missing
  final result.
- Topic and group reconciliation is idempotent and keyed by the durable route,
  with explicit handling for deleted or renamed remote objects.

These semantics should be visible in code types and tests, not comments around
arbitrary procedure ordering.

## Rust architecture

Start with one Cargo package and one binary. Do not create a workspace or a
crate per module. A suitable source layout is:

```text
src/
  main.rs
  config.rs
  domain/
  service/
  store/
  control/
  wakterm/
  channels/
    telegram.rs
    signal.rs
    slack.rs
  supervisor.rs
```

The domain module contains route, request, delivery, and callback state
machines. It has no network, SQLite, process, systemd, or CLI dependency. The
service module applies policy and coordinates durable transitions. Adapters
translate external inputs into typed service commands and execute typed effects.
The supervisor owns every long-running task, restart policy, cancellation token,
and health state.

Use bounded Tokio channels between adapters, service, and store. A full memory
queue must apply backpressure or spill to the durable inbox rather than grow
without limit. External calls need classified errors, bounded deadlines, and
exponential backoff with jitter and a cap. Permanent configuration or
capability errors enter a degraded state and retry slowly only after a relevant
change or operator action. The current two-second incompatible-Wakterm loop must
be impossible.

The same executable provides:

- `panetone daemon` for the service
- `panetone send` as the existing v1 control client
- `panetone status --json` for local health and backlog inspection
- `panetone doctor` for configuration, permission, schema, channel, and Wakterm
  capability checks without sending messages
- `panetone migrate` for an explicit, offline state migration

Do not expose internal Rust types directly as protocol JSON. Define stable
request and response DTOs at the control boundary and convert them to domain
commands. Continue to support `panetone.control.v1` exactly, with semantic
defaults normalized before hashing. Unknown additive fields can be ignored in
v1, but a semantic change requires a new schema. Maintain golden JSON fixtures
that both the Python and Rust clients and servers must pass.

Keep newline-delimited JSON over the Unix socket. It is debuggable and adequate
for one local caller per connection. Do not introduce HTTP, gRPC, protobuf, a
remote listener, or authentication tokens. Preserve `0700` runtime directory,
`0600` socket, same-UID peer checks on Linux, symlink refusal, stale-socket
probing, and inode-safe cleanup.

## Dependencies

Use the smallest conventional dependency set that supports the service:

- Tokio for async Unix sockets, subprocesses, timers, signals, and network
  adapters
- serde and serde_json for control and external protocols
- clap for the single binary CLI
- rusqlite with bundled SQLite, isolated behind the store task
- tracing and a journald-compatible subscriber for structured logs
- reqwest with rustls for Telegram and Slack HTTP
- tokio-tungstenite for Slack Socket Mode
- uuid and sha2 for identifiers and semantic hashes
- thiserror for boundary errors, with anyhow only at application edges
- rustix only where standard-library Unix permission or peer-credential support
  is insufficient

Signal JSON-RPC is small enough to implement over a Tokio Unix stream. The
currently used Telegram and Slack endpoint surfaces are also small enough for
thin local adapters instead of large bot frameworks. Do not create a generic
channel SDK or plugin ABI.

Commit `Cargo.lock`, declare an MSRV, pin the toolchain used for releases, and
record the build revision in `panetone --version`. Run rustfmt, clippy for all
targets with warnings denied, tests, `cargo deny`, and RustSec advisory checks.
Update one dependency family at a time and exercise adapter contract tests for
every update.

## Observability

Use structured logs with request ID, operation ID, route ID, channel, phase,
attempt, latency, outcome, and classified error. Message bodies, bot tokens,
phone numbers, raw credentials, and full session paths are omitted by default.
A debug mode may include safe excerpts only when explicitly enabled.

`panetone status --json` should report:

- binary version and build revision
- process uptime and database schema version
- Wakterm version, negotiated capabilities, and connection state
- each configured channel state and last successful receive/send time
- durable inbox, outbox, indeterminate, and dead-letter counts
- oldest pending item age and next retry time
- control socket path and ownership checks
- last error per adapter without secrets

On startup, log the effective non-secret configuration, database path and
schema, socket path, enabled adapters, and capability negotiation result. Use
systemd and journald as the initial monitoring surface. Do not build a web
dashboard, Prometheus endpoint, distributed tracing backend, or alerting system
yet.

## Security and deployment

Move credentials out of the repository to a mode `0600` configuration or
credential source. A systemd credential directory is preferable once the
deployment is scripted. Non-secret configuration may use a checked example
TOML file, but the initial Rust implementation does not need a dynamic config
framework. Validate all paths, IDs, limits, and allowlists at startup and refuse
partially valid security configuration.

Keep Panetone and Wakterm under the same systemd manager so dependency ordering
is real rather than implied across a system-manager and user-manager boundary.
Prefer user services with lingering on this host so agents can inspect,
restart, and verify both services without root. Install the release binary
atomically under the service user's executable path. Keep the runtime directory
mode `0700`, and create the control socket and databases as `0600` under the
appropriate XDG runtime and state directories.

Move both Wakterm and Panetone to system services only if a concrete
system-level ownership or privilege requirement appears. If that happens,
install both under `/etc/systemd/system/` with `User=mihai` and system-labeled
executable paths. Do not mix managers merely to move Panetone alone. Within
either manager, use `After=wakterm-mux-server.service` and at most `Wants=`, not
`Requires=`.
Panetone must remain alive, retain inbound work, expose degraded health, and
reconnect if Wakterm is restarted. Configure `Restart=on-failure`, a bounded
restart delay, graceful stop, and a timeout that allows the store and adapters
to quiesce. Remove all source-file watching and in-process execution of build
tools. Development reload belongs in a separate developer command.

The installed daemon and CLI must be the same binary. Installation records the
binary hash and version, runs `doctor`, starts or restarts the service, and
verifies `status` before promotion. Keep the previous binary and a pre-migration
database backup for rollback. A release must refuse to open a database with a
newer unsupported schema rather than attempting a downgrade.

Reasonable systemd hardening includes `NoNewPrivileges`, a private temporary
directory, a read-only system, a writable managed state directory, and an
address-family allowlist for Unix and Internet sockets. Do not add brittle
system-call filters until the real adapter behavior has been observed.
Application-level database encryption is not an initial goal because local key
storage would provide little benefit. Rely on restrictive permissions and host
disk protection.

## Migration sequence and gates

### Phase 0: contain current production failures - complete

Do this before writing substantial Rust code:

1. Remove production hot reload and restart Panetone once to collapse the
   nested `uv` process chain.
2. Install matching repository CLI and Wakterm versions, or disable unsupported
   return watching through a startup capability check.
3. Replace the two-second permanent-error loop with capped exponential backoff.
4. Move or restrict the credential file to mode `0600`.
5. Pin the current Python runtime dependencies for the remaining migration
   period.

Gate completed: one supervised cgroup with one `uv` launcher and one Python
child, matching installed Panetone CLI hashes, no unsupported-command log
storm, restrictive credential and socket permissions, a committed runtime
lock, an enabled lingering user service, and all 43 focused tests passing.

### Phase 1: freeze contracts and build the conformance suite

Document short architecture decisions for ownership, route identity,
idempotency normalization, per-side-effect delivery semantics, SQLite
migrations, and shutdown. Convert current protocol examples and representative
state into sanitized golden fixtures. Add a black-box harness that can run the
same control v1 and state-machine cases against Python and Rust.

Begin by making the test boundary hermetic. Add a dedicated test entry point
that resolves the locked Python dependencies without executing `bridge.py`.
Run every fixture and black-box case with temporary XDG runtime, config, and
state directories, a fake Wakterm socket, synthetic credentials, and recording
channel adapters. The harness must fail if it attempts an external network call
or opens a configured production path. Live discovery is a separate explicit
mode that records its revisions and effective non-secret configuration before
it starts.

Then review the implemented Panetone-to-Wakterm boundary. Record the installed
Wakterm revision and capability response, load Wakterm's v1 golden fixture from
that recorded revision, and verify its hash. Panetone documents the normalized
assistant output, plan, turn, lifecycle, ordering, cursor, restart, and routing
behavior it currently consumes without proposing new provider semantics. One
bounded independent review covers schema compatibility, prompt admission,
catalog ordering, cursor gaps, retention, crash boundaries, unknown additive
fields, unknown event kinds, and classified errors. Panetone then shadows the
durable Codex event stream against its current reader with every outbound side
effect replaced by a recording sink and a cursor separate from production.

Capture fixtures for all currently enabled behavior: four harnesses, Telegram,
Signal, Slack, debate routing, collaboration, topic recreation, source cursor
recovery, one-way control send, and return-final callback. Callback conformance
uses the reviewed Wakterm v1 fixture and a compatible fake, while retaining
compatibility cases for the older `1ac79325d` return-request boundary. It must
also cover the explicit `return_final_unavailable` result from an older
installed Wakterm and unsupported return mode for other target harnesses.
Include remote API errors, rate limits, partial multi-chunk sends, process
restart, deleted remote topics, stale panes, duplicate titles, and incompatible
Wakterm capability.

Gate: the conformance command cannot reach production paths or external
services; current Python behavior is reproducible from fixtures; every intended
semantic difference is written down; no production state format remains
undocumented; the exact Wakterm revision, capability response, and fixture hash
are recorded; the Wakterm Agent API contract has passed independent review; and
the Codex shadow run has zero unexplained normalized event differences. The
shadow run is discovery evidence only. It does not authorize a production
cutover or require the remaining three provider shadows before Phase 2.

### Phase 2: implement the Rust core offline

Create the single package, domain state machines, store task, schema migrations,
control v1 server and client, supervisor, status, doctor, and fake adapters. Do
not connect it to production bots. Implement deterministic time through an
injected clock and fault injection after every durable transition and external
effect boundary. The fake Wakterm adapter must implement the reviewed Agent API
fixtures, including cursor resumption, `cursor_too_old`, version mismatch,
observer failure, lifecycle change, and durable request completion.

Gate: Rust passes all control golden fixtures, model and store tests, restart
tests, socket security tests, and crash-point tests under sanitizers where
practical. Clippy, formatting, dependency policy, and advisory checks are clean.

### Phase 3: implement real adapters and remove duplicate authority

Implement the real Wakterm Agent API adapter first, then Telegram, Signal, and
Slack. Use local fake HTTP, WebSocket, Unix JSON-RPC, and Wakterm servers for
deterministic adapter tests. Do not implement provider session readers in Rust
Panetone. Treat Wakterm's existing Codex, Claude, Gemini, and OpenCode
projections as candidates that still require consumer evidence. Panetone
shadows each one against the existing Python reader before any production
switch. Fix an unexplained difference in the layer that owns it; add or change
a Wakterm projection only when the comparison identifies a concrete missing or
incorrect provider event.

Cut over Codex, Claude, Gemini, and OpenCode one at a time. Each cutover records
a durable watermark, disables the old reader before enabling event delivery,
and proves there is neither an output gap nor dual delivery. Remove that
provider's Python session discovery, transcript parsing, file cursor, TTY
rediscovery, and `send_and_verify` fallback as soon as its promotion gate
passes. Do not wait for all providers before deleting ownership that has already
moved safely.

Replay captured source and channel traces through Python and Rust with all
outbound network operations replaced by recording sinks. Compare normalized
route decisions, durable transitions, chunks, and delivery policies. Do not run
both implementations against a production inbound bot or let both perform a
side effect.

Gate: zero unexplained decision differences on the fixture corpus, every
provider uses authoritative Wakterm events with no gap or dual delivery, all
enabled channel contracts pass, bounded queues remain bounded under fault
injection, and every task failure appears in status and logs.

### Phase 4: migrate state offline

Implement an idempotent migration command that requires the Python service to
be stopped. It copies and hashes every legacy state file, opens a new database,
migrates JSON state, pending output and cursors, Signal history, control
requests, return routes, destination states, and the Wakterm cursor in one
transaction, then writes a migration manifest. It never modifies the originals.

Run migration twice against copies to prove idempotency. Validate row counts,
route mappings, cursor positions, pending message payload hashes, request IDs,
and destination states. Refuse ambiguous or malformed legacy state instead of
guessing.

Gate: a restore drill returns to Python and the original state, while a second
migration produces an identical validated Rust database.

### Phase 5: controlled production cutover

Stop Python, snapshot legacy state, migrate, atomically install the Rust binary
and its unit in the selected systemd manager, and start with outbound delivery
paused. Run `doctor` and inspect `status`. Then enable one canary route and
validate, in order:

1. Telegram inbound to Wakterm and assistant output back to Telegram
2. one-way local control with visible audit
3. for a compatible Codex target, return-final registration, completion, and
   both callbacks; when the installed Wakterm lacks durable agent requests,
   `return_final_unavailable` with no side effect plus an explicit second-send
   report-back
4. Signal receive, archive, route, and send
5. Slack receive and response
6. topic or group recreation with pending output
7. daemon restart while output is queued
8. daemon restart around each safe and indeterminate control boundary
9. Wakterm restart and version mismatch recovery

Gate: no lost accepted inbound message, no duplicate prompt, every uncertain
external call is visibly indeterminate, every queued item survives restart,
installed CLI and daemon versions match, and rollback has been exercised.

### Phase 6: soak and retire Python

Run the Rust service for at least seven days of normal use with daily backlog
and indeterminate-state review. Require no unexplained restart, no growing task
or file-descriptor count, no stuck outbox item, no dropped Telegram update, and
no unbounded log loop. Perform one deliberate service restart and one Wakterm
restart during the soak.

After the gate passes, move Python to a clearly labeled legacy directory or tag
for one release, update the README to the supported Rust install and recovery
flow, then remove legacy runtime dependencies. Do not maintain two production
implementations indefinitely.

## Testing strategy

The Rust test pyramid should be:

1. Pure table-driven state-machine tests for every legal and illegal transition,
   race resolution, retry policy, and route ambiguity.
2. Property tests only where the state space earns them, especially framing,
   UTF-16 chunking, normalization, and transition invariants.
3. SQLite integration tests for every schema version, WAL crash and reopen,
   corruption and unsupported-version diagnostics, idempotency tombstones,
   inbox and outbox recovery, and retention.
4. Adapter contract tests against local fake servers for rate limiting,
   disconnects, malformed responses, partial sends, duplicate updates, and
   timeout classification.
5. Unix socket tests for path ownership, mode, symlinks, stale and live sockets,
   peer credentials, request limits, malformed framing, slow clients, and inode
   replacement.
6. Process-level tests that launch fake Wakterm and Panetone binaries, kill them
   at declared crash points, restart them, and inspect durable results.
7. Cross-repository Wakterm Agent API conformance tests for supported version
   pairs, capability negotiation, cursor resumption, retention gaps, and
   classified terminal states.
8. A small manual or scheduled live suite using dedicated test destinations.

Use fake time rather than sleeps. Test names and scenario evidence are the
acceptance record, not the raw test count. CI should run unit, integration,
formatting, clippy, license, source, and advisory checks. Live service tests are
promotion gates, not required for every local edit.

## Principal risks

- Behavioral scope is larger than the README suggests. Telegram, Signal,
  Slack, debate, collaboration, four harness sources, durable output, and local
  orchestration are all active. Mitigation: fixture inventory and one explicit
  parity ledger.
- External APIs cannot provide exactly-once effects. Mitigation: declare
  at-most-once or at-least-once per effect and preserve visible IDs.
- Provider store formats change. Mitigation: move observation authority toward
  Wakterm and keep compatibility readers isolated and removable.
- A large Rust port can reproduce Python globals behind mutexes. Mitigation:
  pure state machines, one store owner, bounded message passing, and dependency
  direction tests.
- State migration can lose pending work or route identity. Mitigation: offline
  copy-only migration, hashes, invariant reports, and a tested rollback.
- Rust ecosystem SDKs can expand the dependency and compile surface. Mitigation:
  thin adapters over the small endpoint set and one package.
- Feature work during migration can keep moving the target. Mitigation: freeze
  nonessential behavior until the cutover or implement each necessary fix in
  both the fixture contract and replacement.

## Explicit non-goals

Do not build these yet:

- microservices or separate daemons per channel
- a permanent Python adapter sidecar
- a generic plugin, scripting, or dynamic channel ABI
- a remote TCP control API
- HTTP, gRPC, protobuf, Kafka, NATS, or another broker
- event sourcing or a distributed database
- exactly-once claims for remote APIs
- a web dashboard, Prometheus, OpenTelemetry backend, or alert manager
- automatic replay of indeterminate agent prompts
- generalized workflow DAGs, scheduling, agent pools, or multi-host routing
- hot reload in production
- application-level database encryption without a credible key-management plan
- broad Wakterm session parsing in Panetone now that Wakterm supplies
  authoritative events
- multiple Rust crates before a measured compile, reuse, or dependency-boundary
  need appears

## Final recommendation

Proceed with a Rust replacement, but treat it as a reliability migration rather
than a language rewrite. The live deployment failures have been contained and
Wakterm's Agent API boundary is implemented. Next freeze Panetone's current
contracts, verify the implemented boundary as a consumer, and run one hermetic
Codex shadow comparison. Then build one Rust binary and one SQLite-backed
service offline, verify it against the Python implementation and fault-injected
local adapters, migrate state with the old service stopped, and make one
reversible cutover. Keep
Wakterm authoritative for terminals and provider turns, Panetone authoritative
for messaging workflows and channel delivery, and SQLite authoritative for
durable Panetone state.

This route reaches a cleaner long-term system without paying for transitional
microservices, generic frameworks, or indefinite dual implementations. The
acceptance criterion is not that the code is Rust. It is that process ownership,
state transitions, failure semantics, deployed artifacts, and recovery behavior
are explicit and demonstrably correct.
