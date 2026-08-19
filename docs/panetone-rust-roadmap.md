# Panetone Rust roadmap

Date: 2026-08-19

Status: the completed `phase1-contracts` implementation has been merged into
`main`. The Rust candidate exists, Slack is removed, and the remaining work is
to try the candidate against the workflows the sole user actually wants. The
Python Panetone service is intentionally stopped. Wakterm is running as a user
service and advertises the required Agent API v1 capabilities.

## Goal

Replace the Python bridge with one Rust Panetone daemon that is useful for its
one current user. Completion is based on direct observation of the selected
workflows, not exhaustive parity with every historical feature or continuous
uptime.

Panetone should run as a systemd user service with lingering so an agent can
inspect and restart it without root. A system service is not a goal unless a
concrete system-level privilege or ownership requirement appears.

## Supported scope

The intended transports are:

- Telegram
- Signal when configured
- Debate as an ordinary Signal route

Slack was removed in commit `599ccf3`. No Slack credentials, connection,
adapter, input, output, dependency, fixture target, or acceptance gate should
return. Legacy Slack preferences may remain only as migration metadata, and a
pending Slack delivery must be handled explicitly rather than replayed.

The first usable candidate should support only the workflows currently wanted:

- observe selected Wakterm agents through the Agent API
- deliver agent output to Telegram
- route Telegram replies to the correct agent
- route Signal and Debate when those workflows are enabled
- send a one-way local control message
- use `--return-final` when that workflow is wanted
- report enough status and errors for manual diagnosis

Collaboration, topic or group recreation, additional harnesses, and old routing
edge cases are included only when the user exercises them. An unused feature is
not a release blocker.

## Current implementation

The merged Rust code already contains the work previously called Phases 1
through 5A:

- domain state machines and a versioned SQLite store
- the local control protocol and CLI
- Wakterm Agent API catalog, admission, return, and event adapters
- Telegram and Signal adapters
- held startup and promotion controls
- legacy state migration
- status, doctor, and deployment rehearsal commands
- focused regression and adapter tests

Those components are implementation candidates, not reasons to retain the old
phase gates. The detailed documents under `docs/` remain useful for explaining
contracts and prior decisions, but they do not require exhaustive revalidation
before a single-user trial.

## Verification on 2026-08-19

The merged candidate passed:

- Rust formatting
- Clippy for all targets and features with warnings denied
- all 88 Rust tests
- all 83 isolated Python tests
- a locked release build
- a read-only Rust `doctor` check against the active Wakterm user service

The doctor check negotiated Agent API v1, found the required catalog,
admission, return, and event capabilities, and read 14 catalog agents. It used
a temporary empty Rust database and did not connect a channel, submit a prompt,
start Panetone, or restart Wakterm.

## One merged phase: make the Rust replacement useful

### 1. Establish a safe candidate

Build the merged source with the committed lockfile. Run the existing offline
tests and safe local adapter checks. Ordinary tests must use temporary paths,
fake endpoints, and synthetic credentials. They must not start the production
bridge, poll a real channel, submit a real prompt, or restart Wakterm.

Keep focused automated coverage for mistakes that are costly or hard to notice:

- routing to the wrong agent
- admitting the same prompt twice
- retrying an indeterminate prompt or callback
- corrupting or ambiguously migrating durable state
- using production credentials or paths during ordinary tests
- reintroducing Slack

Do not expand this into a new parity framework, generalized fault laboratory,
or coverage target. Add a regression test when an observed failure warrants
one.

### 2. Choose the state starting point

Because Panetone is stopped and has one user, choose the least costly option at
trial time:

- start from a fresh Rust database when old routes and history are not needed
- run the existing copy-only migration when retaining topic, group, request, or
  delivery state is useful

Never replay an uncertain legacy prompt. Preserve a snapshot before migration,
but do not make a full rollback drill or every historical row a gate when the
user has explicitly discarded that work.

### 3. Run one manual user-service trial

Install or launch the Rust candidate as a systemd user service in a held or
otherwise non-delivering state. Verify `doctor`, `status`, the loaded unit,
binary revision, state path, Wakterm socket, and negotiated capabilities. Do
not restart the working Wakterm service as part of Panetone setup.

Enable one real route and exercise only the desired workflows. Fix a failure in
the layer that owns it, rerun the failed workflow, and add a focused regression
test when the failure could recur silently.

### 4. Judge completion by observation

The candidate is feature complete when the user can perform the selected
checklist without an unexplained result:

- Panetone starts and exposes understandable status.
- It sees the intended Wakterm agents.
- Agent output reaches the selected Telegram topic.
- A Telegram reply reaches the correct agent.
- Signal works for a selected route when Signal remains wanted.
- Debate works through Signal when Debate remains wanted.
- One-way local control works when used.
- `--return-final` works when used.
- A deliberate Panetone restart does not duplicate a prompt or bind output to
  the wrong agent.
- Removed Slack configuration is rejected without making a connection.
- Failures are visible enough to diagnose from status and logs.

Unchecked optional items are deferred, not failed. There is no seven-day soak,
all-provider shadow requirement, uptime target, exhaustive Python parity gate,
or requirement to exercise unused remote API failures.

### 5. Retire Python after the trial

Once the selected checklist passes, keep the stopped Python implementation and
its state snapshot only as a short-term recovery artifact. Do not maintain two
active implementations or continue adding features to both.

## Stop conditions

Stop and reassess only when one of these occurs:

- the candidate can route a prompt or output to the wrong agent
- a prompt may be duplicated after an uncertain result
- migration cannot distinguish safe pending work from uncertain work
- the Wakterm contract lacks information required by an exercised workflow
- Telegram or Signal behavior fails repeatedly without a new causal hypothesis

Missing parity for an unused feature, an intentionally stopped service, and a
recoverable manual setup issue are not architecture blockers.

## Triggers for more machinery

Reintroduce broader conformance, soak, or deployment gates only if Panetone
gains additional users, unattended uptime requirements, external automation,
or repeated compatibility regressions. Until then, prefer direct observation,
small fixes, and focused tests.
