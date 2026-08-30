# Panetone Rust roadmap

Date: 2026-08-21

Status: production state has crossed the Python-to-Rust boundary. The Python
service is permanently retired. The Rust database is authoritative even while
Panetone is intentionally stopped.

## Goal

Maintain one small Rust daemon for its one current user. Completion is based on
direct observation of wanted workflows, not exhaustive historical parity or a
continuous-uptime target.

Panetone runs as a systemd user service so an agent can inspect and restart it
without root. A system service is not a goal without a concrete privilege or
ownership requirement.

## Supported scope

- Telegram
- supported route establishment and bootstrap-output disposition for external
  launchers
- Signal when configured
- Debate group chats as ordinary Signal-bound routes
- one-way local sends
- `--return-final` when wanted and supported by Wakterm

Slack is removed.
Collaboration, group recreation, additional harnesses, and old routing edge
cases are included only when the user exercises them.

## Completed cutover cleanup

Schema version 7 makes the current Rust state the only runtime authority. The
Python migration bundle remains a cold archive, but the application no longer
contains or exposes:

- the offline Python migration command and implementation
- global promotion hold
- per-route promotion enablement
- idempotent promotion operator actions
- legacy control, return, Debate, Signal, and disposition tables
- Python rollback and promotion procedures

Schema version 7 is the supported post-cutover format. It removes persisted
Wakterm agent bindings and route status. Schema 6 databases upgrade in place;
older schema copies are rejected. The cold migration bundle is not a runtime
input.

Normal production startup rebases the passive Wakterm event cursor to the catalog head while preserving output already captured in the outbox. A deliberate `PANETONE_REPLAY_OFFLINE_OUTPUT=true` start resumes from the stored cursor. Accepted channel input and explicit workflow effects remain durable in either mode. A missing Telegram update offset initializes from Telegram and existing offsets are never overwritten. Routes resolve from Wakterm's current effective titles, so ordinary agent restarts need no operator reconciliation.

The retired Python runtime, Python parity suite, hidden conformance daemon,
root-owned deployment path, phase reports, and unused store compaction API are
also removed from the active tree. The final Python source remains available at
commit `b5b75ef1ec6de303f80a65e82075647bc62bc66d` for historical inspection.

## Observed production behavior

The live Rust trial demonstrated:

- startup and understandable status
- Wakterm agent discovery
- agent output delivered to Telegram
- Telegram replies admitted when idle and steered into active turns when busy
- durable outbox recovery after a Wakterm interruption
- restart without duplicate prompt admission
- fresh route establishment without legacy JSON or direct SQLite access
- exact projected versus unrouted output disposition for launcher bootstrap

The trial also exposed two independent Wakterm issues. One client subscription
lifetime bug caused dead CLI queues to accumulate, and one old running build did
not deliver responses back to Telegram correctly. Fixes belong in Wakterm and
must be verified in the binary actually loaded by the service.

## Remaining work is driven by use

- Verify the loaded Wakterm build contains the subscriber-lifetime fix before
  the next long Panetone run.
- Exercise Signal and Debate when they are next wanted.
- Exercise `--return-final` when it is next wanted.
- Resolve or discard stale current-Rust inbox or outbox records when they no
  longer represent useful work.

Unchecked optional items are deferred. There is no soak period, full provider
matrix, Python parity gate, or uptime target.

## Stop conditions

Stop and reassess when:

- a message can route to the wrong agent;
- a prompt may duplicate after an uncertain result;
- the Wakterm contract lacks identity required by an exercised workflow; or
- Telegram or Signal fails repeatedly without a new causal hypothesis.

Missing parity for an unused feature, an intentionally stopped service, and a
recoverable manual setup issue are not architecture blockers.

## Triggers for more machinery

Add broader conformance, recovery, or deployment machinery only after a
specific repeated failure demonstrates its benefit over a local fix. Additional
users, unattended uptime requirements, or external automation would justify a
new review. Until then, prefer direct observation and focused regression tests.
