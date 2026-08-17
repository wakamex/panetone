# Rust Phase 3 adapter boundary

Phase 3 replaces the Phase 2 recording boundaries with production-shaped
adapters, but it does not enable them in production. Every adapter test uses a
temporary HTTP, WebSocket, Unix, process, mux, or SQLite endpoint. No test loads
channel credentials, sends a real message, submits a prompt to a production
agent, or restarts a production service.

## Wakterm

`WaktermCli` invokes the narrow versioned Agent API with config loading and mux
auto-start disabled. It applies bounded deadlines and output limits, sends
prompt bytes over stdin, and preserves structured admission receipts. Route
binding uses an ephemeral pane ID only between two fresh catalog reads. Prompt
admission then uses the opaque agent ID and incarnation ID with the same durable
request ID on every attempt.

The current loaded Wakterm capability profile does not advertise
`event_stream.v1`. Panetone therefore does not start or persist a general event
consumer in this profile. Codex final callbacks continue to use the existing
durable return-request terminal stream. The experimental Codex output page is
limited to side-effect-free shadow discovery.

Claude, Codex, Gemini, and OpenCode output remain under the existing Python
reader until Wakterm publishes and advertises the corresponding durable event
projection. This is the explicit pre-cutover scope decision for all four
providers. No legacy reader removal patch is safe yet because there is no live
general event watermark from which to prove a gap-free, single-owner cutover.

Use the isolated mux for read-only live preflight:

```sh
dev/wakterm-dev build
dev/wakterm-dev serve

target/debug/panetone doctor \
  --socket /tmp/panetone-phase3/control.sock \
  --journal /tmp/panetone-phase3/state.sqlite3 \
  --wakterm-bin /code/wakterm/target/debug/wakterm \
  --wakterm-socket /run/user/1000/panetone-wakterm-dev/wakterm/sock
```

The explicit preflight reads the actual version, capabilities, and catalog. It
does not submit a prompt or start a channel adapter.

## Channels

Telegram and Debate use the Bot API with bounded JSON responses. Slack outbound
uses `chat.postMessage` with the durable effect UUID as `client_msg_id`. Signal
uses newline-delimited JSON-RPC over its Unix socket. Tokens are private fields
and transport errors do not include request URLs, authorization headers, or
remote response bodies beyond a short sanitized API description.

Inbound Telegram offsets advance only after every relevant update is durable.
Slack Socket Mode acknowledgements are sent only after the envelope is durable.
Signal notifications use a dedicated subscription connection and stable
sender-plus-timestamp identity. Repeated external identities are acknowledged
without creating duplicate inbox work.

Messaging mirrors remain at-least-once. A timeout or disconnected response can
therefore be retried with the same effect ID. Wakterm prompt and callback
admissions remain at-most-once. Losing their structured response moves the
durable workflow or callback to indeterminate and does not silently redeliver.

## Conformance and promotion boundary

The permanent tests cover:

- exact Wakterm catalog joins, admissions, terminal resume, and subprocess
  limits against a fake CLI
- current capability preflight against the isolated development mux
- Telegram, Debate, Slack, and Signal outbound requests against local servers
- rate limits, deleted destinations, partial and malformed responses, timeouts,
  disconnects, and secret-free errors
- Telegram, Slack, and Signal inbound persistence and acknowledgement order
- the real adapters running through the durable audit-first workflow, including
  duplicate UUID suppression and indeterminate Wakterm response handling
- Python-current and Rust-target control traces against one shared fixture,
  including effect counts and the two documented target behavior changes
- current and fixture-only future event profiles, ordered resume, retention
  gaps, lifecycle replacement, and unknown event rejection
- supervised task failures and durable workflow and destination failures in
  `status` and `doctor`; store and control admission remain statically bounded

Run the Phase 3 gate with:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
uv --no-config run --locked --script tests/run_python_suite.py
python3 tests/run_control_conformance.py --profile python-current
python3 tests/run_control_conformance.py --profile target \
  --backend 'target/debug/panetone conformance-backend'
```

Dependency license and advisory checks remain required. Phase 3 evidence is
discovery and local adapter evidence only. State migration, production binary
installation, service restart, real credential loading, and provider ownership
cutover remain Phase 4 and Phase 5 work.

## Completion evidence

Phase 3 completed locally on 2026-08-17. Seventy Rust tests and all 78 legacy
Python tests passed. Rust formatting and Clippy with warnings denied passed.
RustSec found no vulnerable dependencies, and cargo-deny passed advisories,
bans, licenses, and sources with duplicate-version warnings only.

The read-only live preflight used isolated Wakterm development mux version
`20260817-010725-6b6c2079`. It advertised `catalog.v1`,
`prompt_admission.v1`, `return_request_terminal_stream.v1`, and the
discovery-only `codex_output_shadow.experimental.v1`. It did not advertise
`event_stream.v1`, so Panetone correctly reported
`general_event_consumer: false` and created no event cursor. The production mux
remained active with the same PID and start timestamp before and after this
test. No production channel, prompt, deployment, restart, or credential was
used.
