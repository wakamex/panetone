# Panetone conformance fixtures

The Phase 1 corpus records observable contracts before the Rust replacement is implemented. Tests may read local fixture files and temporary databases, but they must not contact Telegram, Signal, Slack, or a live agent.

## Control v1 black-box runner

Run the Python-current profile with:

```sh
python3 tests/run_control_conformance.py --profile python-current
```

The default backend uses the production `ControlServer`, `ControlJournal`, parser, and dispatcher with a deterministic recording handler. The handler records a possible delivery boundary without calling Wakterm or a channel.

Any replacement backend can run the same cases if its command accepts:

```text
--socket PATH --journal PATH --effect-log PATH
```

For example:

```sh
python3 tests/run_control_conformance.py \
  --profile target \
  --backend '/path/to/panetone-conformance-backend'
```

The runner owns a temporary directory, starts and stops the backend, speaks newline-delimited control v1 over the Unix socket, and counts durable effect records in the JSONL effect log. The crash case kills the backend after its recording effect and verifies that restart returns an indeterminate response without a second effect.

`python-current` and `target` have two declared differences. Current Python hashes omitted `return_final: false` and `timeout_ms: 0` differently from explicit defaults. The target profile requires those semantic equivalents to share one idempotency hash. A future compatibility migration must account for old journal hashes before production changes to the target behavior.

Current Python also treats Wakterm's busy refusal after the audit as indeterminate. The target profile requires a durable queued acknowledgement and no prompt effect, as specified by ADR 0005. The deterministic current backend cannot pass the target profile until that queue state exists, which prevents discovery evidence from being mistaken for implementation evidence.

## Provider observation fixtures

`tests/fixtures/agent-observation/provider-observations.json` records sanitized Claude, Codex, Gemini, and OpenCode observations that the Python bridge currently makes visible. The fixture tests cover text and plan rendering, ignored records, provider order, partial JSONL records, rewritten Gemini history, and rebuilt OpenCode row identities.

These are consumer requirements, not a shared parser specification. Wakterm owns provider parsing. Rust Panetone must consume normalized Wakterm events and must not use this fixture to implement provider readers.

`consumer-requirements.json` states the semantic boundary expected from the Wakterm Agent API without dictating its wire field layout. Once Wakterm publishes golden wire fixtures, Panetone pins a compatible copy or validates them directly across repositories.

## Legacy state and channel decisions

`tests/fixtures/legacy-state/representative.json` contains every legacy state source, all four pending output destinations, all control terminal classes, Signal inbox states, and a callback whose legacy error cannot be attributed to one destination. It contains no production path, token, chat ID, phone number, message, or provider identifier.

The state fixture is migration input and rollback evidence. A migration test must copy it, migrate twice, and compare validated identities, row counts, cursor or watermark mappings, payload hashes, and delivery states. It must reject ambiguous route recovery.

Route and channel fixtures freeze title resolution, destination selection, text formatting, and retry fairness. They document current behavior even where the target ledger calls for a deliberate improvement.

## Discovery and promotion

A Wakterm output shadow run replaces every outbound effect with a recording sink and compares ordered user-visible projections. Differences must be classified as equivalent formatting, an intended change, a Python reader defect, a Wakterm Agent API defect, or indeterminate correlation.

Shadow output is discovery evidence. It cannot authorize a production provider cutover. Promotion requires a separate live gate with one active delivery owner, a persisted cutover watermark, no output gap, and no dual delivery.

`tests/run_codex_shadow.py` performs the Codex comparison without submitting a
prompt or invoking a messaging adapter. Capture both tail baselines while the
target is idle:

```sh
uv run --script tests/run_codex_shadow.py baseline wakterm_codex \
  --cwd /code/wakterm --state /tmp/panetone-codex-shadow.json
```

After a separately submitted target turn is complete, compare everything since
that baseline:

```sh
uv run --script tests/run_codex_shadow.py compare \
  --state /tmp/panetone-codex-shadow.json
```

The state file is created with mode `0600` and cannot be overwritten. A source
replacement, invalid cursor, session change, unknown event kind, or agent change
returns `indeterminate` with classification `correlation_gap`. Unequal ordered
message projections return `difference` with classification `unexplained`.
Only an exact ordered match satisfies the Phase 1 discovery check.
