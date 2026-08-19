# Phase 1 Codex shadow report

Date: 2026-08-16

Label: discovery only

This run compared one completed Codex turn through Panetone's legacy JSONL
reader and Wakterm's experimental normalized output page. The comparison path
was recording-only. It did not call Telegram, another messaging adapter, or the
Panetone control socket.

## Environment

- Installed Wakterm: `20260816-223515-fbf1d441`
- Target: `wakterm_codex`
- Target state before baseline: idle
- Target state before comparison: idle
- Wakterm output schema: `wakterm.agent-output-shadow.experimental.v1`
- Panetone comparison schema: `panetone.codex-shadow.v1`

The stimulus was submitted separately through the existing Wakterm agent
interface after both readers established a tail baseline. It requested one
short, no-tool, two-line response containing ASCII, accented Unicode, Greek,
and a check mark. Wakterm returned an observer-backed submission
acknowledgement before the turn ran.

## Result

- Status: `match`
- Classification: `equivalent`
- Legacy complete records examined: 9
- Legacy visible assistant messages: 1
- Wakterm assistant events: 1
- Ordered text differences: 0
- Unexplained differences: 0
- Wakterm replay identity: stable
- Cursor or session gaps: 0

Both projections returned exactly:

```text
PANETONE_SHADOW_α
second line: café ✓
```

The permanent runner also tests empty intermediate pages, stable replay,
unexplained text differences, explicit Wakterm gaps, legacy source replacement,
and private non-overwriting state files.

## Scope

This validates the Codex observation boundary for a controlled turn. It is not
production promotion evidence and does not authorize a provider cutover. The
experimental Wakterm page still lacks the durable Agent API contract required
for production consumption, including durable sequence retention, capability
negotiation, lifecycle ordering, classified prompt admission, and an explicit
`cursor_too_old` result.
