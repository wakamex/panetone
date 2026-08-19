# ADR 0001: Wakterm and Panetone authority boundaries

Status: accepted for Phase 1

## Context

Both services currently discover provider sessions and interpret provider output. That duplication has already allowed stale-session selection and gives two components conflicting answers about terminal truth.

## Decision

Wakterm is authoritative for live panes, agent and process incarnations, provider session identity, prompt acceptance, provider turn boundaries, normalized agent events, and durable event ordering.

Panetone is authoritative for user-facing routes and aliases, messaging-channel bindings, authorization, control-request idempotency, audit-before-prompt policy, workflow state, notification delivery, and its consumed Wakterm cursor.

The shared boundary is a versioned Wakterm Agent API. Its public identifiers are opaque agent, process-incarnation, provider-turn, request, and event identifiers. Provider paths, file cursors, parsers, TTY matching, and pane implementation details remain private to Wakterm.

The API must provide:

- capability and schema negotiation
- a stable agent catalog with lifecycle and incarnation state
- prompt submission with a durable receipt
- normalized assistant-message, plan, turn-transition, final, observer-error, and lifecycle events
- monotonic durable sequence numbers and resumable reads
- explicit retention gaps through `cursor_too_old`
- classified compatibility and observer errors

Panetone initially consumes the CLI or wire protocol. It must not depend on the existing broad `wakterm-client` crate.

## Consequences

No shared provider-parsing library will be created. Wakterm owns the wire schema, golden wire fixtures, and authoritative implementation. Panetone owns consumer requirements, shadow comparison, cutover watermarks, and removal of its legacy readers.

A small Wakterm-owned Rust DTO crate may be introduced only after two Rust consumers duplicate meaningful compatibility code. It may contain DTOs and compatibility rules, but no mux, terminal, provider, persistence, executor, or Panetone policy code.

Existing Python provider readers remain temporary compatibility code. Rust Panetone will not reimplement them.
