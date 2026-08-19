# ADR 0002: Route identity and resolution

Status: accepted for Phase 1

## Context

Operators address agents by a readable title, while pane IDs, tab IDs, processes, sessions, and provider turns can all change across restarts. Treating any ephemeral identifier as durable route identity risks delivering to a stale or unrelated agent.

## Decision

The CLI continues to locate a route by exact case-insensitive title. Resolution must return not found when no live route matches and ambiguous when more than one live route matches. No side effect may occur until resolution succeeds.

The long-term store assigns every Panetone route a stable UUID. A route records its current display title, messaging-channel bindings, and current opaque Wakterm agent and process-incarnation binding. Pane, tab, process, provider-session, and provider-turn identifiers are observations attached to a binding, not durable route identity.

A live refresh may change an ephemeral binding but cannot silently merge two durable routes. A missing agent makes the route unavailable without deleting its durable channel bindings. Reappearing agents are rebound only through an unambiguous reconciliation rule.

Control requests persist both the user-supplied source and target locators and the resolved route identities used for delivery. Later retries and callbacks use the persisted route identity and explicitly re-resolve its live binding. They do not reinterpret the original title as a different route.

## Consequences

The current Python implementation has title-derived runtime routes and does not yet persist route UUIDs. This is a documented intended difference for the Rust store migration, not a requirement to retrofit the temporary Python state.

Golden cases must cover case-insensitive exact matching, missing routes, duplicate titles, disappearing agents, incarnation changes, and preserved channel bindings.
