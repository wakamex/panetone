# ADR 0002: Route identity and resolution

Status: accepted and implemented

## Context

The user addresses a workspace by the effective title Wakterm derives from its
current tab and agent working directories. Tabs, panes, agents, and processes
may all disappear during a no-save deployment and be recreated later in a
different layout.

## Decision

The durable Panetone route is its UUID, workspace title, and messaging-channel
bindings. It does not persist a Wakterm agent, incarnation, pane, tab, or
availability status.

Before each admission Panetone reads Wakterm's current effective titles and catalog. An exact case-insensitive title identifies one workspace route, and every live agent pane with that title belongs to it regardless of its physical tab. Panetone then submits through the selected pane's current agent and incarnation pair. That pair protects the individual admission from a process replacement between lookup and submission, but it is not route state.

One route may contain multiple live agent panes across one or more tabs. A channel reply prefers the agent that produced the quoted message. Otherwise Panetone prefers the last agent that produced visible output for the route, then the lowest live pane ID. This extends the normal multi-pane routing rule without treating a workspace as one agent.

Zero matching agent panes leave the work pending or return unavailable. Busy retries and final callbacks resolve the workspace again. Workflow records retain the exact pair used for an attempted admission so receipts and terminal results can still be validated.

## Consequences

Closing a tab and later reopening the same workspace requires no Panetone
reconciliation. Schema version 7 removes old route `agent` and `status` fields.
Golden cases cover effective-title lookup, multiple panes and tabs, preferred replies, missing routes, exact admission identity, and preserved channel bindings.

The supported `route.ensure` control method establishes a fresh route when Wakterm currently exposes at least one agent with the matching effective title. It creates or accepts a Telegram topic binding, persists only the stable route data, and returns the current live agents plus Panetone's event-cursor baseline. Repeating the method inspects the same binding without creating another topic.

Panetone also establishes missing Telegram routes automatically for live agent titles at startup and on Wakterm lifecycle or visible-output events. This restores manual shell launches without the Python bridge's periodic full catalog scan. `route.ensure` remains the synchronous launcher handshake.
