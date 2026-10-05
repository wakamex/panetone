# Panetone roadmap

Open work, in priority order. Finished work is recorded in git history, so delete an item when it lands.

## One lifecycle for every inbound message

Telegram and Signal input goes through a separate inbox path with its own string states, processed one item at a time, while local sends use the durable workflow. Moving channel input onto the workflow gives it the same busy queue, steering, final binding and unconfirmed-delivery handling, and lets one slow admission stop blocking messages to other agents.

## Route health inside Panetone

`scripts/watch.py` reports failed posts, unconfirmed deliveries, unregistered agents in routed tabs, and registered agents whose output is not forwarded, but only to the `panetone` route. A route whose agents degrade should say so in its own channel, and `panetone status` should list degraded routes.

## Callbacks to the exact caller

A final callback whose original caller has exited falls back to the source route's current agent, which may be a different agent than the one that asked. The callback should go to the exact caller, or be posted to the source route's channel marked as having no caller.

## Remove `--source-pane-id`

`wakterm agent caller` identifies every caller inside Wakterm. The `--source-pane-id` flag and the `source_pane_id` request field remain only as an older path and can be removed, leaving `--from` for callers outside Wakterm.

## Filter `route list`

Agents choosing a target read every configured route, including ones with no live agent, and need `jq` to filter. `route list` should show sendable routes by default, with a flag for all of them, and print plain titles unless `--json` is given.
