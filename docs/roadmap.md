# Panetone roadmap

Open work, in priority order. Finished work is recorded in git history, so delete an item when it lands.

## Replies to the exact caller

A reply whose original caller has exited goes to the source route's current agent, which may be a different agent than the one that asked. It should go to the exact caller, or be posted to the source route's channel marked as having no caller.

## Remove Wakterm return correlation

Requests that asked Wakterm to correlate them with a provider turn still finish through the terminal stream, return deliveries and the `return-terminals` and `pending-returns` workers. Once `panetone status` shows no unresolved returns, remove that path from Panetone and tell Wakterm its return-request support has no caller.

## Remove `--source-pane-id`

`wakterm agent caller` identifies every caller inside Wakterm. The `--source-pane-id` flag and the `source_pane_id` request field remain only as an older path and can be removed, leaving `--as` for callers outside Wakterm.

## Filter `route list`

Agents choosing a target read every configured route, including ones with no live agent, and need `jq` to filter. `route list` should show sendable routes by default, with a flag for all of them, and print plain titles unless `--json` is given.

## Deferred: concurrent inbound delivery

Telegram and Signal input is admitted one message at a time, and an admission can wait about 18 seconds for Wakterm to confirm the turn, so a slow target can delay messages to other agents. Trigger: a channel message visibly delayed behind another agent's delivery. Fix: admit inbound messages for different agents concurrently while keeping each agent's messages in order. Moving channel input onto the durable workflow is justified only if inbox-specific bugs keep recurring.
