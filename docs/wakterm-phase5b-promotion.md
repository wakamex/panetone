# Wakterm Phase 5B preflight and production promotion

This checklist separates disposable compatibility evidence from the later
production mux restart. The development preflight is safe to run at any time.
The production checklist is a labeled maintenance operation that interrupts
every PTY-backed agent harness.

## Disposable preflight

Run from the Panetone development checkout:

```sh
dev/phase5b-wakterm-preflight
```

The script requires exact Wakterm commit
`5486543a664135807e97051bbd90e8573be369cb`. It includes the rebased Agent API
and provider event series, the notification-backlog disconnect fix, and the
tab-focus suppression fix. The script builds the debug Wakterm CLI and mux from
a disposable detached worktree, so a newer or locally dirty main
checkout cannot change the candidate. It starts a fresh mux with private runtime
and state roots, verifies the v1 capability, catalog, and event-page contracts,
and runs Panetone's real CLI adapter test. It saves mode-`0600` JSON evidence
below `.dev/evidence/`, then removes the worktree and disposable mux state.
Third-party build artifacts are reused from the ignored private
`.dev/wakterm-target/` cache, but mux state and observer cursors are never
reused.

Pass an explicit evidence path when a promotion record should live elsewhere:

```sh
dev/phase5b-wakterm-preflight /private/evidence/wakterm-preflight.json
```

This run does not read production Wakterm configuration, connect to the
production socket, launch an agent, submit a prompt, install a binary, call
systemd, or restart a service. The already completed disposable live-provider
turns are separate producer evidence. The preflight verifies Panetone against
the unchanged normalized contract.

## Current production limitation

The active Wakterm mux is a user service running
`~/.local/bin/wakterm-mux-server`. `/code/wakterm/deploy.sh` builds and copies
user binaries, and `--restart` restarts that user mux. It saves terminal layout,
but it does not restore agent harness processes.

Wakterm's lifecycle documentation explicitly says reliable automatic
restoration of adopted PTY harnesses is not implemented yet. A restored shell
pane is not a restored Codex, Claude, Gemini, or OpenCode session.

The target Rust Panetone unit instead requires a system Wakterm service, a
system-labelled `/usr/local/bin/wakterm`, and `/run/wakterm/sock`. The
development launcher cannot satisfy that gate. Wakterm's `install.sh --system`
installs binaries but does not by itself create the required system unit or
restore agents. Do not combine an improvised manager migration with the
Panetone cutover.

## Production deployment readiness

Before authorizing a Wakterm production restart:

1. Save the passing preflight evidence and verify its Wakterm commit, binary
   hashes, capability set, Panetone commit, and `production_effects: false`.
2. Choose and review the Wakterm-owned system-service installation procedure.
   It must install exact release binaries, use `/run/wakterm/sock`, run as
   `mihai`, preserve restrictive state permissions, and provide binary and unit
   rollback. If that procedure does not exist, production deployment is
   blocked.
3. Inventory every live agent route from the production pane list and Agent API
   catalog. For each agent, record the route, harness, working directory,
   provider session identity, and an already tested exact resume command. Do
   not record tokens or inherited environment secrets.
4. Bring every agent to an observed idle boundary. Ask it to checkpoint work
   and confirm that resuming the recorded provider session is safe.
5. Save the Wakterm layout and record the active unit, PID, start timestamp,
   socket inode, installed versions and hashes, source commit, catalog, event
   head, and effective configuration without secret values.
6. Back up the installed Wakterm binaries, unit, environment, layout, session
   state, and durable Agent API store. Record hashes and test that the rollback
   binaries start against a copied state directory.
7. Stop Panetone before replacing the installed Wakterm CLI. A new v62 CLI
   talking to the old mux, or the old CLI talking to the new mux, is not an
   allowed mixed-codec operating state.

## Labeled production restart

During the maintenance window:

1. Confirm the preflight evidence still names the exact candidate and that no
   source or release artifact changed afterward.
2. Stop the current Panetone service and verify that no control request or
   channel worker remains active.
3. Stop the user Wakterm service once. Install the reviewed system binaries and
   unit without starting a second mux. Disable the user unit so both managers
   cannot own a mux simultaneously.
4. Start the system Wakterm service and verify exactly one mux process and one
   `/run/wakterm/sock` inode.
5. Reconnect and restore the terminal layout. Manually run each recorded exact
   provider resume command in its intended pane and working directory.
6. For every route, wait for a fresh catalog entry with a live process
   incarnation and confirm that the expected provider session was resumed.
   A shell pane, a renamed pane, or a newly created provider session is not a
   successful restoration.
7. On a fresh connection, verify v62, `catalog.v1`, `prompt_admission.v1`,
   `return_request_terminal_stream.v1`, and `event_stream.v1`. Read an event
   page from the fresh catalog lower bound and record the response.
8. Start the existing Python Panetone service only after every required agent
   is restored, then verify its capability negotiation and ordinary one-way
   routing. This Wakterm maintenance must finish before the separate Rust
   Panetone cutover begins.

## Rollback

If the system mux, codec, event store, socket, or any required agent session
cannot be restored, keep Panetone stopped. Stop the candidate mux, restore the
recorded binaries, unit, configuration, and state, start exactly one previous
mux, and manually restore every agent again. Verify the old catalog and socket
before restarting Panetone.

Do not claim rollback success because terminal tabs reappeared. Every required
provider session and Panetone route must be directly verified.
