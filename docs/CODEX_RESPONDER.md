# Codex IRC Responder

`irc-codex-responder` is an optional, foreground adapter that gives one IRC
identity continuous model attention through a dedicated Codex App Server
thread. It is an IRC coordination identity, not a repository coding agent.
It cannot attach to or adopt a ChatGPT, Codex UI, or other pre-existing thread.

The responder is disabled in normal `rmcp-irc` builds. The gateway continues
to run independently and never starts Codex.

## Boundary and data flow

```text
Ergo IRC <-TCP-> irc-mcp <-HTTP MCP-> irc-codex-responder <-stdio JSONL-> Codex App Server
                                  owns IRC        owns one private thread
```

The responder owns both connections. It calls the IRC tools, opens and drains
the attention watch, validates model output, sends accepted messages, and
persists delivery state. Codex is never given the MCP URL, an MCP bearer token,
an IRC connection, or callable tools.

Codex receives explicitly untrusted observations: the MOTD, topics, recent
history, and one attention page. Its only accepted result is a structured
object containing zero to eight short `PRIVMSG` actions. The responder rejects
other targets, operations, control characters, duplicate messages, text over
350 UTF-8 bytes, and private replies to anyone who did not privately message
the responder in that event batch.

## Requirements

- Codex CLI 0.147.0 or later, installed and authenticated where the responder
  runs. Run `codex login` there first.
- An `irc-mcp` Streamable HTTP endpoint speaking MCP `2026-07-28`.
- Exactly three distinct nickname candidates.
- A private, persistent state directory.
- For GMS, a built gateway image and an explicit running development container
  attached to the internal IRC network.

The adapter uses the stable stdio JSONL App Server API only. It does not enable
`experimentalApi` or use the WebSocket transport. The relevant App Server
contract is documented in the
[official Codex App Server documentation](https://learn.chatgpt.com/docs/app-server).

## Build and inspect

Build only the opt-in binary:

```bash
cargo build --release --locked \
  --features codex-responder \
  --bin irc-codex-responder
```

Inspecting its version or help does not start App Server or connect to IRC:

```bash
irc-codex-responder --version
irc-codex-responder run --help
```

The default `irc-mcp` binary does not require or launch the responder. A
gateway package may carry the responder as an inert distribution file while
its supervisor continues to start only Ergo and `irc-mcp`.

## Direct launch

```bash
irc-codex-responder run \
  --mcp-url http://irc:8080/mcp \
  --state-dir /workspace/.gms/irc-codex/api-review \
  --nickname-candidate Hecate \
  --nickname-candidate Tefnut \
  --nickname-candidate Skadi \
  --purpose "Coordinate API work and answer human mentions" \
  --location "development container api, synemantic worktree"
```

Important defaults:

| Setting | Default |
|---|---|
| Allowed channel | `#control` |
| Model | Omitted; inherit the Codex default |
| Reasoning effort | `low` |
| Attention page | 100 events |
| Safety check | 60 seconds |
| Model-turn timeout | 5 minutes |

Use `--full-traffic-target TARGET` repeatedly for task channels whose complete
traffic should wake the responder. Use `--allowed-channel TARGET` repeatedly
to expand the channel send allowlist. `#control` is always included. A generic
authenticated deployment may name an environment variable with
`--bearer-token-env`; its value is neither persisted nor passed to Codex.

Do not put secrets in `--purpose`, `--location`, channel topics, or IRC
messages. Those values become model input and thread history.

## GMS: install-on-launch workflow

Run the host-owned launcher on the Docker host, not inside an ordinary
development container:

```bash
gms-irc-host responder --container dev-api -- \
  --nickname-candidate Hecate \
  --nickname-candidate Tefnut \
  --nickname-candidate Skadi \
  --purpose "Coordinate API work and answer human mentions" \
  --location "dev-api, synemantic API worktree"
```

`--container` is always explicit. `--profile` defaults to the container name;
specify it when a container hosts more than one identity or when a stable
logical name is clearer:

```bash
gms-irc-host responder --container dev-api --profile api-review -- ...
```

Each invocation performs the same idempotent installation:

1. Verify the configured gateway image is present and running.
2. Verify the selected development container is running, has a writable
   `/workspace`, and is attached to the IRC network.
3. Create a stopped temporary container from the pinned gateway image and
   extract `/usr/local/bin/irc-codex-responder`.
4. Copy the binary to a unique temporary path in the development container.
5. Verify `--version`, then run it in the foreground with
   `http://irc:8080/mcp` and the selected profile.
6. Remove the temporary executable and host copy when the session ends.

The executable is therefore reinstalled on every launch. Recreating a
development container requires no special repair step: run the same command
again. To run several responders, invoke the command once per container in a
separate foreground terminal, with distinct profiles and nickname candidates.

The executable is disposable. Only this directory is intended to persist:

```text
/workspace/.gms/irc-codex/<profile>/
├── responder.lock
├── state.json
├── codex-home/
│   ├── auth.json
│   └── ... App Server thread data ...
```

Each process also creates an empty mode-0700 working directory beneath the
system temporary directory after verifying that none of its ancestors is a Git
repository. It is removed on clean exit. It deliberately does not live beneath
`/workspace`, because a seemingly empty child directory would still inherit an
ancestor repository boundary.

The host launcher reserves `--mcp-url` and `--state-dir` so a GMS invocation
cannot accidentally leave the internal gateway or mix profiles. Run
`gms-irc-host responder --help` for the complete host-side reference.

## Profile identity and sensitivity

A new state directory creates exactly one non-ephemeral App Server thread with
service name `rmcp_irc_responder`. The thread ID is recorded by the adapter;
there is deliberately no CLI option for supplying one. Every later launch
resumes that exact recorded thread. If resume fails, the responder exits rather
than silently creating a replacement.

The state directory is mode 0700, and its state, lock, and isolated
authentication files are mode 0600. State writes are atomic and fsynced. An
exclusive process-lifetime lock prevents two responders from using the same
profile concurrently. A profile is permanently bound to its first MCP endpoint.

Treat the entire profile as sensitive durable data. It contains a copied Codex
authentication file, model thread history, IRC identity handles, cursors, and
possibly a pending reply outbox. Back it up and move it only as one private
unit. Do not edit `state.json` by hand and do not copy one profile to create a
second identity.

The isolated `CODEX_HOME` is seeded only from `auth.json` in the authenticated
development container. User MCP servers, apps, plugins, skills, project
configuration, and general Codex configuration are not imported.

## Attention and turn lifecycle

On startup the adapter verifies Codex authentication before connecting to IRC.
It then connects using the three nickname candidates, opens one compound
attention watch, and opens the exact consolidated subscription filter returned
by the gateway. Readiness requires both:

1. the subscription acknowledgement covers `modelResumeResource`; and
2. `irc.attention.check.delivery.mode` is `notification` and explicitly covers
   that resource.

The first turn must send a `hello` to `#control`. After bootstrap:

- A matching resource notification triggers an immediate attention check.
- A 60-second host-side safety check detects missed notifications.
- A quiet check advances the quiet cursor but starts no Codex turn.
- One non-quiet attention page produces at most one serialized Codex turn.
- `has_more` pages drain sequentially.
- Notifications that arrive during a turn remain pending and are checked after
  the serialized turn completes.

After a valid structured response, the responder first persists a pending
outbox. It sends actions sequentially, checkpoints each accepted send, and
commits the page's `resume_cursor` only after the entire outbox completes. A
failed or rejected turn retains the cursor for retry.

There is one unavoidable at-least-once crash window: an IRC message may be
duplicated if the gateway accepts `irc.send` and the process dies before the
next outbox checkpoint reaches disk.

## Isolation and fail-closed behavior

App Server runs in an empty non-repository directory with:

- read-only sandboxing and no network access;
- approval policy `never`;
- shell and unified execution disabled;
- apps, plugins, multi-agent, goals, hooks, memories, Code Mode, image tools,
  computer/browser tools, and web search disabled;
- no configured MCP servers; and
- persistent developer instructions limiting the thread to IRC coordination.

The JSONL client treats App Server commands, file changes, tool calls, plan
items, permission/approval/input requests, elicitation, hooks, or process and
filesystem activity as policy violations. It interrupts the active turn and
sends no reply for that attempt.

All model output is parsed again outside Codex. The adapter accepts only:

```json
{
  "actions": [
    {"target": "#control", "text": "one short IRC line"}
  ]
}
```

An invalid response or policy violation gets one corrective retry containing
only the local validation failure. A second failure emits a deterministic
availability warning, closes attention, disconnects cleanly, and exits
nonzero. Only validated `PRIVMSG` actions are sent, with multiline handling set
to `reject_if_too_long` and compact tool results.

## Shutdown and recovery

Ctrl-C and SIGTERM interrupt an active model turn, announce that continuous
monitoring is ending, close the attention watch, disconnect IRC, stop App
Server, and retain the adapter-owned thread profile. The next invocation
reinstalls the executable and resumes that thread.

Dropped subscriptions, expired IRC handles, and expired attention watches are
reopened with bounded exponential backoff. A crashed App Server is restarted
and the recorded thread is resumed. No recovery path creates a replacement
thread. If App Server authentication or required attention delivery cannot be
recovered within 60 seconds, the responder announces loss of availability,
cleans up IRC, and exits nonzero.

## Cost model

There is one model turn for bootstrap and one for each non-quiet attention
page. Quiet IRC causes no model turns: notifications and the 60-second safety
check are handled by the adapter itself. A rejected structured response can
cost one additional corrective turn. The model also receives current MOTD,
topics, recent history, and the attention page, so busy channels and large
histories increase input-token cost.

Use narrow `--full-traffic-target` selections and allowlists. The responder is
not a general always-on coding agent and should not be used as one.

## Troubleshooting

`no Codex authentication found`
: Run `codex login` in the selected development container. The IRC container's
  lack of credentials is intentional.

`Codex CLI ... is too old`
: Upgrade Codex in the development container to 0.147.0 or later. An
  incompatible stable response shape also produces a compatibility error.

`another responder may already be running`
: The profile lock is held. Stop the existing foreground process or choose a
  distinct `--profile`; do not remove a live lock file.

`state profile is bound to MCP endpoint`
: Use the original endpoint or a new state directory. Endpoint rebinding is
  rejected to prevent accidental thread/cursor adoption.

`did not prove notification delivery`
: The gateway did not acknowledge the required model-resume resource or did
  not report notification-backed delivery. Verify the gateway version and its
  Streamable HTTP subscription path.

Responder exits after two validation failures
: Inspect stderr and IRC history. The fail-closed exit is intentional; fix the
  model/configuration issue and restart the same profile.

## Canary and rollout

Land and deploy `rmcp-irc` before changing GMS's `irc.mcpCommit`. Then rebuild
the gateway image and canary one foreground development-container profile.
Verify:

1. `irc-codex-responder --version` runs without starting App Server.
2. A human mention receives one short reply in the originating target.
3. Quiet IRC produces no model turns.
4. Restarting the same profile resumes the recorded thread.
5. A gateway restart recovers the IRC handle and attention subscription.
6. Forbidden tool or file activity is rejected without an IRC send.
7. Ctrl-C announces reduced availability, closes the watch, and disconnects.

Only after the canary should additional development containers be started.
The GMS IRC container remains credential-free, Internet-isolated, and unaware
of Codex throughout this rollout.
