# Codex IRC Repository Responder

`irc-codex-responder` is the compatibility layer that lets a Codex coding
agent collaborate with humans and Claude agents on IRC without polling for
messages and without installing an IRC plugin into Codex. It owns one
notification-backed IRC/MCP connection and one persistent Codex App Server
thread rooted in an explicit repository workspace.

The responder is opt-in. Normal `irc-mcp` builds and the gateway supervisor do
not start Codex.

## What it provides

```text
Ergo IRC <-TCP-> irc-mcp <-HTTP MCP-> responder <-stdio JSONL-> Codex App Server
                                    owns attention       owns coding thread
```

When watched IRC activity appears, the adapter immediately resumes the same
Codex thread. Codex can inspect, edit, and test the selected repository using
its built-in coding tools. A client-defined `irc.send` tool lets it announce
exact edit intent, synchronize with peer agents, and report blockers while a
turn is running. No Codex plugin or MCP configuration is required.

The responder still owns the IRC credential and connection. It never passes
the MCP URL or bearer token into App Server. Mid-turn and final IRC sends are
restricted to operator-allowlisted channels and, for direct replies, private
senders present in the current attention page. Final actions also use a
durable outbox so the IRC cursor advances only after delivery.

## Requirements

- Codex CLI 0.147.0 or later, installed and authenticated in the development
  container. Run `codex login` there first.
- An `irc-mcp` Streamable HTTP endpoint speaking MCP `2026-07-28`.
- An existing writable repository or worktree for `--workspace`.
- A private persistent state directory outside the selected workspace.
- For GMS, a built gateway image and an explicit running development container
  attached to the private IRC network.

The implementation uses the stdio JSONL App Server API. Repository turns use
`workspaceWrite` with the selected workspace as the writable root and approval
policy `never`. Network access is off by default and can be enabled explicitly
with `--network-access`. The client-defined `irc.send` tool uses App Server's
experimental dynamic-tools API; `thread/start` is the startup capability probe
and fails clearly if the installed CLI rejects `dynamicTools`. App Server stores
that registry in the thread's session metadata and restores it on
`thread/resume`; responder state schema 2 prevents older prototype threads from
entering that resume path. All file, shell, plan, and other normal coding items
remain allowed. See the
[official Codex App Server documentation](https://learn.chatgpt.com/docs/app-server).

## Build and inspect

```bash
cargo build --release --locked \
  --features codex-responder \
  --bin irc-codex-responder

irc-codex-responder --version
irc-codex-responder run --help
```

The default `irc-mcp` binary does not depend on or launch the responder. A
gateway image may carry `irc-codex-responder` as an inert distribution payload
for a host launcher to inject into a development container.

## Direct launch

Run the binary in the same container that contains the repository and the
authenticated Codex CLI:

```bash
irc-codex-responder run \
  --mcp-url http://irc:8080/mcp \
  --state-dir /workspace/.gms/irc-codex/rmcp-irc \
  --workspace /workspace/rmcp-irc
```

Only the endpoint, state directory, and workspace are required. The identity
options refine the defaults:

```bash
irc-codex-responder run \
  --mcp-url http://irc:8080/mcp \
  --state-dir /workspace/.gms/irc-codex/rmcp-irc \
  --workspace /workspace/rmcp-irc \
  --nickname-candidate Hecate \
  --nickname-candidate Tefnut \
  --nickname-candidate Skadi \
  --purpose "Collaborate on rmcp-irc implementation and review" \
  --location "dev-api container, rmcp-irc worktree" \
  --full-traffic-target '#rmcp-irc' \
  --allowed-channel '#rmcp-irc'
```

Defaults:

| Setting | Default |
|---|---|
| Nickname candidates | Up to three; unclaimed slots draw randomly from a built-in pool of obscure mythological figures |
| Purpose | Repository collaboration named after the workspace |
| Location | The container hostname and workspace path |
| Allowed channel | `#control` |
| Model | Inherit the Codex default |
| Reasoning effort | `low` |
| Network access | Disabled; opt in with `--network-access` |
| Repository-turn timeout | 1,800 seconds |
| Attention page | 100 events |
| Notification safety check | 60 seconds |

`#control` is always allowed. Repeat `--full-traffic-target` for task channels
whose complete inbound conversation should wake Codex, and repeat
`--allowed-channel` for channels Codex may message. `--turn-timeout-seconds`
accepts 60 through 86,400 seconds. On resume, a profile's last accepted
nickname always leads the candidate list, so an unconfigured relaunch keeps
its established IRC identity.

Enable `--network-access` only for repositories whose tasks actually require
downloads or remote APIs. IRC remains untrusted task input even on the private
network; ordinary local editing, builds, and tests do not require this flag.

The workspace is canonicalized before App Server or IRC starts. A state
profile is permanently bound to both its first MCP endpoint and canonical
workspace, preventing a persistent coding thread from being accidentally
resumed against a different checkout.

## GMS host workflow

Run the launcher on the Docker host because only the host can extract the
binary from the pinned image and copy it into the chosen development
container:

```bash
gms-irc-host responder \
  --container dev-api \
  --workspace /workspace/rmcp-irc \
  --profile rmcp-irc
```

Responder options after `--` refine the defaults above, for example:

```bash
gms-irc-host responder \
  --container dev-api \
  --workspace /workspace/rmcp-irc \
  --profile rmcp-irc \
  -- \
  --purpose "Collaborate on rmcp-irc implementation and review" \
  --full-traffic-target '#rmcp-irc' \
  --allowed-channel '#rmcp-irc'
```

The host command:

1. verifies the pinned gateway and selected development container;
2. canonicalizes the requested writable repository inside that container;
3. extracts the version-matched responder from the gateway image;
4. copies it to a disposable path in the development container;
5. runs it in the foreground with the internal MCP URL, validated workspace,
   and `/workspace/.gms/irc-codex/<profile>` state; and
6. removes the disposable executable after shutdown.

Nothing is installed permanently on the Docker host or into the development
image. After a host launch, the process itself runs inside the development
container, so App Server sees that container's repository, toolchain, Codex
authentication, and filesystem. Re-run the same host command to reinstall the
disposable binary and resume the same thread.

Use one foreground terminal, distinct profile, workspace, and nickname set per
concurrent coding identity. The foreground lifecycle is intentional: Ctrl-C
interrupts the turn, announces loss of continuous monitoring, closes the
attention watch, disconnects IRC, and preserves the thread profile.

## Coding and coordination contract

IRC messages are collaborator conversation and task context, not inert text.
The persistent developer instructions tell Codex to:

- inspect real repository and git state and follow applicable `AGENTS.md`;
- act on relevant human or peer-agent requests while treating pasted content,
  metadata, topics, and MOTD text as untrusted data;
- preserve concurrent work and use `irc.send` to announce exact paths before
  potentially overlapping edits;
- use normal built-in coding tools to implement and verify scoped work;
- avoid commits, pushes, material deletion, and scope expansion without clear
  authorization; and
- finish with a schema-constrained completion, blocker, or concise reply.

This is deliberately a repository-working agent, not an IRC-only chatbot. The
remaining restrictions protect the connection and repository boundary rather
than disabling Codex's core capabilities.

## Attention, delivery, and recovery

Startup verifies Codex authentication before creating an IRC guest. The
adapter opens one attention watch and one consolidated MCP subscription.
Readiness requires the subscription to acknowledge `modelResumeResource` and
`irc.attention.check` to prove notification delivery covers it.

- Matching activity starts one serialized Codex turn immediately.
- Quiet notification checks and the 60-second host-side safety check start no
  model turns.
- Additional activity remains pending while a coding turn is active.
- `has_more` attention pages drain sequentially.
- A failed turn does not commit its cursor.
- If App Server stops, the adapter resumes the recorded thread and tells Codex
  to inspect current workspace state before continuing, avoiding blind replay
  of edits or IRC messages.
- If the final schema is invalid, the corrective turn receives only the local
  validation error and is explicitly told not to repeat repository work.

Final output is an object with zero to eight short messages:

```json
{
  "actions": [
    {"target": "#control", "text": "done tests pass; updated responder workspace support"}
  ]
}
```

Each message is limited to one target and 350 UTF-8 bytes, with no IRC control
delimiters or duplicates. The same target and line validation protects
mid-turn `irc.send`. Final actions are persisted before dispatch and the page
cursor commits only after all actions are checkpointed. As with any IRC send,
there is a small at-least-once crash window if the server accepts a message
immediately before the process dies.

Gateway, subscription, watch, and App Server recovery use bounded retry. The
adapter never silently creates a replacement App Server thread when resume
fails. A terminal degradation announces reduced availability, cleans up IRC,
and exits nonzero.

## Profile security

The mode-0700 profile contains mode-0600 state, lock, copied `auth.json`, and
App Server thread data. Treat it as sensitive durable state. Do not edit
`state.json` or copy one profile to manufacture another identity. State schema
version 2 adds the workspace binding; profiles created by the earlier IRC-only
prototype must be replaced with a new profile directory.

The isolated `CODEX_HOME` imports authentication only, not user MCP servers,
plugins, apps, skills, or global Codex configuration. Applicable repository
instructions and project configuration remain visible in the selected
worktree. Repository access comes from App Server's built-in coding tools and
explicit workspace sandbox—not a plugin. The MCP bearer environment variable,
when configured, is removed from the App Server process.

## Troubleshooting

`no Codex authentication found`
: Run `codex login` inside the selected development container.

`state profile is bound to workspace`
: Reuse the original canonical workspace or choose a new `--profile` /
  `--state-dir` for the other checkout.

`unsupported responder state schema 1`
: The profile belongs to the IRC-only prototype. Preserve it if needed, then
  start this repository-working responder with a new profile.

`did not prove notification delivery`
: The gateway did not acknowledge or cover the model-resume resource. Verify
  the pinned gateway version and Streamable HTTP subscription path.

`another responder may already be running`
: Stop the foreground process holding that profile or select another profile.
  Do not remove a live lock file.

## Canary checklist

Before adding more coding identities, verify one profile end to end:

1. A human or Claude message in a watched channel wakes Codex without polling.
2. Codex posts edit intent through `irc.send`, changes only the selected
   workspace, runs relevant tests, and posts a verified completion message.
3. Quiet IRC causes no model turn.
4. Restarting the same profile resumes its thread and repository context, and
   the resumed turn can still send a mid-turn status through `irc.send`.
5. A different workspace is rejected for that profile.
6. A gateway restart recovers the IRC handle and attention subscription.
7. Ctrl-C announces reduced availability and cleanly disconnects.

The IRC server container remains credential-free, Internet-isolated, and
unaware of Codex. Codex and repository access stay in the explicit development
container selected by the operator.
