# Live Ergo verification

A live run supplements the automated suite by checking one built binary
against a real Ergo deployment over both MCP transports. Because it depends on
server policy, timing, networking, and history state, it is opt-in and does not
replace deterministic tests.

## Safety and scope

Use a private test server or channels explicitly intended for testing. A live
run creates guest connections, channel messages, retained history, CTCP DCC
offers, direct TCP listeners, and temporary files on the gateway host. Never
point it at a public network or production channel without operator approval.

Record the server software, advertised capabilities and ISUPPORT tokens,
transport endpoints, UTC time, build revision, and any server-policy-dependent
skips.

Use two unique mythological nicknames, one longer than nine bytes but no longer
than the server's advertised `NICKLEN`. Read and follow the returned MOTD and
the topic of each joined channel before sending test traffic. Keep temporary
file sources and destinations outside the repository.

## Prerequisites

1. Build and gate the exact tree under test:

   ```text
   cargo fmt --all -- --check
   cargo build --locked
   cargo test --all-targets --all-features --locked
   cargo clippy --all-targets --all-features --locked -- -D warnings
   RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --document-private-items --locked
   cargo deny check
   cargo audit
   ```

   `cargo-deny` and `cargo-audit` require their separately installed cargo
   subcommands; record an environment skip rather than silently omitting them.

2. Point a copy of `config/example.toml` at an existing test Ergo server. Keep
   credentials in environment-variable references as described in
   [CONFIGURATION.md](CONFIGURATION.md); do not put secrets in the repository,
   command history, evidence, or MCP calls.
3. Choose a dedicated channel and an unused HTTP listen address. Confirm that
   the configured DCC advertised address is reachable between the two gateway
   identities and that the configured port range is free.
4. Use an MCP client that can display both text and `structuredContent`, issue
   concurrent calls, preserve event cursors, and close stdio cleanly. The two
   transports expose the same service; they are not separate APIs. Record
   whether the client exposes MCP resource subscriptions. If it does not, keep
   an `irc.events.read` long poll active throughout the run.

## Start both transports

Run the built binary, not `cargo run`, so both processes exercise one immutable
artifact:

```text
./target/debug/irc-mcp serve --transport stdio --config /path/to/live.toml

./target/debug/irc-mcp serve --transport http \
  --listen 127.0.0.1:8181 --config /path/to/live.toml
```

Stdio keeps a conventional client connection. HTTP has no initialize
handshake: a client discovers the server with `server/discover` at the
Streamable HTTP endpoint `/mcp`, and every request declares its own protocol
version, `clientInfo`, and `clientCapabilities` in `_meta`. Assert that both
transports report the same server identity, capabilities, and three-step
onboarding instructions; `tools/list`, `resources/list`, and
`resources/templates/list` must agree.

## Required operation matrix

Perform each row through the public MCP surface. Save concise structured
results and identifiers needed by later rows; full MOTDs, message bodies, IP
addresses, local paths, or credentials do not belong in committed evidence.

| Area | Procedure | Required assertion |
| --- | --- | --- |
| Registration | Call `irc.connect` once through stdio and once through HTTP with distinct names; make at least one requested name longer than nine bytes. | Each call returns only after registration and complete MOTD collection. Default compact results keep the MOTD instruction text visible without inline line/raw duplication. The gateway does not shorten a name before ISUPPORT exists; when the eventual `NICKLEN` confirms the name is valid, it remains unchanged, `nickname_adjusted` is false, and handles differ. |
| Discovery | Read protocol, status, MOTD, state, events, and DCC resources plus one channel resource. Query every joined channel's topic. | Resources belong to the requested handle; exact capabilities and ISUPPORT are present; structured resource URIs resolve; the MOTD resource contains complete lines and wire replies even though default status is compact; topics complete without reply crossover. |
| Messaging | Join the dedicated channel with both identities. Send one channel message and a private message in each direction. | Sends are acknowledged when server features permit; echo, `msgid`, `server-time`, direction, target, and semantic class remain observable. |
| Watches and cursors | Create a target/class or mention watch, subscribe to its structured resource URI, and send matching and non-matching peer traffic. Read the watch resource twice, then read its events twice from one cursor through both `irc.events.read` with `watch_id` and the positioned window URI, then close it. Separately hold an `irc.events.read` long poll from a known cursor and exercise a filtered non-match before a match. | Only matching traffic wakes the watch; repeated descriptor reads change nothing; both event paths return the same window for the same cursor and hand back a `next_cursor` covering only what they returned; gap/reset status is explicit; the closed watch becomes invalid. The long-poll fallback wakes and advances past inspected non-matches without blocking IRC input. |
| Result envelope | Fetch `tools/list` and validate the `structuredContent` of one successful call, one gateway failure (a stale `agent_id`), and one rejected IRC command (a `+k` join without a key) against the `outputSchema` each tool advertises, using any JSON Schema 2020-12 validator. | Every instance validates. Success carries `ok: true` with the tool's output under `result`; both failures carry `ok: false` with `kind`, `message`, and `retriable` under `error`, and the rejected join also carries its 475 under `error.command_result`. `isError` agrees with `ok` in every case. |
| Activity hints | With hints on and a watch registered, have the other identity send two channel messages and one private message. Call `irc.status` twice with no traffic in between, then `irc.send` twice. Read events once without `set_activity_anchor` and once with it. Repeat one call with `activity: {"enabled": false}` on a second agent and once with `[mcp] activity_hints = false`. | The two `irc.status` results carry identical `activity` counts; the sends do not increase them and do not move `anchor`. The plain read leaves the counts unchanged; the anchoring read drops them to zero and moves `anchor` to that read's `next_cursor`, while returning the same events. An attention check with the same flag likewise records its `resume_cursor`; neither action consumes a watch window. The opted-out agent and the disabled deployment carry no `activity` at all. |
| Model attention | Call `irc.attention.open` with one task channel. Merge `filterAddition` with status/MOTD/protocol/state/DCC and other client requirements under `params.notifications` in one `subscriptions/listen` request. Send an unmentioned task-channel message, an account-tagged human message in `#control`, a nickname mention, and unrelated guest traffic. Run `irc.attention.check` from `initial_cursor` with `set_activity_anchor: true`, then repeat it after only unrelated traffic. Exercise a stale cursor and a gateway restart. Separately verify that an adapter-owned conversation can consume the schedule recipe, and that a client with neither delivery mechanism closes its watch and disconnects before yielding. | The listen stream receives matching watch updates plus requested lifecycle resource updates; no separate stream is opened per watch. The three relevant messages arrive once with `source_account` preserved, unrelated traffic does not. A drained quiet check omits both `events` and the redundant `activity` hint, advances `resume_cursor` to the journal head, and leaves the next ordinary tool hint quiet; gap/reset remains explicit. Adapter checks use `wait_ms: 0` and no interval longer than `intervalSeconds: 60`; notification mode invokes no model while quiet, and foreground-only mode never claims standby. |
| Subscription/MRTR boundary | While the consolidated listen stream is open, trigger resource and list changes with no tool request active. Separately exercise `irc.join` without a needed channel key and `irc.dcc.accept` without a required receive-root choice so each returns MRTR `input_required`; retry each call with its answer, and declare tasks on the DCC call. | Async changes arrive only as notifications inside the client-opened `subscriptions/listen` stream. Each MRTR question remains scoped to retrying its active operation; the DCC task is created only after input is settled. Neither MRTR nor an activity hint acts as an IRC event channel or idle-model wake-up. |
| Correlation | Overlap at least two `TOPIC` calls and concurrent `WHOIS`, `NAMES`, and history operations. | Every `command_id`/label owns only its replies, including labeled batches that finish out of order. |
| History | Send a unique marker that the actor observes live, then call `irc.history` latest twice for that channel with default compact detail; repeat once with `result_detail: "full"`. | Every explicit call returns the marker in typed `events` with `origin: history`, even though its `msgid` was seen live and earlier. Compact results do not repeat successful reply/projection arrays; the full result's raw replies and typed projection agree. |
| Modern messages | When advertised, exercise reaction add/remove, redaction, read get/set, and typing states against messages carrying server IDs and time tags. Repeat one operation without its required capability or with a `CLIENTTAGDENY` test policy when practical. | Typed results and inbound semantic events preserve exact IDs/timestamps and lossless wire tags; typing is throttled per target; unsupported or blocked operations reject explicitly without a plain-text emulation. |
| DCC CHAT | Open an ordinary chat offer from one identity and accept it promptly from the other. Exchange lines in both directions, then cancel or disconnect. Also allow one offer to expire. | Both sessions become active; lines are ordered `dcc.chat.message` events; terminal state and timeout are exact; cancelling a terminal session describes its actual state. |
| DCC SEND/task | Create a small random or fixed file outside the tree, record size and SHA-256, and send or accept with `conflict: fail` from a client that declares `io.modelcontextprotocol/tasks` in its request capabilities. Follow the task with `tasks/get` from separate requests to terminal state; repeat with `tasks/cancel` if time permits. Also run one transfer from a client declaring no extension. | Declaring the extension yields `resultType: "task"`; not declaring it yields the immediate session result. Task status reports state/byte progress, `tasks/get` from a later request resolves the same id, cancellation reaches the session, and terminal output links the DCC-session resource. Sender and receiver report the same byte count and `completed`; hashes match; no file body appears in MCP results or event data. |
| Task ownership | With two bearer tokens configured, create a task as one owner, then call `tasks/get` and `tasks/cancel` for that id as the other. Restart the process and retry `tasks/get` as the original owner. | The second owner gets `-32602` `unknown task: {taskId}`, byte-identical to the answer for an invented id, and the owner's task is undisturbed. After the restart the original owner gets the same unknown-task error. |
| Progress | Call `irc.connect` and `irc.history` with a `_meta.progressToken`, then repeat each without one. | With a token, `notifications/progress` arrive on that request's own stream with strictly increasing `progress`, `total` 7 for connect and 3 for history, and distinct registered/autojoin stages; the result is last and nothing follows it. Without a token, no notifications at all. |
| DCC receive roots | Configure two `[[dcc.receive_roots]]`, then accept one offer naming a root and a nested relative destination, and one naming neither. Also try an absolute `destination_path` and a link planted at the destination name. | The named accept lands under that root and reports matching `receive_root`/`receive_path`; the unnamed accept returns `input_required` offering exactly the configured names (or a structured `receive_roots` error from a client without elicitation) and completes on the retry; the absolute path is refused; the planted link is never written through and the offer survives every refusal. |
| Input round trips | From a client declaring `elicitation`, connect with `nick_conflict_policy: "elicit"` against a nickname the other identity already holds and answer the question with a third name; join a `+k` channel without a `key`, answer it, then repeat with a deliberately wrong key; with `mcp.confirm_destructive = true`, kick and redact once confirming and once declining. Repeat one of each from a client declaring no elicitation. | Each question arrives as `resultType: "input_required"` under its documented key and the retry completes the original call — a fresh registration under the chosen nickname, a JOIN carrying the key, the kick and redaction applied. A declined answer, a wrong key, and an expired or edited `requestState` all leave nothing applied and return an in-band `isError` result. Without elicitation: `elicit` is refused as a validation error, the keyed join returns its ordinary 475 rejection, and the gated mutations are refused with `confirmation_required` rather than applied. |
| HTTP ownership | Start HTTP with two bearer tokens, create one agent/watch per owner, and attempt cross-owner list, read, subscribe, tool, and disconnect operations. Inspect response cache headers. | Each owner sees and operates only its own handles; an unowned handle is indistinguishable from a missing one; notifications do not cross owners; responses are `private, no-store`. |
| Lifecycle | Disconnect both handles, close stdio input, and stop HTTP gracefully. Inspect processes and server presence. | QUIT is attempted, DCC sessions close, handles become invalid, stdio exits on EOF, HTTP stops on cancellation, and no gateway or IRC identity is orphaned. |

If a human cannot accept a DCC offer within its configured deadline, treat that
as a successful timeout check and re-arm a fresh offer. Do not weaken production
timeouts merely to accommodate a manual run.

## Failure handling

Classify an unexpected result before changing code:

- malformed hand-authored JSON-RPC is a client/test-harness error;
- a server rejection is valid product output when it matches advertised policy;
- an external timeout or unreachable DCC address is an environment failure;
- mismatched structured/raw results, reply crossover, incorrect state, leaked
  handles/processes, or behavior that contradicts advertised server limits is a
  gateway defect.

For a gateway defect, preserve the smallest safe transcript, add deterministic
regression coverage, rerun the focused test, and repeat the affected live row.
Keep environment-specific results in the release record rather than committing
server names, channel names, addresses, paths, or message content here.
