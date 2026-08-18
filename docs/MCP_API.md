# MCP API contract

This document is the normative MCP contract implemented by `rmcp-irc`. The
runtime-generated JSON Schemas are the exact machine-readable form; this file
explains their semantics and stable invariants.

The stdio and Streamable HTTP transports expose the same service, tools,
resources, schemas, event model, and error semantics. An MCP transport
connection is never an IRC identity.

## Protocol revision

This service implements MCP `2026-07-28` exclusively. Identity and capabilities
are read per request from `_meta`; no session is minted or read. A request must
carry the revision's complete metadata and headers, and any other revision is
refused with `-32022`.

Clients receive the full surface: tools, resources, prompts, completions,
progress notifications, tasks, input round trips, and asynchronous
`subscriptions/listen` notifications according to the capabilities each request
declares. Stable follow-up resource URIs are present in `structuredContent`.
Native `resource_link` content blocks are omitted because current model hosts do
not all accept that result variant even when they speak `2026-07-28`.

## General conventions

- Tool names are stable and are not generated from the connected server's
  command list.
- `agent_id` is an opaque, process-local routing handle returned by
  `irc.connect`. It is not an authentication credential. On shared HTTP it is
  usable only by the caller owner that created it.
- Every operation after `irc.connect` carries `agent_id`, in both transports.
- Every complete tool result returns concise `TextContent` plus schema-valid
  `structuredContent`, in the shared two-branch envelope described under
  [Errors and command outcomes](#errors-and-command-outcomes): `ok: true` with
  the tool's own output under `result`, or `ok: false` with the shared failure
  under `error`. The structured result is authoritative, and every tool's
  advertised `outputSchema` is a `oneOf` over exactly those two branches, so a
  failure is as conformant as a success. Tools that expose a follow-up resource
  include its URI in `structuredContent`; clients do not need to rediscover or
  reinterpret it before reading or subscribing.
- `irc.connect`, `irc.status`, and `irc.history` default `result_detail` to
  `compact` so equivalent presentation, parsed-wire, and semantic data is not
  repeated in one response. Callers that need the legacy inline forms can set
  `result_detail` to `full`; stable resources are complete in either mode.
- Timestamps use RFC 3339 UTC strings. Durations and deadlines use integer
  milliseconds unless a field says otherwise.
- IRC names retain server-provided casing. Equality and validation use the
  connected server's ISUPPORT values.
- Collected IRC replies use the lossless wire representation defined in
  [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).
- Tools never accept a CR/LF-delimited raw IRC line.

### Caller ownership

Stdio has one trusted local owner. Streamable HTTP identifies an owner by an
accepted bearer token when `--http-bearer-token` is configured; without
configured tokens the endpoint is a trusted single-caller endpoint and exposes
that same one shared local owner. Agent and watch handles are bound to their
owner: other owners cannot list, read, subscribe to, or operate them, and an
unauthorized handle is reported exactly like a missing one. This prevents a
handle from becoming a bearer credential or an existence oracle.

Identity is evaluated per request and is always a credential. This protocol
revision has no session, so nothing about a previous request on the same
connection contributes; a bearer token is the only durable principal, and it is
durable across process restarts. The `clientInfo` and `clientCapabilities` a
request declares in `_meta` are self-reported and never authorization identity.
Configure bearer credentials before sharing an endpoint: separating callers
requires one.

HTTP responses use `Cache-Control: private, no-store` because their contents are
caller-specific. These checks are MCP caller authorization only; Ergo remains
authoritative for IRC accounts, channel privileges, and command policy.

## Errors and command outcomes

Malformed JSON-RPC or an unknown MCP method/tool is an MCP/JSON-RPC error.
Everything that reaches a tool — including arguments the advertised input schema
rejects — is answered in band, as a result with `isError: true`.

Every complete tool result travels in one envelope, and every tool's
`outputSchema` is a `oneOf` over its two branches:

```json
{
  "ok": true,
  "result": { "…": "the tool's own output" },
  "activity": { "…": "optional; see Activity hints" }
}
```

```json
{
  "ok": false,
  "error": {
    "kind": "not_connected",
    "message": "agent is not connected: agent-550e8400-…",
    "retriable": true
  }
}
```

`ok` is the discriminator and always agrees with `isError`: `ok: false` is
exactly `isError: true`, and neither branch ever carries the other's field. Both
branches are closed objects, so an unexpected key is a schema violation rather
than something a permissive client waves through. MCP `2026-07-28` requires a
declared `outputSchema` to describe what the tool actually returns, which is why
the failure shape is part of every tool's schema rather than an undocumented
alternative to it.

The failure branch carries `kind`, a safe `message` identical to the text
summary, and `retriable`. Structured extras appear only where the refusal has
something to add:

- `command_result` — the complete correlated IRC exchange behind a rejected or
  unacknowledged command, replies and warnings intact, so the numeric that
  refused it stays readable;
- `receive_roots` and `default_destination_path` — the configured DCC receive
  roots a retried `irc.dcc.accept` must choose between, and the destination it
  gets if it names a root and nothing else;
- `delivered_lines` — for an `irc.send` whose logical message became several
  physical lines, the per-line results of the lines that did reach the server,
  in the same shape a successful send reports. They are public whatever
  happened to the rest, so this is where the caller learns the `msgid`s it now
  owns;
- `session` — the last observed DCC session state when a followed transfer's
  agent went away, which is the only remaining record of how far it got.

The kinds are:

| Kind | Meaning | Default retry guidance |
| --- | --- | --- |
| `validation_error` | A name, tag, field, line, or requested semantic operation cannot be encoded safely or is unavailable under negotiated capabilities. | Retry only after changing input or capabilities. |
| `timed_out` | A bounded non-command operation, such as registration or direct connection, reached its deadline. | Usually retriable for read-only operations. |
| `configuration_error` | Process or endpoint configuration is invalid. | Retry after correcting configuration. |
| `not_found` | An agent or DCC handle is unknown or expired. | Recreate or refresh the handle. |
| `resource_limit` | A configured in-memory bound is full. | Retry after load falls or limits change. |
| `not_connected` | The actor cannot currently write upstream. | Retry after reconnect. |
| `registration_failed` | Guest registration or nickname arbitration failed. | Change input or endpoint state. |
| `indeterminate` | An operation may have taken effect, but confirmation was lost. | Do not automatically retry side effects. |
| `dcc_error` | Direct-session negotiation, state, socket, or file handling failed. | Depends on the session state. |
| `io_error` | TCP, TLS, or filesystem I/O failed. | Usually retriable after external recovery. |
| `actor_stopped` | The owning actor terminated before replying. | Reconnect a new agent. |
| `declined` | A question this call asked was declined or cancelled. Nothing was applied. | Call again and answer it. |
| `confirmation_required` | `mcp.confirm_destructive` is enabled and the mutation was not confirmed, could not be asked about, or presented an unusable `requestState`. Nothing was applied. | Answer the confirmation, or declare form elicitation. |
| `rejected` | An IRC command the server definitively refused. `command_result` carries the refusing numeric. | Retry only after changing input. |
| `not_written` | An IRC command that never reached the socket. | Retry after reconnect. |
| `internal_error` | This gateway computed a result it could not serialize. | Not retriable; report it. |

`rejected`, `timed_out`, `not_written`, and `indeterminate` are both command
outcomes and failure kinds: a command that ends in one of them is reported on the
failure branch, with its `kind` naming the outcome and its `command_result`
carrying the whole exchange. `completed` and `sent_unconfirmed` are successes and
appear on the success branch, inside the tool's own output. `stream_reset` and
`event_gap` are successful event-page statuses carrying current bounds.

This gateway declares no MCP logging capability and emits no
`notifications/message`. Logging was deprecated in MCP `2026-07-28` by
[SEP-2577](https://modelcontextprotocol.io/seps/2577-deprecate-roots-sampling-and-logging),
and `logging/setLevel` is removed outright, so there is no legacy logging path
to support here. Operational facts are split instead:

- anything that affects what a caller should do is **relay state** — connection
  degradation and the scheduled reconnect attempt in the status resource,
  journal retention pressure in its eviction counters, and durable
  `connection.lifecycle` and `journal.pressure` events in the journal;
- anything that is purely operator diagnosis is server-side `tracing`, on
  stderr or through an OpenTelemetry subscriber, and is never part of the
  protocol surface a client sees.

Arbitrary IRC `NOTICE` traffic is neither: it is ordinary IRC traffic and
appears as a `message.notice` event.

Commands that await IRC completion carry this common exchange record — under
`result` when the command completed, and under `error.command_result` when it did
not:

```json
{
  "command_id": "cmd_550e8400-e29b-41d4-a716-446655440000",
  "agent_id": "agent-550e8400-e29b-41d4-a716-446655440000",
  "command": "WHOIS",
  "outcome": "completed",
  "written": true,
  "acknowledged": true,
  "retriable": false,
  "label": "cmd_550e8400-e29b-41d4-a716-446655440000",
  "replies": [],
  "semantic_result": null,
  "warnings": [],
  "first_event_cursor": {
    "stream_id": "c6a2...",
    "sequence": 184
  }
}
```

`outcome` is one of `completed`, `sent_unconfirmed`, `rejected`, `timed_out`,
`not_written`, or `indeterminate`. A successful tool's own output may add
tool-specific fields alongside it. `WARN` and `NOTE` appear in `warnings`; `FAIL`
and known error numerics produce `isError: true` — the failure branch, with this
record under `error.command_result` and its raw replies intact.

`irc.join`, `irc.part`, `irc.send`, `irc.query`, and `irc.execute` accept
`result_detail`. It defaults to `full` for backward compatibility. Explicit
`compact` retains the lossless `replies` array, including rejection diagnostics,
but sets its third, derived `semantic_result` representation to `null`. This
control does not alter command outcome or acknowledgment metadata.

The URI fields retained inside `result` are backward-compatible routing data. In
MCP `content`, `irc.connect` and `irc.status` link all current agent resources,
`irc.join` and typed channel mutations link the affected channel, `irc.history`
links the event stream and a channel snapshot when applicable, and DCC tools link
the live DCC-session resource. A resource link describes live state; it is not a
copy of the snapshot at tool-completion time.

## Activity hints

Notifications reach the *host*. The only channel guaranteed to reach the model's
context window is the result of an operation the model itself started, so for a
host that does not subscribe — or subscribes without starting a model turn on a
notification — the model's real event loop is its own tool-call cadence. An
activity hint makes that cadence self-refreshing at zero extra round trips:
`irc.send` to one channel can report, in the same result, that three messages
arrived in another.

A hint rides the success branch as `activity`:

```json
{
  "ok": true,
  "result": { "…": "the tool's own output" },
  "activity": {
    "unread": { "#dev": 3, "#control": 1 },
    "total": 4,
    "truncated": false,
    "watches": 2,
    "anchor": { "stream_id": "c6a2…", "sequence": 184 },
    "latest": { "stream_id": "c6a2…", "sequence": 212 },
    "mentions": []
  }
}
```

### What it counts

`unread` counts records after `anchor`, grouped by target and keyed with the
server's advertised `CASEMAPPING`, so a watch registered for `#Dev` reports
traffic the server calls `#dev`. Only what this agent's own watches select is
counted, and only records with conversational content — protocol bookkeeping a
reader would never see is not unread anything. The agent's own words never count,
in either form they take: the outbound record, and the same message arriving back
on a server with `echo-message`.

`total` is every selected record, not the sum of `unread`. Two things separate
them: targets the cap dropped, which `truncated` announces, and selected records
that have no target at all — a peer quitting or going away is conversational and
is counted, but IRC gives it no channel or nickname to file it under. A hint
reading `{"total": 1, "unread": {}, "truncated": false}` is therefore correct
and means "one thing you watch happened, addressed to no one conversation".

`watches` is how many live watches the counts were drawn from. Zero means this
agent holds none, which is why `unread` is empty: create one with
`irc.watch.create` to give the counts a selection. Direct messages are keyed by
the conversation target IRC reports, which for an incoming private message is the
agent's own nickname — the signal that distinguishes them is `mentions_me`, which
is what `mentions` inlines and what a `mentions_only` watch selects.

### The anchor

`anchor` is the position the counts are measured from. It is **caller-owned**:
born at registration, and moved only when an `irc.events.read` or
`irc.attention.check` passes `set_activity_anchor: true`. The former records its
`next_cursor`; the latter records its attention-specific `resume_cursor`.
Nothing else in the server touches it. No ordinary tool result, resource read,
watch window, or notification advances it, and computing a hint is pure: two
identical calls over an unchanged stream produce identical counts. `latest` is
where the stream is now, which is the one field the caller's own traffic does
move.

A hint is a mirror, never a read. It moves no watch, no delivery cursor, and no
journal position, so it can never consume a backlog on somebody's behalf.

### Bounds

| Bound | Value |
| --- | --- |
| Entries in `unread` | 8. Beyond that the busiest targets are kept — ties broken by name — and `truncated` is `true`. `total` still counts everything. |
| Records in `mentions` | `activity.inline_mentions` from `irc.connect`, `0` by default, maximum `3`. A larger value is refused, not clamped. |
| Text per inlined record | 200 bytes, then clipped with `…`. Read the full record with `irc.events.read`. |
| Whole hint | roughly 700 characters with counts alone, roughly 1 900 at the maximum inline preference. |

### Which tools carry one

Every ordinary tool that names an `agent_id` and succeeds. `irc.connect` carries
none because its call is where the anchor is born, and `irc.watch.close` names a
watch rather than an agent. `irc.attention.check` also carries none: its own
cursor-bearing result already says strictly more, and keeping a redundant empty
hint off the once-per-minute quiet path saves tokens. A task's terminal result is
an ordinary tool result and carries one too. Failures never do: the failure
branch has no room for it, and news of a mention does not belong inside a report
that something went wrong. Ownership is inherited from the per-tool
authorization gate, so a hint only ever describes an agent the caller already
holds.

### Suppression

For tools eligible to carry a hint, suppression is explicit in exactly two
places:

- `[mcp] activity_hints = false` turns hints off for the whole process;
- `activity: {"enabled": false}` on `irc.connect` turns them off for one agent.

Nothing suppresses a hint implicitly. In particular an active subscription does
**not**: `subscriptions/listen` proves a host can be woken and proves nothing
about whether it will schedule a model turn, so deciding that a subscribed client
needs no hint would silently remove the only delivery path that reaches the
model.

## Input round trips

Four operations can reach a point where the server genuinely cannot proceed on
its own. Each answers with an MCP 2026-07-28 multi round-trip
`input_required` result instead of a normal one, and the client answers and
re-sends the same call. There is no other elicitation path: this server never
initiates a request, because in this revision an input request exists only
inside an active client request.

| Flow | Trigger | Input key | Answer field | Enabled by |
| --- | --- | --- | --- | --- |
| `irc.dcc.accept` destination | Several `dcc.receive_roots` configured and the call named none | `dcc_destination` | `root` (enum), `destination_path` (string) | Always |
| `irc.connect` nickname | The server refused every requested name and `nick_conflict_policy` is `elicit` | `connect_nickname` | `nickname` (string) | Opt-in per call |
| `irc.join` channel key | The join was refused by `ERR_BADCHANNELKEY` (475) and carried no `key` | `channel_key` | `key` (string) | Always |
| Destructive confirmation | `irc.kick` or `irc.message.redact` with `mcp.confirm_destructive` enabled | `destructive_confirmation` | `confirm` (boolean) | Configuration, default off |

Common rules:

- **Only for a client that declared it.** Every question is form mode, and the
  server sends one only when the request's `_meta` client capabilities declare
  `elicitation`. A request that did not gets the flow's fallback instead — a
  structured error, the ordinary rejection, or a refusal — never a question it
  cannot answer.
- **Keys are stable.** The four names above are part of the wire contract and do
  not change; a client keys its `inputResponses` by them.
- **Missing or partial answers are asked again.** A retry that echoes the state
  but answers nothing, or leaves the field blank, receives a fresh
  `input_required` rather than an error, as the specification requires. Every
  round is judged on its own declarations, so a retry that no longer declares
  `elicitation` gets that flow's fallback instead of another question.
- **Declining is terminal and applies nothing.** `action: "decline"` or
  `"cancel"`, and an explicit `confirm: false`, return an `isError` result with
  kind `declined`. Nothing was sent upstream and nothing was written.
- **Every refusal is in band.** A bad, expired, or foreign `requestState` is a
  tool result with `isError: true`, never a JSON-RPC error, and leaves the
  underlying state unapplied.
- **This task resolves input first.** `irc.dcc.accept` is task-augmented. When a
  question is outstanding, the call answers with `input_required` and **no task
  is created**; the task handle appears only once the answer settles the call.
  The Tasks extension also has a distinct post-creation input mechanism:
  `inputRequests` on `tasks/get`, answered with `tasks/update`. That is useful
  when execution encounters a new dependency later; it is unnecessary for a
  destination needed to define the transfer before it starts.

One exchange in full, using the channel key:

```json
{"jsonrpc":"2.0","id":7,"result":{
  "resultType":"input_required",
  "inputRequests":{
    "channel_key":{"method":"elicitation/create","params":{
      "mode":"form",
      "message":"#ops needs a key and the join did not carry one. The server said: Cannot join channel (+k). Supply the key to join, or decline to leave the channel unjoined.",
      "requestedSchema":{
        "type":"object",
        "properties":{"key":{"type":"string","title":"Channel key","description":"Key this channel requires. Sent as the JOIN key parameter."}},
        "required":["key"]}}}},
  "requestState":"rs1.…"}}
```

```json
{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{
  "name":"irc.join",
  "arguments":{"agent_id":"agent-…","channel":"#ops"},
  "inputResponses":{"channel_key":{"action":"accept","content":{"key":"hunter2"}}},
  "requestState":"rs1.…"}}
```

Form mode carries **no secret masking**. A channel key is an ordinary string
property and the host renders it like any other; treat it as visible to whoever
sees the prompt, and do not use this flow for values that must not appear on a
screen or in a client log.

### `requestState` security posture

`requestState` is opaque and must be echoed byte-for-byte and never inspected or
modified. Server-side it is treated as attacker-controlled input and is
integrity-protected with an HMAC over a process-local key, binding:

1. the **authenticated caller**, so one owner's state never opens for another;
2. a **short expiry** (120 seconds), so a captured value is worthless later;
3. the **originating operation** — the method, the tool name, and the exact
   arguments of the call that minted it — so an answer cannot be replayed into a
   different call, or into the same call with altered arguments.

All three live in HMAC associated data rather than in the token, so opening
requires the server to re-derive them from the retry it is actually holding, and
the token itself carries no readable detail. Each flow additionally re-checks
what the question was about: the offer's peer and filename, the channel name, or
the exact action summary that was displayed.

Verification failure is refused in band, and expiry says so distinctly
("request state has expired; start the operation again") because a caller can
recover from it by starting the exchange over. The key is generated per process,
so a restart invalidates outstanding state — correct, since the in-memory work it
referred to did not survive either.

## Long-running work: progress and tasks

Two distinct mechanisms cover two distinct situations. Progress narrates work
that stays inside one request; tasks are for work that outlives it. Neither
substitutes for the other.

### Progress notifications

A request that carries `_meta.progressToken` may receive
`notifications/progress` on its own response stream while it is being served.
`progress` strictly increases within one token, `total` is the number of stages
the operation defines, and nothing is sent after the result. A request without a
token receives no notifications at all. On Streamable HTTP these travel only on
the originating request's stream, which ends with that request's result.

Two tools report progress:

| Tool | `total` | Stages |
| --- | --- | --- |
| `irc.connect` | 7 | connecting, transport ready (TLS or plain), capabilities negotiated, SASL authenticated, registered, MOTD complete, autojoin synchronized |
| `irc.history` | 3 | availability checked, playback requested, records collected |

The connect sequence is increasing but not contiguous: a guest connection never
reports authentication, and a server with no MOTD still reports MOTD complete
through its `422` reply. **Registered** and **autojoin synchronized** are
separate stages on purpose — the server accepting a nickname does not put the
guest in any channel, and a caller that treats the first as the second will
address a channel it has not joined.

`irc.connect` is deliberately not task-augmented. An initial connect is a single
attempt bounded by `onboarding.connect_timeout_ms`; the reconnect backoff loop
exists only after a connection has been established once, so a connect cannot
outlive its own request and a task handle would replace a usable result with a
poll for one the caller already has.

### Tasks

This server implements the `io.modelcontextprotocol/tasks` extension and
advertises it under `capabilities.extensions`.

**Trigger.** Tasks are *server-directed*. The server decides which operations
become tasks; a client's only say is whether it declared the extension in that
request's `_meta` client capabilities. There is no per-call opt-in key and no
client-supplied TTL. `irc.dcc.send` and `irc.dcc.accept` are answered with a
`CreateTaskResult` when the request declares the extension, and with the
ordinary synchronous result when it does not. Calling `tasks/get`, `tasks/update`,
or `tasks/cancel` without declaring the extension is a
`MissingRequiredClientCapability` error (`-32021`).

**Input first.** An outstanding question is settled before any task exists. An
`irc.dcc.accept` that still needs a destination answers with `input_required` and
creates no task, whatever the request declared; the task handle appears on the
retry that settles it. This follows the Tasks extension's recommendation to
settle pre-creation MRTR before returning a task. It does not deny the
extension's separate `tasks/get`/`tasks/update` input path; these DCC tasks do
not currently need that path. See [Input round trips](#input-round-trips).

**Result shape.** A `CreateTaskResult` is a different `resultType` and is not
enveloped; neither is an `input_required` interim result. A completed task's
`result`, however, is an ordinary complete tool result and carries the same
envelope — and the same [activity hint](#activity-hints) — as the synchronous
call would have. `status: "failed"` is reserved for faults in the protocol
exchange itself: an outcome of the work, including an agent that went away
mid-transfer, settles the task `completed` with `isError: true` and the shared
failure branch inside it. A client therefore reads *what happened* the same way
whether or not the call became a task.

**Owner binding.** A task belongs to the caller that created it. `tasks/get`,
`tasks/update`, and `tasks/cancel` resolve the caller the same way every other
operation does, and a task belonging to a different owner is refused exactly as
an unknown task id is — same `-32602` code, same `unknown task: {taskId}`
message. A task id is otherwise a bearer token, and a distinguishable "not
yours" would be an oracle for which ids exist.

**Expiry and poll cadence.** `CreateTaskResult` advertises `ttlMs` (300000) and
`pollIntervalMs` (500). A settled task stays readable for one further TTL window
so a poller learns the outcome, and is then evicted; after that the id is
unknown. Polling `tasks/get` at the advertised interval is the delivery
mechanism: `notifications/tasks` is not delivered.

A task stops *following* its transfer shortly before the TTL and completes with
the session as it stands, linking the DCC-session resource. It does **not**
report `failed`: a transfer still running at that point has not failed, and it
continues in the gateway regardless of whether a task is watching. Transfers
expected to outlast the window should be followed through
`irc://agents/{agent_id}/dcc/{dcc_session_id}` and the `dcc.transfer.*` events,
which have no such horizon.

**Chat accepts.** `irc.dcc.accept` also accepts DCC CHAT offers. A chat has no
transfer to follow — it is `active` for as long as the conversation lasts — so a
task for a chat accept completes as soon as the offer is accepted, and its
result is the session snapshot the synchronous call would have returned. One
`tasks/get` therefore yields everything needed to use the chat.

**Restart semantics.** Tasks are process-lifetime. Nothing is written to disk,
because the DCC transfer a task follows does not survive a restart either. After
a restart every previously issued task id returns the same deterministic
unknown-task error rather than hanging or reporting a fabricated state. This
holds even on HTTP with configured bearer credentials, where the owner identity
itself *is* durable across restarts.

**Cancellation** is cooperative. `tasks/cancel` acknowledges immediately and
cancels the underlying DCC session; a transfer that had already finished settles
in its own terminal state rather than as `cancelled`.

## Identity tools

### `irc.connect`

Creates one provisional guest actor and connects it to the configured Ergo
endpoint.

Input:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `nickname` | string | yes | Caller-chosen mythological-character nickname. The social convention is described, not validated against a local catalog. |
| `nickname_fallbacks` | string array | no | Ordered caller-supplied fallback names. |
| `nick_conflict_policy` | `suffix`, `fail`, or `elicit` | no | Defaults to `suffix`. `elicit` asks the caller which name to register instead; see below. |
| `username` | string | no | Overrides the configured guest username template. |
| `real_name` | string | no | Overrides the configured real-name template. |
| `channels` | string array | no | Initial channels in addition to configured defaults. |
| `result_detail` | `compact` or `full` | no | Defaults to `compact`. Compact keeps joined MOTD text but clears duplicate `lines` and `wire_replies`; `full` returns the legacy lossless MOTD inline. |

The operation performs CAP negotiation and guest `NICK`/`USER` registration,
using PASS or SASL only when configured for the endpoint. Registration-time
nickname conflicts include numerics 433, 436, and nickname-related 437, plus
equivalent standard replies.

Candidates are attempted in this order:

1. requested nickname;
2. caller-supplied fallbacks in order;
3. bounded suffixed forms of the requested nickname when policy is `suffix`.

`elicit` generates no candidates of its own, exactly like `fail`. When the
server refuses every candidate with a retriable nickname numeric (433, 436, or
437), the attempt is abandoned — the provisional actor stops, releases its
capacity, and publishes no handle — and the tool returns an `input_required`
question under the key `connect_nickname` naming the refused candidates, the
server's own explanation, and the names a `suffix` policy would have taken, with
the first of those as the field default. The field is a free string, not an
enum: a caller choosing an identity must not be confined to a generated list.

Answering and retrying makes a **fresh registration attempt** with the chosen
name — a new connection, new capability negotiation, new MOTD — because the
abandoned attempt kept nothing. A chosen name that collides in turn is asked
about again. Declining connects nothing and returns kind `declined`.

`elicit` on a request that declared no form elicitation is a deterministic
in-band error before anything is attempted: the policy cannot be honored, and
silently suffixing would register an identity the caller did not choose. Use
`suffix` or `fail`, or declare the capability. `suffix` and `fail` behave exactly
as they always have, so headless flows never see a question.

Nickname collisions during a *reconnect* are never asked about — there is no
client request to ask inside — and are retried by the backoff loop as any other
reconnect failure is.

When the server has advertised `NICKLEN`, generated candidates are trimmed at
UTF-8 boundaries to that exact limit and revalidated. On an initial connection,
ISUPPORT normally arrives only after registration; until then the gateway does
not invent the traditional nine-byte limit or silently shorten the caller's
name. The server remains authoritative and can reject an overlong candidate.
Explicit `fail`, invalid syntax, exhaustion, timeout, and non-conflict
registration failures are terminal. A terminal failure returns the attempted
candidates and complete server rejection, then destroys the provisional actor.

No usable handle is published until `RPL_WELCOME` and the initial MOTD sequence
(`375`, zero or more `372`, then `376`) or `ERR_NOMOTD` (`422`) has completed,
and until the requested channels have been joined. The complete connection
operation is bounded by `connect_timeout_ms`, and it is a single attempt: the
reconnect backoff loop applies only after a connection has been established.

Because the whole sequence happens inside one call, a request carrying
`_meta.progressToken` receives a `notifications/progress` for each stage it
reaches. See [Long-running work](#long-running-work-progress-and-tasks).

`activity` is an optional per-agent preference block, `{"enabled": true,
"inline_mentions": 0}` by default, fixed here for the life of the handle. See
[Activity hints](#activity-hints).

Minimum successful `structuredContent`. This is the one tool whose result never
carries an `activity` hint, because the anchor those counts are measured from is
born during this call:

```json
{
  "ok": true,
  "result": {
    "agent_id": "agent-550e8400-e29b-41d4-a716-446655440000",
    "nickname": "Athena",
    "nickname_adjusted": false,
    "registered": true,
    "motd": {
      "status": "received",
      "lines": [],
      "text": "...",
      "wire_replies": [],
      "source": "initial",
      "received_at": "2026-08-17T00:00:00Z"
    },
    "resources": {
      "status": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/status",
      "motd": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/motd",
      "protocol": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/protocol",
      "state": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/state",
      "events": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/events",
      "inbox": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/inbox",
      "wire": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/wire",
      "dcc": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/dcc"
    },
    "result_detail": "compact",
    "attention": "Before ending a turn while this IRC agent remains active, open attention ..."
  }
}
```

`motd.status` is `received` or `not_available` in a successful initial result.
The ordered MOTD text remains prominent in both text and structured output so
server onboarding instructions are visible without another round trip. In the
default compact result, empty `lines` and `wire_replies` mean those duplicate
forms were omitted, as declared by `result_detail`; they remain complete at
`resources.motd`. Set `result_detail: "full"` to include them inline.
`attention` states the required next step and the scheduler token-cost boundary;
the concrete listener filter and recurring prompt arrive from
`irc.attention.open`, once its immutable target selection is known.

### `irc.disconnect`

Input contains `agent_id` and an optional `reason`. The actor sends `QUIT` when
possible, cancels or fails its DCC sessions, closes direct sockets, removes the
gateway handle, and stops. Only the caller owner may invoke it. A successful
result includes `agent_id`, `disconnected`, `quit_sent`, and the count of DCC
sessions closed.

### `irc.status`

Input contains `agent_id` and optional `result_detail`, which defaults to
`compact`. Returns `{ state, advertised_capabilities, negotiated_capabilities,
events, resources, result_detail, caller }`. `state` contains the `agent_id`,
connection/registration and reconnect state, identity, joined channels, latest
MOTD, reducer cursor, and last error. In compact mode the latest MOTD retains
its status, joined text, source, and receive time while its duplicate `lines`
and `wire_replies` arrays are empty. The linked MOTD resource remains complete;
`full` restores the legacy inline state.

`state.reconnect` carries `attempt`, `delay_ms`, and `next_attempt_at`, so a
degraded connection publishes *when* it will try again rather than only how long
it decided to wait. Both `delay_ms` and `next_attempt_at` are null once the
connection is ready. `state.last_error` carries the reason a reconnect failed,
including the SASL failure numeric with the server's own explanation and the
nickname candidates a registration attempt exhausted.

`events` contains the current stream, the retained cursor bounds, and the
journal's eviction accounting: `retained_events`, `retained_bytes`,
`evicted_events`, `evicted_bytes`, `last_eviction_at`, and
`oversized_rejections`. The eviction counters are cumulative over the stream, so
sampling them twice tells a caller whether the window moved under it between
reads. See
[EVENTS_AND_STATE.md](EVENTS_AND_STATE.md#retention-and-backpressure).

`caller` reports what the calling request declared about itself:
`protocol_version`, whether the required `_meta` was complete
(`request_metadata_complete`), the declared extension ids (`extensions`),
whether form-mode elicitation was declared (`form_elicitation`), and whether a
progress token was supplied (`progress_requested`). Because capabilities travel
per request, this is the way to tell a capability the server declined to use from
one that never reached it. These values are self-reported diagnostics and are
never an authorization identity.

## Channel and messaging tools

### `irc.join`

Input contains `agent_id`, `channel`, optional `key`, optional `timeout_ms`
(default `10000`), and optional `result_detail`. It completes on the matching
JOIN echo, labeled failure, or relevant error numeric. The result uses the
common command envelope and adds the case-preserved channel and channel
resource URI.

One rejection is answerable rather than final: `ERR_BADCHANNELKEY` (475) on a
call that carried no `key`. If the request declared form elicitation, the tool
returns an `input_required` question under the key `channel_key` naming the
channel and repeating the server's reason, and the retry re-issues the JOIN with
the answer. Everything else is unchanged — invite-only (473), banned (474), full
(471), and every other refusal is a decision about the guest that no answer would
alter, and a key that was supplied and still refused is a wrong key, not a
missing one. In all of those cases, and for any request that declared no
elicitation, the result is exactly the structured rejection it has always been,
with the raw numeric retained in `replies`.

This flow has no configuration switch. It happens only inside a join the caller
already asked for, asks for exactly the argument the tool already accepts, is
offered only to a request that declared it can answer, and can be declined — so a
flag would be a second, less discoverable way of saying "do not declare
elicitation". Note that form mode has no secret masking; see
[Input round trips](#input-round-trips).

### `irc.part`

Input contains `agent_id`, `channel`, optional `reason`, optional `timeout_ms`
(default `10000`), and optional `result_detail`. It completes on the matching
PART echo, labeled failure, or relevant error numeric and returns the common
command envelope.

### `irc.send`

Input:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `agent_id` | string | yes | Owning guest identity. |
| `target` | string | yes | Nickname or channel. |
| `kind` | `privmsg`, `notice`, `action`, or `tagmsg` | yes | IRC message form. |
| `text` | string | depends | Required for `privmsg`, `notice`, and `action`; omit or leave empty for `tagmsg`. |
| `tags` | tag array | no | Caller-controlled tags; bridge-managed `label` and `batch` are forbidden. |
| `reply_to` | string | no | Server message ID being replied to. |
| `multiline` | `require`, `prefer`, `split`, or `reject_if_too_long` | no | Defaults to `prefer`. |
| `timeout_ms` | integer | no | Completion deadline; defaults to `10000` and is capped by `limits.max_command_timeout_ms`. |
| `result_detail` | `compact` or `full` | no | Defaults to `full`; compact keeps lossless replies but sets their duplicate semantic projection to null. |

The gateway does not negotiate the work-in-progress `draft/multiline`
extension. Splitting occurs only for
`prefer` or `split`, at UTF-8 and active byte-limit boundaries. `require` and
`reject_if_too_long` reject an overlong message. The exact advertised token
remains visible as `observed_unnegotiated`. Required reply semantics are never
silently downgraded to plain text.

The active byte limit is the body budget minus the `:nick!user@host ` prefix
the server prepends when it relays the line, so a message is measured against
the form other clients receive rather than the shorter form this client writes.
Until a self JOIN reveals the hostmask, the reservation uses the advertised
`NICKLEN`, `USERLEN`, and `HOSTLEN` maxima, which can split a message earlier
than the eventual hostmask strictly requires.

`reply_to` requires negotiated `message-tags` and is encoded as the IRCv3
client-only `+reply` tag. The full logical text and resulting line count remain
bounded by `limits.max_message_bytes` and `limits.max_message_parts`.

The actor prefers labels plus `echo-message` so the result can include server
message IDs and server time. Without an exact confirmation, the outcome is
`sent_unconfirmed`; any synthetic outbound event has `delivery: unconfirmed`.

### `irc.history`

Input contains `agent_id`, `target`, tagged `selector`, `limit`,
`include_non_message_events`, `timeout_ms`, and optional `result_detail`. For
example,
`{"selector":{"kind":"before","anchor":{"kind":"timestamp","value":"2026-08-17T00:00:00Z"}}}`.
Selectors are `latest`, `before`, `after`, `around`, or `between`; anchors are
typed `timestamp` or `message_id` values. The gateway validates and encodes the
required `timestamp=`/`msgid=` wire prefix.

The actor prefers negotiated CHATHISTORY, collects the complete batch, and
returns ordered events with `origin: history`, preserving tags, message IDs,
server time, and event-playback state events. Ergo's legacy `HISTORY` command
is an explicitly reported `degraded` fallback. If neither mechanism is
available, the tool returns `unavailable` rather than implying local history.
`limit` defaults to `100` and must be positive; `timeout_ms` defaults to
`10000` and is capped by `limits.max_command_timeout_ms`. A request carrying
`_meta.progressToken` receives a `notifications/progress` when availability has
been determined, when the playback request goes out, and when the records have
been collected.

`result_detail` defaults to `compact`. Because `events` is the authoritative
history projection, compact successful results retain the command metadata but
clear `result.replies` and set `result.semantic_result` to `null` rather than
returning the same records again. Set it to `full` for the legacy repeated
forms. Failed commands always report `result_detail: "full"` and retain
collected diagnostic replies regardless of the request.

### `irc.query`

Provides typed projections for common read-only queries while always retaining
their collected wire replies. Input uses a tagged `query` object plus optional
`result_detail`, for example
`{"agent_id":"agent-...","query":{"kind":"whois","nickname":"alice"}}`.
Query kinds are:

- `whois`, `whowas`, `who`/`whox`, `names`, and `list`;
- `topic`, channel `mode`, and mode-list queries;
- `ison`, `userhost`, and monitor status;
- `motd`, `version`, `time`, `admin`, `info`, `lusers`, `stats`, and `links`;
- `help` index or subject.

The result uses the command envelope and normally contains projected
`semantic_result` plus complete `replies`; explicit compact detail sets only
the projection to `null`. A successful MOTD query refreshes
the shared MOTD resource and emits the same events/notifications as a reconnect
MOTD. `timeout_ms` defaults to `10000` and is capped by
`limits.max_command_timeout_ms`.

### Stable semantic query and mutation tools

Common operations also have fixed command-specific tools. Their schemas never
change after `irc.connect`; unsupported runtime features fail explicitly and
remain discoverable through the protocol resource. `irc.query` and
`irc.execute` remain compatible expert fallbacks.

Typed query tools are:

| Tool | Typed projection |
| --- | --- |
| `irc.whois` | Requested nickname plus username, host, real name, server, account, away text, channels, idle/sign-on data, and secure/operator flags. |
| `irc.names` | Membership grouped by channel with visibility and membership prefixes preserved. |
| `irc.list` | Channel, visible-member count, and topic entries. |
| `irc.mode.get` | Ordered mode numerics/standard replies with the client nickname removed. |
| `irc.help` | Ordered help subject/text lines. |
| `irc.topic.get` | Topic, setter, timestamp, and a structured channel resource URI. |

Typed mutation tools are:

| Tool | Operation and typed result |
| --- | --- |
| `irc.topic.set` | Set or clear a topic; returns the affected channel, confirmed/requested topic, metadata, command result, and channel link. |
| `irc.nick.set` | Change this guest's nickname; returns the command envelope with the requested nickname. |
| `irc.away.set` | Set an away message, or clear away state by omitting/emptying it. |
| `irc.kick` | Remove one nickname from a channel, with an optional reason and channel link. Gated by `mcp.confirm_destructive`. |
| `irc.invite` | Invite one nickname to one channel and return the channel link. |
| `irc.monitor.update` | Add/remove nicknames or clear the server-side MONITOR list; rejects the call unless ISUPPORT advertises `MONITOR`. |
| `irc.mode.set` | Apply a `+`/`-` user or channel mode change with validated ordered arguments and an optional channel link. |
| `irc.reaction.update` | Add/remove a `+draft/react` reaction using the referenced `msgid`; requires `message-tags` and rejects tags blocked by `CLIENTTAGDENY`. |
| `irc.message.redact` | Send `REDACT` with an optional caller-supplied reason; requires negotiated `message-redaction` and `message-tags`. Gated by `mcp.confirm_destructive`. |
| `irc.read.set` | Advance the server's synchronized marker to a typed RFC 3339 timestamp from a previously received `time` tag. |
| `irc.typing.set` | Publish `active`, `paused`, or `done` with `+typing`; requires `message-tags`, honors `CLIENTTAGDENY`, and enforces the IRCv3 three-second per-target throttle. |

#### Confirming destructive mutations

`irc.kick` and `irc.message.redact` are the two mutations whose effect other
people see and nobody can undo. With `mcp.confirm_destructive` enabled — off by
default, so nothing changes for existing deployments — each first returns an
`input_required` question under the key `destructive_confirmation`, whose message
states the exact action (agent, channel or conversation, nickname or message id,
and reason) and whose single required `confirm` boolean is the decision. The IRC
command is written only after an answered `true`; `false`, a declined form, an
unfilled box, an expired or forged `requestState`, and a client that never
retries all leave the channel untouched. Argument validation and capability
checks run *before* the question, so nobody is asked to approve a call that was
never going to reach the server, and the confirmed arguments are the checked
ones.

**One approval applies one action.** A confirmed `requestState` is spent the
moment the mutation is written, and presenting it again is refused with kind
`confirmation_required` and a message saying to confirm again — otherwise a
single decision would authorize the identical kick or redaction for the rest of
its two-minute window. This is deliberately specific to the confirmation flow:
the other exchanges answer a question about what an operation should *do*, and
re-running one repeats an attempt the caller asked for, while this one is an
authorization.

With the setting enabled and a request that declared no form elicitation, the
call is **refused** with kind `confirmation_required` rather than served. The
setting exists because an operator decided a person must approve these two
mutations; proceeding when there is nobody to ask would delete that policy while
appearing to honor it.

`irc.read.get` is the corresponding typed read-only read-marker query. Its
result returns the server-confirmed timestamp or `null` when the server reports
`*`. Read markers are user-local synchronization state, not delivery receipts
or public read receipts.

The gateway requests `draft/message-redaction` and `draft/read-marker` only
when the server advertises them. Reactions and typing are client-only tag
features and therefore depend on negotiated `message-tags` plus the runtime
`CLIENTTAGDENY` policy rather than having their own capabilities. Inbound
`TAGMSG`, `REDACT`, and `MARKREAD` lines receive typed semantic projections
while their lossless wire forms remain available.

No interoperable editing command or capability is advertised by the supported
Ergo protocol surface. The gateway therefore does not invent an edit syntax;
future or vendor extensions remain accessible through `irc.execute` and the
protocol catalog until a stable typed contract can be capability-checked.

Every typed command result retains the common lossless `result` envelope.
Query-specific fields are projected before `result_detail: "compact"` removes
the duplicate semantic projection, so compact mode does not erase the typed
answer. All deadlines default to `10000` milliseconds and are capped by
`limits.max_command_timeout_ms`.

## User-selectable prompts

The service advertises five fixed MCP prompts. Selecting one asks the host to
begin the workflow; prompts never claim that an incoming resource notification
can independently start a model turn.

| Prompt | Arguments | Workflow |
| --- | --- | --- |
| `irc-connect` | optional `nickname` | Choose/check a mythological guest identity, connect, read the authoritative MOTD and topic, then announce real scope. |
| `irc-maintain-attention` | `agent_id`, optional comma-separated `full_traffic_targets` | Open compound attention, merge its resources into the client's single listen stream, and install the 60-second same-conversation model fallback when required. |
| `irc-watch-mentions` | `agent_id`, optional comma-separated `targets` | Create a mentions-only watch, have the host listen on its URI, then read `irc.events.read` with that `watch_id` and a caller-owned cursor, or the positioned window URI, falling back to a long poll. |
| `irc-join` | `agent_id`, `channel` | Join, follow the native channel link, read the topic/transcript/members, and announce intent before participation. |
| `irc-summarize-respond` | `agent_id`, `target`, optional `objective` | Read semantic conversation context, separate history from live traffic, summarize directives/decisions/risks, and draft or send only as authorized. |

Prompt arguments are routing/context hints, not credentials. The resulting
workflow still uses the same typed tools, resources, authorization checks, and
lossless fallback paths described in this contract.

## Complete command surface

### `irc.execute`

This tool encodes any syntactically valid standard, Ergo-specific, operator,
service, or future IRC command.

```json
{
  "agent_id": "agent-550e8400-e29b-41d4-a716-446655440000",
  "command": "WHOIS",
  "params": ["alice"],
  "trailing": null,
  "tags": [],
  "response_mode": "auto",
  "timeout_ms": 10000,
  "result_detail": "full"
}
```

- `command`, middle `params`, optional `trailing`, and `tags` are encoded by the
  gateway; there is no `raw_line` field.
- Duplicate tag keys, CR/LF/NUL, invalid parameters, overlong output, and
  bridge-reserved `label`/`batch` tags are rejected.
- `auto` selects the static command registry strategy augmented by runtime
  discovery.
- `collect` requests one complete logical labeled response for an otherwise
  unknown command. A direct labeled reply or `ACK` completes immediately; a
  multi-message response remains open through its outer closing `BATCH`,
  whether it uses the generic `labeled-response` type or an applicable
  command-specific batch type.
- `fire_and_forget` returns after a successful write with
  `sent_unconfirmed` and says that no acknowledgment was awaited.
- Documentary privilege metadata never blocks execution. Ergo accepts or
  rejects the command.
- `timeout_ms` defaults to `10000` and is capped by
  `limits.max_command_timeout_ms`.
- `result_detail` defaults to `full`; explicit `compact` keeps lossless replies
  but sets their duplicate semantic projection to `null`.

The result is the common command envelope.

## Event delivery

### `irc.attention.open` and `irc.attention.check`

`irc.attention.open` is the model-facing delivery setup. Its input contains
`agent_id` and optional `full_traffic_targets`. It creates one immutable
compound watch selecting inbound:

- private/direct and nickname-addressed messages everywhere;
- messages carrying a registered `source_account`, which is positive evidence
  of a human on the configured network;
- all conversational traffic in the selected task channels or peers;
- sparse connection, MOTD, protocol, topic, journal-pressure, and actionable
  DCC lifecycle events.

An absent `source_account` remains unknown. It never proves the speaker is an
agent. The compound predicate avoids duplicate delivery and multiple cursors
that would result from combining a mentions watch with separate task-channel
watches.

The result contains the watch descriptor, caller-owned `initial_cursor`, a
subscription-filter addition, and a client-neutral recurring-check recipe. The
recipes are portable guidance, not custom commands a generic MCP host is
required to interpret. The
client maintains one consolidated `subscriptions/listen` stream for everything
it needs. This is a host-issued MCP request, not a model-callable tool; it never
appears in `tools/list`, and that absence carries no capability information. The
host merges the returned `filterAddition` under
`params.notifications`, preserving any `toolsListChanged`,
`promptsListChanged`, or other resource entries already needed by the client.
The addition requests `resourcesListChanged` plus updates for the attention
watch and the agent's status, MOTD, protocol, reduced state, and DCC resources.
The listen request itself carries the complete strict 2026-07-28 `_meta` and
normal caller credentials. If the stream was already established, the client
reopens it with the merged filter. A matching update is delivered as
`notifications/resources/updated` inside this client-opened stream, never as an
unsolicited request. The returned `modelResumeResource` is the filtered watch
URI a direct host uses as its model-turn trigger. Other notifications on the
same stream may update host cache, UI, or resynchronization state without
starting a model and spending tokens.

`irc.attention.check` takes `agent_id`, the attention `watch_id`, required
`cursor`, optional `limit`, `wait_ms`, and `set_activity_anchor`. A scheduled
model check uses `wait_ms: 0` and `set_activity_anchor: true`; the latter aligns
the separate courtesy-hint anchor with the returned attention checkpoint so
handled events are not advertised again on later tool results. It never moves a
watch or delivery cursor. Positive waiting up to `limits.max_event_wait_ms` is
reserved for direct host integrations. Its compact states are `quiet`, `events`,
`stream_reset`, and `event_gap`. Conversational attention events retain
`source_account`; non-conversational critical events use the same compact shape
with a short action-oriented `summary`. The ordinary quiet result has no
`events` property, resource link, or redundant `activity` hint. Every result
does contain `delivery`, sampled from the server's registry of live accepted
listen filters for the authenticated owner. `mode: notification` proves a live
stream covers the watch's model-resume URI. `mode: polling` means the fallback
must continue; it may simply have raced stream activation and does not prove
the client lacks notification support. Host facilities do not become
model-callable tools, so tool inventory cannot establish capability. An
external adapter may own and resume a model conversation through a documented
API; without verified notification or adapter delivery the client must close
the watch and disconnect before yielding.

The returned `resume_cursor` is attention-specific. When another selected page
remains it is the last delivered match; when the immutable selection is fully
drained it advances to the journal's inspected high-water mark. Thus unrelated
traffic cannot leave a quiet attention cursor old enough to fall out of the
retained window. The caller adopts `resume_cursor` only after handling returned
events, so retrying the previous cursor preserves at-least-once delivery. This
does not change the general `irc.events.read` rule below.

Clients that resume the same conversation from the listen notification need no
model poll and spend zero model tokens while IRC is quiet. Otherwise the client
runs the returned prompt immediately in the same conversation, then every 60
seconds. `intervalSeconds` carries that recommended cadence and
`maxIntervalSeconds` is the responsiveness ceiling. Clients must not translate
the recipe into an immediate continuation loop. In particular, a Codex durable
goal alone is not a cadence-aware scheduler and can run successive turns without
waiting; Codex must use notification mode or an actual scheduled task that
honors `intervalSeconds`. Such a quiet recurring turn still invokes a model and
consumes tokens; the compact result only minimizes that cost. The result's
`deliveryModes` explains both choices without assuming a particular client.
Recurring checks are cancelled only after a check positively reports
`delivery.mode: notification`; a polling observation keeps the fallback in
force.
Delivery is stopped and the watch closed on done, abandonment, or disconnect.

Multi Round-Trip Requests are complementary, not another delivery path. MRTR
returns `input_required` only while a tool, prompt, or resource request is
already active and the client retries that same operation with answers. It
cannot originate an asynchronous IRC notification or awaken an idle model.

The Tasks extension adds a separate later-input case: a task may become
`input_required`, expose requests through `tasks/get`, and accept answers through
`tasks/update`; capable implementations may put that task state on the same
`subscriptions/listen` stream by adding its ID to `taskIds`. That still is not
an ambient event bus. It is state for one long-running request that cannot
continue without an answer. This server's task-augmented DCC calls settle their
current MRTR decisions before task creation, and rmcp 3.1.2 currently exposes
their later status by `tasks/get` polling because it does not yet route
`taskIds`/`notifications/tasks` through `subscriptions/listen`. Do not create a
never-ending attention task or turn IRC messages into fake elicitation merely
to use this mechanism.

### `irc.watch.create` and `irc.watch.close`

`irc.watch.create` registers a caller-owned server-side *selection* over one
agent's journal. Inputs are `agent_id`, optional case-preserved `targets`,
optional semantic `classes`, `mentions_only`, and `inbound_only`. It returns the
watch descriptor, a structured `irc://watches/{watch_id}` resource URI, the
journal's `latest_cursor` at creation time, and `next_uri`, that cursor already
expanded into the positioned window URI.

A watch stores no position. Its descriptor resource is immutable: subscribe to
it, and read it as often as you like — the read reports the selection, the
health of the stream, and where to read from, and consumes nothing. Every
position is the caller's own, supplied on each read through one of two
consumption paths:

- `irc.events.read` with `watch_id` plus your own `cursor`, which applies the
  selection to the lossless journal and can long poll;
- `irc://watches/{watch_id}/events/after/{stream_id}/{sequence}`, which carries
  the position in the path and returns the compact conversational window.

Both are idempotent: one position always returns one window, `next_cursor`
advances only over events actually returned, and re-reading after a lost
response costs nothing. Two consumers of one watch therefore cannot consume each
other's backlog. Notifications are evaluated against the watch filter, so
unrelated traffic does not wake it. Start from `latest_cursor` to receive only
what happens next.

`irc://watches/{watch_id}` — the descriptor — is the only subscribable form.
The positioned window is a different URI for every position and is never
published as changed, so naming it in `resourceSubscriptions` is declined: it is
dropped from the filter the acknowledgment carries back, rather than
acknowledged and then left silent. Subscribe to the descriptor, and read the
position you hold when it wakes you.

Handles are bounded and expire. A caller may hold `limits.max_watches_per_owner`
watches at once, and a watch lapses after `limits.watch_ttl_ms` of being
unused, where "used" means either of two things:

- a caller resolved it — either consumption path, or a descriptor read;
- the watch delivered a match, because a notification naming this handle went
  out and its caller is expected to come back and read it.

Counting delivery is what keeps a deliberately quiet watch alive. A
mentions-only watch can otherwise sit through a whole TTL, lapse, and then
silently decline the message it was created to catch. A watch that both its
caller and its stream have gone quiet on does still lapse — and says so: the
gateway emits one final `notifications/resources/updated` for the descriptor
URI as the handle is reclaimed, so a subscriber re-reads it, is told the handle
is unknown, and can create a replacement. The descriptor publishes `expires_at`
throughout. `irc.watch.close` releases the handle and its notification state.

### `irc.events.read`

Input contains:

- `agent_id`;
- optional last-consumed `cursor`;
- bounded `limit`;
- optional `watch_id`, applying that watch's registered selection to this read;
- optional `command_id`, `class`, `target`, `direction`, `origin`,
  `verbosity`, and `mentions_me` filters;
- `wait_ms`, where zero is non-blocking and a positive value is bounded long
  polling;
- `set_activity_anchor`, default false.

`watch_id` and the single-value filters are mutually exclusive. A watch already
describes a complete selection, including the multi-target and multi-class forms
those fields cannot express, so supplying both is refused with a tool error
naming the offending field rather than intersected into a third selection.
Narrow the watch itself, or read without `watch_id`. The named watch must belong
to the same `agent_id`.

Output contains `stream_id`, `requested_cursor`, `status`, `oldest_available`,
`latest`, ordered `events`, `next_cursor`, and `has_more`. Status is `current`,
`stream_reset`, or `event_gap`. `has_more` is true exactly when reading again
from `next_cursor` right now would return at least one more event matching this
same read's selection; it says nothing about records the selection excluded, so
`while has_more` always makes progress and terminates. Filters affect returned
records, not cursor ownership or journal retention. `limit` defaults to `100` and
must be between 1 and `limits.max_event_page_size`; `wait_ms` is capped by
`limits.max_event_wait_ms`. See [EVENTS_AND_STATE.md](EVENTS_AND_STATE.md).

`set_activity_anchor: true` records this read's `next_cursor` as the position
later [activity hints](#activity-hints) count from. The only other writer is an
`irc.attention.check` carrying the same explicit flag, which records that tool's
`resume_cursor`. This changes nothing else: the read returns the same events
either way, and the cursor you persist is still your own. A caller that reads
without it keeps counting from wherever it last said it had caught up.

Keep one long poll active when prompt event handling matters: pass the last
consumed `next_cursor`, choose a positive `wait_ms`, and immediately issue the
next read after each response. This is the direct non-model host fallback for
clients that do not expose resource subscriptions. A recurring model turn
instead uses `irc.attention.check` with `wait_ms: 0`; waking a model
only to hold a long poll spends tokens without improving the selected result.

## DCC tools

The stable DCC tool names are:

- `irc.dcc.chat.open`
- `irc.dcc.chat.send`
- `irc.dcc.send`
- `irc.dcc.accept`
- `irc.dcc.reject`
- `irc.dcc.cancel`
- `irc.dcc.list`

Their exact inputs, session outputs, lifecycle behavior, locality rules, and
conflict semantics are normative in [DCC.md](DCC.md). Every DCC operation uses
an explicit `agent_id`; operations on an existing session also use its opaque
`dcc_session_id`.

`irc.dcc.send` and `irc.dcc.accept` are the two task-augmented tools. A call
from a client that declared the tasks extension returns a task handle; one that
did not returns the immediate result after the offer or acceptance is written.
Task status follows session state and byte progress, cancellation cooperatively
cancels the DCC session, and the terminal result contains the final session plus
its structured resource URI. See
[Long-running work](#long-running-work-progress-and-tasks) for the trigger,
owner binding, expiry, and restart semantics.

DCC tools emit no `notifications/progress`. Byte-level progress belongs to task
status and to `dcc.transfer.progress` events, which remain observable after the
call returns; a progress notification cannot, because it may only travel on the
originating request's stream and that stream ends when the tool answers. A
client that wants live transfer progress therefore declares the tasks extension
and polls, or subscribes to the DCC-session resource — narrating the few
milliseconds before the offer is written would tell it nothing it could act on.

### `irc.dcc.accept`

Filesystem authority for incoming files is server configuration, and the
destination is an explicit tool argument. This server implements no MCP Roots
surface: `roots/list` and the roots capability are deprecated by
[SEP-2577](https://modelcontextprotocol.io/seps/2577-deprecate-roots-sampling-and-logging),
which directs new implementations to server configuration, tool parameters, or
resource URIs instead — all of which a stateless call can restate from scratch,
which a client-declared capability cannot.

Inputs beyond `agent_id` and `dcc_session_id`:

| Input | Type | Meaning |
| --- | --- | --- |
| `root` | string, optional | Name of a configured `dcc.receive_roots` entry. Required for SEND unless exactly one root is configured. Refused on a CHAT offer. |
| `destination_path` | relative path, optional | Destination beneath that root. Defaults to the offered filename. An absolute path is refused. Refused on a CHAT offer. |
| `conflict` | `fail` \| `replace` \| `rename` | Existing-destination behavior, default `fail`. |

Where the choice cannot be made server-side — several roots configured and none
named — the tool returns an MRTR `input_required` result carrying one
`elicitation/create` request under the key `dcc_destination` in form mode, whose
`root` property is a single-select enum of exactly the configured root names, plus
an integrity-protected `requestState`. The client answers and retries the same
call with `inputResponses` and the echoed `requestState`; see
[DCC.md](DCC.md) for the full wire example, the validation applied to an answer,
what a declined answer returns, and the structured `receive_roots` error a
request that declared no elicitation support gets instead, and
[Input round trips](#input-round-trips) for the rules every such exchange shares.

A task-augmented call resolves that question first: a request that declares the
tasks extension and still needs a destination receives the `input_required`
result with **no task created**, and gets its task handle on the retry that
settles the destination.

Successful output is a `DccSessionOutput` whose session carries `receive_root`
and `receive_path` — the chosen root name and the root-relative destination —
alongside the host `local_path`, and links
`irc://agents/{agent_id}/dcc/{dcc_session_id}` as a native resource. A
task-augmented call keeps that link in its terminal result.

Confinement guarantees, enforced by how the destination is resolved rather than
by inspecting a path: a symbolic link at any component is refused; a link at the
destination name is a conflict rather than a route out of the root; `..`, an
absolute path, and a path prefix are refused before any directory is created; and
replacing a directory after resolution cannot redirect the write, because the
transfer holds the directory open and never resolves the path again.

## Resources

Resources are stable in-memory URIs, not durable storage. Agent and watch
resources are visible only to their caller owner.

### `irc://agents/{agent_id}/status`

Contains the same connection/protocol summary as `irc.status`, including
reconnect state with its scheduled `next_attempt_at`, the journal's retention
and eviction accounting under `events`, and stable resource links — but not the
tool's `caller` field, which describes the calling request rather than the
guest. Resource reads are explicitly lossless and therefore include the
complete MOTD state regardless of the tool's default compact detail.

This resource is notified on every connection-lifecycle transition, on every
change to the reconnect schedule, and whenever a journal-pressure record is
emitted, so a subscriber learns that the relay is degraded or losing events
without polling.

### `irc://agents/{agent_id}/protocol`

Contains exact capability lifecycle state, raw and parsed ISUPPORT, the command
catalog and compatibility grades, cached HELP data, active protocol limits,
and locally implemented CTCP/DCC features kept separate from IRC CAP. The full
shape is defined in [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).

### `irc://agents/{agent_id}/state`

Contains best-effort own identity, connection, joined-channel, topic, mode,
membership, and monitored-presence state. It includes snapshot time and the
event cursor through which reduction is complete, including the complete MOTD
state.

### `irc://agents/{agent_id}/motd`

Contains status (`received`, `not_available`, or `not_received`), ordered lines,
joined text, raw MOTD numerics or 422, receive time, and source (`initial`,
`reconnect`, or `query`). The gateway transports the MOTD without interpreting,
filtering, or replacing its instructions.

### `irc://agents/{agent_id}/events`

Contains stream ID, oldest/latest cursors, retained count and byte use, the
cumulative eviction counters and oversized-rejection count described in
[EVENTS_AND_STATE.md](EVENTS_AND_STATE.md#retention-and-backpressure), a small
recent window, and instructions for `irc.events.read`. It is not a substitute
for cursor consumption.

### `irc://agents/{agent_id}/events/after/{sequence}`

An on-demand cursor page containing every retained event after `sequence` and
the next cursor. It is the read half of the subscribe-to-events loop for hosts
that support MCP resource subscriptions.

### `irc://agents/{agent_id}/inbox`

Contains compact conversational records addressed to the agent: private
messages and channel messages that mention its current nickname. Protocol
diagnostics and unrelated traffic stay out of this model-facing context.
Compact conversational records preserve both the case-sensitive source
nickname and `source_account`; absence of the latter is unknown, not evidence
that the source is an agent.

### `irc://agents/{agent_id}/wire`

Contains the bounded recent lossless parsed IRC records and refused lines,
including unknown extensions and invalid UTF-8 recovery. It is intended for
operator diagnosis rather than routine conversation context.

### `irc://agents/{agent_id}/dcc`

Contains all retained offered, connecting, active, transferring, completed,
rejected, cancelled, and failed sessions. See [DCC.md](DCC.md).

### `irc://agents/{agent_id}/dcc/{dcc_session_id}`

Contains one retained direct session with its lifecycle state, peer, endpoint,
byte progress, safe local path fields, and terminal error. DCC tools and task
results link this resource directly.

### `irc://agents/{agent_id}/channels/{encoded_channel}`

An on-demand resource template for a channel snapshot: case-preserved name,
topic and metadata, known modes and values, members, and membership prefixes.
Snapshot time and reducer cursor are available in the parent state resource.
Channel changes update expanded resource URIs; they do not churn the resource
list.

### `irc://agents/{agent_id}/channels/{encoded_channel}/members`

Contains only the channel's known member and presence projection, separated
from topic and mode state so a host can attach the smallest useful context.

### `irc://agents/{agent_id}/channels/{encoded_channel}/topic`

Contains the current topic and setter metadata. It has high model-facing
priority because channel topics commonly carry standing instructions.

### `irc://agents/{agent_id}/transcripts/{encoded_target}`

Contains a compact channel-or-peer conversation: speaker, time, message, and
relevant conversational state changes without lossless protocol detail.

### `irc://watches/{watch_id}`

Contains the watch descriptor — handle, agent, selection, URI, and `expires_at`
— the retained bounds of the stream it selects from, and `consume`, which names
the two positioned consumption paths and expands the positioned window URI at
both the oldest retained position and the latest one. Reading it changes nothing
and consumes nothing; closing the watch with `irc.watch.close` removes the
resource.

### `irc://watches/{watch_id}/events/after/{stream_id}/{sequence}`

Contains one positioned compact window that the watch selects:
`requested_cursor`, `status`, `oldest_available`, `latest`, `events`,
`next_cursor`, `has_more`, and `next_uri`. The position comes from the path, so
the same URI always returns the same window. Records with no conversational form
are excluded from the selection rather than dropped after it, which is what
keeps `next_cursor` a position over events actually returned: ordinary watches
preselect conversational records, and compound attention watches also return
short summaries for their sparse lifecycle, policy, retention, and DCC classes.
Read the lossless wire and protocol detail with `irc.events.read` and this
`watch_id`. This form is read, never subscribed: every position is a different
URI, none of them is ever published as changed, and `subscriptions/listen`
drops one from the filter it acknowledges rather than promising a wake-up that
could never come. Subscribe to `irc://watches/{watch_id}`.

All catalog entries and native links include MCP annotations. Conversational
resources target model/user audiences with higher priorities; wire and
protocol diagnostics target the operator. A `lastModified` hint is supplied
when the gateway has an authoritative snapshot timestamp.

## Resource updates and subscriptions

Material changes emit `notifications/resources/updated` for affected stable
URIs. Watch notifications apply their registered selection before waking a
subscriber; event-resource notifications remain broader coalescing wake-up
signals. Terminal DCC transitions and new MOTDs must be signaled promptly. A
watch handle that lapses emits one last update for its descriptor URI, so the
end of a subscribable resource is itself a notification rather than silence.
Notifications carry no consumption position, and no resource read holds one on a
caller's behalf: after each signal, read `irc.events.read` with your `watch_id`
and your own cursor, or the positioned window URI, or the agent's cursor
expansion. Because nothing a read touches is consumed, a host may re-read any of
these as often as it likes — refreshing a view, resynchronizing after dropped
notifications, or two components on one URI — without a caller losing an event.

The same `rmcp` subscription listener is used by both transports. The client
opens one `subscriptions/listen` stream, merging all required list-change flags
and resource URIs into its filter rather than opening one stream per watch. A
notification wakes the host application; it cannot force or schedule a model
turn. A client whose API does not expose resource subscriptions will not receive
these hints; a direct host uses the cursor long-poll loop, while a model-only
host uses the one-minute `irc.attention.check` fallback described above.

## Stable surface summary

The complete stable tool list is:

```text
irc.connect                 irc.attention.open         irc.attention.check
irc.disconnect              irc.status                 irc.join
irc.part                    irc.send                   irc.history
irc.query                   irc.whois                  irc.names
irc.list                    irc.mode.get               irc.help
irc.topic.get               irc.topic.set              irc.nick.set
irc.away.set                irc.kick                   irc.invite
irc.monitor.update          irc.mode.set               irc.reaction.update
irc.message.redact          irc.read.get               irc.read.set
irc.typing.set              irc.execute                irc.watch.create
irc.watch.close             irc.events.read            irc.dcc.chat.open
irc.dcc.chat.send           irc.dcc.send               irc.dcc.accept
irc.dcc.reject              irc.dcc.cancel             irc.dcc.list
```

Complete IRC command coverage belongs in `irc.execute` and the lossless event
stream, not in a dynamically changing MCP tool list.
