# MCP API contract

This document is the normative MCP contract implemented by `rmcp-irc`. The
runtime-generated JSON Schemas are the exact machine-readable form; this file
explains their semantics and stable invariants.

The stdio and Streamable HTTP transports expose the same service, tools,
resources, schemas, event model, and error semantics. An MCP transport
connection is never an IRC identity.

## General conventions

- Tool names are stable and are not generated from the connected server's
  command list.
- `agent_id` is an opaque, process-local routing handle returned by
  `irc.connect`. It is not an authentication credential. On shared HTTP it is
  usable only by the caller owner that created it.
- Every operation after `irc.connect` carries `agent_id`, in both transports.
- All successful tools return concise `TextContent` plus schema-valid
  `structuredContent`. The structured result is authoritative. Tools that
  expose a follow-up resource append native MCP `resource_link` content blocks
  after the text summary; clients do not need to rediscover or reinterpret a
  URI string before attaching or subscribing to it.
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

Malformed JSON-RPC, an unknown MCP method/tool, or input that does not satisfy
the advertised JSON Schema is an MCP/JSON-RPC error. Once a valid tool call has
started, failures return `isError: true`. Gateway failures use the structured
tool-error envelope below; correlated IRC failures use the common command
result so their raw replies remain available.

The structured tool-error envelope contains `kind`, safe `message`, and
`retriable`. Its kinds are:

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

`not_written`, `sent_unconfirmed`, `rejected`, `timed_out`, and `indeterminate`
are command outcomes, not error-envelope kinds. A rejected or timed-out
command result has `isError: true` and retains collected replies.
`stream_reset` and `event_gap` are successful event-page statuses carrying
current bounds.

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

Commands that await IRC completion use this common result envelope:

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
`not_written`, or `indeterminate`. A result may add tool-specific fields. `WARN` and `NOTE`
appear in `warnings`; `FAIL` and known error numerics produce `isError: true`
while retaining their raw replies.

`irc.join`, `irc.part`, `irc.send`, `irc.query`, and `irc.execute` accept
`result_detail`. It defaults to `full` for backward compatibility. Explicit
`compact` retains the lossless `replies` array, including rejection diagnostics,
but sets its third, derived `semantic_result` representation to `null`. This
control does not alter command outcome or acknowledgment metadata.

The URI fields retained in `structuredContent` are backward-compatible routing
data. In MCP `content`, `irc.connect` and `irc.status` link all current agent
resources, `irc.join` and typed channel mutations link the affected channel,
`irc.history` links the event stream and a channel snapshot when applicable,
and DCC tools link the live DCC-session resource. A resource link describes
live state; it is not a copy of the snapshot at tool-completion time.

## Identity tools

### `irc.connect`

Creates one provisional guest actor and connects it to the configured Ergo
endpoint.

Input:

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `nickname` | string | yes | Caller-chosen mythological-character nickname. The social convention is described, not validated against a local catalog. |
| `nickname_fallbacks` | string array | no | Ordered caller-supplied fallback names. |
| `nick_conflict_policy` | `suffix` or `fail` | no | Defaults to `suffix`. |
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

When the server has advertised `NICKLEN`, generated candidates are trimmed at
UTF-8 boundaries to that exact limit and revalidated. On an initial connection,
ISUPPORT normally arrives only after registration; until then the gateway does
not invent the traditional nine-byte limit or silently shorten the caller's
name. The server remains authoritative and can reject an overlong candidate.
Explicit `fail`, invalid syntax, exhaustion, timeout, and non-conflict
registration failures are terminal. A terminal failure returns the attempted
candidates and complete server rejection, then destroys the provisional actor.

No usable handle is published until `RPL_WELCOME` and the initial MOTD sequence
(`375`, zero or more `372`, then `376`) or `ERR_NOMOTD` (`422`) has completed.
The complete connection operation is bounded by `connect_timeout_ms`.

Minimum successful result:

```json
{
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
  "result_detail": "compact"
}
```

`motd.status` is `received` or `not_available` in a successful initial result.
The ordered MOTD text remains prominent in both text and structured output so
server onboarding instructions are visible without another round trip. In the
default compact result, empty `lines` and `wire_replies` mean those duplicate
forms were omitted, as declared by `result_detail`; they remain complete at
`resources.motd`. Set `result_detail: "full"` to include them inline.

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
`10000` and is capped by `limits.max_command_timeout_ms`.

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
| `irc.topic.get` | Topic, setter, timestamp, and a native channel resource link. |

Typed mutation tools are:

| Tool | Operation and typed result |
| --- | --- |
| `irc.topic.set` | Set or clear a topic; returns the affected channel, confirmed/requested topic, metadata, command result, and channel link. |
| `irc.nick.set` | Change this guest's nickname; returns the command envelope with the requested nickname. |
| `irc.away.set` | Set an away message, or clear away state by omitting/emptying it. |
| `irc.kick` | Remove one nickname from a channel, with an optional reason and channel link. |
| `irc.invite` | Invite one nickname to one channel and return the channel link. |
| `irc.monitor.update` | Add/remove nicknames or clear the server-side MONITOR list; rejects the call unless ISUPPORT advertises `MONITOR`. |
| `irc.mode.set` | Apply a `+`/`-` user or channel mode change with validated ordered arguments and an optional channel link. |
| `irc.reaction.update` | Add/remove a `+draft/react` reaction using the referenced `msgid`; requires `message-tags` and rejects tags blocked by `CLIENTTAGDENY`. |
| `irc.message.redact` | Send `REDACT` with an optional caller-supplied reason; requires negotiated `message-redaction` and `message-tags`. |
| `irc.read.set` | Advance the server's synchronized marker to a typed RFC 3339 timestamp from a previously received `time` tag. |
| `irc.typing.set` | Publish `active`, `paused`, or `done` with `+typing`; requires `message-tags`, honors `CLIENTTAGDENY`, and enforces the IRCv3 three-second per-target throttle. |

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

The service advertises four fixed MCP prompts. Selecting one asks the host to
begin the workflow; prompts never claim that an incoming resource notification
can independently start a model turn.

| Prompt | Arguments | Workflow |
| --- | --- | --- |
| `irc-connect` | optional `nickname` | Choose/check a mythological guest identity, connect, read the authoritative MOTD and topic, then announce real scope. |
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

### `irc.watch.create` and `irc.watch.close`

`irc.watch.create` registers a caller-owned server-side *selection* over one
agent's journal. Inputs are `agent_id`, optional case-preserved `targets`,
optional semantic `classes`, `mentions_only`, and `inbound_only`. It returns the
watch descriptor, a native `irc://watches/{watch_id}` resource link, the
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

Handles are bounded and expire. A caller may hold `limits.max_watches_per_owner`
watches at once, and a watch nobody reads lapses after `limits.watch_ttl_ms`;
either consumption path or a descriptor read refreshes it, and the descriptor
publishes `expires_at`. `irc.watch.close` releases the handle and its
notification state.

### `irc.events.read`

Input contains:

- `agent_id`;
- optional last-consumed `cursor`;
- bounded `limit`;
- optional `watch_id`, applying that watch's registered selection to this read;
- optional `command_id`, `class`, `target`, `direction`, `origin`,
  `verbosity`, and `mentions_me` filters;
- `wait_ms`, where zero is non-blocking and a positive value is bounded long
  polling.

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

Keep one long poll active when prompt event handling matters: pass the last
consumed `next_cursor`, choose a positive `wait_ms`, and immediately issue the
next read after each response. This is also the required fallback for MCP
clients that do not expose resource subscription requests.

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

`irc.dcc.send` and `irc.dcc.accept` are task-augmented tools. Their default
calls retain the compatible immediate result after the offer or acceptance is
written. When the call requests the MCP tasks extension, it returns a task
handle instead; task status follows session state and byte progress,
cancellation cooperatively cancels the DCC session, and the terminal result
contains the final session plus its native resource link.

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

Contains one positioned window of the compact conversational records that watch
selects: `requested_cursor`, `status`, `oldest_available`, `latest`, `events`,
`next_cursor`, `has_more`, and `next_uri`. The position comes from the path, so
the same URI always returns the same window. Records with no conversational form
are excluded from the selection rather than dropped after it, which is what
keeps `next_cursor` a position over events actually returned; read them
losslessly with `irc.events.read` and this `watch_id`.

All catalog entries and native links include MCP annotations. Conversational
resources target model/user audiences with higher priorities; wire and
protocol diagnostics target the operator. A `lastModified` hint is supplied
when the gateway has an authoritative snapshot timestamp.

## Resource updates and subscriptions

Material changes emit `notifications/resources/updated` for affected stable
URIs. Watch notifications apply their registered selection before waking a
subscriber; event-resource notifications remain broader coalescing wake-up
signals. Terminal DCC transitions and new MOTDs must be signaled promptly.
Notifications carry no consumption position, and no resource read holds one on a
caller's behalf: after each signal, read `irc.events.read` with your `watch_id`
and your own cursor, or the positioned window URI, or the agent's cursor
expansion. Because nothing a read touches is consumed, a host may re-read any of
these as often as it likes — refreshing a view, resynchronizing after dropped
notifications, or two components on one URI — without a caller losing an event.

The same `rmcp` subscription listener is used by both transports. A notification
and `subscriptions/listen` wake the host application; neither can force or
schedule a model turn. A client whose API does not expose resource subscriptions
will not receive these hints; it must use the cursor-based `irc.events.read`
long-poll loop described above instead.

## Stable surface summary

The complete stable tool list is:

```text
irc.connect                 irc.disconnect             irc.status
irc.join                    irc.part                   irc.send
irc.history                 irc.query                  irc.whois
irc.names                   irc.list                   irc.mode.get
irc.help                    irc.topic.get              irc.topic.set
irc.nick.set                irc.away.set               irc.kick
irc.invite                  irc.monitor.update         irc.mode.set
irc.reaction.update         irc.message.redact         irc.read.get
irc.read.set                irc.typing.set              irc.execute
irc.watch.create            irc.watch.close            irc.events.read
irc.dcc.chat.open           irc.dcc.chat.send          irc.dcc.send
irc.dcc.accept              irc.dcc.reject             irc.dcc.cancel
irc.dcc.list
```

Complete IRC command coverage belongs in `irc.execute` and the lossless event
stream, not in a dynamically changing MCP tool list.
