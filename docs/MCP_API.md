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
  `irc.connect`. It is intentionally shareable and is not an authentication
  credential.
- Every operation after `irc.connect` carries `agent_id`, in both transports.
- All successful tools return concise `TextContent` plus schema-valid
  `structuredContent`. The structured result is authoritative.
- Timestamps use RFC 3339 UTC strings. Durations and deadlines use integer
  milliseconds unless a field says otherwise.
- IRC names retain server-provided casing. Equality and validation use the
  connected server's ISUPPORT values.
- Collected IRC replies use the lossless wire representation defined in
  [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).
- Tools never accept a CR/LF-delimited raw IRC line.

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

Arbitrary IRC `NOTICE` traffic is not MCP logging. MCP logging is reserved for
gateway diagnostics associated with an active request.

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
    "dcc": "irc://agents/agent-550e8400-e29b-41d4-a716-446655440000/dcc"
  }
}
```

`motd.status` is `received` or `not_available` in a successful initial result.
The ordered MOTD text is prominent in both text and structured output.

### `irc.disconnect`

Input contains `agent_id` and an optional `reason`. The actor sends `QUIT` when
possible, cancels or fails its DCC sessions, closes direct sockets, removes the
gateway handle, and stops. Any caller holding the shareable handle may invoke
it. A successful result includes `agent_id`, `disconnected`, `quit_sent`, and
the count of DCC sessions closed.

### `irc.status`

Returns `{ state, advertised_capabilities, negotiated_capabilities, events,
resources }`. `state` contains the `agent_id`, connection/registration and
reconnect state, identity, joined channels, latest MOTD, reducer cursor, and
last error. `events` contains the current stream and retained cursor bounds.

## Channel and messaging tools

### `irc.join`

Input contains `agent_id`, `channel`, optional `key`, and optional `timeout_ms`
(default `10000`). It completes on the matching JOIN echo, labeled failure, or
relevant error numeric. The result uses the common command envelope and adds
the case-preserved channel and channel resource URI.

### `irc.part`

Input contains `agent_id`, `channel`, optional `reason`, and optional
`timeout_ms` (default `10000`). It completes on the matching PART echo, labeled
failure, or relevant error numeric and returns the common command envelope.

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

The gateway does not negotiate the work-in-progress `draft/multiline`
extension. Splitting occurs only for
`prefer` or `split`, at UTF-8 and active byte-limit boundaries. `require` and
`reject_if_too_long` reject an overlong message. The exact advertised token
remains visible as `observed_unnegotiated`. Required reply semantics are never
silently downgraded to plain text.

`reply_to` requires negotiated `message-tags` and is encoded as the IRCv3
client-only `+reply` tag. The full logical text and resulting line count remain
bounded by `limits.max_message_bytes` and `limits.max_message_parts`.

The actor prefers labels plus `echo-message` so the result can include server
message IDs and server time. Without an exact confirmation, the outcome is
`sent_unconfirmed`; any synthetic outbound event has `delivery: unconfirmed`.

### `irc.history`

Input contains `agent_id`, `target`, tagged `selector`, `limit`,
`include_non_message_events`, and `timeout_ms`. For example,
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

### `irc.query`

Provides typed projections for common read-only queries while always retaining
their collected wire replies. Input uses a tagged `query` object, for example
`{"agent_id":"agent-...","query":{"kind":"whois","nickname":"alice"}}`.
Query kinds are:

- `whois`, `whowas`, `who`/`whox`, `names`, and `list`;
- `topic`, channel `mode`, and mode-list queries;
- `ison`, `userhost`, and monitor status;
- `motd`, `version`, `time`, `admin`, `info`, `lusers`, `stats`, and `links`;
- `help` index or subject.

The result uses the command envelope and contains projected `semantic_result`
plus complete `replies`. A successful MOTD query refreshes
the shared MOTD resource and emits the same events/notifications as a reconnect
MOTD. `timeout_ms` defaults to `10000` and is capped by
`limits.max_command_timeout_ms`.

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
  "timeout_ms": 10000
}
```

- `command`, middle `params`, optional `trailing`, and `tags` are encoded by the
  gateway; there is no `raw_line` field.
- Duplicate tag keys, CR/LF/NUL, invalid parameters, overlong output, and
  bridge-reserved `label`/`batch` tags are rejected.
- `auto` selects the static command registry strategy augmented by runtime
  discovery.
- `collect` requests labeled collection for an otherwise unknown command.
- `fire_and_forget` returns after a successful write with
  `sent_unconfirmed` and says that no acknowledgment was awaited.
- Documentary privilege metadata never blocks execution. Ergo accepts or
  rejects the command.
- `timeout_ms` defaults to `10000` and is capped by
  `limits.max_command_timeout_ms`.

The result is the common command envelope.

## Event delivery

### `irc.events.read`

Input contains:

- `agent_id`;
- optional last-consumed `cursor`;
- bounded `limit`;
- optional `command_id`, `class`, `target`, `direction`, `origin`, and
  `verbosity` filters;
- `wait_ms`, where zero is non-blocking and a positive value is bounded long
  polling.

Output contains `stream_id`, `requested_cursor`, `status`, `oldest_available`,
`latest`, ordered `events`, and `next_cursor`. Status is `current`,
`stream_reset`, or `event_gap`. Filters affect returned records, not cursor
ownership or journal retention. `limit` defaults to `100` and must be between
1 and `limits.max_event_page_size`; `wait_ms` is capped by
`limits.max_event_wait_ms`. See [EVENTS_AND_STATE.md](EVENTS_AND_STATE.md).

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

## Resources

Resources are stable per-agent URIs. They are in-memory snapshots, not durable
storage.

### `irc://agents/{agent_id}/status`

Contains the same connection/protocol summary as `irc.status`, including
reconnect state and stable resource links.

### `irc://agents/{agent_id}/protocol`

Contains exact capability lifecycle state, raw and parsed ISUPPORT, the command
catalog and compatibility grades, cached HELP data, active protocol limits,
and locally implemented CTCP/DCC features kept separate from IRC CAP. The full
shape is defined in [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).

### `irc://agents/{agent_id}/state`

Contains best-effort own identity, connection, joined-channel, topic, mode,
membership, and monitored-presence state. It includes snapshot time and the
event cursor through which reduction is complete.

### `irc://agents/{agent_id}/motd`

Contains status (`received`, `not_available`, or `not_received`), ordered lines,
joined text, raw MOTD numerics or 422, receive time, and source (`initial`,
`reconnect`, or `query`). The gateway transports the MOTD without interpreting,
filtering, or replacing its instructions.

### `irc://agents/{agent_id}/events`

Contains stream ID, oldest/latest cursors, retained count and byte use, a small
recent window, and instructions for `irc.events.read`. It is not a substitute
for cursor consumption.

### `irc://agents/{agent_id}/dcc`

Contains all retained offered, connecting, active, transferring, completed,
rejected, cancelled, and failed sessions. See [DCC.md](DCC.md).

### `irc://agents/{agent_id}/channels/{encoded_channel}`

An on-demand resource template for a channel snapshot: case-preserved name,
topic and metadata, known modes and values, members, and membership prefixes.
Snapshot time and reducer cursor are available in the parent state resource.
Channel changes update expanded resource URIs; they do not churn the resource
list.

## Resource updates and subscriptions

Material changes emit `notifications/resources/updated` for affected stable
URIs. Event-resource notifications are coalescing wake-up signals; terminal
DCC transitions and new MOTDs must be signaled promptly. Notifications do not
contain a durable consumption position and never replace `irc.events.read`.

The same `rmcp` subscription listener is used by both transports. Whether an
MCP host wakes or invokes an LLM after a notification is host behavior, not a
gateway guarantee.

## Stable surface summary

The complete stable tool list is:

```text
irc.connect                 irc.disconnect             irc.status
irc.join                    irc.part                   irc.send
irc.history                 irc.query                  irc.execute
irc.events.read             irc.dcc.chat.open          irc.dcc.chat.send
irc.dcc.send                irc.dcc.accept             irc.dcc.reject
irc.dcc.cancel              irc.dcc.list
```

Complete IRC command coverage belongs in `irc.execute` and the lossless event
stream, not in a dynamically changing MCP tool list.
