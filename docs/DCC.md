# DCC direct data plane

DCC is a first-class, in-memory peer data plane. Its offer and negotiation are
transported inside IRC CTCP messages; established DCC CHAT and SEND traffic
then bypasses Ergo over direct TCP sockets owned by the initiating agent's DCC
manager.

The gateway supports ordinary and reverse DCC CHAT/SEND and interoperable
ACCEPT/RESUME when required by the selected resume flow. Implemented CTCP/DCC
variants appear in the protocol resource as local features, never as
Ergo-advertised IRC capabilities.

## Locality and trust boundary

- In stdio mode, paths and listeners normally belong to the local MCP client
  machine running the gateway process.
- In HTTP mode, paths and listeners belong to the central gateway host, not the
  remote MCP caller's machine.
- Peer nickname/account metadata is informative, not an authorization source.
- Incoming negotiation must be a direct message to the actor's current nick.
  Private and local peer addresses are rejected unless explicitly enabled for
  a trusted local/container network.
- Incoming filenames are a single path component and receive destinations are
  confined beneath the configured download root.
- Every transfer destination and conflict behavior is explicit unless safe
  automatic-accept defaults apply.

## Session model

Every offer creates an opaque process-local `dcc_session_id` owned by one
`agent_id`. A session snapshot contains:

```json
{
  "id": "dcc-...",
  "kind": "send",
  "direction": "inbound",
  "peer": "alice",
  "state": "offered",
  "reverse": false,
  "token": null,
  "endpoint": null,
  "filename": "report.txt",
  "local_path": null,
  "transferred_bytes": 0,
  "total_bytes": 1204,
  "created_at": "2026-08-17T10:00:00Z",
  "updated_at": "2026-08-17T10:00:00Z",
  "error": null
}
```

`kind` is `chat` or `send`; `direction` is `inbound` or `outbound`. Lifecycle
states are:

```text
offered -> connecting -> active | transferring -> completed
    \-----------> rejected | cancelled | failed
```

Invalid transitions are tool execution errors. Terminal sessions may remain in
memory for observation, but are removed when the owning actor is destroyed.
Session handles never survive actor/process restart.

## Incoming offers

The IRC receive path parses CTCP without removing the original wire text. A
valid incoming DCC offer:

1. remains observable as its IRC/CTCP wire event;
2. creates a bounded session record;
3. emits `dcc.chat.offered` or `dcc.transfer.offered`;
4. updates `irc://agents/{agent_id}/dcc`;
5. waits for explicit accept/reject unless automatic acceptance for that DCC
   kind is enabled.

Malformed offers produce visible negotiation-failure events and never open a
socket. Unknown or unsupported DCC variants remain observable as CTCP/wire data
and are graded `passthrough` or `unavailable` in the protocol catalog.

`automatic_accept_chat` and `automatic_accept_send` are separate and disabled
by default. SEND uses the configured download directory and `fail` conflict
behavior. Automatic acceptance never replaces an existing file.

## `irc.dcc.chat.open`

Input:

| Field | Required | Meaning |
| --- | --- | --- |
| `agent_id` | yes | Owning guest identity. |
| `target` | yes | Peer nickname. |
| `reverse` | no | Prefer reverse DCC when true; otherwise ordinary negotiation is preferred. |

The manager reserves a listener/connection slot, builds an ordinary or reverse
DCC CHAT offer, and sends it through CTCP. The tool returns the session snapshot
once the offer is written; it does not wait for the peer. Connection progress
is delivered through DCC events and the DCC resource. The offer's outbound IRC
wire event remains observable; the DCC tool result does not claim peer
acceptance. An ordinary offer's listener remains available for `offer_ttl_ms`;
the shorter `connect_timeout_ms` applies only when this gateway actively opens
a TCP connection to an endpoint advertised by the peer.

## `irc.dcc.chat.send`

Input contains `agent_id`, `dcc_session_id`, and `text`. The session must be an
established CHAT session owned by the agent. One logical line is encoded and
written through the bounded direct-socket writer. CR, LF, NUL, and payloads
exceeding the direct-chat limit are rejected rather than creating extra lines.

Both inbound and outbound lines emit `dcc.chat.message` events with peer,
direction, text, session ID, and timestamps. Since the data bypasses Ergo,
there is no IRC `wire` object or server delivery acknowledgment after
negotiation.

## `irc.dcc.send`

Input:

| Field | Required | Meaning |
| --- | --- | --- |
| `agent_id` | yes | Owning guest identity. |
| `target` | yes | Peer nickname. |
| `source_path` | yes | Local readable file on the gateway host. |
| `filename` | no | Advertised filename; defaults to the source basename. Path separators are not advertised. |
| `reverse` | no | Prefer reverse DCC when true. |

Before sending an offer, the gateway validates that the source is a regular
file, records its size, reserves a bounded DCC slot, and prepares the listener
or reverse token. The transfer task opens the file and reports any permission
or later I/O failure through session state/events. It streams bytes through the
configured buffer; the body is never loaded wholly into memory and never
appears in MCP results, logs, resources, or events.

The tool returns after the offer is written with a session snapshot. Progress,
completion, cancellation, and failure are asynchronous. Successful completion
requires the negotiated DCC acknowledgment behavior, not merely end-of-file on
the source read.

## `irc.dcc.accept`

Input contains:

- `agent_id` and offered `dcc_session_id`;
- `destination_path` for SEND, omitted for CHAT;
- `conflict`: `fail`, `replace`, or `rename` for an existing SEND destination.

`fail` leaves the existing file untouched and the session fails before writing
the destination. `replace` does not replace the destination until the transfer
completes; failure cleanup does not delete the pre-existing file.
`rename` chooses a non-existing sibling path deterministically and reports the
actual path. The gateway receives into a temporary file and atomically renames
it on success where the filesystem permits, so a partial file is not presented
as a completed transfer.

For CHAT, acceptance establishes the direct connection. For SEND, acceptance
resolves a relative path beneath `download_directory`, rejects a path whose
resolved parent escapes that root, and starts a bounded streamed transfer. An
absolute path is accepted only when it is already below the same root. The
result is the updated session snapshot.

The destination's parent directory must already exist and be writable by the
gateway process. The gateway never creates an implicit directory tree from a
peer-supplied offer.

## `irc.dcc.reject`

Input contains `agent_id` and offered `dcc_session_id`. It marks the session
`rejected`, closes reserved listener/socket state, emits a terminal event, and
updates the DCC resource. Ordinary DCC defines rejection as ignoring the offer,
so this terminal state is local: the offerer cannot distinguish rejection from
an ignored or lost offer and eventually records an ambiguous offer-expiry
failure. The gateway does not invent a peer-control reply or treat a nickname
as authorization for one.

## `irc.dcc.cancel`

Input contains `agent_id` and `dcc_session_id`. It may cancel an offered,
connecting, active, or transferring session. The manager closes direct sockets
and file handles, performs safe partial-file cleanup, marks the session
`cancelled`, and returns its final snapshot. Cancelling an already terminal
session is idempotent and returns that terminal state. Closing an established
ordinary DCC socket has no interoperable terminal-reason frame. The peer can
observe EOF (or an incomplete SEND), but cannot truthfully infer whether the
local cause was cancellation, shutdown, or another clean CHAT close; the
gateway therefore preserves `cancelled` locally without fabricating that state
for the peer.

## `irc.dcc.list`

Input contains `agent_id` and may include state/kind/peer filters. It returns
all matching active and recently completed in-memory sessions in stable
handle order. Every record includes kind, peer, direction, endpoint,
filename/path where safe, byte counts, timestamps, state, and terminal error.
At most `max_sessions` recent terminal sessions are retained per agent.

The same collection is readable at `irc://agents/{agent_id}/dcc`.

## Reverse DCC and resume

Reverse DCC is used when the offering side cannot accept an inbound direct
connection. The gateway generates a bounded opaque token, advertises the
variant, matches the peer response to the pending session, and then connects in
the reversed direction. Tokens are process-local correlation values, not
authorization credentials.

DCC RESUME/ACCEPT validates the requested offset against the local partial file
and advertised total size, and byte accounting resumes from that offset.
Cumulative 32-bit DCC acknowledgements are expanded monotonically across wrap
boundaries; partial and duplicate ACKs are tolerated, while ACKs beyond bytes
written fail visibly. Unsupported or ambiguous negotiation never silently
restarts or appends.

## Events and resource updates

Required event classes include:

- `dcc.chat.offered`, `dcc.transfer.offered`, and negotiation failures;
- `dcc.connected`;
- `dcc.chat.message` for both directions;
- `dcc.chat.closed` when the peer closes a chat cleanly;
- `dcc.transfer.progress` with transferred and total bytes;
- `dcc.transfer.completed`;
- `dcc.rejected`, `dcc.cancelled`, and `dcc.failed`.

Progress records and resource notifications may coalesce to protect the event
journal. Terminal transitions may not be lost or indefinitely delayed. Events
contain metadata and byte counts only, never file bodies.

## Bounds, timeouts, and shutdown

Each agent enforces the configured session and per-peer offer counts, offer
TTL, port interval, active-connect deadline, idle deadline, transfer buffer,
and maximum receive size. `offer_ttl_ms` covers the wait on a listener-backed
offer; `connect_timeout_ms` covers an active TCP connect to a peer endpoint.
The byte limit also applies when a peer omits the SEND size. Exhaustion returns
a bounded resource error without dropping unrelated sessions.

On `irc.disconnect` or orderly process shutdown, active DCC sessions become
cancelled or failed as appropriate, terminal events are emitted when still
observable, and direct sockets close. A network reconnect to Ergo does not
necessarily interrupt an already established DCC socket, but new CTCP control
cannot be assumed available until the IRC actor is ready again.

## CTCP discovery

When enabled, CTCP `CLIENTINFO` advertises the implemented CTCP commands,
including DCC. CTCP query/reply support and DCC variants are reported under
`local_ctcp` and `local_dcc` evidence in the protocol resource, clearly
separate from CAP LS results.
