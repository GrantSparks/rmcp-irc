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
- Incoming filenames are a single path component, and receive destinations are
  confined beneath one of the configured named receive roots.
- Filesystem authority is server-owned and the destination choice is
  caller-owned: configuration names the roots, and a tool call names which root
  and where beneath it. A call cannot name a directory, and no resolved path
  crosses a process boundary as an argument.
- Every transfer destination and conflict behavior is explicit unless safe
  automatic-accept defaults apply.

## Receive roots

`[[dcc.receive_roots]]` declares the named directories incoming files may be
written into; see [CONFIGURATION.md](CONFIGURATION.md). They are the hard
security boundary. `download_directory` seeds one root named `downloads` when no
root is declared, so existing configurations keep exactly the root they had, and
declaring roots replaces that default entirely.

Confinement is enforced by resolution, not by comparing strings. The gateway
opens the configured root as a directory handle, then walks a relative
destination one component at a time, opening each directory relative to the
handle for the directory above it and refusing to traverse a symbolic link. The
transfer receives that held directory plus a leaf name, and every later
operation — the existence probe, the temporary file, the commit rename, failure
cleanup — addresses an entry inside it. Consequences worth stating plainly:

- A symbolic link at any component of the destination is refused, not followed.
- A symbolic link *at the destination name* is treated as an occupied name by
  conflict handling. `fail` stops, `rename` moves aside, and `replace` replaces
  the link itself; none of them writes through it.
- Replacing a directory after it was resolved cannot redirect the write, because
  nothing resolves the path a second time. The write lands beneath the chosen
  root or it fails.
- `..`, an absolute path, and a platform path prefix are refused by name before
  any directory is created.

On Unix this uses directory-relative `openat`/`mkdirat`/`statat`/`renameat`
against held descriptors. Platforms without directory-relative operations fall
back to open-then-verify — each created directory is re-resolved and confirmed to
be the one beneath the root — which refuses the same links without the
capability guarantee that no second resolution happens at all.

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
  "receive_root": null,
  "receive_path": null,
  "transferred_bytes": 0,
  "total_bytes": 1204,
  "created_at": "2026-08-17T10:00:00Z",
  "updated_at": "2026-08-17T10:00:00Z",
  "error": null
}
```

`kind` is `chat` or `send`; `direction` is `inbound` or `outbound`.

`local_path` is a path on the gateway host: the source of an outgoing SEND, or
the committed destination of an accepted incoming one. For an accepted incoming
SEND, `receive_root` and `receive_path` restate that destination in the terms the
caller chose it — a configured root name and a path relative to that root — which
is the pair a caller can reason about without any authority over host paths. Both
are set when the offer is accepted and restated on completion, because a `rename`
conflict settles the committed name only then. They are absent for CHAT and for
outgoing sessions.

Lifecycle states are:

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

For a client that declared no tasks extension the tool returns after the offer
is written, with a session snapshot; progress, completion, cancellation, and
failure are then asynchronous. When the request's client capabilities declare
`io.modelcontextprotocol/tasks`, the server answers with a task handle instead:
task status follows the transfer's state and byte progress, task cancellation
cancels the DCC session, and the terminal result contains the final session and
a native link to `irc://agents/{agent_id}/dcc/{dcc_session_id}`. The choice is
the server's and depends only on that declaration — there is no per-call opt-in
key. Successful completion requires the negotiated DCC acknowledgment behavior,
not merely end-of-file on the source read.

## `irc.dcc.accept`

Input contains:

- `agent_id` and offered `dcc_session_id`;
- `root` for SEND: the name of a configured `dcc.receive_roots` entry;
- `destination_path` for SEND: a **relative** path beneath that root;
- `conflict`: `fail`, `replace`, or `rename` for an existing SEND destination.

Both `root` and `destination_path` are omitted for CHAT, which writes nothing;
supplying either on a CHAT offer is refused rather than ignored.

### Resolving the destination

| `root` | `destination_path` | Result |
| --- | --- | --- |
| given | given | Validated against configuration and accepted. |
| given | omitted | The offered filename, already reduced to one ordinary path component. |
| omitted | either | With exactly one configured root, that root. With several, the caller is asked. |
| any | absolute | Refused. The root name carries the filesystem authority; a path that supplied its own root is claiming authority configuration never granted. |
| any | any, unknown root | Refused, listing the configured root names. |

An offer that advertised no filename requires `destination_path`.

### Asking the caller which root

When several roots are configured and the call named none, the gateway cannot
choose without inventing an authority decision. If the calling request declared
form elicitation, the tool answers with an MCP `input_required` result rather
than a session:

```json
{
  "resultType": "input_required",
  "inputRequests": {
    "dcc_destination": {
      "method": "elicitation/create",
      "params": {
        "mode": "form",
        "message": "Choose where to receive report.txt (1204 bytes) offered by alice.",
        "requestedSchema": {
          "type": "object",
          "properties": {
            "root": { "type": "string", "enum": ["downloads", "media"], "default": "downloads" },
            "destination_path": { "type": "string", "default": "report.txt" }
          },
          "required": ["root"]
        }
      }
    }
  },
  "requestState": "rs1.…"
}
```

The client gathers the answer and re-sends the **same** `irc.dcc.accept` call
with its original arguments plus `inputResponses` keyed `dcc_destination` and the
`requestState` echoed byte-for-byte:

```json
{
  "name": "irc.dcc.accept",
  "arguments": { "agent_id": "…", "dcc_session_id": "dcc-…", "conflict": "fail" },
  "inputResponses": {
    "dcc_destination": {
      "action": "accept",
      "content": { "root": "media", "destination_path": "august/report.txt" }
    }
  },
  "requestState": "rs1.…"
}
```

The answer is validated exactly like an explicit argument: same configuration
lookup, same refusal of an absolute path, and additionally checked against the
set of roots the question actually offered.

`requestState` is opaque and integrity-protected. It is bound to the calling
caller identity, to this tool call's arguments, and to a short expiry, and the
offer it was minted for is re-checked by peer and advertised filename on
redemption. A state from another caller, for another offer, for altered
arguments, expired, or modified in any way is refused as a tool error that leaves
the offer untouched; only the offer's own `offer_ttl_ms` retires it. There is no
separate one-time-redemption bookkeeping, because a second acceptance of the same
offer is already refused by the session lifecycle.

`action: "decline"` or `"cancel"` is terminal for the call: the tool returns an
error result with `kind: "declined"`, nothing is written, and the offer stays
pending until it expires or is accepted or rejected.

A request that declared no elicitation support gets a structured error instead of
a question, carrying `receive_roots` and `default_destination_path` so its next
attempt can name both explicitly.

A caller that also declared the tasks extension gets this question *before* any
task exists: an accept that still needs a destination answers `input_required`
with no task handle, and the handle appears on the retry that settles it. The
rules this exchange shares with the gateway's other input round trips — stable
keys, re-asking a partial answer, in-band refusals, and the `requestState`
posture — are in
[MCP_API.md](MCP_API.md#input-round-trips).

### Acceptance and conflict behavior

`fail` leaves the existing file untouched and the session fails before writing
the destination. `replace` does not replace the destination until the transfer
completes; failure cleanup does not delete the pre-existing file.
`rename` chooses a non-existing sibling name deterministically and reports the
actual destination in `local_path` and `receive_path`. The gateway receives into a
temporary file inside the resolved directory and renames it there on success, so a
partial file is not presented as a completed transfer and the commit cannot be
re-routed.

For CHAT, acceptance establishes the direct connection. For SEND, acceptance
resolves the destination beneath the chosen root, creating any directories the
relative path names inside it, and starts a bounded streamed transfer. The default
result is the updated session snapshot, including `receive_root` and
`receive_path`. When the request declares the tasks extension in its `_meta`
client capabilities, the server instead returns a task handle and follows the
same progress, cancellation, terminal-result, and native-resource-link behavior
as task-augmented `irc.dcc.send` — a task-augmented call must supply `root` and
`destination_path` explicitly where the server cannot choose, because a task
handle is created only for work that is already fully decided.

The gateway never creates a directory tree from a peer-supplied offer: an offered
filename is a single path component, and only a caller's explicit
`destination_path` can name directories to create.

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
