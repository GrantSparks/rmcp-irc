# Events, cursors, and reduced state

This document is the normative asynchronous-delivery and state-reduction
contract. IRC is a long-lived event protocol; MCP resource notifications only
signal that something changed. Ordered delivery therefore comes from explicit
positions over each agent's bounded in-memory journal — held by a watch on the
caller's behalf, or by the caller itself through `irc.events.read`.

## Event production rule

Every accepted inbound IRC line produces a semantic event, a wire/protocol
event, or both. Every outbound line is also observable. A reply used to finish
a command collector remains in the journal and must not be consumed
exclusively by that collector.

PING/PONG traffic is journaled like other protocol traffic and can be selected
with explicit class/direction/verbosity filters. No valid unknown command,
numeric, tag, batch type, capability value, or ISUPPORT token is silently
discarded.

DCC control offers arrive through IRC and carry wire data. Direct DCC chat and
transfer lifecycle events have no IRC wire line after negotiation, so their
`wire` field is absent.

## Event envelope

The stable event envelope is:

```json
{
  "cursor": {
    "stream_id": "c6a2...",
    "sequence": 184
  },
  "agent_id": "agent-550e8400-e29b-41d4-a716-446655440000",
  "direction": "inbound",
  "class": "message.channel",
  "origin": "live",
  "verbosity": "semantic",
  "target": "#agents",
  "server_time": null,
  "received_at": "2026-08-17T10:00:00.021Z",
  "correlation": {
    "command_id": null,
    "label": null,
    "role": null
  },
  "semantic": {
    "class": "message_channel",
    "event": {
      "class": "message_channel",
      "source": {
        "name": "alice",
        "user": "u",
        "host": "host",
        "account": null
      },
      "channel": "#agents",
      "text": "hello"
    }
  },
  "wire": {
    "raw": ":alice!u@host PRIVMSG #agents :hello",
    "raw_base64": null,
    "parse_status": "complete",
    "tags": [],
    "prefix": {
      "raw": "alice!u@host",
      "name": "alice",
      "user": "u",
      "host": "host"
    },
    "command": "PRIVMSG",
    "params": ["#agents"],
    "trailing": "hello"
  }
}
```

Field rules:

| Field | Contract |
| --- | --- |
| `cursor` | Position assigned by the owning actor journal. |
| `agent_id` | Explicit guest identity that owns the event. |
| `direction` | `inbound`, `outbound`, or `internal`. |
| `class` | Stable semantic/protocol class used by filters. |
| `origin` | `live`, `history`, `synthetic`, or `gateway`. |
| `verbosity` | `semantic` when a typed projection exists, otherwise `wire`; a record may expose both projections. |
| `target` | Optional case-preserved nickname or channel used by event filters. |
| `server_time` | Server-provided time when available; absent otherwise. Receipt time is never mislabeled as server time. |
| `received_at` | Gateway receipt/creation time in RFC 3339 UTC. |
| `correlation` | Optional command ID, IRC label, and collector role. It survives collector completion. |
| `semantic` | Class-specific structured projection, or null for wire-only events. |
| `wire` | Complete lossless IRC representation when an IRC line exists. |

If `server-time` is unavailable, semantic projections may include a local time
with an explicit `time_provenance: local`; `server_time` remains absent. A
gateway event ID is never presented as a server message ID.

## Required semantic classes

Class names may become more specific while retaining these families:

| Input or transition | Required class or family |
| --- | --- |
| Channel `PRIVMSG` | `message.channel` |
| Private `PRIVMSG` | `message.private` |
| CTCP ACTION | `message.action` |
| `NOTICE` | `message.notice` (CTCP framing remains visible in wire data) |
| Generic `TAGMSG` | `message.tagged`; extension tags remain lossless in `wire` |
| `TAGMSG` with `+draft/react` and `+reply` | `message.tagged` with a typed reaction change and referenced message ID |
| `TAGMSG` with `+typing` | `presence` with typed `active`, `paused`, or `done` state |
| `REDACT` | `message.tagged` with target, message ID, source, and optional reason |
| `MARKREAD` | `protocol.reply` with target and synchronized timestamp or cleared marker |
| CTCP query/reply | `ctcp` |
| `JOIN`, `PART`, `KICK`, `INVITE`, `QUIT` | `membership` |
| `NICK`, `ACCOUNT`, `AWAY`, `CHGHOST`, `SETNAME` | `presence` |
| `MODE`, `TOPIC` | `channel.state` |
| `CAP ACK/NAK/NEW/DEL`, `005` | `protocol.compatibility` |
| `FAIL`, `WARN`, `NOTE`, `ACK`, numerics | `protocol.reply` or a more specific correlated class |
| Completed MOTD sequence | `server.motd` with ordered text and raw replies |
| History/event-playback batch | Normal semantic class with `origin: history` |
| Unknown command, numeric, batch, or semantics | `protocol.unknown` with full wire data |
| Connection transitions | `connection.lifecycle`, with typed state in `semantic` |
| Incoming DCC control | `dcc.chat.offered`, `dcc.transfer.offered`, acceptance/rejection, or negotiation failure |
| Direct DCC data | `dcc.chat.message`, `dcc.connected`, `dcc.transfer.progress`, `dcc.transfer.completed`, `dcc.cancelled`, or `dcc.failed` |

Semantic projection is additive. Unknown tags and fields stay in `wire` even
when the gateway understands the surrounding message.

## Outbound events and echo handling

When `echo-message` is active, the server echo is the canonical semantic sent
event. The outbound write remains observable as a wire-verbosity record and
does not create a second semantic message. Labels and message IDs associate the
echo with its command.

Without `echo-message`, the gateway publishes the outbound semantic message
with `origin: synthetic`; the command outcome is `sent_unconfirmed`. It does
not claim a server-assigned message ID or confirmed delivery. Live and
recovered-history duplicates are suppressed only by exact server message ID;
the gateway never drops a merely similar message.

## Cursor model

Each agent actor owns one bounded `VecDeque` journal with:

- a random `stream_id` created with the actor;
- a monotonically increasing unsigned 64-bit `sequence`;
- configured retained event-count and serialized-byte limits;
- a coalescing wake-up primitive that never stores per-reader position.

The cursor is the pair `(stream_id, sequence)`. Callers supply their last
consumed cursor on every read. The server stores no per-MCP-client cursor and
does not infer one from an HTTP connection, stdio process, or conversation.

### Read behavior

- No cursor starts at the oldest retained event.
- A matching cursor returns later matching events in order.
- An unknown stream or sequence ahead of the current stream returns
  `stream_reset`, plus current bounds.
- A cursor older than the retained range returns `event_gap`, plus current
  bounds.
- After reporting a reset or gap, the same response may deliberately begin at
  the oldest retained event and returns a `next_cursor` for continuation.
- Filters do not advance a hidden cursor. `next_cursor` is explicit and is the
  only position a caller should persist.
- `wait_ms > 0` waits only until an event/resource change or the bounded
  deadline; zero is non-blocking.

A network reconnect does not change `stream_id` while the actor and journal
remain alive. Actor recreation or process restart creates a new stream. Ring
eviction advances the oldest available sequence.

## Retention and backpressure

Both count and serialized-byte bounds must hold. An individual event larger
than the entire byte budget is rejected from the journal with an observable
gateway diagnostic; it must not cause unbounded allocation. File bodies are
never event payloads.

Slow MCP clients do not block IRC reads, PING/PONG, command completion, DCC
streams, or other readers. Resource notifications may coalesce, but journal
records and terminal DCC transitions do not. Falling behind results in an
explicit `event_gap`, never an unbounded per-client buffer.

## History and recoverability

Ergo history can repair only retained chat/state playback supported by the
server. Recovered records use `origin: history`. The gateway never implies that
evicted transient presence, protocol negotiation, direct DCC data, or process
local events were recovered.

Active DCC sessions do not survive actor/process restart. Observable sessions
are marked failed during orderly shutdown; otherwise their old handles simply
become invalid with the old agent stream.

## Reduced state

The actor maintains advisory, best-effort state for:

- connection and registration lifecycle;
- own nickname, account, username/host, real name, modes, and away state;
- latest MOTD, source, and receipt time;
- advertised and negotiated capabilities and all ISUPPORT tokens;
- joined channels;
- topics and topic metadata;
- channel modes and known values;
- members, account/away/host data, and membership prefixes;
- monitored nickname presence;
- DCC session lifecycle and progress.

Name comparison, parsing, and limits use server-advertised `CASEMAPPING`,
`CHANTYPES`, `PREFIX`, `CHANMODES`, `TARGMAX`, `NICKLEN`, channel lengths, and
related ISUPPORT values instead of hard-coded assumptions.

Every state resource includes `snapshot_at` and `through_cursor`, identifying
when it was built and the last event incorporated. State is advisory; callers
use `irc.query` when they require a current authoritative server response.

## Initial synchronization and reconnect

After initial registration and on every reconnect, the actor:

1. completes nickname conflict handling before readiness;
2. collects and publishes the current MOTD;
3. restores remembered joins where possible;
4. resynchronizes channel membership, topics, and modes with explicit queries;
5. requests missed history using the best advertised mechanism;
6. marks recovered events as history and deduplicates exact server message IDs
   against live/reconnect playback;
7. updates affected resources and emits wake-up notifications.

Initial `irc.connect` waits for registration and MOTD completion, but initial
joins and full state resynchronization must not delay its MOTD result
indefinitely. On socket loss, the existing actor remains published in a
`reconnecting` state and uses bounded exponential backoff.

Deduplication is a reconnect-recovery rule, not an explicit-query filter.
Every `irc.history` call projects its complete correlated reply batch into that
call's typed `events`, including message IDs already observed live or returned
by an earlier explicit history call. Concurrent history calls select events by
their owning `command_id`; they never share a cursor-only projection window.

## Resource notification contract

`notifications/resources/updated` is a hint that a stable URI changed. It does
not carry a durable event position and is not an alternative event transport.
Clients respond by reading the indicated resource.

### Watches

A watch is the intended way to consume events. `irc.watch.create` registers a
selection — targets, event classes, addressed-to-me, direction — and returns
`irc://watches/{watch_id}`. Subscribing to that URI and reading it on each
notification is a complete delivery loop:

- the notification is evaluated against the watch's own filter, so a watch on
  one channel is not woken by unrelated traffic;
- each read returns everything matching since the previous read and advances
  the watch, so no cursor argument is needed;
- the read reports `status`, which is `current` unless records were lost, in
  which case the reader is told explicitly rather than silently restarted;
- `irc.watch.close` releases the handle.

`irc.events.read` remains available for replay and for clients without
subscription support. Such a client must keep a bounded long poll active,
immediately continuing from each returned `next_cursor`. This fallback uses
the same durable cursor contract.

### What realtime delivery does and does not mean

The gateway can deliver a notification to a capable MCP **host** as soon as
something happens. It cannot make that host start a model turn. MCP has no
mechanism by which a server requires a client to invoke its model, so:

- *realtime delivery* — the host learns immediately, and a subsequent read
  returns the new records with no polling interval in between — is what this
  gateway provides;
- *autonomous participation* — the model itself waking on a message and
  answering it — additionally requires the host or runtime to schedule a turn
  when a subscribed resource changes.

A host that does not schedule turns will still see everything, at whatever
point it next runs the model. Nothing is lost in the meantime: the watch holds
its position, and a read after any delay returns the backlog with an explicit
`status` if the retained window was exceeded.
