# Architecture

## Runtime overview

Both MCP transports use the same service and gateway:

    MCP transport
      └─ IrcMcpService
          └─ Arc<Gateway>
              └─ AgentActor (one IRC identity and socket writer)
                  ├─ IRC framing, discovery, and correlation
                  ├─ reduced state and bounded event journal
                  ├─ reconnect and history recovery
                  └─ DCC sessions and direct streams

Streamable HTTP creates lightweight request handlers around a shared
`Gateway`. Stdio creates the same service for the process lifetime. Transport
handlers route requests; IRC protocol and identity state remain inside the
agent actors.

## Identity

| Term | Meaning |
| --- | --- |
| MCP client | A local or internal-network caller. |
| Caller owner | An authenticated HTTP bearer identity, or the single local identity used by stdio and by a trusted HTTP endpoint with no configured credentials. Resolved per request; never inferred from self-declared client metadata. |
| `agent_id` | A process-local routing handle for an IRC identity created by `irc.connect`; it is not an account or credential. |
| IRC connection | The current TCP/TLS connection owned by an agent actor. |
| Command ID | A gateway-generated identifier for one outbound operation. |
| IRC label | An opaque value used to correlate replies with a command. |
| Event cursor | A `(stream_id, sequence)` position in one agent's journal. |
| DCC session ID | A process-local handle for one direct chat or transfer. |

Every operation after `irc.connect` includes an explicit `agent_id`. The
gateway never confuses caller identity with IRC identity: `agent_id` still
selects the IRC actor. On shared HTTP, however, each agent and watch handle is
bound to the caller owner that created it. Other owners cannot list, read,
subscribe to, or operate that handle; an unauthorized handle is reported the
same way as a missing one. Stdio has one trusted local owner.

## Component responsibilities

| Component | Responsibility |
| --- | --- |
| MCP service | Tool/prompt schemas, caller authorization, resource URIs, structured results, progress notifications, task creation, and scoped resource notifications. |
| Gateway | Agent-handle lookup, publication, removal, process-wide agent limits, and the shared task ledger. |
| Agent actor | IRC registration, the exclusive socket writer, command collection, state, journal, reconnects, and DCC ownership for one identity. |
| IRC modules | Framing, wire data, encoding, capability discovery, batches, correlation, and semantic projection. |
| DCC modules | CTCP negotiation, direct sockets, streaming, and session lifecycle. |
| Configuration | Endpoint settings, credential references, onboarding defaults, and operational limits. |

The gateway is intentionally in-memory. Agent handles, state, event cursors,
pending commands, DCC sessions, and task handles do not survive process restart.

Everything shared between requests lives on the gateway rather than on the MCP
handler, because Streamable HTTP constructs a fresh handler for every request.
The task ledger is the clearest case: a task created while answering one request
is resolved, updated, and cancelled by requests that arrive later, each with its
own handler instance.

## Agent lifecycle

1. `irc.connect` validates input and creates a provisional actor.
2. The actor opens the configured TCP or TLS connection.
3. It performs CAP discovery and guest `NICK`/`USER` registration, using PASS
   or SASL only when configured.
4. It resolves nickname collisions using caller fallbacks and bounded suffix
   attempts.
5. It waits for `RPL_WELCOME` and a complete MOTD response.
6. It joins requested channels and finishes state synchronization.
7. The gateway publishes the handle and returns the connection result.
8. `irc.disconnect` removes the handle, attempts `QUIT`, closes DCC sessions,
   and stops the actor.

A failed provisional connection never publishes a usable handle.

Steps 2 through 6 all happen inside the single `irc.connect` call, bounded by
`onboarding.connect_timeout_ms`. A caller that supplies a `progressToken` sees
each of them reported as `notifications/progress`; the actor publishes stages
over a bounded channel that the tool drains while awaiting registration, since
the actor itself has no MCP peer. Registration and autojoin are reported
separately because they are different facts. This is a single attempt: the
reconnect backoff loop exists only after step 7, so a connect never outlives its
own request and is never answered with a task handle.

## Command path

Tools create structured outbound messages rather than raw IRC lines. The actor
validates and encodes each message, registers any reply collector, and writes
through its exclusive bounded writer. Registration and `PONG` traffic receive
priority over ordinary commands.

When `labeled-response` is negotiated, unique labels allow concurrent replies
and batches to be routed independently. Without labels, reply-bearing commands
are serialized conservatively because generic error numerics can be
ambiguous. Commands that do not wait for replies may remain concurrent.

Every inbound and outbound IRC line is added to the event journal even when it
also completes a command. Cancelling an MCP request stops waiting but cannot
undo a command already written to IRC.

## Receive path

    framed bytes
      → owned wire message
      → batch and correlation routing
      → optional semantic projection
      → reduced state
      → bounded event journal
      → resource notification

The owned wire message is retained before semantic interpretation. Unknown
commands, numerics, tags, batch types, capabilities, and ISUPPORT tokens remain
observable. A command collector never consumes its replies exclusively.

## Events and state

Each actor journal has a random stream ID, a monotonic sequence, and configured
count and byte limits. Readers supply their own cursor. A wrong stream reports
`stream_reset`; an evicted position reports `event_gap`. Slow readers cannot
block IRC input, command completion, or other readers.

Reduced state is an advisory projection of events. Each snapshot records the
cursor through which it was built. Callers that require an authoritative
current answer use `irc.query`.

See [EVENTS_AND_STATE.md](EVENTS_AND_STATE.md) for the event envelope, cursor
rules, and reduced state.

## Bounds and concurrency

The process and each actor enforce configured bounds for:

- active agents;
- queued and pending commands;
- batches and replies retained per collector;
- IRC line size;
- event count and serialized bytes; and
- DCC sessions, listeners, queues, and transfer buffers.

Exhaustion returns an explicit error or event gap. HTTP handlers do not own
actors, sockets, cursors, or identity, and a long poll releases cleanly when
its MCP request is cancelled.

## DCC

CTCP offers pass through the normal IRC receive path and create session records
and semantic events. After negotiation, the agent's DCC manager owns the direct
sockets. Chat lines and transfer progress enter the journal; file contents
flow directly between disk and the socket through a bounded buffer.

Filesystem authority for incoming files is configuration and only
configuration: named receive roots are the boundary, and a tool call chooses a
root name plus a relative destination. Accepting one resolves that choice into a
directory the process holds open, which is what the transfer receives — so no
resolved path is ever passed between layers as an argument, and the write cannot
be redirected after the confinement decision was made.

See [DCC.md](DCC.md) for network locality, session states, reverse connections,
resume behavior, and destination handling.

## Reconnect and shutdown

On IRC socket loss, the actor retains its handle, journal stream, remembered
joins, and advisory state while it reconnects with bounded exponential
backoff. After registration it refreshes discovery and MOTD data, restores
joins, resynchronizes channel state, and requests recoverable history. Only
exact server message IDs are used to suppress duplicates; transient protocol
and presence events cannot be recovered.

Stdio exits on EOF. HTTP stops on process cancellation. Actor shutdown attempts
`QUIT`, closes direct sockets, marks observable DCC sessions terminal, and
invalidates the agent handle. Credentials are resolved only for connection
setup and are excluded from logs, events, resources, and tool results.
