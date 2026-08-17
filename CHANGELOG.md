# Changelog

Notable user-facing changes are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Semantic
Versioning. While the version is below `1.0.0`, a minor bump may contain
breaking changes to the CLI, configuration, or MCP surface.

## [Unreleased]

## [0.2.0] - 2026-08-17

### Added

- Tool results that expose follow-up state now include native MCP
  `resource_link` content blocks alongside backward-compatible structured URI
  fields, covering connect, status, join, history, events, channel mutations,
  and DCC operations.
- Stable command-specific tools and typed result projections now cover WHOIS,
  NAMES, LIST, MODE and HELP reads, topic reads/changes, nickname and away
  changes, KICK, INVITE, MONITOR updates, and MODE mutations; `irc.query` and
  `irc.execute` remain available as compatibility and expert fallbacks.
- Four fixed MCP prompts guide connect, mentions-watch, join, and
  summarize/respond workflows while preserving the boundary between realtime
  host delivery and host-triggered model execution.
- Negotiated modern-message support now includes typed reaction, redaction,
  read-marker, and typing tools plus inbound semantic projections. Reaction
  and typing tags honor `CLIENTTAGDENY`, typing is throttled per target, and
  redaction/read-marker capabilities are requested only when advertised.
- Watch handles. `irc.watch.create` registers a server-side selection over one
  guest's event stream — by target, event class, addressed-to-me, or
  direction — and returns a native resource link. Reading that resource yields
  everything matching since the previous read and advances the watch, so
  subscribing and reading is a complete delivery loop that needs no cursor
  argument and no tool call. `irc.watch.close` releases the handle, and
  `irc.events.read` remains available as a replay and compatibility fallback.
- Conversation-oriented resources: an inbox of everything addressed to the
  agent, a transcript per channel or peer, and channel members and topic as
  separately addressable resources. These carry compact records — who spoke,
  where, when, and what they said — rather than lossless protocol detail.
- A wire resource holding the lossless parsed protocol records and refused
  lines, so diagnostics no longer share a resource with model context.
- Resource annotations. Every resource and resource link now publishes an
  audience, a priority, and a last-modified hint from one shared descriptor
  catalog, so what a tool link says about a URI and what `resources/list` says
  about the same URI cannot drift apart.
- DCC transfers as MCP tasks. `irc.dcc.send` and `irc.dcc.accept` accept the
  tasks extension on the call and then run as a task for the whole transfer,
  reporting byte progress, honouring `tasks/cancel` by cancelling the session,
  and settling with the terminal session and a link to it. Without the
  extension they behave exactly as before. Each direct session is now
  addressable at `irc://agents/{agent_id}/dcc/{session_id}`.
- Caller ownership for handles. Agent and watch handles are now bound to the
  caller that created them: the resource catalog lists only what that caller
  owns, and naming somebody else's handle fails exactly as naming a handle
  that does not exist, so the tool surface cannot be used to discover them.
  HTTP callers are identified by `Authorization: Bearer` when
  `--http-bearer-token` is configured and by MCP session otherwise; stdio has
  one local caller that owns everything, so its behaviour is unchanged.
- HTTP responses are marked `Cache-Control: private, no-store`, since every
  response carries whatever the calling identity owns.

### Fixed

- Watch and resource notifications that are dropped because a subscriber fell
  behind now trigger an explicit resynchronization — the resource list and
  every live URI are republished — instead of being skipped silently and
  leaving a subscriber believing its last read was still current.
- The event resource's recent window read from the oldest retained cursor, so
  once more than one page of events was retained it was not the newest window
  at all.
- Channel state changes now notify the expanded
  `irc://agents/{agent_id}/channels/{channel}` resource and its member and
  topic resources, which previously changed without ever notifying their own
  subscribers.
- Inbound traffic no longer invalidates the aggregate state and status
  resources unless their contents actually changed, and a busy channel no
  longer invalidates a quiet one beside it.
- `irc.connect` and `irc.status` now default to compact MOTD results that keep
  the joined instruction text visible without repeating its line array and
  expanded raw numerics; `result_detail: "full"` restores the legacy inline
  form, and the MOTD resource remains lossless.
- Successful `irc.history` results now default to one authoritative event form
  instead of repeating the same raw and semantic records in the command
  envelope; callers can opt into `full`, while failures always retain complete
  diagnostics.
- Correlated join, part, send, query, and execute tools now accept a
  backward-compatible `result_detail` control: the default `full` form is
  unchanged, while `compact` keeps lossless replies and sets their duplicate
  semantic projection to `null`.
- Rejected or timed-out `irc.send` and `irc.history` calls no longer pair
  `isError: true` with success-phrased text content.
- Event-delivery guidance now states that MCP clients without resource
  subscription support must keep a cursor-based `irc.events.read` long poll
  active instead of waiting for unavailable wake-up notifications.
- TOPIC and MODE mutations and MONITOR status/mutation operations now select
  collectors from their structured parameter shape instead of waiting for
  query-only numerics that never arrive.
- Single-reply, numeric, and echo collectors now retain an entire outer
  labeled-response batch, propagate the parent command correlation to every
  child, and report completion or rejection only when that batch closes.
- Explicit `irc.execute` collection now completes when a multi-message labeled
  response batch closes instead of retaining all replies until the request
  times out.
- Typed ISON, USERHOST, VERSION, and TIME queries now use static single-reply
  collectors, including on servers without `labeled-response` support.
- MCP input schemas now identify where `agent_id` handles come from, publish
  deadline bounds and exact enum tokens, explain event cursor reuse, and state
  which message kinds require text.
- `irc.send` now reserves the `:nick!user@host ` prefix the server prepends
  when it relays a message, so text that previously overran the 512-byte body
  budget is split instead of being silently truncated by the server. The self
  JOIN echo is recorded as the identity's hostmask so the reservation matches
  what the server actually sends.

## [0.1.0] - 2026-08-17

Initial public release.

### Added

- Stdio and Streamable HTTP MCP transports with the same IRC and DCC tool and
  resource surface.
- Native TCP/TLS guest connections, CAP/SASL/ISUPPORT/HELP discovery,
  reconnects, state resynchronization, and history recovery.
- Lossless IRC events, structured command encoding and correlation, and
  bounded caller-owned cursor journals.
- Ordinary and reverse DCC CHAT/SEND with streamed transfers, resume support,
  progress reporting, cancellation, and safe destination handling.

[Unreleased]: https://github.com/GrantSparks/rmcp-irc/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/GrantSparks/rmcp-irc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/GrantSparks/rmcp-irc/releases/tag/v0.1.0
