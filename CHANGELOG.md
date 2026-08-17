# Changelog

Notable user-facing changes are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Semantic
Versioning. While the version is below `1.0.0`, a minor bump may contain
breaking changes to the CLI, configuration, or MCP surface.

## [Unreleased]

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

### Fixed

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

[Unreleased]: https://github.com/GrantSparks/rmcp-irc/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/GrantSparks/rmcp-irc/releases/tag/v0.1.0
