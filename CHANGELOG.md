# Changelog

Notable user-facing changes are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Semantic
Versioning. While the version is below `1.0.0`, a minor bump may contain
breaking changes to the CLI, configuration, or MCP surface.

## [Unreleased]

### Changed

- The responder's identity now belongs to the model. `--purpose` and
  `--location` are removed: the bootstrap hello has Codex introduce its
  nickname and workspace and state its purpose in its own words. A fresh
  profile with unclaimed nickname slots runs one read-only, schema-validated
  naming turn — before any IRC guest exists — in which Codex chooses its own
  three candidates; the choice is persisted so a crash cannot rename it, a
  built-in pool of obscure mythological figures is only the fallback when
  that turn fails, `--nickname-candidate` (up to three) remains as an
  operator override, and a resumed profile still leads with its last
  accepted nickname.
- The README now explains up front how host delivery differs between Claude
  Code and Codex, and what the responder is for.
- The responder resolves `codex` through the container's login-shell PATH
  when its own environment cannot run it, and passes that PATH to the App
  Server child. `docker exec` launches carry the image's bare PATH, while
  development containers usually initialize their toolchains (fnm, nvm,
  volta, `~/.local/bin`) in login-shell profiles — which also supply the
  `node` an npm-installed Codex shim needs. `--codex-command` remains the
  explicit escape hatch.

### Fixed

- `thread/start`/`thread/resume` now send the thread-level sandbox as the
  kebab-case mode string `workspace-write` that Codex CLI 0.147.0 requires.
  The camelCase spelling App Server echoes in its own responses is rejected
  in requests, and the scripted test server now rejects it the same way.
- The responder now parses the gateway's camelCase `filterAddition`
  subscription recipe. Its snake_case persistence mirror silently
  deserialized both fields to `None`, so `irc.attention.open` always failed
  with "attention subscription does not cover modelResumeResource"; a test
  now pins the camelCase wire shape.
- Final turn replies whose text exceeds the 350-byte IRC line cap are now
  split at word boundaries into follow-on messages instead of rejected. The
  output schema's `maxLength` counts characters while the gate counts UTF-8
  bytes, so an em-dash-rich reply could satisfy the schema, fail the byte
  gate twice, and terminate the responder as degraded. If splitting exceeds
  the eight-action budget the last kept line ends with a visible ellipsis;
  mid-turn `irc.send` keeps the strict rejection because the model sees that
  error and can correct it within the turn.
- The network coordination MOTD is now owned and packaged by rmcp-irc as part
  of the IRC protocol contract. Attention onboarding distinguishes verified
  notification-backed, adapter-backed, and foreground-only operation. Returned
  subscription and camel-case schedule data are portable recipes rather than
  mandatory custom host commands.

## [0.3.0] - 2026-08-18

### Changed

- `irc.attention.check` now reports server-observed delivery state. The gateway
  tracks each authenticated owner's live accepted `subscriptions/listen`
  filters, so a check distinguishes polling from proven notification coverage
  of its watch URI. Recurring checks stop only after positive notification
  confirmation; a negative observation remains polling because it may race
  host activation. Onboarding also states explicitly that
  `subscriptions/listen` is a host-issued MCP request, not a callable tool, and
  its absence from `tools/list` is not evidence of missing support.
- Default guest connection metadata now identifies the MCP runtime reported by
  the initialize handshake. `{client}`, `{client_full}`, and `{agent_short}`
  onboarding template fields let WHOIS distinguish Codex, Claude Code, and
  other hosts while explicit per-call username and real-name overrides retain
  precedence.

- Native `resource_link` content blocks are omitted from tool results because
  current model hosts do not all accept them. Stable resource URIs remain in
  structured output.
- The gateway is now built around MCP protocol revision `2026-07-28`. Client
  identity and capabilities are evaluated per request from `_meta`
  (`io.modelcontextprotocol/protocolVersion`, `clientCapabilities`); there is no
  session lifecycle, `Mcp-Session-Id` is never read or minted, and a request
  that declares `2026-07-28` must carry that revision's request metadata and
  headers. Other protocol revisions and incomplete modern requests are refused;
  tasks, input round trips, `subscriptions/listen`, `server/discover`, and the
  top-level MRTR fields are available according to each request's declarations.
  See [protocol revision](docs/MCP_API.md#protocol-revision). On unauthenticated
  trusted HTTP (loopback, or the explicit network opt-in) all callers share the
  one local owner; bearer tokens remain the durable principal for shared
  endpoints. (#10)
- Watch reads are idempotent and event positions are wholly caller-owned. (#9)
  `irc://watches/{watch_id}` is now an immutable filter/health descriptor —
  host-initiated re-reads consume nothing. Events are consumed through
  `irc.events.read` with the new `watch_id` selector and an explicit cursor, or
  through the position-bearing
  `irc://watches/{watch_id}/events/after/{stream_id}/{sequence}` resource
  template. Event pages report honest `has_more`, watch handles are bounded per
  owner (`limits.max_watches_per_owner`) and expire when unused
  (`limits.watch_ttl_ms`) — delivering a match counts as use, and a lapsed
  watch announces its retirement with one final resource-updated notification —
  and `irc.watch.create` returns the stream's latest cursor so consumption can
  start from "now".
- DCC transfers become MCP tasks by server direction: a request that declares
  the `io.modelcontextprotocol/tasks` extension in its `_meta` client
  capabilities receives a task handle for `irc.dcc.send`/`irc.dcc.accept`; the
  per-call metadata opt-in is gone. One process-wide, owner-bound task ledger
  backs `tasks/get`/`tasks/update`/`tasks/cancel` across stateless HTTP
  requests; another owner's task id answers exactly like an unknown one, and
  tasks do not survive process restart. A tool-level failure — including the
  agent disconnecting mid-transfer — settles the task as `completed` with an
  error result; `failed` is reserved for protocol faults. (#7)
- Every tool result is one schema-declared envelope discriminated by `ok`:
  successes carry the tool's output under `result`, failures carry the shared
  `error` shape (kind, message, retriability, and the correlated command result
  or DCC root listing when one exists), and each tool's advertised
  `outputSchema` is a closed `oneOf` of exactly those two branches — so error
  results now conform to the schema they are returned under. A partially
  delivered multi-line `irc.send` failure retains the delivered lines'
  correlated results and message ids. (#5)
- The delivery-contract documentation states the accurate protocol position:
  resource notifications and `subscriptions/listen` wake the host application
  but cannot force or schedule a model turn; server-initiated sampling is
  deprecated (SEP-2577) and is not implemented; autonomous participation
  belongs to the host scheduler or a direct LLM integration outside the MCP
  relay. (#4)

### Added

- Subscription-backed model attention. `irc.attention.open` tells the host how
  to merge an agent's watch and lifecycle resources into one
  `subscriptions/listen` stream and provides a provider-neutral one-minute
  model wake-up fallback, closing the gap between host wake-ups and model
  turns from the host's side of the contract.
- Multi round-trip input requests. Where a call needs a time-bounded human
  decision and the request declared form elicitation, the tool returns
  `resultType: "input_required"` with a form and an integrity-protected
  `requestState` (HMAC-bound to the caller, the exact operation and arguments,
  and a 120-second expiry), and the retried call redeems it: DCC SEND
  destination (receive root and relative path), nickname choice on collision
  under the new `elicit` conflict policy, channel key on `ERR_BADCHANNELKEY`,
  and — when `[mcp] confirm_destructive` is enabled — confirmation of
  `irc.kick` and `irc.message.redact`, each confirmation redeemable exactly
  once. Declining, expiry, or a tampered state refuses in-band and leaves the
  underlying action unapplied; input is resolved before any task is created.
  (#6)
- Named DCC receive roots. `[[dcc.receive_roots]]` declares the directories
  incoming files may land in; `download_directory` seeds the default root.
  `irc.dcc.accept` takes a root name plus a relative destination, and
  confinement is enforced with capability-style directory handles that refuse
  symlinks at every component, so a write cannot be redirected outside the
  chosen root even by a check-to-open race. Session snapshots report
  `receive_root` and `receive_path`. (#11)
- Progress notifications. `irc.connect` reports seven registration milestones
  (registration and autojoin synchronization distinct) and `irc.history`
  reports its phases on the originating request's stream whenever the request
  carries a `progressToken`. (#7)
- Bounded activity hints. Successful results of agent-scoped tools carry an
  optional `activity` digest — per-watched-target unread counts since a
  caller-owned anchor, the latest cursor, and optionally up to three inlined
  mention/DM events chosen at connect — computed without touching any watch,
  cursor, or delivery state. The anchor moves only when `irc.events.read` is
  called with `set_activity_anchor`. Suppression is explicit configuration
  (`[mcp] activity_hints`, or the per-agent connect preference), never inferred
  from a subscription. (#5)
- Operational relay state instead of the deprecated MCP logging capability:
  journal eviction accounting (`evicted_events`, `evicted_bytes`,
  `last_eviction_at`, `oversized_rejections`) in every journal-stats surface, a
  rate-limited durable `journal.pressure` event that warns before unread
  events are lost, the scheduled reconnect attempt (`next_attempt_at`) in
  status, and richer SASL/registration failure detail. (#8)
- `irc.status` reports the calling request's declared protocol version,
  extensions, and elicitation support, so capability negotiation is debuggable
  per request. (#10)

### Fixed

- Model attention no longer returns the agent its own words. With
  `echo-message` negotiated, the server's copy of a message the agent sent
  arrives inbound carrying the agent's own nickname, so every such line
  qualified for attention inside a `full_traffic_targets` channel and was paid
  for on each scheduled check. Journal events now record `authored_by_me`
  alongside `mentions_me`, and `irc.attention.open` selection refuses
  self-authored conversational events on both the notification and the read
  path.
- That self-authored exclusion now covers every class, not just conversation.
  A `channel.state` record is selected for attention as a sparse operational
  signal, ahead of the conversational rules, so an agent's own `irc.topic.set`
  or `irc.mode.set` still woke it — twice, once for the outbound request and
  once for the server's echo. Attention selection drops self-authored events
  first, and somebody else curating the same channel is unaffected.
- Compact event projections report no speaker instead of an empty one for the
  agent's own outgoing lines. Such a line carries no prefix to parse a nickname
  from, which surfaced as `source: ""` and summaries opening with a stray space
  (`" set the topic to: ..."`). `source` is now absent and those summaries read
  as `topic set to: ...`.
- `irc.topic.set` reports who set the topic and when. A mutation is answered by
  the server's echo of the `TOPIC` line rather than by `RPL_TOPICWHOTIME`, and
  only the latter was read, so an agent that had just set a topic was told
  `set_by`/`set_at` were unknown by the very reply naming both. The echo's
  prefix and `time` tag now fill them, and a `333` still outranks an echo when
  both arrive.
- `irc.status` with `result_detail: compact` no longer repeats the whole MOTD
  body on every call. Status is polled, and re-serving several kilobytes of
  unchanged text was the most expensive part of asking a simple question.
  Compact status keeps the MOTD's status, source, and receipt time; `irc.connect`
  still returns the text, and the linked MOTD resource remains complete.
- A rejected IRC command names the reply that refused it in its summary text.
  The numerics always travelled in the structured failure, but the one-line
  summary — the part some clients show alone — said only `NICK: Rejected.`,
  which does not distinguish 433 (choose another nickname) from 432 (this one
  is malformed). Rejections now read `NICK: Rejected. (433 Nickname is already
  in use)`; timeouts and unwritten commands are unchanged, having no refusal to
  quote.
- `irc.send` splits an overlong message between words instead of at the raw
  byte budget, so text carried over several IRC lines no longer breaks in the
  middle of a word. The split stays byte-lossless and still falls back to the
  hard boundary for a token longer than one line.

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

- Shared-HTTP ownership is now attached before a newly connected agent is
  published, eliminating a brief local-owner window. Session-only HTTP mode
  fails closed when a request lacks an initialized MCP session, and resource
  subscription updates are authorized against the listening caller before
  delivery.
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

[Unreleased]: https://github.com/GrantSparks/rmcp-irc/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/GrantSparks/rmcp-irc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/GrantSparks/rmcp-irc/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/GrantSparks/rmcp-irc/releases/tag/v0.1.0
