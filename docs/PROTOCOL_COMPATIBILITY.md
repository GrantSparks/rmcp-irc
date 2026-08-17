# Protocol compatibility model

The compatibility resource combines static bridge knowledge with exact runtime
discovery. Neither HELP text nor a generated tool list is treated as a formal
protocol schema.

## Catalog sources

- `standard_registry`: built-in command and collector knowledge;
- `cap_ls`: exact capability names and optional values;
- `isupport`: complete raw and parsed `005` tokens;
- `help_index`: commands reported by the connected Ergo server;
- `help_subject`: descriptive text requested on demand; and
- `local_ctcp` and `local_dcc`: gateway features, separate from IRC CAP.

## Mapping grades

| Grade | Meaning |
| --- | --- |
| native | The gateway understands semantics and exposes a typed projection. |
| passthrough | Structured encode/decode and raw observation are available. |
| degraded | A visible fallback loses some semantics. |
| unavailable | Not advertised or intentionally unsupported. |
| observed_unnegotiated | Advertised but not requested because behavior is not implemented. |

Privilege is documentary metadata only. It never blocks irc.execute; Ergo
accepts or rejects the command.

## Implemented capability policy

The actor requests only behavior implemented in the receive, correlation, and
state paths. The current request set is `batch`, `draft/chathistory` (or its
equivalent advertised spelling), `cap-notify`, `labeled-response`,
`message-tags`, `server-time`, `echo-message`, `account-tag`,
`standard-replies`, `multi-prefix`, `userhost-in-names`, `extended-join`,
`away-notify`, `account-notify`, `chghost`, `setname`, and `invite-notify`.
SASL PLAIN is added only when configured and advertised.

`draft/multiline`, read markers, reactions, typing, redaction, metadata, and
event-playback are deliberately not requested. Their exact advertisements stay
visible as `observed_unnegotiated`; callers can still inspect unknown wire data
and use `irc.execute` where no negotiated client behavior is required. The
multiline draft is intentionally excluded because its published specification
remains work in progress and recommends against production use.

MONITOR is discovered through ISUPPORT and supported through typed read-only
queries. CTCP ACTION/CLIENTINFO/PING/TIME/VERSION and ordinary/reverse DCC
CHAT/SEND plus ACCEPT/RESUME are local features, separate from CAP.

Exact draft and stable capability spellings map to one internal feature ID
while remaining distinct exact tokens in the resource.

## Discovery lifecycle

Every new connection and reconnect performs this ordered discovery flow:

1. Open plain TCP or TLS according to endpoint configuration.
2. Send `CAP LS 302` and collect all continuation lines.
3. Preserve every advertised capability name and optional value exactly.
4. Request only the intersection of advertised capabilities and behavior the
   gateway actually implements.
5. Perform guest `NICK`/`USER` registration, adding PASS/SASL only when
   configured for that endpoint.
6. Collect `RPL_WELCOME`, every `RPL_ISUPPORT` token, and the complete MOTD or
   no-MOTD reply.
7. End CAP negotiation at the protocol-appropriate point and enter the ready
   state.
8. Probe `HELP INDEX` after readiness and cache recognized 704–706 responses;
   retrieve `HELP <subject>` on demand.
9. Process `CAP NEW` and `CAP DEL` for the life of the connection.

Unknown capabilities are recorded with `observed_unnegotiated`; they are never
requested speculatively. CAP rejection is retained as discovery state. A
reconnect rebuilds connection-specific advertised/negotiated state, refreshes
MOTD/ISUPPORT/HELP evidence, and produces compatibility resource updates.

## Protocol resource shape

`irc://agents/{agent_id}/protocol` contains at least:

```json
{
  "resource": "protocol",
  "data": {
    "catalog": {
      "capabilities": {
        "draft/example": {
          "name": "draft/example",
          "value": "value",
          "feature": "draft/example",
          "status": "observed_unnegotiated",
          "mapping": "observed_unnegotiated"
        }
      },
      "isupport": {
        "CASEMAPPING": {
          "raw": "CASEMAPPING=ascii",
          "name": "CASEMAPPING",
          "value": "ascii",
          "negated": false
        }
      },
      "commands": {},
      "help": {},
      "ctcp_commands": ["ACTION", "CLIENTINFO", "DCC", "PING", "TIME", "VERSION"],
      "dcc_variants": ["ACCEPT", "CHAT", "RESUME", "REVERSE", "SEND"]
    },
    "line_budget": {
      "max_body_bytes": 512,
      "max_tag_bytes": 4096
    }
  }
}
```

Capability lifecycle status distinguishes observed, requested, negotiated,
rejected, and removed exact tokens. Draft and stable names can share an
internal `feature` while remaining separate resource keys.

ISUPPORT entries retain each token's raw spelling, parsed `name`, optional
value, and negation state. Runtime helpers derive line length, case mapping,
channel and membership prefixes, mode classes, target counts, monitor support,
and name limits. Unknown tokens remain in the generic map.

The `commands` map is the union of static registry and HELP INDEX evidence.
Runtime discovery may add descriptions and availability, but never changes
wire parsing or invents a typed projection. `privilege` is documentary and is
never a local execution check.

## Lossless wire contract

All inbound and outbound IRC data passes through one structured model before
correlation or semantic projection:

```text
WireMessage
  raw_bytes       complete line without CRLF
  raw             UTF-8 text when valid
  raw_base64      complete bytes when text is not valid UTF-8
  parse_status    complete | partial | invalid
  tags[]          case-sensitive opaque key plus absent/empty/value distinction
  prefix          complete raw prefix plus name/user/host boundaries
  command         case-preserved spelling
  params[]        ordered middle parameters
  trailing        absent, empty, or non-empty trailing parameter
```

Requirements:

- tags and prefixes are parsed for registration, CAP, AUTHENTICATE, numerics,
  ordinary commands, and unknown extensions alike;
- raw bytes are retained before semantic interpretation;
- structurally recoverable fields survive invalid UTF-8 and raw bytes are
  exposed as base64;
- dispatch compares command names case-insensitively while preserving spelling;
- tag keys remain case-sensitive and distinguish a flag from an explicit empty
  value;
- IRC formatting and CTCP are parsed additively without removing original text;
- server-advertised line limits apply, with 512 bytes including CRLF when none
  is advertised and the configured ceiling always enforced;
- outbound message text reserves the `:nick!user@host ` prefix the server adds
  when it relays the line, using the hostmask observed on the self JOIN and
  falling back to the advertised `NICKLEN`, `USERLEN`, and `HOSTLEN` maxima
  until one is seen, because a server truncates an overlong relayed line
  instead of rejecting it;
- outbound encoding never splits a UTF-8 code point and rejects NUL, CR, LF,
  duplicate tags, reserved bridge tags, invalid middle/trailing structure, and
  overlong output.

### Parser boundary

`ircv3_parse` handles valid UTF-8 message syntax. Gateway-owned code handles
byte-stream framing, negotiated line limits, invalid-UTF-8 recovery, strict
outbound validation, and the owned `WireMessage` representation. A parser
dependency's borrowed message type is never exposed through MCP.

## Static command registry

Known commands have a static `CommandSpec`:

```text
name
phase                  registration | registered | lifecycle
required_capabilities
response_strategy
state_effects
privilege              documentary only
mapping
```

The required response strategies are:

| Strategy | Completion rule |
| --- | --- |
| `ack` | One complete logical labeled response: a direct reply/ACK or an outer batch. |
| `single_reply` | One matching reply or error completes the command. |
| `numeric_sequence` | Collect until a command-specific terminal numeric. |
| `batch` | Collect a complete batch of the expected type, including nested batches. |
| `echo` | Complete on a matching server echo/state transition. |
| `connection_lifecycle` | Registration, QUIT, or reconnect transition determines completion. |
| `unconfirmed` | No reliable signal exists; a successful write becomes `sent_unconfirmed`. |

Initial numeric terminators are:

| Query family | Terminal numeric(s) |
| --- | --- |
| WHOIS | 318 |
| WHOWAS | 369 |
| WHO / WHOX | 315 |
| NAMES | 366 |
| LIST | 323 |
| MOTD | 376 or 422 |
| LINKS | 365 |
| STATS | 219 |
| HELP | 706, plus compatible server-specific completion recorded in the catalog |
| INFO | 374 |
| ban / exception / invite mode lists | 368, 349, or 347 as appropriate |
| CHATHISTORY / event playback | complete advertised history batch |

Registry entries also cover registration, single-reply ISON, USERHOST, VERSION,
and TIME queries, JOIN/PART echoes, messages, QUIT, and server/operator commands
needed for structured execution. Unknown commands remain valid `irc.execute`
inputs when syntactically encodable.

## Correlation and collectors

When `labeled-response` is negotiated, every outbound command gets a unique
opaque label no longer than 64 bytes, mapped to an in-memory pending record:

- command and agent IDs;
- MCP request cancellation context;
- command specification and collector;
- write time and deadline;
- collected wire replies;
- whether a state-changing reply was already projected/journaled.

Collectors are registered before the write. Labeled ACK and single replies
complete directly; multi-message results remain open through their complete
batch. FAIL completes as a tool execution error, WARN/NOTE attach structured
information, and known numeric errors retain raw replies.

Cancellation detaches waiting without reversing an already-written command. A
socket failure before write is `not_written`; after write but before definitive
confirmation it is `indeterminate`. Unknown commands may use labeled
collection; without a definitive signal they return `sent_unconfirmed`.

Without labeled responses, reply-bearing collectors serialize conservatively:
generic error numerics make even nominally different query families ambiguous.
Write-only unconfirmed operations remain concurrent. No collector may remove
its replies from wire-level event delivery.

## Capability fallbacks

Every fallback is visible both in the protocol catalog and the affected tool
result.

| Missing capability | Required behavior |
| --- | --- |
| `labeled-response` | Use command-specific collectors and serialize reply-bearing operations conservatively; write-only operations remain concurrent. |
| `echo-message` | Keep the outbound wire event and return `sent_unconfirmed`; do not manufacture a server echo, message ID, or delivery claim. |
| multiline | Split only when the caller's policy permits; otherwise reject overlong messages. |
| `server-time` | Use receipt time only with explicit local provenance; leave `server_time` absent. |
| message IDs | Use a gateway event ID only; never present it as a server message ID. |
| CHATHISTORY | Use Ergo `HISTORY` only as a reported `degraded` fallback; otherwise return `unavailable`. |
| read markers | Return `unavailable`; do not emulate persistent read state. |
| reply/reaction semantics | Reject operations requiring exact semantics instead of silently degrading to plain text. |
| standard replies | Parse known numerics and notices through the static registry and retain raw forms. |
| unknown advertised capability | Report `observed_unnegotiated` and do not request it automatically. |

## Protocol correctness boundary

The gateway validates syntax, framing, negotiated features, and bounded
resource use. It does not reinterpret IRC privileges or decide which command,
channel, nickname, or peer is permitted. Syntactically valid structured
commands are sent to Ergo, whose response is authoritative.

This boundary permits rejecting unsafe/unrepresentable NUL, CR, LF, malformed
tags, reserved correlation fields, invalid names, or overlong messages without
creating a gateway authorization policy.
