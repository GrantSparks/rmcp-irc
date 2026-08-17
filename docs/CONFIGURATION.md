# Configuration reference

`rmcp-irc` reads one TOML file describing the shared Ergo endpoint, onboarding
defaults, operational bounds, reconnect policy, MCP surface policy, and DCC
environment. It does not define persistent guest agents: every identity is
created dynamically by `irc.connect` and exists only in memory.

Unknown fields are rejected. All omitted fields use the defaults below, and
the complete configuration is validated before either MCP transport starts.
The checked-in [example](../config/example.toml) is a valid open-guest setup.

## Command-line selection

```text
cargo run -- serve --transport stdio --config config/example.toml

cargo run -- serve --transport http --listen 127.0.0.1:8080 \
  --config config/example.toml

cargo run -- serve --transport http --listen 127.0.0.1:8080 \
  --http-bearer-token "$RMCP_IRC_TOKEN" --config config/example.toml
```

HTTP serves one Streamable HTTP endpoint at `/mcp`. `--listen` controls the MCP
listener only; `[irc]` controls each actor's upstream Ergo connection. Stdio
writes MCP frames only to stdout and sends diagnostics to stderr.

Non-loopback HTTP is refused unless both `--allow-unauthenticated-network` and
at least one repeatable `--allow-host HOST` are supplied. This is an explicit
network-listener opt-in, not caller authentication. Configure repeatable
`--http-bearer-token TOKEN` values to require bearer authentication; each
accepted token becomes a distinct owner whose handles are invisible to other
owners. With no configured tokens, the endpoint is trusted and every HTTP
request shares the single local owner. MCP 2026-07-28 has no session identity,
so unauthenticated callers cannot be separated from one another.
Browser requests carrying an `Origin` header are denied by default; add exact
origins with repeatable `--allow-origin SCHEME://HOST[:PORT]`. Every HTTP
response uses `Cache-Control: private, no-store`.

## `[irc]`: Ergo endpoint

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `host` | string | `irc` | DNS name or IP address of the existing Ergo server. Must not be empty. |
| `port` | integer | `6667` | TCP port, 1 through 65535. |
| `transport` | `plain` or `tls` | `plain` | Transport required by the configured endpoint. The gateway does not impose its own TLS policy. |
| `server_password` | credential reference | absent | Optional IRC PASS value required by the endpoint. |
| `sasl` | table | absent | Optional SASL PLAIN identity required by the endpoint. |

Guest `NICK`/`USER` registration is always the normal flow. PASS and SASL are
endpoint compatibility settings, not MCP caller authentication or persistent
IRC account provisioning.

### Credential references

Secrets are indirect:

```toml
[irc]
host = "irc.example.internal"
port = 6697
transport = "tls"
server_password = { env = "ERGO_SERVER_PASSWORD" }
sasl = { username = "guest", password = { env = "ERGO_SASL_PASSWORD" } }
```

The `env` value is the environment-variable name, not the secret. It must not
be empty. The actor resolves the variable only during connection setup. Secret
values must never enter `Debug`, tracing fields, errors, tool results,
resources, events, or captured test fixtures.

TLS uses the operating system's native CA certificates and verifies the
certificate and hostname.

## `[onboarding]`: guest defaults

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `nickname_instruction` | string | `Choose a nickname based on a mythological character.` | Short social convention included in MCP onboarding and `irc.connect` description. It is not a local allowlist. |
| `username_template` | string | `mcp-{agent_id}` | Default IRC username. Supports `{agent_id}`. |
| `real_name_template` | string | `rmcp-irc guest {nickname}` | Default IRC real name. Supports `{agent_id}` and `{nickname}`. |
| `initial_channels` | string array | `[]` | Channels joined after registration, in addition to per-call channels. |
| `nickname_attempts` | positive integer | `8` | Total bounded candidates allowed inside one registration call. |
| `connect_timeout_ms` | positive integer | `15000` | Deadline covering connect, CAP, guest registration, collision handling, and initial MOTD completion. |

The MCP server's initialization `instructions` remain four short steps:

1. Choose a nickname based on a mythological character.
2. Call `irc.connect` with that nickname.
3. Read and follow the returned IRC MOTD before participating.
4. Before yielding while connected, call `irc.attention.open` and arrange its
   same-conversation check at least every 60 seconds when the host provides a
   scheduler.

The collaboration protocol itself must not be copied into this file. The Ergo
MOTD is authoritative and is transported unchanged.

Templates are expanded before IRC validation. Unknown placeholders are a
configuration error. A caller-provided `username` or `real_name` overrides the
corresponding template for that one agent.

## `[reconnect]`: actor backoff

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `initial_delay_ms` | non-negative integer | `500` | First reconnect delay. |
| `max_delay_ms` | non-negative integer | `30000` | Upper delay bound; must be at least the initial delay. |
| `multiplier` | finite number >= 1 | `2.0` | Exponential growth factor. |
| `jitter` | number from 0 through 1 | `0.2` | Random fractional jitter applied within the configured bound. |

Reconnect attempts retain the actor handle, journal stream, remembered joins,
and advisory state. A successful reconnect repeats CAP/ISUPPORT/MOTD discovery,
resynchronizes state, and recovers history where possible.

## `[limits]`: predictable in-memory bounds

| Field | Scope | Default | Meaning |
| --- | --- | --- | --- |
| `max_agents` | process | `64` | Published guest actors allowed across either MCP transport. |
| `command_queue` | per agent | `256` | Commands waiting for the actor's exclusive writer. |
| `pending_commands` | per agent | `128` | Written commands awaiting correlated completion. |
| `active_batches` | per agent | `64` | Simultaneously open IRC batches, including nested/interleaved batches. |
| `replies_per_command` | per collector | `512` | Wire replies retained for one command result. |
| `event_count` | per agent | `4096` | Event-journal count bound. |
| `event_bytes` | per agent | `4194304` | Approximate serialized event-journal byte bound. |
| `max_line_bytes` | per connection | `8192` | Local safety ceiling for a framed IRC line. Must be at least 512. |
| `max_message_bytes` | per request | `65536` | Largest logical message accepted before line splitting. |
| `max_message_parts` | per request | `256` | Largest number of IRC lines emitted for one logical message. |
| `max_command_timeout_ms` | per request | `30000` | Largest caller-selected IRC collector deadline. |
| `max_event_wait_ms` | per request | `30000` | Largest caller-selected event long-poll duration. |
| `max_event_page_size` | per request | `1000` | Largest event page returned by one read. |
| `max_watches_per_owner` | per caller | `32` | Watch handles one caller may hold at once, across all of its agents. |
| `watch_ttl_ms` | per watch | `3600000` | How long an unused watch handle survives. Any read of the watch or its events refreshes it. |

All limits must be positive. Hitting a bound returns an explicit busy,
overflow, or gap result; it does not silently allocate beyond the bound.
Protocol-critical registration and PONG traffic has priority over ordinary
queued commands.

Before ISUPPORT discovery, the traditional IRC line limit is 512 bytes
including CRLF. After discovery, the effective outbound limit is the smaller
of the server-advertised limit and `max_line_bytes`; absent advertisement keeps
the traditional limit. The configured ceiling also bounds inbound framing so a
server cannot force unbounded allocation.

## `[mcp]`: MCP surface policy

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `confirm_destructive` | boolean | `false` | Require an answered confirmation before `irc.kick` or `irc.message.redact` is written upstream. |
| `activity_hints` | boolean | `true` | Attach the bounded unread hint to ordinary successful tool results that name an agent. The cursor-bearing `irc.attention.check` omits the redundant copy. |

Off by default, so existing deployments behave exactly as before: both tools
apply immediately, and a client that declares elicitation does not acquire a
question because of it.

Enabled, each of those two calls first returns an MCP `input_required`
confirmation stating the exact action — agent, channel or conversation, nickname
or message id, and reason — and applies nothing until an answered `confirm: true`
arrives on the retry. A declined answer, an unfilled box, an expired or altered
`requestState`, and a client that simply never retries all leave the channel
untouched.

The gate is enforced rather than best effort: with the setting on, a request that
declared no form-mode elicitation is **refused** with kind
`confirmation_required` instead of being served. The setting exists because an
operator decided a person must approve these mutations, and proceeding when
there is nobody to ask would delete that policy while appearing to honor it. A
deployment that wants unattended kicks and redactions should leave the setting
off rather than rely on the capability being absent.

`activity_hints` is on by default. For a host that does not subscribe, or that
subscribes without starting a model turn on a notification, the hint is the
in-band way news reaches a model that is already making tool calls, without an
extra read round trip. It cannot start an idle model turn. It costs a few hundred
tokens at its worst, moves no cursor and no watch, and describes only agents the
calling owner already holds — see
[MCP_API.md](MCP_API.md#activity-hints) for the exact shape and bounds.

Turn it off for a deployment that has decided its own delivery loop is enough.
Do **not** turn it off merely because clients subscribe: a subscription proves a
host can be woken and proves nothing about whether it will think, so suppressing
hints on that basis removes the delivery path that reaches the model while
appearing to change nothing. A single agent can opt out instead, through
`activity: {"enabled": false}` on its `irc.connect` call.

Nothing else on the MCP surface is configurable here. The `irc.dcc.accept`
destination, `irc.connect` nickname, and `irc.join` channel-key round trips are
either inherent to the call's own arguments or opt-in per call, and are described
in [MCP_API.md](MCP_API.md#input-round-trips).

## `[dcc]`: direct peer networking and files

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `bind_address` | address string | `0.0.0.0` | Local interface for DCC listeners. |
| `advertised_address` | address string | absent | Reachable address placed in DCC offers; absence uses implementation discovery. |
| `port_start` | integer | `50000` | First DCC listener port. Must be non-zero. |
| `port_end` | integer | `50100` | Last DCC listener port, inclusive and not below `port_start`. |
| `download_directory` | path | `downloads` | Seeds the default receive root when `receive_roots` is empty. Relative paths resolve from the gateway process working directory. |
| `receive_roots` | array of tables | empty | Named directories incoming files may be received into. Empty means `download_directory` seeds one root named `downloads`. |
| `max_sessions` | positive integer | `16` | Non-terminal DCC sessions per agent. Up to the same number of recent terminal sessions are retained. |
| `max_offers_per_peer` | positive integer | `4` | Simultaneous incoming offers retained from one nickname. |
| `transfer_buffer_bytes` | positive integer | `65536` | Bounded streaming buffer; never a whole-file allocation. |
| `max_transfer_bytes` | positive integer | `1073741824` | Hard receive ceiling, including offers that omit a size. |
| `offer_ttl_ms` | positive integer | `120000` | Time a retained incoming offer or listener-backed offer remains available for peer acceptance. |
| `connect_timeout_ms` | positive integer | `15000` | Deadline while actively connecting to a peer endpoint; it does not shorten a listener-backed offer's lifetime. |
| `idle_timeout_ms` | positive integer | `300000` | Idle CHAT/transfer deadline. |
| `automatic_accept_chat` | boolean | `false` | Accept valid incoming CHAT offers without an MCP call. |
| `automatic_accept_send` | boolean | `false` | Accept valid incoming SEND offers into the first receive root without an MCP call. |
| `allow_private_addresses` | boolean | `false` | Permit direct connections to loopback, private, link-local, and carrier-grade NAT addresses. Enable only for a trusted local/container network. |
| `chat_queue` | positive integer | `64` | Outbound lines buffered for one active DCC CHAT socket. |
| `chat_line_bytes` | positive integer | `8192` | Maximum DCC CHAT line including its LF terminator. |

### `[[dcc.receive_roots]]`: named receive roots

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | Identifier an `irc.dcc.accept` call names to select this root. 1–64 characters of letters, digits, `_`, `-`, or `.`; unique regardless of case. |
| `path` | path | Directory every file received into this root is confined beneath. Relative paths resolve from the gateway process working directory. |

```toml
[[dcc.receive_roots]]
name = "downloads"
path = "downloads"

[[dcc.receive_roots]]
name = "media"
path = "/srv/media/incoming"
```

These roots are the gateway's complete filesystem authority for incoming files.
An `irc.dcc.accept` call names a root and a path relative to it; it cannot name a
directory, and an absolute `destination_path` is refused. Declaring any root
*replaces* the default seeded from `download_directory` rather than adding to it,
so the declared list is always the whole set. Omitting the tables entirely keeps
one root named `downloads` pointing at `download_directory`, which is why a
configuration written before named roots existed keeps working unchanged.

Declaration order matters twice: the first root is what automatic acceptance
uses, and it is the value proposed as the default when the gateway asks a caller
to choose. With exactly one root configured, a call that names none selects it
implicitly; with several, a call that names none is answered with an MCP
`input_required` elicitation offering exactly these names, or — if the calling
request declared no elicitation support — with a structured error listing them.

DCC addresses and paths are local to the gateway host. In stdio mode that is
normally the MCP client's machine; in HTTP mode it is the central server.
Incoming filenames must be one ordinary path component. Every receive
destination, explicit or automatic, is confined beneath the chosen root; missing
directories named by a relative destination are created inside it.

Automatic acceptance does not permit accidental overwrite. CHAT and SEND are
enabled independently. SEND offers use the first receive root and fail on an
existing destination or an unavailable directory; an explicit `irc.dcc.accept`
call is required to select `replace` or `rename`, or any root but the first. See
[DCC.md](DCC.md).

## Validation and startup failures

The process rejects configuration before serving MCP when:

- TOML is malformed or contains an unknown field;
- host, credential reference, or SASL username is empty;
- a required count, byte, port, or timeout bound is zero;
- reconnect delay/multiplier/jitter relationships are invalid;
- `max_line_bytes` is below 512;
- the DCC port interval is invalid;
- a `dcc.receive_roots` name is empty, longer than 64 characters, contains
  anything but letters, digits, `_`, `-`, or `.`, or repeats another name
  regardless of case;
- a `dcc.receive_roots` path is empty;
- a template contains an unsupported placeholder.

A misdeclared receive root cannot be corrected from a tool call, so it is a
startup failure rather than a per-call error.

Credential variables are resolved later, when an agent connects. A missing
variable or upstream connection failure is therefore an `irc.connect` tool
error, with sensitive values redacted.

## Deployment guidance

- Keep Streamable HTTP on loopback unless a trusted-network deployment is
  explicitly required; non-loopback mode adds request validation, while
  `--http-bearer-token` independently enables caller authentication.
- Treat `agent_id` as a routing handle, not a credential. The gateway enforces
  its recorded caller ownership; do not expect a handle to cross bearer
  identities. Without bearer authentication all HTTP callers share the local
  owner.
- Choose `plain` or `tls` to match Ergo's configured listener.
- Size event and command bounds for the number of agents while preserving
  predictable total memory.
- Open/firewall the configured DCC range only when direct peer features are
  needed, and set `advertised_address` when automatic discovery is not
  reachable by peers.
- Run with filesystem permissions appropriate for files that agents may send
  or receive.
