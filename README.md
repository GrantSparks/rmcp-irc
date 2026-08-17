# rmcp-irc

[![Crates.io](https://img.shields.io/crates/v/rmcp-irc.svg)](https://crates.io/crates/rmcp-irc)
[![CI](https://github.com/GrantSparks/rmcp-irc/actions/workflows/ci.yml/badge.svg)](https://github.com/GrantSparks/rmcp-irc/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/rmcp-irc.svg)](#license)
[![Minimum Rust Version](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)

`rmcp-irc` is a native Rust MCP gateway for Ergo IRC networks. It serves local
MCP clients over stdio or multiple clients through one Streamable HTTP
endpoint. Each agent created with `irc.connect` has an independent IRC
connection, state snapshot, and bounded event stream.

## Features

- Native TCP/TLS IRC connections with CAP, SASL, ISUPPORT, HELP, and MOTD
  discovery.
- Resource-first context: compact inboxes and transcripts for models, separate
  topics/members for channels, and lossless wire data for diagnosis. Native
  links and annotations tell hosts what to attach and subscribe to.
- Targeted watch resources with subscription wake-ups and wholly caller-owned
  cursors: reading a watch never consumes anything, so host-initiated re-reads
  are harmless. Bounded `irc.events.read` long polling remains the fallback.
- Stable typed tools for common IRC operations, including negotiated reaction,
  redaction, read-marker, and typing support. Structured `irc.execute` covers
  other commands; no tool accepts raw IRC lines.
- Four reusable MCP prompts for connecting, watching mentions, joining with
  context, and summarizing/responding.
- Lossless inbound and outbound wire events, including unknown extensions and
  invalid UTF-8 represented as base64.
- Correlated command replies, caller-owned event cursors, and explicit stream
  reset and retention-gap handling.
- Equivalent stdio and Streamable HTTP surfaces, with HTTP agent/watch handles
  isolated by authenticated bearer identity.
- Reconnection, channel restoration, state resynchronization, and history
  recovery when the server supports it.
- Bounded in-memory queues, journals, collectors, and DCC sessions.
- Progress notifications for the calls that block longest — connect reports each
  registration stage, history reports each phase of a playback.
- Direct DCC CHAT and streamed DCC SEND, including reverse connections and
  resume negotiation; file transfers run as MCP tasks for clients that declare
  the tasks extension, with task handles bound to the caller that created them.

The gateway connects to an existing Ergo server; it does not provision or
configure one. Ergo remains responsible for accounts, permissions, channel
policy, and retained history.

> [!IMPORTANT]
> Streamable HTTP binds every agent and watch handle to its caller. Configure
> repeatable `--http-bearer-token` values for durable authenticated identities;
> without them the endpoint is trusted and every caller shares one local owner.
> Non-loopback binding still requires an explicit trusted-network opt-in and
> allowed Host values. Agent IDs remain routing handles, not credentials.

## Quick start

Rust 1.88 or newer is required. Docker is optional and is only needed if you
want the local Ergo server below.

### Start a local IRC server with Docker

Skip this section if you already have an Ergo server. The example creates an
internal-only Docker network: Ergo is reachable by attached containers as
`irc:6667`, but no IRC port is exposed on the host or LAN.

Clone the repository, then run the setup once from its root:

    git clone https://github.com/GrantSparks/rmcp-irc.git
    cd rmcp-irc
    mkdir -p .local/ergo
    cp examples/docker/ergo/* .local/ergo/
    docker network create --internal rmcp-irc-net
    docker run -d --name rmcp-irc-ergo --restart unless-stopped \
      --network rmcp-irc-net --network-alias irc \
      --user "$(id -u):$(id -g)" \
      --mount type=bind,src="$PWD/.local/ergo",dst=/ircd \
      ghcr.io/ergochat/ergo@sha256:72db5b93b437ea0512fb79d9d4d035f0916ccba07ed736dec19e8d5b5a73f972

The image is the same multi-architecture Ergo 2.19.1 build used in our own
setup. Configuration, MOTD, and server state remain under `.local/ergo`, where
they are easy to inspect or back up.

Attach an existing development container to the IRC network as a second
network, preserving its normal Internet-capable network:

    docker network connect rmcp-irc-net MY_DEV_CONTAINER
    docker exec MY_DEV_CONTAINER getent hosts irc

For a Compose-based dev container, add the external network to its service:

```yaml
services:
  dev:
    networks:
      - default
      - rmcp-irc

networks:
  rmcp-irc:
    name: rmcp-irc-net
    external: true
```

The checked-in [gateway configuration](config/example.toml) and the built-in
defaults already use `irc:6667` over plain TCP, so no gateway configuration is
needed for this local setup.

To give attached containers IRC and MCP at one address instead, there is a
single-container image carrying Ergo and the gateway together:
[examples/docker](examples/docker/README.md).

### Install and start the gateway

Install the `irc-mcp` binary from crates.io:

    cargo install rmcp-irc --locked

Or build it from a checkout:

    cargo build --release --locked

The built binary is `./target/release/irc-mcp`. For the Docker setup, run the
gateway inside the attached development container:

    irc-mcp serve --transport stdio

For an existing Ergo deployment, create a small `rmcp-irc.toml`:

```toml
[irc]
host = "irc.example.net"
port = 6697
transport = "tls"
```

Then run:

    irc-mcp serve --transport stdio --config /path/to/config.toml

See the [configuration reference](docs/CONFIGURATION.md) for credentials,
limits, reconnect behavior, and DCC settings.

### Register with an MCP client

For Claude Code:

    claude mcp add rmcp-irc -- \
      /absolute/path/to/irc-mcp serve --transport stdio

For Codex:

    codex mcp add rmcp-irc -- \
      /absolute/path/to/irc-mcp serve --transport stdio

Run these commands in the attached development container and use the binary
path reported by `command -v irc-mcp`. For an existing server, append
`--config /path/to/rmcp-irc.toml` to the server command.

After MCP initialization, call `irc.connect`. The result includes the accepted
nickname, the server's MOTD, the agent ID, and links to the agent's resources.
Read and follow the MOTD before participating.

### Streamable HTTP

Start the shared endpoint:

    irc-mcp serve --transport http --listen 127.0.0.1:8080 \
      --config /path/to/config.toml

The MCP endpoint is `http://127.0.0.1:8080/mcp`:

    claude mcp add --transport http rmcp-irc http://127.0.0.1:8080/mcp
    codex mcp add rmcp-irc --url http://127.0.0.1:8080/mcp

Both transports expose the same service. Every operation after `irc.connect`
requires an explicit `agent_id`; an HTTP connection is not an IRC identity.
For shared HTTP, pass one or more `--http-bearer-token TOKEN` options and send
the corresponding `Authorization: Bearer TOKEN` header. Each token sees and
operates only its own agents, watches, and resources, and keeps that identity
across process restarts. Without configured tokens there is nothing to separate
callers by — this protocol revision has no sessions — so every caller shares the
single local owner. Browser `Origin` requests are denied unless explicitly
allowlisted. A trusted container-network deployment must additionally pass
`--allow-unauthenticated-network --allow-host HOST`; the bundled image already
does this for its `irc` network alias. HTTP responses are marked
`Cache-Control: private, no-store`.

## MCP surface

| Area | Tools |
| --- | --- |
| Identity | `irc.connect`, `irc.disconnect`, `irc.status` |
| Channels and messages | `irc.join`, `irc.part`, `irc.send`, `irc.history`, typed topic/reaction/redaction/read/typing tools |
| Queries and commands | `irc.query`, `irc.execute` |
| Events | `irc.watch.create`, `irc.watch.close`, `irc.events.read` |
| DCC | `irc.dcc.chat.open`, `irc.dcc.chat.send`, `irc.dcc.send`, `irc.dcc.accept`, `irc.dcc.reject`, `irc.dcc.cancel`, `irc.dcc.list`; SEND/accept run as MCP tasks for clients declaring the tasks extension |

The stable typed semantic surface additionally includes WHOIS, NAMES, LIST,
HELP, topic, nickname, away, invite, kick, monitor, and mode tools. Four
user-selectable MCP prompts guide connect, mention-watch, join, and
summarize/respond workflows. Negotiated IRCv3 tools cover reactions, message
redaction, synchronized read markers, and privacy-sensitive typing indicators
without implying that a resource notification can force or schedule a model
turn: notifications wake the host application, and this protocol version has no
server-initiated sampling with which to do more.

Resources are the primary context plane: per-agent URIs expose connection
status, protocol discovery, MOTD, reduced state, a compact inbox and
conversation transcripts, lossless wire diagnostics, event bounds, DCC
sessions, and separate channel state/member/topic views under
`irc://agents/{agent_id}/...`. `irc.watch.create` turns a durable event filter
into a subscribable `irc://watches/{watch_id}` resource. The watch holds no
position: its descriptor read is pure, and its events are read either with
`irc.events.read` plus a `watch_id` and the caller's own cursor, or from
`irc://watches/{watch_id}/events/after/{stream_id}/{sequence}`. This lets
subscription-capable hosts wake on relevant activity and retrieve only the new
matching context, while any re-read stays idempotent; `irc.events.read` long
polling remains the explicit fallback. See the [MCP API](docs/MCP_API.md) for inputs, results,
errors, and resource shapes.

## Events and state

Each agent has a bounded in-memory journal identified by a random `stream_id`
and monotonic sequence. Callers maintain their own cursors:

- an actor or process restart causes `stream_reset` for an old stream;
- journal eviction causes `event_gap` and returns the retained bounds;
- a `journal.pressure` event and the eviction counters in `status` warn before
  that gap opens, so a client knows to read soon;
- an IRC reconnect keeps the stream while the actor remains alive, publishing
  its next attempt time in `status`; and
- resource notifications are wake-up signals, while cursor reads provide
  ordered delivery.

Clients that do not expose MCP resource subscriptions cannot receive those
wake-up signals. They must keep an `irc.events.read` long poll active, passing
the previous `next_cursor` into the next call, to remain responsive.

State resources are best-effort snapshots. Use `irc.query` when a current
server response is required. See [events and state](docs/EVENTS_AND_STATE.md).

## DCC locality

DCC traffic bypasses Ergo after CTCP negotiation. File paths and listeners
therefore belong to the machine running the gateway: usually the MCP client's
machine in stdio mode and the shared server in HTTP mode. File contents are
streamed and never placed in MCP results or the event journal.

Incoming DCC offers must be direct messages to the actor, private/local peer
addresses are opt-in, advertised filenames cannot contain paths, and every
receive has a byte ceiling. Configuration declares named receive roots, which are
the gateway's complete filesystem authority; `irc.dcc.accept` names one of them
plus a relative destination, and resolution holds each directory open rather than
re-walking a path, so no link or swapped directory can move a write outside the
chosen root. Where the root is genuinely the caller's choice, the tool asks for it
through an MCP elicitation and completes on the retry. See the
[DCC guide](docs/DCC.md).

## Documentation

| Topic | Document |
| --- | --- |
| Configuration and deployment | [Configuration](docs/CONFIGURATION.md) |
| Tools, results, errors, and resources | [MCP API](docs/MCP_API.md) |
| Events, cursors, state, and reconnects | [Events and state](docs/EVENTS_AND_STATE.md) |
| IRC discovery, wire data, and fallbacks | [Protocol compatibility](docs/PROTOCOL_COMPATIBILITY.md) |
| DCC sessions, files, and networking | [DCC](docs/DCC.md) |
| Runtime structure and concurrency | [Architecture](docs/ARCHITECTURE.md) |
| Opt-in interoperability checks | [Live Ergo testing](docs/LIVE_TESTING.md) |
| External protocol specifications | [References](docs/REFERENCES.md) |

## Development

Install the Git hooks once per clone, which run the checks below automatically:

    pre-commit install

Or run the same checks by hand before submitting a change:

    cargo fmt --all -- --check
    cargo test --all-targets --all-features --locked
    cargo clippy --all-targets --all-features --locked -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --document-private-items --locked

See [CONTRIBUTING.md](CONTRIBUTING.md) for test and documentation expectations,
[CHANGELOG.md](CHANGELOG.md) for released changes, [SECURITY.md](SECURITY.md)
for the security policy, and [RELEASING.md](RELEASING.md) for the release
process.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
