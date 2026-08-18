# Docker examples

Two ways to run the pieces this repository expects: an Ergo server on its own,
and a single image that carries Ergo and the gateway together.

## Files

| Path | Purpose |
| --- | --- |
| `ergo/ircd.yaml`, `ergo/ircd.motd` | Minimal Ergo configuration for a local, internal-only test network. |
| `Dockerfile` | Single-container image: upstream Ergo plus the `irc-mcp` gateway. |
| `entrypoint.sh` | Supervisor that runs both processes as one service. |
| `rmcp-irc.toml` | Gateway configuration baked into that image. |

The separate-container quick start is in the [main README](../../README.md).

## Single-container image

The gateway holds no IRC state of its own: it connects to Ergo as an ordinary
client. Putting both in one image is therefore a packaging choice, not an
architectural one — it gives attached containers one address for IRC and MCP,
one lifecycle, and one thing to deploy. Running the two as separate containers
on the same network is equally valid and keeps their restarts independent.

Build from the repository root:

    docker build -f examples/docker/Dockerfile -t rmcp-irc-ergo:local .

The build compiles `irc-mcp` against musl in a Rust builder stage and copies the
static binary into the upstream Ergo image, which is pinned by digest in the
`ERGO_IMAGE` build argument. Ergo itself is unmodified.

Run it on an internal-only network, with Ergo's state and the gateway working
directory bind-mounted so server state and DCC files stay inspectable and
persistent on the host:

    mkdir -p .local/ergo
    mkdir -p .local/mcp/downloads
    cp examples/docker/ergo/* .local/ergo/
    docker network create --internal rmcp-irc-net
    docker run -d --name rmcp-irc --restart unless-stopped \
      --network rmcp-irc-net --network-alias irc \
      --user "$(id -u):$(id -g)" \
      --mount type=bind,src="$PWD/.local/ergo",dst=/ircd \
      --mount type=bind,src="$PWD/.local/mcp",dst=/var/lib/rmcp-irc \
      rmcp-irc-ergo:local

Attached containers then reach IRC at `irc:6667` and MCP at
`http://irc:8080/mcp`:

    docker network connect rmcp-irc-net MY_DEV_CONTAINER
    docker exec MY_DEV_CONTAINER claude mcp add --transport http rmcp-irc http://irc:8080/mcp

The Codex CLI reaches the same endpoint with
`codex mcp add rmcp-irc --url http://irc:8080/mcp`, but first needs
`[features] mcp_2026_07_28 = true` in its `~/.codex/config.toml` to negotiate the
protocol this gateway requires; Claude Code needs no such flag. See the README's
[Register with an MCP client](../../README.md#register-with-an-mcp-client) section.

No port is published to the host or the LAN. **Streamable HTTP has no built-in
MCP authentication**, and agent IDs are routing handles rather than credentials:
every container attached to the network can drive every agent on the gateway.
Keep the network internal, or put an access-control layer in front of it.

## Behavior

- `docker stop` stops both processes and exits cleanly.
- `docker kill -s HUP` reaches Ergo, so rehashing `ircd.yaml` and `ircd.motd`
  works as it does for a standalone server.
- If either process exits, the entrypoint stops the other and exits non-zero, so
  a restart policy recreates a complete service.
- The health check requires both the IRC port and the MCP listener.

## Environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `MCP_LISTEN` | `0.0.0.0:8080` | Streamable HTTP listen address; the endpoint is `/mcp`. |
| `MCP_ALLOWED_HOST` | `irc` | Docker-network hostname accepted by the HTTP Host guard. |
| `MCP_CONFIG` | `/etc/rmcp-irc/rmcp-irc.toml` | Gateway configuration. Mount your own file to override. |
| `MCP_DCC_ADVERTISED_ADDRESS` | `auto` | Address advertised for DCC listeners. `auto` uses the container's non-loopback address; empty disables the injection; ignored when the configuration already has a `[dcc]` table. |
| `MCP_WORKDIR` | `/var/lib/rmcp-irc` | Gateway working directory; the default relative `download_directory` resolves to `downloads` inside it. |
| `IRC_PORT` | `6667` | IRC port checked by the health check and the readiness wait. |
| `SHUTDOWN_TIMEOUT` | `10` | Seconds each process gets to exit on `SIGTERM` before `SIGKILL`. |

## DCC

DCC bypasses the IRC server after CTCP negotiation, so the gateway itself is the
transfer endpoint:

- Listeners use `dcc.port_start`–`dcc.port_end` (50000–50100 by default). Peers
  on the same Docker network reach them directly, so nothing has to be
  published; publish that range only for peers outside the network.
- The address in the DCC offer comes from `MCP_DCC_ADVERTISED_ADDRESS`, because
  the gateway's own route to Ergo is loopback here and advertising `127.0.0.1`
  would be unreachable for every peer.
- The generated `[dcc]` table permits private addresses because the internal
  Docker network is the intended trust boundary. A mounted custom `[dcc]`
  table must set `allow_private_addresses = true` when peers use that network.
- Transfers terminate at the gateway, not in the calling agent's container:
  accepted files land in `MCP_WORKDIR/downloads`, and `irc.dcc.send` can only
  read paths visible inside this container. The run command above bind-mounts
  `.local/mcp` as `MCP_WORKDIR`, making those files available on the host. Other
  containers can share them by bind-mounting that same host directory.
