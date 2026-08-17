# Single-container Ergo + rmcp-irc gateway.
#
# Build from the repository root so the crate source is in the context:
#
#     docker build -f examples/docker/Dockerfile -t rmcp-irc-ergo:local .
#
# The final stage is the upstream Ergo image itself, so the IRC server is
# exactly the one you would run on its own; this image adds the irc-mcp binary,
# a gateway configuration, and a supervisor that runs both processes. See
# examples/docker/README.md for how to run it.

# Rust 1.88 Alpine multi-architecture manifest, pinned by digest.
ARG RUST_IMAGE=rust:1.88-alpine@sha256:9dfaae478ecd298b6b5a039e1f2cc4fc040fc818a2de9aa78fa714dea036574d
# Ergo 2.19.1, multi-architecture, digest-pinned. Override to move versions.
ARG ERGO_IMAGE=ghcr.io/ergochat/ergo@sha256:72db5b93b437ea0512fb79d9d4d035f0916ccba07ed736dec19e8d5b5a73f972

FROM ${RUST_IMAGE} AS builder

# musl-static build prerequisites. cmake, make, perl, and a C toolchain are for
# aws-lc-sys (the rustls cryptography provider); clang/llvm supply libclang for
# the targets where aws-lc-sys has to generate bindings itself.
RUN apk add --no-cache build-base cmake make perl clang-dev llvm-dev
ENV LIBCLANG_PATH=/usr/lib

WORKDIR /src
COPY . .
# The builder image tag pins the toolchain here; the checked-in rust-toolchain
# pin exists for local development checkouts and would only force a download.
RUN rm -f rust-toolchain.toml \
    && cargo build --release --locked --bin irc-mcp \
    && strip target/release/irc-mcp

FROM ${ERGO_IMAGE}

COPY --from=builder /src/target/release/irc-mcp /usr/local/bin/irc-mcp
COPY examples/docker/entrypoint.sh /usr/local/bin/irc-mcp-entrypoint
COPY examples/docker/rmcp-irc.toml /etc/rmcp-irc/rmcp-irc.toml

# The container is normally run as an arbitrary uid:gid (matching the owner of
# the bind-mounted /ircd), so the gateway's writable directory is world-writable
# with the sticky bit rather than owned by a fixed account.
RUN chmod 0755 /usr/local/bin/irc-mcp-entrypoint \
    && mkdir -p /var/lib/rmcp-irc/downloads \
    && chmod 1777 /var/lib/rmcp-irc /var/lib/rmcp-irc/downloads

ENV IRC_PORT=6667 \
    MCP_LISTEN=0.0.0.0:8080 \
    MCP_ALLOWED_HOST=irc \
    MCP_CONFIG=/etc/rmcp-irc/rmcp-irc.toml \
    MCP_WORKDIR=/var/lib/rmcp-irc \
    MCP_DCC_ADVERTISED_ADDRESS=auto

# 6667 IRC, 8080 Streamable HTTP MCP at /mcp. Streamable HTTP has no built-in
# authentication: publish these only onto a trusted network.
EXPOSE 6667/tcp 8080/tcp

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD nc -z 127.0.0.1 "${IRC_PORT}" && nc -z 127.0.0.1 "${MCP_LISTEN##*:}"

ENTRYPOINT ["/usr/local/bin/irc-mcp-entrypoint"]
