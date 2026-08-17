#!/bin/sh
#
# Entrypoint for the single-container Ergo + rmcp-irc image.
#
# Runs the upstream Ergo server and the irc-mcp gateway side by side in one
# container and behaves like a single service:
#
#   - SIGTERM/SIGINT stop both processes, then exit 0;
#   - SIGHUP is forwarded to Ergo only, so `docker kill -s HUP` still rehashes
#     ircd.yaml and ircd.motd;
#   - if either process exits on its own the entrypoint stops the other and
#     exits non-zero, so a restart policy recreates a complete service rather
#     than leaving a half-running container behind.
#
# Everything is overridable through the environment; see the ENV block in the
# Dockerfile for the defaults.

set -eu

log() {
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') [entrypoint] $*" >&2
}

ERGO_ENTRYPOINT="${ERGO_ENTRYPOINT:-/ircd-bin/entrypoint.sh}"
ERGO_BIN="${ERGO_BIN:-/ircd-bin/ergo}"
IRC_PORT="${IRC_PORT:-6667}"
MCP_BIN="${MCP_BIN:-/usr/local/bin/irc-mcp}"
MCP_CONFIG="${MCP_CONFIG:-/etc/rmcp-irc/rmcp-irc.toml}"
MCP_LISTEN="${MCP_LISTEN:-0.0.0.0:8080}"
MCP_ALLOWED_HOST="${MCP_ALLOWED_HOST:-irc}"
MCP_WORKDIR="${MCP_WORKDIR:-/var/lib/rmcp-irc}"
MCP_DCC_ADVERTISED_ADDRESS="${MCP_DCC_ADVERTISED_ADDRESS:-auto}"
IRC_READY_TIMEOUT="${IRC_READY_TIMEOUT:-30}"
SHUTDOWN_TIMEOUT="${SHUTDOWN_TIMEOUT:-10}"

ergo_pid=""
mcp_pid=""
stopping=""

#######################################
# Effective gateway configuration
#######################################
# Prints the configuration path irc-mcp should use. When DCC advertisement is
# enabled and the supplied configuration has no [dcc] table, a copy carrying the
# container's own address is written to the writable working directory.
effective_config() {
    address="$MCP_DCC_ADVERTISED_ADDRESS"
    if [ -z "$address" ]; then
        printf '%s\n' "$MCP_CONFIG"
        return 0
    fi
    if grep -q '^[[:space:]]*\[dcc\]' "$MCP_CONFIG"; then
        log "$MCP_CONFIG defines [dcc]; leaving DCC configuration untouched"
        printf '%s\n' "$MCP_CONFIG"
        return 0
    fi
    if [ "$address" = auto ]; then
        address="$(hostname -i 2>/dev/null | tr ' ' '\n' | grep -v '^127\.' | head -n 1 || true)"
        if [ -z "$address" ]; then
            log "no non-loopback address found; DCC listeners would be unreachable"
            printf '%s\n' "$MCP_CONFIG"
            return 0
        fi
    fi

    generated="$MCP_WORKDIR/rmcp-irc.effective.toml"
    if ! {
        cat "$MCP_CONFIG"
        printf '\n[dcc]\nadvertised_address = "%s"\nallow_private_addresses = true\n' "$address"
    } > "$generated"; then
        log "cannot write $generated; using $MCP_CONFIG unchanged"
        printf '%s\n' "$MCP_CONFIG"
        return 0
    fi
    log "DCC listeners will be advertised as $address"
    printf '%s\n' "$generated"
}

#######################################
# Process control
#######################################
start_ergo() {
    if [ -x "$ERGO_ENTRYPOINT" ]; then
        set -- "$ERGO_ENTRYPOINT" run
    else
        set -- "$ERGO_BIN" run
    fi
    log "starting IRC server: $*"
    "$@" &
    ergo_pid=$!
}

wait_for_irc() {
    elapsed=0
    while [ "$elapsed" -lt "$IRC_READY_TIMEOUT" ]; do
        if nc -z 127.0.0.1 "$IRC_PORT" 2>/dev/null; then
            log "IRC server accepting connections on 127.0.0.1:$IRC_PORT"
            return 0
        fi
        kill -0 "$ergo_pid" 2>/dev/null || return 1
        sleep 1
        elapsed=$((elapsed + 1))
    done
    # Not fatal: the gateway opens upstream connections lazily, on irc.connect.
    log "IRC server not listening after ${IRC_READY_TIMEOUT}s; starting the gateway anyway"
    return 0
}

start_gateway() {
    # Checked here rather than inside the command substitution below, where an
    # exit would only leave the subshell.
    if [ ! -r "$MCP_CONFIG" ]; then
        log "gateway configuration is not readable: $MCP_CONFIG"
        exit 1
    fi
    config="$(effective_config)"
    if [ -z "$config" ]; then
        log "could not determine the gateway configuration to use"
        exit 1
    fi
    mkdir -p "$MCP_WORKDIR/downloads" 2>/dev/null || true
    log "starting MCP gateway on $MCP_LISTEN (endpoint /mcp, config $config)"
    (
        cd "$MCP_WORKDIR" || exit 1
        exec "$MCP_BIN" serve --transport http --listen "$MCP_LISTEN" \
            --allow-unauthenticated-network --allow-host "$MCP_ALLOWED_HOST" \
            --config "$config"
    ) &
    mcp_pid=$!
}

# stop_process PID NAME - TERM, then KILL after SHUTDOWN_TIMEOUT seconds.
stop_process() {
    pid="$1"
    name="$2"
    [ -n "$pid" ] || return 0
    kill -0 "$pid" 2>/dev/null || return 0
    log "stopping $name (pid $pid)"
    kill -TERM "$pid" 2>/dev/null || true
    waited=0
    while [ "$waited" -lt "$SHUTDOWN_TIMEOUT" ]; do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 1
        waited=$((waited + 1))
    done
    log "$name did not stop within ${SHUTDOWN_TIMEOUT}s; sending KILL"
    kill -KILL "$pid" 2>/dev/null || true
}

on_terminate() {
    [ -z "$stopping" ] || return 0
    stopping=1
    log "shutdown requested"
    stop_process "$mcp_pid" "MCP gateway"
    stop_process "$ergo_pid" "IRC server"
    exit 0
}

on_hup() {
    # Ergo reloads ircd.yaml and ircd.motd on SIGHUP; the gateway has no
    # equivalent and is deliberately left running.
    if [ -n "$ergo_pid" ] && kill -0 "$ergo_pid" 2>/dev/null; then
        log "forwarding SIGHUP to the IRC server (rehash)"
        kill -HUP "$ergo_pid" 2>/dev/null || true
    fi
}

#######################################
# Main
#######################################
trap on_terminate TERM INT
trap on_hup HUP

start_ergo
wait_for_irc || true
start_gateway

# Supervise. sleep keeps the shell in a foreground command, so trapped signals
# are handled promptly between iterations.
while :; do
    if ! kill -0 "$ergo_pid" 2>/dev/null; then
        wait "$ergo_pid" 2>/dev/null || status=$?
        log "IRC server exited (status ${status:-0})"
        stop_process "$mcp_pid" "MCP gateway"
        break
    fi
    if ! kill -0 "$mcp_pid" 2>/dev/null; then
        wait "$mcp_pid" 2>/dev/null || status=$?
        log "MCP gateway exited (status ${status:-0})"
        stop_process "$ergo_pid" "IRC server"
        break
    fi
    sleep 1
done

# Always non-zero: neither process is expected to exit while the container runs,
# so an orderly-looking exit is still a failure of the combined service.
status="${status:-0}"
[ "$status" -ne 0 ] || status=1
exit "$status"
