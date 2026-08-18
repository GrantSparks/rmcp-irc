# Security policy

Supported releases and the current `main` branch receive security fixes.

`rmcp-irc` is intended for trusted local or internal-network MCP clients. On
Streamable HTTP, optional `--http-bearer-token` credentials separate callers
into isolated owners; beyond that the gateway provides no agent ACLs, command
allowlists, channel policy, or DCC peer authorization. The configured Ergo
server controls IRC permissions.

The gateway validates IRC framing, bounds memory and direct-session resources,
redacts configured credentials, and requires explicit DCC file-conflict
behavior. These protections do not make an exposed HTTP endpoint safe for
untrusted clients.

The normative boundaries are detailed in
[the MCP error contract](docs/MCP_API.md#errors-and-command-outcomes),
[configuration reference](docs/CONFIGURATION.md), and
[DCC direct data plane](docs/DCC.md).

## Reporting

Report vulnerabilities through a private GitHub security advisory for this
repository. If private advisories are unavailable, contact the maintainers
privately before opening an issue. Do not publish credentials, exploit details,
private network addresses, or transferred file contents.

Include the affected version or commit, deployment mode, negotiated IRC
capabilities, minimal reproduction, and whether the issue occurs before or
after an IRC write.

## Deployment notes

- Bind Streamable HTTP only to the intended trusted network.
- On shared Streamable HTTP, configure one or more `--http-bearer-token`
  credentials; without them every caller shares one trusted local owner.
- Use plain TCP or TLS according to the configured Ergo endpoint.
- Put optional PASS/SASL values in environment variables referenced by the
  configuration file.
- Treat `agent_id` as a shareable routing handle, not a secret authorization
  token.
- Understand that DCC paths refer to the gateway host in HTTP mode.
- Run the gateway with operating-system permissions appropriate for files it
  may send or receive.
