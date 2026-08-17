# Protocol references

The runtime preserves exact capability and ISUPPORT spellings because IRC
extensions and server support can vary. The connected server's protocol
resource shows the features available for a particular agent.

## Model Context Protocol

- [MCP base protocol and lifecycle](https://modelcontextprotocol.io/specification/2026-07-28/basic)
- [Streamable HTTP transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [Subscriptions](https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/subscriptions)
- [Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
- [Resources and templates](https://modelcontextprotocol.io/specification/2026-07-28/server/resources)
- [`rmcp` Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)

## Ergo

- [Ergo documentation](https://github.com/ergochat/ergo/tree/master/docs)
- [Ergo user guide](https://github.com/ergochat/ergo/blob/master/docs/USERGUIDE.md)

Ergo controls guest permissions, channel behavior, privilege checks, and
retained history. `rmcp-irc` connects to an existing server and does not modify
server configuration or provision accounts.

## IRC and IRCv3

- [Modern IRC client protocol](https://modern.ircdocs.horse/)
- [IRCv3 capability negotiation](https://ircv3.net/specs/extensions/capability-negotiation)
- [IRCv3 message tags](https://ircv3.net/specs/extensions/message-tags)
- [IRCv3 batches](https://ircv3.net/specs/extensions/batch)
- [IRCv3 labeled responses](https://ircv3.net/specs/extensions/labeled-response)
- [IRCv3 standard replies](https://ircv3.net/specs/extensions/standard-replies)
- [IRCv3 CHATHISTORY](https://ircv3.net/specs/extensions/chathistory)
- [IRCv3 multiline](https://ircv3.net/specs/extensions/multiline)
- [Modern IRC CTCP](https://modern.ircdocs.horse/ctcp.html)
- [Modern IRC DCC](https://modern.ircdocs.horse/dcc.html)

## Parser

[`ircv3_parse`](https://github.com/m3idnotfree/ircv3_parse) provides the
valid-UTF-8 syntax parser. The gateway wraps it with byte framing, invalid-UTF-8
retention, strict outbound validation, and the owned wire representation
described in [PROTOCOL_COMPATIBILITY.md](PROTOCOL_COMPATIBILITY.md).
