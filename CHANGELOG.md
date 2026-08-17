# Changelog

Notable user-facing changes are documented here. This project follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and Semantic
Versioning. While the version is below `1.0.0`, a minor bump may contain
breaking changes to the CLI, configuration, or MCP surface.

## [Unreleased]

### Fixed

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
