# Contributing to rmcp-irc

Thank you for contributing.

## Getting started

Use Rust 1.88 or newer. Build and test a clean checkout with:

    cargo build --locked
    cargo test --all-targets --all-features --locked

Before changing protocol, concurrency, event, or DCC behavior, read the
relevant document under `docs/`. Keep unrelated refactoring separate from
behavioral changes.

## Git hooks

This repository uses [pre-commit](https://pre-commit.com/) to install its Git
hooks. After installing `pre-commit` 3.2 or newer, run this once in the clone:

    pre-commit install

The configuration installs all three hook types used by the project:

- `pre-commit` checks formatting and common file errors;
- `commit-msg` removes unsolicited AI-assistant attribution while preserving
  human co-author trailers; and
- `pre-push` runs the attribution hook's regression tests, then runs Rustfmt,
  the tests, Clippy, and the documentation build with the same flags used by
  CI.

The pre-push suite can also be run at any time, including before a commit:

    pre-commit run --hook-stage pre-push --all-files

To bypass a hook for an exceptional local operation, use pre-commit's `SKIP`
mechanism and name the skipped hook explicitly. Do not use `--no-verify` as a
routine part of the contribution workflow.

## Core rules

- Preserve exact wire data before adding semantic projections.
- Never infer AgentId from an HTTP connection, MCP client name, or
  conversation.
- Keep agent identity independent of MCP transport connections and client
  names.
- Do not expose a usable agent handle before RPL_WELCOME and initial MOTD
  completion.
- Keep outbound IRC operations structured; do not add a raw-line tool input.
- A reply used by a collector must remain observable in the event journal.
- Bound queues, collectors, batches, events, and DCC resources.
- Keep stdio stdout protocol-clean; diagnostics belong on stderr.
- Keep credentials out of Debug, tracing fields, errors, results, resources,
  and events.
- Treat HELP as runtime discovery, not as a parsing schema.

## Checks

    cargo test --all-targets --all-features --locked
    cargo clippy --all-targets --all-features --locked -- -D warnings
    cargo fmt --all -- --check
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --document-private-items --locked

If the optional tools are installed, also run:

    cargo deny --all-features --locked check
    cargo audit

## Tests

Add focused coverage for changed behavior. Prefer unit tests for state machines
and bounds, transcript tests for IRC exchanges, and integration tests for
cross-component or transport behavior. Protocol tests should preserve the raw
wire representation alongside semantic results.

Tests must not depend on timing alone when a deterministic channel, paused
Tokio clock, or explicit barrier can prove the behavior.

## Documentation

Update the relevant public document and examples when behavior changes. Public
Rust types and fields require API documentation. Keep server-specific
participation instructions in the Ergo MOTD rather than copying them into this
repository.

Update CHANGELOG.md for user-visible changes and
docs/PROTOCOL_COMPATIBILITY.md when a command or capability grade changes.

## Pull requests

A pull request should describe:

- the user-visible or protocol behavior changed;
- fallback and compatibility behavior;
- resource limits or security boundaries affected;
- tests run;
- related documentation updates.

Avoid bundling unrelated refactors with protocol changes. Keep generated and
local configuration out of commits.

Every pull request runs the same gates as `main`: formatting, the test matrix
on stable and beta, Clippy, documentation, an MSRV 1.88 check, `cargo deny`,
and a package payload inspection. Beta is an early-warning job; the aggregate
`CI Success` check covers the required gates.

## Releases

Maintainers publish releases by pushing a `vX.Y.Z` tag; see
[RELEASING.md](RELEASING.md). While the crate is below `1.0.0`, a minor bump
may carry breaking changes, so record them under `## [Unreleased]` in
CHANGELOG.md as they land rather than at release time.
