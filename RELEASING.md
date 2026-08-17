# Releasing rmcp-irc

Publication to crates.io is irreversible. Run a release only from a clean
`main` commit whose pull request passed every required CI job.

Releases are tag-driven: pushing a `vX.Y.Z` tag runs the full CI pipeline and,
on success, publishes the crate. Do not publish by hand.

## 1. Choose and record the version

1. Choose the next semantic version. While the crate is below `1.0.0`, a minor
   bump may carry breaking changes; a patch bump must not.
2. Set `version` in `Cargo.toml`.
3. Run `cargo check` to refresh the `rmcp-irc` entry in `Cargo.lock`, confirm no
   unrelated dependency versions changed, then use `--locked` below.
4. Move the release notes from `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`,
   restore an empty `Unreleased` heading, and update the comparison links at
   the bottom of `CHANGELOG.md`.

`.github/scripts/validate-release.sh` rejects a tag unless the tag, the
manifest version, the lockfile entry, and the dated changelog heading all
agree, and it refuses to run at all if `publish = false` is set.

## 2. Validate the release commit

Run the same gates CI enforces:

    cargo fmt --all -- --check
    cargo test --all-targets --all-features --locked
    cargo clippy --all-targets --all-features --locked -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --document-private-items --locked
    cargo +1.88.0 check --all-targets --all-features --locked
    cargo deny --locked check

`cargo-deny` requires a separately installed subcommand.

Inspect the publish payload before tagging, and confirm it contains no
credentials, local configuration, downloads, or fixture secrets:

    cargo package --locked --list
    cargo package --locked

Run the [live Ergo checks](docs/LIVE_TESTING.md) for changes affecting
interoperability, transports, reconnects, or DCC. These are not part of CI.

## 3. Tag and publish

After the release pull request is merged and required checks pass on `main`:

    git tag -s vX.Y.Z -m "rmcp-irc X.Y.Z"
    git push origin vX.Y.Z

The tag workflow then:

1. runs the full test, lint, MSRV, dependency-policy, and packaging matrix;
2. validates that the tagged commit is an ancestor of `main`;
3. validates tag, manifest, lockfile, and changelog consistency;
4. dry-runs and publishes the crate with `--locked`; and
5. polls crates.io until the exact version resolves from the index.

The publish step is idempotent: if the version is already on crates.io, it is
skipped rather than retried.

After success, verify the crates.io page, then create the GitHub release from
the signed tag using the matching changelog section.

## 4. Failure recovery

Never reuse a published version for changed source, and never move or recreate
a release tag to bypass a failed validation gate.

- If a gate fails before publication, fix the problem on `main`, delete the
  unpublished tag, and tag the corrected commit.
- If publication itself fails, diagnose the dry-run or publish output. If no
  package content changes, rerun the failed job from the same tagged commit.
  If the source or package metadata must change, choose a new patch version
  and repeat the process from step 1.

## Repository prerequisites

- `CRATES_TOKEN` — a crates.io API token with publish scope for this crate,
  stored as a secret on the `crates-io` environment.
- Branch protection on `main` requiring the `CI Success` check.
