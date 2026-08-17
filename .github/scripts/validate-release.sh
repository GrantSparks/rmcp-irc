#!/usr/bin/env bash
set -euo pipefail

# Verifies that a release tag, the package manifest, the lockfile, and the
# changelog all agree before anything is published. Publication is
# irreversible, so every disagreement is a hard failure.

release_tag="${1:-${GITHUB_REF_NAME:-}}"
if [[ ! "${release_tag}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]]; then
    echo "release tag must have the form vX.Y.Z (received: ${release_tag:-<empty>})" >&2
    exit 1
fi

release_version="${release_tag#v}"

python3 - "${release_version}" <<'PY'
import json
import pathlib
import re
import subprocess
import sys
import tomllib

expected = sys.argv[1]
root = pathlib.Path.cwd()
crate = "rmcp-irc"

metadata = subprocess.run(
    ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
    check=True,
    capture_output=True,
    text=True,
).stdout

packages = {package["name"]: package for package in json.loads(metadata)["packages"]}
actual = packages[crate]["version"]
if actual != expected:
    raise SystemExit(f"{crate} version {actual} does not match tag version {expected}")

manifest = tomllib.loads((root / "Cargo.toml").read_text())
if manifest["package"].get("publish") is False:
    raise SystemExit("Cargo.toml sets publish = false; the crate cannot be released")

lock = tomllib.loads((root / "Cargo.lock").read_text())
locked = next(
    (package["version"] for package in lock["package"] if package["name"] == crate),
    None,
)
if locked != expected:
    raise SystemExit(f"Cargo.lock {crate} version {locked!r} does not match {expected}")

changelog = (root / "CHANGELOG.md").read_text()
heading = re.compile(
    rf"^## \[{re.escape(expected)}\] - \d{{4}}-\d{{2}}-\d{{2}}$", re.MULTILINE
)
if not heading.search(changelog):
    raise SystemExit(
        f"CHANGELOG.md must contain a dated '## [{expected}] - YYYY-MM-DD' heading"
    )
PY

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "version=${release_version}" >> "${GITHUB_OUTPUT}"
fi

echo "release metadata is consistent for ${release_tag}"
