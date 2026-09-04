#!/usr/bin/env bash
# Dry-run of what release.yaml would do. Publishing happens in CI, never from a workstation.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v release-plz >/dev/null 2>&1; then
  echo "release-plz not installed: cargo install release-plz --locked" >&2
  exit 1
fi

release-plz update --dry-run
release-plz release --dry-run

if command -v cargo-semver-checks >/dev/null 2>&1; then
  cargo semver-checks check-release --workspace
else
  echo "skipping API compatibility check: cargo install cargo-semver-checks"
fi
