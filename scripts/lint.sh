#!/usr/bin/env bash
# Everything ci.yaml's `check` job runs, in the same order.
set -euo pipefail
cd "$(dirname "$0")/.."

export RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings}"
export SQLX_OFFLINE="${SQLX_OFFLINE:-true}"

cargo fmt --all --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --no-deps --all-features

if command -v cargo-hack >/dev/null 2>&1; then
  cargo hack check --workspace --feature-powerset --no-dev-deps
else
  echo "skipping feature powerset: cargo install cargo-hack"
fi

if command -v cargo-machete >/dev/null 2>&1; then
  cargo machete
else
  echo "skipping unused-dependency check: cargo install cargo-machete"
fi

if command -v cargo-deny >/dev/null 2>&1; then
  cargo deny check
else
  echo "skipping licence/ban audit: cargo install cargo-deny"
fi
