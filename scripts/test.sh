#!/usr/bin/env bash
# Run the workspace test suite. Extra arguments go to cargo test, e.g. ./scripts/test.sh -p reliar-outbox.
#
# No DATABASE_URL is exported on purpose: SRS §8.2 forbids running tests against a shared or
# long-lived database. Provider tests boot their own ephemeral Postgres through testcontainers and
# create an isolated database per test. The compose stack in deploy/compose is for the examples.
# CI may substitute a service container by setting DATABASE_URL in the environment itself.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available; testcontainers-backed integration tests will fail." >&2
  echo "Start Docker, or run a subset: ./scripts/test.sh -p reliar-core" >&2
fi

# RELIAR-27, third line of defence after `Drop`/`watchdog` (review 4 major 3): scoped to
# `reliar-`-named + labelled containers so another project's testcontainers are never touched;
# networks have no label API in testcontainers 0.27, so name prefix is all there is to sweep by.
sweep_testcontainers() {
  local ids
  ids="$(docker ps -aq --filter label=org.testcontainers.managed-by=testcontainers --filter name=^reliar-)"
  if [ -n "$ids" ]; then
    echo "$ids" | xargs -r docker rm -f -v >/dev/null 2>&1 || true
  fi

  local networks
  networks="$(docker network ls -q --filter name=^reliar-)"
  if [ -n "$networks" ]; then
    echo "$networks" | xargs -r docker network rm >/dev/null 2>&1 || true
  fi
}
trap sweep_testcontainers EXIT

cargo test --workspace --all-features "$@"
