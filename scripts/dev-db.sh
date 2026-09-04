#!/usr/bin/env bash
# Start the local Postgres the examples talk to. The tests do not use it — see scripts/test.sh.
#
#   ./scripts/dev-db.sh          Postgres only
#   ./scripts/dev-db.sh pooler   Postgres + PgBouncer in transaction mode on 127.0.0.1:6432
#   ./scripts/dev-db.sh down     stop everything
set -euo pipefail
cd "$(dirname "$0")/.."

compose_file=deploy/compose/docker-compose.yaml
secret=deploy/compose/secrets/postgres_password

if [ "${1:-up}" = "down" ]; then
  docker compose -f "$compose_file" down
  exit 0
fi

if [ ! -f "$secret" ]; then
  echo "missing $secret — see deploy/compose/secrets/README.md" >&2
  exit 1
fi

password=$(cat "$secret")

if [ "${1:-}" = "pooler" ]; then
  # The pgbouncer image has no *_FILE support, so the secret is passed through the environment of
  # this process only.
  RELIAR_PG_PASSWORD="$password" docker compose -f "$compose_file" --profile pooler up -d --wait
  echo "Postgres on 5432, PgBouncer (transaction mode) on 6432."
  echo "A transaction-mode pooler drops startup options, so set search_path on the role instead:"
  echo "  psql \"postgres://reliar:\$RELIAR_PG_PASSWORD@127.0.0.1:5432/reliar\" \\"
  echo "    -c 'ALTER ROLE reliar SET search_path = reliar, public'"
else
  docker compose -f "$compose_file" up -d --wait postgres
  echo "Postgres is up. Point the examples at it with:"
  echo "  export DATABASE_URL='postgres://reliar:${password}@127.0.0.1:5432/reliar?options=-c%20search_path%3Dreliar,public'"
fi
