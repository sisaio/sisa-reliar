# deploy/docker

Dockerfiles for images this repository builds itself — none yet.

Expected residents:

- `Dockerfile.tests` — the `tests/system` harness image, when a cross-crate system test needs to
  run inside the compose network rather than on the host.
- `Dockerfile.tools` — one-shot images for `tools/*` binaries (for example a migration runner
  built from `reliar-store-postgres`'s public `migrate()` API) for teams that run migrations
  through their own pipeline.

Local infrastructure Reliar only *consumes* (Postgres, later NATS) is pulled as an upstream image
and lives in `deploy/compose/docker-compose.yaml`; nothing needs a Dockerfile for that.
