# Local secrets

Real secrets are **never** committed. Each `*.example` file here is a template: copy it without the
`.example` suffix and edit the copy. `.gitignore` in this directory ignores everything but the
templates and this README.

```sh
cp postgres_password.example postgres_password
```

`docker-compose.yaml` mounts these as Docker secrets (`/run/secrets/<name>`), which is why
`configs/postgres.env` sets `POSTGRES_PASSWORD_FILE` rather than a literal password.

The `pgdog` service (profile `pooler`) is the exception: PgDog needs the upstream password inside
its own `users.toml`, and supports no `*_FILE` variant for it. `configs/pgdog.toml` therefore
carries no credential at all, and compose generates `users.toml` from `${RELIAR_PG_PASSWORD}` —
export it from this same secret for the one command, so the value never reaches a committed file:

```sh
RELIAR_PG_PASSWORD=$(cat postgres_password) \
  docker compose -f ../docker-compose.yaml --profile pooler up -d --wait
```
