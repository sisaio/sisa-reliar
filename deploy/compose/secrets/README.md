# Local secrets

Real secrets are **never** committed. Each `*.example` file here is a template: copy it without the
`.example` suffix and edit the copy. `.gitignore` in this directory ignores everything but the
templates and this README.

```sh
cp postgres_password.example postgres_password
```

`docker-compose.yaml` mounts these as Docker secrets (`/run/secrets/<name>`), which is why
`configs/postgres.env` sets `POSTGRES_PASSWORD_FILE` rather than a literal password.

The `pgbouncer` service (profile `pooler`) is the exception: that image reads `DB_PASSWORD` from
the environment and supports no `*_FILE` variant. `scripts/dev-db.sh pooler` reads
`postgres_password` and passes it through that one process's environment, so the value still never
reaches a committed file.
