# sisa-reliar

High-performance Rust toolkit for transactional outbox, inbox, idempotency, durable
messaging, and SQL-first background jobs.

**Status: pre-0.1, Phase 1 in progress** — the workspace, toolchain, and CI are in place; the
crates are being built. Nothing is published to crates.io yet, and the public API is unstable.

## Guarantees

Reliar delivers **at-least-once**. A message staged in your database transaction is published at
least once, and **duplicates are expected**. There are three windows that produce them, and **all
three are unavoidable in this release**:

- **The crash window** (§22) — the broker accepts the publish, then the worker dies before the
  outcome is persisted. The lease expires and another worker republishes the message.
- **The slow-batch window** (§22.1) — no crash at all. A worker holds a large batch, publishes it
  more slowly than the lease lasts, and the lease expires mid-batch while the worker is perfectly
  healthy; a second worker reclaims the tail and republishes it. Lease renewal and a per-publish
  timeout make this rare, and the `locked_by` guard stops the losing worker from corrupting state,
  but the duplicate itself remains. In practice this is the common one.
- **The drain window** (§26.1) — on cancellation, `run()` drains in-flight publishes for at most
  `DispatcherSettings::drain_timeout`; a publish still unresolved at the timeout is released
  rather than awaited further, and its eventual outcome — success or failure — carries the same
  duplicate risk as the other two, just triggered by shutdown instead of a lease.

Consumers must therefore be idempotent — the inbox and idempotency crates will exist for exactly
that. There is **no exactly-once delivery**. There is **no ordering guarantee by default**:
`Ordering::Unordered` is the only value this release supports, and guarantees nothing about order
— not globally, not per conversation, not per aggregate, not approximately. An opt-in, narrower-scope
ordered strategy (`Ordering::PerKey`) is **planned for 0.2**; selecting it before then is a
configuration error, never a silent no-op.

## Quickstart

- **No database:** `cargo run -p outbox-basic` — the whole claim → publish → complete loop against
  the in-memory `test-support` fakes. Walkthrough: `docs/guides/getting-started.md`.
- **A real database:** `examples/axum-outbox` — an Axum handler writing a business row and an
  outbox row in one transaction, `reliar-store-postgres`'s `migrate()`, and a dispatcher tied to
  graceful shutdown. Walkthrough: `docs/guides/postgres.md`.
- **The frozen public API:** `docs/architecture/phase1-contract.md`; the crate map and both
  request/delivery paths: `docs/architecture/overview.md`.

## Development

Requires the toolchain pinned in `rust-toolchain.toml` and Docker for the integration tests.

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features   # integration tests start their own Postgres container
```

See `CONTRIBUTING.md` for the full gate list, the compose stack for the examples, the house
rules; the SRS in the sibling [sisa-reliar-backlog](https://github.com/sisaio/sisa-reliar-backlog)
repo for the architecture baseline; and `docs/decisions/` for the ADRs.

## Licence

MIT — see `LICENSE`.
