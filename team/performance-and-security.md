# Performance & Security

Cross-cutting requirements every story must satisfy, grounded in what Reliar is: a **library**
that other applications embed. Owners by role below; the **reviewer** checks all of it on the diff,
and the gates live in `definition-of-done.md`. Items the design already guarantees are marked ✓
(uphold them, don't undo them).

## Architect — design time

**Performance**
- Set a **budget per hot path** (claim batch latency, publish throughput per worker, idle DB load per
  poll) and name the bench that measures it (`benches/outbox-throughput`).
- **Static dispatch** ✓, `bytes::Bytes` payloads ✓, lazily allocated headers ✓ — keep them.
- Decide **batching** shapes (claim N, complete/fail as batches) and **bounded concurrency** knobs;
  decide the idle strategy (poll interval + backoff, optional `LISTEN/NOTIFY` wake-up).
- Design **indexes from the real queries** (partial index on pending rows ordered by `available_at`;
  an index that serves the retention purge). No speculative indexes ✓ (SRS §24).
- Plan **retention** so the outbox table stays small (purge published/dead rows after a configurable age).

**Security**
- Trust model: Reliar runs **inside the application's trust boundary**; it never opens ports, never
  runs migrations implicitly ✓, never logs payloads ✓.
- **Supply chain:** `deny.toml` (licenses, advisories, bans), `cargo audit` in `security.yaml`, minimal
  default features, pinned toolchain. Every new dependency is justified in the PR.
- **Reserved header namespace** (`reliar-*`) cannot be set by applications ✓ (validated `Headers` newtype).

## Engineer — implementation

**Performance**
- Never block the Tokio runtime; never hold a lock or a SQL transaction across broker I/O ✓ (SRS §21).
- Avoid clones/allocations on the per-message path; borrow `&SerializedEnvelope`; reuse buffers.
- Use prepared statements (sqlx macros ✓), `= ANY($1)`/`UNNEST` for batch updates, `LIMIT` on
  every claim/purge query, short transactions.
- `EXPLAIN (ANALYZE, BUFFERS)` the claim and purge queries once against a seeded table; record the
  plan in the card when adding or changing an index.
- Backoff with jitter so many workers don't synchronize on the database.

**Security**
- **Parameterized queries only** ✓ (sqlx macros — no string SQL). `#![forbid(unsafe_code)]` ✓.
- No `unwrap`/`expect` on data read from the database or the wire; malformed rows/messages become
  typed errors (permanent → dead), never panics that kill the worker.
- Payload `bytea` may contain PII: never log it, never include it in error `Display`, and make
  retention/purge easy to configure.
- Examples and tests read connection strings from `DATABASE_URL`; nothing secret in the repo.
- Public API must not force callers into unsound patterns (no `'static` payload borrows, no
  `Send`-less futures that block spawning).

## Reviewer — the lens on every diff

**Performance:** per-row DB round trips in a loop · missing/oversized index for a new query shape ·
blocking call or lock/transaction held across `.await` on network I/O · unbounded query (no `LIMIT`) ·
needless hot-path allocation or `clone()` · unbounded spawning · `dyn` on a hot path.

**Security / robustness:** non-macro SQL · payload or header logging · `unwrap` on untrusted data ·
implicit migration · reserved-header bypass · new dependency without justification or license issue ·
lease/completion not guarded by `locked_by` · lock-heavy or irreversible migration.

## Gates (enforced in `definition-of-done.md`)

- Hot paths touched have a bench and stay within budget; no per-row round trips; indexes justified by EXPLAIN.
- No payload/header logging; no `unwrap` on untrusted data; `cargo deny` + `cargo audit` clean.
- Migrations are lock-safe, forward-only, and run only through the explicit `migrate()` API.
