# ADR 0031 — The `async-nats` pin, its minimal feature set, and the NATS test substrate

**Status:** Accepted — 2026-09-04 · **amended 2026-09-04** (§1, §4) and **2026-09-05** (§5, §6)
— see [Amendments](#amendments)
**SRS:** §7.1, §8.2, §40, §41, §42, §43.B
**Builds on:** [ADR 0021](0021-testcontainers-and-pooler-test-substrate.md) (testcontainers is the
integration substrate), [ADR 0022](0022-workspace-ci-and-yaml-policy.md) (workspace/CI/YAML policy),
[ADR 0024](0024-msrv-1-88-and-msrv-policy.md) / [ADR 0025](0025-provider-crate-msrv.md) (MSRV policy)
**Related:** RELIAR-2 (Phase 2), RELIAR-31, contract `../architecture/phase2-contract.md` §7

## Context

Phase 2 needs a JetStream server for local runs and in CI, a dependency pin for `async-nats`, and a
home for the end-to-end Postgres-outbox → NATS test. Four constraints shape the answers, and three
of them were verified against the real thing rather than assumed:

1. `async-nats` **0.50.0** declares `rust-version = "1.88.0"`. The workspace floor is already 1.88
   (ADR 0024), so — unlike sqlx (ADR 0025) — this driver forces **no** per-crate MSRV override.
2. Its default feature set is large (`ring`, `nkeys`, `crypto`, `kv`, `object-store`, `service`,
   `websockets`, `nuid`, four `server_2_1x` flags) and includes a `chrono` *optional* feature, which
   `deny.toml` bans graph-wide.
3. GitHub Actions **service containers cannot override a container's `CMD`** — they accept only
   `image`, `env`, `ports`, `volumes` and `options`. The official `nats` image's default command is
   `nats-server --config /etc/nats/nats-server.conf`, which starts **without JetStream**. A `nats`
   service container is therefore incapable of serving JetStream, and a suite pointed at one fails
   on the first publish (ADR 0029 §3).
4. The obvious probes for "is JetStream serving" are vacuous. Verified against `nats:2.14-alpine`:
   `/healthz`, `/healthz?js-enabled-only=true` and `/healthz?js-server-only=true` all return
   `{"status":"ok"}` on a server started **without** `-js`. `/jsz?config=1` discriminates — it
   reports `"disabled": true` and omits `store_dir` when JetStream is off.

## Decision

### 1. Dependency

`[workspace.dependencies]` pins

```toml
async-nats = { version = "0.50", default-features = false, features = ["jetstream"] }
```

`jetstream` is the only feature Reliar needs; everything else is the **application's** choice and
reaches the same compiled `async-nats` through Cargo feature unification, because the host must
depend on `async-nats` anyway to build the `Context` (ADR 0029 §1). In particular a
rustls crypto provider (`ring` / `aws-lc-rs`) is *not* enabled here: a library that forces one on
its host is deciding the host's TLS stack. `chrono` stays off, so `deny.toml`'s graph-wide ban is
untouched. Verified: the crate compiles with exactly this feature set, and `cargo deny check
licenses bans sources` over the resulting 130-package graph passes against the existing allow-list —
**`deny.toml` needs no change** (new licences encountered: `Apache-2.0`, `Apache-2.0 AND ISC`,
`ISC`, `BSD-3-Clause`, `Unicode-3.0`, `CDLA-Permissive-2.0`, all already allowed).

No `server_2_1x` feature is enabled: `Context::publish_with_headers` needs none. Enabling one later
to reach a newer server API is additive.

### 2. Local: a `nats` service in `deploy/compose`

`nats:2.14-alpine`, `command: ["-js", "-m", "8222"]`, client port bound to `127.0.0.1:4222`, with a
healthcheck that actually proves JetStream is serving:

```yaml
test: ["CMD-SHELL", "wget -q -O- 'http://127.0.0.1:8222/jsz?config=1' | grep -q store_dir"]
```

Per constraint (4) this fails both when the server is down and when it is up without JetStream —
which is the only kind of healthcheck worth writing (the compose file already carries the same
lesson for PgDog). JetStream storage stays inside the container: it is dev infrastructure, and a
persistent volume would make a stale stream survive a `down`/`up` and quietly change test outcomes.

### 3. CI: an explicit `docker run` step, not a service container

`test.yaml` starts NATS as a step (`docker run -d … nats:2.14-alpine -js -m 8222`), waits on the
same `/jsz` probe, and exports `NATS_URL=nats://127.0.0.1:4222` for the job. Constraint (3) leaves
no alternative that serves JetStream, and the step form has two side benefits: the flags are visible
next to the probe, and the image pin sits in the same file as everything else CI pins. The
`postgres` service container is unchanged.

### 4. Tests: `NATS_URL` when set, testcontainers otherwise

The same rule §8.2 and ADR 0021 already set for Postgres. The NATS-touching tests of
`reliar-transport-nats` live in **one** `harness = false` binary that starts **one**
`GenericImage("nats", "2.14-alpine")` with `-js -m 8222` when `NATS_URL` is unset, holds it as a
local (never a `static`), and drops it before `main` returns — the RELIAR-27 pattern, for the same
reason (testcontainers 0.27 has no reaper). Readiness is a **functional retry loop**, not a log
line — see [Amendment A](#amendments). A `GenericImage` rather than
`testcontainers-modules::nats`: the module cannot enable JetStream and hides the tag from the
pin-equality gate.

Because a CI-provided server is **shared across test binaries**, every scenario creates its own
stream over its own subject prefix and deletes it at the end (contract §7). Nothing depends on an
empty server.

### 5. The image-pin equality gate covers NATS

`ci.yaml`'s "compose and the tests pin the same images" step is extended to compare **five**
places, two of them Rust consts: compose's `nats:` image, `test.yaml`'s `docker run` image, the
`NATS_IMAGE`/`NATS_TAG` consts in the transport crate's test harness, the same const pair in
`tests/system`'s e2e harness (§6 — it starts its own NATS container), and `CONTRIBUTING.md`'s
prose line. The prose comparison is drift-only (an absent mention passes, a different image does
not); the other four are equality. Each per-package comparison is **keyed on the package
existing** (`cargo metadata`), not on the file existing: once `reliar-transport-nats` or
`reliar-system-tests` is a member, missing consts fail the job rather than skipping it — a guard
that can silently stop guarding is worse than no guard (ADR 0025 §4's lesson, and the reason the
Postgres probes are keyed this way). The same step likewise compares `tests/system`'s
`POSTGRES_TAG` against compose.

### 6. The end-to-end test is a `tests/system` workspace package

The Phase 2 e2e scenario (story C9) needs `reliar-store-postgres` **and** `reliar-transport-nats`.
Putting it in either crate's `tests/` would make one provider depend on the other, which conventions
§2 forbids in both directions — a dev-dependency is still a manifest dependency, still resolved at
publish time, and still drags sqlx into the transport crate's build. It therefore lands in
`tests/system/`, the real member package SRS §6 and §7.1 already reserve for exactly this, with
`publish = false` and both providers as **dev-dependencies** (so the MSRV job, which does not build
dev-dependencies, is unaffected — ADR 0025 §4). `[workspace] members` gains `"tests/*"` in the same
change that creates the package, never before: Cargo rejects a glob that matches nothing.

The rule this package exists to respect is itself gated: `ci.yaml`'s `purity` job runs a
"no provider depends on another provider" step over `cargo metadata --no-deps`, failing if any
`reliar-store-*`/`reliar-transport-*` member declares another provider as a dependency of **any**
kind — normal, dev or build. Without it the shortcut this section rejects (a `reliar-store-postgres`
dev-dependency inside `reliar-transport-nats`) compiles, tests, and passes every other job; a
prohibition with no gate is a comment. The check is keyed on `cargo metadata`, so it covers
providers that do not exist yet without listing them.

## Consequences

- One NATS version is pinned in five places, two of them Rust consts (§5), and drift fails CI
  rather than producing a "works locally" difference.
- Reliar's own dependency footprint stays small (`jetstream` only), and no host inherits a crypto
  provider, a KV/object-store client or a websocket stack from us.
- CI's NATS is a step, so it is not visible in the job's `services:` block. That is a documented
  deviation from story C10's wording ("a NATS service container"), forced by constraint (3) and
  recorded here so the next reader does not "fix" it back into a service that cannot serve
  JetStream.
- `tests/system` is a new package to maintain, and the first one — the workspace's `members` glob
  and the `check`/`test` jobs pick it up automatically, so the cost is a directory, not a workflow.
- Local runs still pay one container start per test binary when `NATS_URL` is unset. Unchanged from
  Postgres, and the reason the suite is one binary.

## Alternatives considered

- **`async-nats` with default features.** Rejected: it pulls `ring`, `nkeys`, `kv`, `object-store`,
  `websockets` and `service` into every downstream build for a publisher that needs none of them,
  and it decides the host's TLS backend.
- **Enable `ring` "so TLS works".** Rejected: TLS *does* work — the host's own `async-nats`
  features unify with ours (and its defaults already include `ring`). Enabling it here would only
  force the choice on a host that wanted `aws-lc-rs` or `fips`.
- **A `nats` service container with a mounted config enabling JetStream.** Services start before
  `actions/checkout`, so a repo-provided config file does not exist yet; a config baked into a
  custom image would make CI depend on an image we publish for one flag.
- **No NATS in CI at all — let testcontainers boot it there too.** Tempting (it is fewer moving
  parts) but it never exercises the `NATS_URL` path that CI is the only consumer of, and it pays a
  container start per binary on every run.
- **`testcontainers-modules`' `nats` module.** Cannot enable JetStream, and its tag is internal, so
  the pin-equality gate would have nothing to compare.
- **The e2e test inside `reliar-transport-nats/tests/`.** Rejected in §6: provider-on-provider
  dependency, plus sqlx and a 1.94 MSRV in a crate that needs neither.

## Amendments

### Amendment A — 2026-09-04 — `tokio` is a runtime dependency, and server readiness is the JetStream retry loop

Prompted by the round-2 crate review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-nats-crate-review-2.md`, m9) and by the
S2 harness experience the PO recorded on RELIAR-30.

**1. `tokio` moves from dev-dependencies to dependencies.** §1 listed the crate's runtime deps
without `tokio`, and contract §1 marked it S2-only — but §4.2's publish path is
`tokio::time::timeout(...)` over the send and the ack, with a `tokio::time::Instant` for the
measured `after_ms` (ADR 0028 Amendment A). That is a runtime dependency, and the earlier listing
was simply wrong. The entry is `tokio = { workspace = true }`: the workspace pin already carries
`time` (and `rt`, `sync`, `macros`) with `default-features = false`, so no feature is added. The
dev-dependency entry stays, adding only `rt-multi-thread` + `macros` for `#[tokio::test]`.

**No new third-party crate enters the graph**: `reliar-outbox` — a mandatory dependency of this
crate — already depends on `tokio` at the same workspace pin, so `cargo tree` and `cargo deny` are
unchanged. The dependency rule is untouched: `tokio` is a runtime, not a transport or a store.

**2. Readiness is a JetStream retry loop, not a log line.** §4 said the container is "waited on the
`/jsz?config=1` → `store_dir` probe". In practice `WaitFor::message_on_stdout("Server is ready")`
**races** nats-server: the line is printed before JetStream is accepting API requests, and the first
`connect()` can itself fail. The rule is therefore explicit:

> The harness retries a *functional* probe — `connect()` **and** a JetStream API call (a stream
> create/lookup) — with a bounded number of attempts and a short delay, and treats a failure of the
> **first `connect()`** as one more retryable attempt rather than a fatal error. No log-based wait
> is used as the readiness signal, for the same reason a wall-clock `sleep` is not: neither is a
> statement about the thing being ready.

**3. Test-substrate consequence of "needs a live `Context`".** Two contract test ids were written as
unit tests but cannot be: constructing a `NatsPublisher` requires a `Context`, which requires a
connected `Client`. **U12** (the `NatsConfigError` cases) moves into the one `nats` binary as
**N7**, and **U10** (a custom resolver is honoured) is **withdrawn** — **N6** already asserts that
the subject the resolver produced is the subject the stream received, which is the stronger claim.
Contract §7 reflects both.

**Consequences.** The dependency listing matches reality, so `cargo machete` and a reviewer reading
§1 agree with the code. The readiness rule removes the flake class RELIAR-27 hit, and keeps every
NATS test deterministic without a sleep. No CI or compose change follows from this amendment.

### Amendment B — 2026-09-05 — the pin gate covers `tests/system`, and §6's rule becomes a CI gate

Prompted by the platform review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-platform-adr-review-1.md`, M1/M2).

**1. §5's count was wrong and its coverage was short.** The e2e package created by §6 pins Postgres
and NATS in consts of its own — it starts both containers — and `CONTRIBUTING.md` names the NATS
image in prose. That is five places for NATS, not three, and `tests/system` was in none of the
comparisons: its consts could drift to a tag CI never runs and nothing would say so. §5 is corrected
above and `ci.yaml`'s step now reads `tests/system/tests/e2e/main.rs` for `POSTGRES_TAG`,
`NATS_IMAGE` and `NATS_TAG`, keyed on `reliar-system-tests` being a workspace member. Every
per-package probe in the step is now keyed on `cargo metadata` rather than on a directory, which is
what §5 always claimed.

**2. §6's prohibition is enforced.** "Providers never depend on each other" had no gate, so the
shortcut §6 exists to prevent would have passed CI. `ci.yaml`'s `purity` job gains a
"no provider depends on another provider" step over `cargo metadata --no-deps`, covering every
dependency kind (a dev-dependency is the one that would actually have been used). The `purity` job
additionally asserts the number of pure crates it checked, so it can no longer report success having
skipped them all.

**Consequences.** No change to any pinned version, image, or crate — this amendment adds gates and
corrects a count. A contributor who bumps the NATS tag now edits five places or CI tells them which
one they missed, and a cross-provider dependency fails in `ci` instead of at review time.
