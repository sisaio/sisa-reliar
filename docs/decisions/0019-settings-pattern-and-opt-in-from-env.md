# ADR 0019 — One `*Settings` struct per feature, with an opt-in `from_env`

**Status:** Accepted — 2026-09-04
**SRS:** §7.1, §7.2, §21.1, §22.2, §23.1, §43.A.29–30
**Decisions:** human decision 13

## Context

A worker has a dozen tunables: batch size, lease, in-flight cap, publish timeout, poll intervals,
drain timeout, backoff shape, retention windows. Passing them positionally is unreadable and every
addition is a breaking change. Passing loose `Duration`s invites unit confusion.

The bigger question is **configuration sourcing**. It is tempting for a library to read
`RELIAR_OUTBOX_LEASE_MS` itself — every host would otherwise write the same twenty lines. But a
library that reads the environment implicitly makes the host's configuration precedence a lie: the
host layers files, then env, then flags, and then Reliar quietly reads env again behind it.

## Decision

- **One `*Settings` struct per feature** — `OutboxSettings`, `DispatcherSettings`,
  `PostgresOutboxSettings`; later `InboxSettings`, `IdempotencySettings`, `CacheSettings`. The
  v1.1-draft name `DispatcherConfig` is retired: one name for one thing.
- Each is `#[non_exhaustive]` + `Debug` + `Clone` + `Default` (the accepted §23.1 defaults) + **builder
  methods** (`fn lease(mut self, d: Duration) -> Self`), so adding a field is never a breaking change.
- `serde::Serialize`/`Deserialize` derived **behind the crate's `serde` feature**, with
  `#[serde(default, deny_unknown_fields)]` so a host can load them from its own `config.toml` and a
  typo fails loudly. **Durations serialize as integer milliseconds** (`lease_ms`) — no new
  date/duration dependency and no ambiguous `"30s"` parsing.
- **The library SHALL NEVER read the environment implicitly.** No constructor, `Default` or
  `build()` touches `std::env`. Configuration precedence stays entirely the host's business, and
  this is asserted in a test that sets every variable to an absurd value and checks they are ignored
  (§43.A.30). **The rule has no convenience exceptions**: `WorkerId::generate()` therefore produces
  `pid:uuid7` and does **not** read `HOSTNAME`, even though a hostname in the lease guard would be
  useful to an operator. A host that wants one sets `worker_id` explicitly.
  *(Clarified 2026-09-04, RELIAR-13 review 1.)*
- An **opt-in** `from_env(prefix)` helper exists purely as a convenience:
  `OutboxSettings::from_env("RELIAR_OUTBOX_")`,
  `PostgresOutboxSettings::from_env("RELIAR_STORE_POSTGRES_")`. It starts from `Default`, overrides
  **only the variables that are present**, and returns `Err(SettingsError)` for an unparseable or
  out-of-range value — **never a silent fallback to the default**. A silent fallback is how a
  production deployment runs for a month with a lease it never configured.
- **`build()` validates and returns a configuration error rather than panicking.** v0.1 rejects at
  least: `max_in_flight == 0`; a `lease` not comfortably longer than `publish_timeout` (§21.1);
  `Ordering::PerKey` before 0.2, with an error naming the version that will support it (ADR 0013);
  and a `PostgresOutboxSettings.schema` disagreeing with `MigrateOptions.schema` (ADR 0017).
- `build()` additionally **warns** (does not fail) at startup when
  `lease > batch_size × publish_timeout ÷ max_in_flight` does not hold, since true publish latency
  is unknown at construction (§21.1).
- **The connection pool stays the host's.** Reliar SHALL NOT own or read a `DATABASE_URL`.

## Consequences

- Every tunable is discoverable in one rustdoc page per feature, with its default and its env name.
- Adding a setting is additive forever, which matters because the worker loop will grow tunables.
- Hosts that want env configuration write one line; hosts with their own config system are not
  fought. Both paths produce the same struct.
- Millisecond integers make settings trivially serializable and unambiguous, at the cost of reading
  `lease_ms = 30000` instead of `"30s"`. Accepted — it removes a dependency and a parser.
- `deny_unknown_fields` on settings and **ignore-unknown** on stored `MetadataRest` (ADR 0012) are
  deliberately opposite: a config typo must fail; a stored row from a newer writer must not.
- Validation in `build()` rather than at first use means misconfiguration is caught at startup,
  which is also the only case where `run()` is permitted to return `Err` (ADR 0014).

## Alternatives considered

- **Positional constructor arguments.** Rejected: unreadable and breaking on every addition.
- **Implicit `from_env` inside `Default`/`new`.** Rejected: silently overrides the host's own
  configuration precedence and makes tests depend on the ambient environment.
- **A single global `ReliarConfig`.** Rejected: couples unrelated features and forces the inbox and
  cache crates to exist before they are built.
- **Panic on invalid settings.** Rejected: a library must not abort its host; `build()` returns an
  error the host can log and act on.
- **`humantime`-style duration strings.** Rejected: a new dependency and an ambiguous parse for a
  field that is always machine-written.
