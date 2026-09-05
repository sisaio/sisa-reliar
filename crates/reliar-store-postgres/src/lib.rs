//! `reliar-store-postgres` is Reliar's PostgreSQL provider: the schema, the explicit
//! [`migrate`] API, and [`PostgresOutboxStore`] — the only crate where an `sqlx`/Postgres type
//! appears (SRS §20–§26, §35, ADR 0002).
//!
//! # MSRV
//!
//! This crate declares `rust-version = "1.94"`, six releases above the workspace floor
//! (`1.88`): `sqlx` 0.9 requires it. Pure crates (`reliar-core`, `reliar-outbox`) stay reachable
//! on `1.88` for hosts bringing their own store (ADR 0025).
//!
//! # Features
//!
//! - `json` (**default**) — [`PostgresOutboxStore<JsonSerializer>`]'s default type parameter
//!   and the [`PostgresOutboxStore::new`]/[`PostgresOutboxStore::with_settings`] convenience
//!   constructors (forwards `reliar-core/json`). Not hard-enabled: a deployment supplying its
//!   own [`reliar_core::Serializer`] should not have to pull in `serde_json`. Under
//!   `--no-default-features`, [`PostgresOutboxStore::connect`] is the only constructor.
//! - `serde` (off by default) — `Serialize`/`Deserialize` on [`PostgresOutboxSettings`],
//!   `#[serde(default, deny_unknown_fields)]` so a typo'd config key is a hard error, durations
//!   as integer milliseconds. `serde` itself is always a dependency regardless of this feature —
//!   it also drives the crate's private `MetadataRest` JSONB contract (ADR 0012), which is not
//!   feature-gated.
//!
//! # `search_path` setup
//!
//! Every Reliar object lives in **one configurable schema, `reliar` by default**, with
//! unprefixed table names (`outbox`). `sqlx::query!` checks SQL at compile time, so every
//! identifier in every statement is a static, unqualified literal — the schema is resolved at
//! connection time through `search_path`, never compiled in (ADR 0017).
//!
//! - **The host puts `reliar` first** on the connection URL: `?options=-c%20search_path%3Dreliar,public`.
//! - **Behind a transaction-mode pooler that drops startup `options`** (some reject the
//!   parameter outright with `08P01`), use a server-side default instead:
//!   `ALTER ROLE <app> SET search_path = reliar, public`. This is the portable mechanism —
//!   verify it against your own pooler build/version rather than assuming: `PgDog`
//!   (`ghcr.io/pgdogdev/pgdog:v0.1.46`, the pooler this crate's suite runs behind) was found to
//!   **pass the `options` parameter through** to the upstream server instead of dropping it, so
//!   the URL-`options` path above works unmodified behind it too, with no `ALTER ROLE` required
//!   — but a different pooler, or a different `PgDog` configuration, could behave either way
//!   (§43.A.35).
//! - [`PostgresOutboxStore::connect`]/[`PostgresOutboxStore::new`] verify **once at
//!   construction** that the unqualified name `outbox` resolves to the configured schema, and
//!   fail fast — naming the configured schema, the observed `search_path`, and the `ALTER ROLE`
//!   remedy — rather than surprise-failing on the first `acquire`.
//! - [`migrate`] does not depend on the caller's `search_path`: it creates the schema itself and
//!   qualifies its own bookkeeping table name (ADR 0018).
//!
//! # Guarantees
//!
//! - **Migrations never run implicitly.** [`migrate`] is the only entry point, and it is
//!   idempotent and safe under concurrent callers (SRS §35).
//! - **The claim is one statement.** [`PostgresOutboxStore`]'s `acquire` (via
//!   [`reliar_outbox::OutboxStore`]) uses a `FOR UPDATE SKIP LOCKED` claim that commits before
//!   the call returns; no network I/O ever happens while a Reliar transaction is open (ADR 0006).
//! - **`enqueue` joins the caller's own transaction** — atomicity is visible in the signature —
//!   and performs no I/O beyond the one `INSERT` (plus, opt-in, a `search_path` wrap).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod duration_serde;
mod error;
mod migrate;
mod records;
mod settings;
mod store;

pub use error::{EnqueueError, PostgresStoreError};
pub use migrate::{MigrateError, MigrateOptions, migrate};
pub use reliar_core::SettingsError;
pub use settings::PostgresOutboxSettings;
pub use store::{EnqueueOptions, PostgresOutboxStore};

#[cfg(feature = "json")]
pub use reliar_core::JsonSerializer;
