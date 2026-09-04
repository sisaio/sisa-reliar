//! Explicit migration entry point (SRS §35, §35.1, ADR 0018, contract §7 J3/J4).
//!
//! **Never invoked implicitly** — no constructor, `Default`, or `acquire` runs a migration.
//! Reliar's bookkeeping lives in its own schema's `_migrations` table, never the shared,
//! one-per-database `_sqlx_migrations` sqlx would otherwise write to, so this can be added to a
//! database a host already migrates with its own tooling without either side noticing the other.

use core::fmt;

use sqlx::migrate::Migrator;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, Executor, PgPool};

/// The crate's migrations, embedded at compile time from `migrations/` — the single source of
/// truth (ADR 0018): `cargo publish` packages only files under the crate's own directory, and
/// `sqlx::migrate!` resolves relative to `CARGO_MANIFEST_DIR` at compile time, so the SQL must
/// live here rather than at the repository root.
static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Where [`migrate`] creates Reliar's schema and its bookkeeping table.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct MigrateOptions<'a> {
    /// The schema to create (`CREATE SCHEMA IF NOT EXISTS`) and use for both the data tables
    /// and the `_migrations` bookkeeping table. SHALL agree with
    /// [`crate::PostgresOutboxSettings::schema`] — [`crate::PostgresOutboxStore::connect`]'s
    /// startup verification fails otherwise, since `outbox` will not resolve where it expects.
    pub schema: &'a str,
}

impl Default for MigrateOptions<'_> {
    fn default() -> Self {
        Self { schema: "reliar" }
    }
}

impl<'a> MigrateOptions<'a> {
    /// Sets [`Self::schema`]. `#[non_exhaustive]` forbids struct-literal construction outside
    /// this crate, so this is the only way to migrate into a non-default schema.
    #[must_use]
    pub const fn schema(mut self, schema: &'a str) -> Self {
        self.schema = schema;
        self
    }
}

/// [`migrate`]'s failure. **Provider-owned**, not a re-export of `sqlx::migrate::MigrateError`
/// (contract §7 J3/J4): a rejected schema identifier has no variant in `sqlx`'s own type to
/// report it as, since that check happens before any `sqlx::migrate` code runs at all.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrateError {
    /// `options.schema` is not a valid PostgreSQL identifier
    /// (`[A-Za-z_][A-Za-z0-9_$]*`, at most 63 bytes). Checked **before** the name reaches
    /// `dangerous_set_table_name`, which is string interpolation into DDL.
    InvalidSchema {
        /// The rejected schema name.
        schema: String,
    },
    /// Any failure from `sqlx::migrate::Migrator::run` or the dedicated connection's own setup
    /// (a connection failure, a checksum mismatch against an already-applied file, …).
    Sqlx {
        /// The underlying `sqlx` migration error.
        source: sqlx::migrate::MigrateError,
    },
}

impl fmt::Display for MigrateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema { schema } => write!(
                f,
                "{schema:?} is not a valid PostgreSQL identifier (expected \
                 [A-Za-z_][A-Za-z0-9_$]*, at most 63 bytes)"
            ),
            Self::Sqlx { source } => write!(f, "migration failed: {source}"),
        }
    }
}

impl std::error::Error for MigrateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx { source } => Some(source),
            Self::InvalidSchema { .. } => None,
        }
    }
}

impl From<sqlx::migrate::MigrateError> for MigrateError {
    fn from(source: sqlx::migrate::MigrateError) -> Self {
        Self::Sqlx { source }
    }
}

impl From<sqlx::Error> for MigrateError {
    fn from(source: sqlx::Error) -> Self {
        Self::Sqlx {
            source: sqlx::migrate::MigrateError::Execute(source),
        }
    }
}

/// Applies Reliar's migrations. **Never invoked implicitly** (SRS §35).
///
/// Creates `options.schema` if it does not exist, keeps bookkeeping in
/// `<schema>._migrations` — never `_sqlx_migrations` — and serializes concurrent callers with
/// an advisory lock, so every caller after the first observes `Ok(())`. **Idempotent.**
/// Self-contained: does not depend on the caller's `search_path` (ADR 0018) — `create_schema`
/// plus the qualified bookkeeping table name make it work over a pool whose URL never set one.
///
/// # Errors
///
/// Returns [`MigrateError::InvalidSchema`] when `options.schema` is not a valid PostgreSQL
/// identifier, or [`MigrateError::Sqlx`] for a connection failure, a checksum mismatch against
/// an already applied file, or any other failure `sqlx::migrate::Migrator::run` reports.
pub async fn migrate(pool: &PgPool, options: MigrateOptions<'_>) -> Result<(), MigrateError> {
    // Validated once, before it is ever interpolated into `dangerous_set_table_name`/`SET
    // search_path` below, both of which build SQL text from this value rather than binding it
    // as data (contract §7 J4).
    if !crate::error::is_valid_schema_name(options.schema) {
        return Err(MigrateError::InvalidSchema {
            schema: options.schema.to_owned(),
        });
    }

    // `Migrator` has no `Clone` impl, but every field is public (`migrate!()` relies on that to
    // construct the static in a const-promotable context), so a field-by-field copy is the
    // sanctioned way to get a mutable instance without touching the static (ADR 0018).
    let mut migrator = Migrator {
        migrations: MIGRATOR.migrations.clone(),
        ignore_missing: MIGRATOR.ignore_missing,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
        table_name: MIGRATOR.table_name.clone(),
        create_schemas: MIGRATOR.create_schemas.clone(),
    };
    migrator.create_schema(options.schema.to_owned());
    migrator.dangerous_set_table_name(format!("{}._migrations", options.schema));
    migrator.set_locking(true);

    // `SET search_path` (unqualified migration SQL needs it, ADR 0018) is session-level and
    // sqlx never resets it on release, so this is a dedicated connection, never `pool.acquire()`.
    let connect_options = pool.connect_options();
    let mut conn = PgConnection::connect_with(&connect_options).await?;
    conn.execute(sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET search_path = \"{}\", public",
        options.schema.replace('"', "\"\"")
    ))))
    .await?;
    migrator.run(&mut conn).await?;
    conn.close().await?;
    Ok(())
}
