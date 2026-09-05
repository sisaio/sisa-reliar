//! Hand-rolled error enums for the PostgreSQL provider (SRS §23, ADR 0008, contract §4, §7
//! J1–J4).
//!
//! No `thiserror`, no `anyhow`. Every `Display` is payload/credential-free: a decode failure
//! names the message id and a truncated detail, never the offending bytes. Classification is a
//! **per-variant table, never a blanket rule** — a `Database` failure is classified by the
//! wrapped SQLSTATE's class, not assumed transient.

use core::fmt;

use reliar_core::{Classify, FailureKind, MessageId};

/// A failure of a [`crate::PostgresOutboxStore`] `OutboxStore`/`OutboxDeadLetters` *call* —
/// never a property of one row's content. Row-content problems surface as
/// [`reliar_outbox::PoisonedRow`]s instead (ADR 0008).
#[derive(Debug)]
#[non_exhaustive]
pub enum PostgresStoreError {
    /// The unqualified name `outbox` does not resolve, or resolves to a different schema than
    /// configured. Carries the configured schema and the observed `search_path`; the `ALTER
    /// ROLE` remedy is in the `Display` text. **Permanent.**
    SchemaResolution {
        /// The schema `PostgresOutboxSettings::schema` named.
        configured: String,
        /// The `search_path` Postgres reported at construction.
        observed: String,
    },
    /// `outbox` resolved to the configured schema, but the relation itself is missing —
    /// `migrate()` has not been run. **Permanent.** Mapped from SQLSTATE `42P01` on **every**
    /// path, not just startup verification (contract §7 J2).
    NotMigrated {
        /// The configured schema.
        schema: String,
    },
    /// Connection lost, statement timeout, pool exhausted, deadlock, or any other `sqlx`
    /// failure not mapped to a more specific variant above. Classified by the wrapped
    /// SQLSTATE's **class** (never blanket-transient — see the `Classify` impl below).
    Database {
        /// The underlying `sqlx` error.
        source: sqlx::Error,
    },
    /// A claimed or listed row could not be turned into an `OutboxRecord` (a corrupt JSONB
    /// remainder, an unparseable promoted column). Surfaces as a poisoned row, never as an
    /// `acquire`/`list_dead` failure. **Permanent** — the bytes on disk do not change between
    /// attempts.
    Decode {
        /// The row's message id.
        id: MessageId,
        /// A short, payload-free description of what failed to decode.
        detail: String,
    },
    /// The row's `metadata_version` is not one this build knows how to read. **Permanent** —
    /// it needs a newer reader, not another try.
    UnknownMetadataVersion {
        /// The row's message id.
        id: MessageId,
        /// The unrecognised version.
        version: i32,
    },
    /// `enqueue` inserted a `MessageId` that already exists (`pk_outbox` violation).
    /// **Permanent** — a reused id never succeeds on retry; the row is already there
    /// (contract §7 J1).
    DuplicateMessage {
        /// The id the caller tried to reuse.
        id: MessageId,
    },
    /// `PostgresOutboxSettings::schema` or `MigrateOptions::schema` is not a valid PostgreSQL
    /// identifier (`[A-Za-z_][A-Za-z0-9_$]*`, at most 63 bytes) — checked once, before it is
    /// ever interpolated into `SET search_path`/`dangerous_set_table_name` (contract §7 J4).
    /// **Permanent** — configuration, not weather.
    InvalidSchema {
        /// The rejected schema name.
        schema: String,
    },
}

impl fmt::Display for PostgresStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaResolution {
                configured,
                observed,
            } => write!(
                f,
                "outbox did not resolve to schema \"{configured}\" (observed search_path: \
                 \"{observed}\"); set search_path so \"{configured}\" comes first, e.g. \
                 ALTER ROLE <role> SET search_path = {configured}, public"
            ),
            Self::NotMigrated { schema } => write!(
                f,
                "relation \"{schema}.outbox\" does not exist; call \
                 reliar_store_postgres::migrate(&pool, ..) before constructing the store"
            ),
            Self::Database { source } => write!(f, "database error: {source}"),
            Self::Decode { id, detail } => write!(f, "row {id} could not be decoded: {detail}"),
            Self::UnknownMetadataVersion { id, version } => {
                write!(f, "row {id} carries unknown metadata_version {version}")
            }
            Self::DuplicateMessage { id } => {
                write!(f, "message id {id} already exists in the outbox")
            }
            Self::InvalidSchema { schema } => write!(
                f,
                "{schema:?} is not a valid PostgreSQL identifier (expected \
                 [A-Za-z_][A-Za-z0-9_$]*, at most 63 bytes)"
            ),
        }
    }
}

impl std::error::Error for PostgresStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database { source } => Some(source),
            _ => None,
        }
    }
}

/// Per-variant classification table (contract §7 J1) — **no blanket "everything else is
/// transient"**. A wrong verdict is not cosmetic: `Transient` burns the dispatcher's retry
/// budget on a failure that can never succeed; `Permanent` kills a message that would have gone
/// through on the next attempt.
impl Classify for PostgresStoreError {
    fn kind(&self) -> FailureKind {
        match self {
            Self::SchemaResolution { .. }
            | Self::NotMigrated { .. }
            | Self::InvalidSchema { .. }
            | Self::DuplicateMessage { .. }
            | Self::Decode { .. }
            | Self::UnknownMetadataVersion { .. } => FailureKind::Permanent,
            Self::Database { source } => classify_sqlstate(source),
        }
    }
}

/// Classifies a `sqlx::Error` by its wrapped SQLSTATE **class**, never by message text
/// (contract §7 J1):
///
/// - **Transient** — `08*` (connection exception), `40*` (transaction rollback: deadlock,
///   serialization failure), `53*` (insufficient resources), `55*` (object in use), `57014`
///   (`query_canceled`, i.e. a `statement_timeout`), and any pool/IO error with no SQLSTATE at all.
/// - **Permanent** — `22*` (data exception), `23*` (integrity constraint violation), `42*`
///   (syntax error or access rule violation — includes `42P01`, mapped to `NotMigrated` before
///   this function ever sees it).
/// - Anything unrecognised classifies **Transient** — an unknown fault is more often weather
///   than logic — but is logged at `warn` with its SQLSTATE so this table can be extended.
pub(crate) fn classify_sqlstate(err: &sqlx::Error) -> FailureKind {
    let sqlx::Error::Database(db) = err else {
        // No SQLSTATE at all: a connection/IO/pool-exhaustion failure, not a data problem.
        return FailureKind::Transient;
    };
    let Some(code) = db.code() else {
        return FailureKind::Transient;
    };
    match code.as_ref().get(..2) {
        Some("08" | "40" | "53" | "55") => FailureKind::Transient,
        Some("57") if code.as_ref() == "57014" => FailureKind::Transient,
        Some("22" | "23" | "42") => FailureKind::Permanent,
        _ => {
            tracing::warn!(sqlstate = %code, "unrecognised SQLSTATE; classifying transient");
            FailureKind::Transient
        }
    }
}

impl From<sqlx::Error> for PostgresStoreError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database { source }
    }
}

/// Maps a `sqlx::Error` to a typed error, keying on SQLSTATE and constraint **name** — never on
/// message text (§24.1) — so `42P01` maps to `NotMigrated` **on every path**, not just startup
/// verification (contract §7 J2), and everything else falls through to `Database` for
/// [`classify_sqlstate`] to classify.
pub(crate) fn map_operational_error(schema: &str, err: sqlx::Error) -> PostgresStoreError {
    if is_undefined_table(&err) {
        return PostgresStoreError::NotMigrated {
            schema: schema.to_owned(),
        };
    }
    PostgresStoreError::Database { source: err }
}

/// [`crate::PostgresOutboxStore::enqueue`]/`enqueue_with` failures. `enqueue` runs on the
/// **host's** write path, where the host decides whether to retry its own transaction, so this
/// implements [`Classify`] on the same rules as [`PostgresStoreError`] rather than making the
/// host re-derive which SQLSTATEs are worth retrying (contract §4).
#[derive(Debug)]
#[non_exhaustive]
pub enum EnqueueError<E> {
    /// The configured [`reliar_core::Serializer`] rejected the body. **Permanent** — the same
    /// body serializes the same way every time.
    Serialize {
        /// The serializer's own error.
        source: E,
    },
    /// The envelope's `MessageId` already exists (`pk_outbox` violation) — `enqueue` uses a
    /// plain `INSERT` with no `ON CONFLICT`, so a reused id aborts the caller's transaction
    /// rather than silently losing a message. **Permanent** — the id is already taken.
    Duplicate {
        /// The id the caller tried to reuse.
        id: MessageId,
    },
    /// Any other `sqlx` failure, classified by SQLSTATE exactly as
    /// [`PostgresStoreError::Database`] (including `42P01`, which classifies permanent under
    /// the `42*` rule).
    Database {
        /// The underlying `sqlx` error.
        source: sqlx::Error,
    },
}

impl<E: fmt::Display> fmt::Display for EnqueueError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize { source } => {
                write!(f, "failed to serialize the envelope body: {source}")
            }
            Self::Duplicate { id } => write!(f, "message id {id} already exists in the outbox"),
            Self::Database { source } => write!(f, "database error: {source}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for EnqueueError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize { source } => Some(source),
            Self::Database { source } => Some(source),
            Self::Duplicate { .. } => None,
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> Classify for EnqueueError<E> {
    fn kind(&self) -> FailureKind {
        match self {
            Self::Serialize { .. } | Self::Duplicate { .. } => FailureKind::Permanent,
            Self::Database { source } => classify_sqlstate(source),
        }
    }
}

/// Maps a `sqlx::Error` from an `enqueue` `INSERT` to a typed error, keying on the constraint
/// **name** — never on message text (§24.1) — so `pk_outbox` maps to `Duplicate` and every
/// other failure (including `42P01`) stays `Database`, for [`classify_sqlstate`] to classify.
pub(crate) fn map_enqueue_error<E>(id: MessageId, err: sqlx::Error) -> EnqueueError<E> {
    if is_constraint_violation(&err, "pk_outbox") {
        return EnqueueError::Duplicate { id };
    }
    EnqueueError::Database { source: err }
}

/// `true` when `err` is a unique/check-constraint violation on `constraint`. Keys on the
/// **name**, never on message text (§24.1's naming rule exists precisely so this map is stable
/// across PostgreSQL versions).
pub(crate) fn is_constraint_violation(err: &sqlx::Error, constraint: &str) -> bool {
    match err {
        sqlx::Error::Database(db) => db.constraint() == Some(constraint),
        _ => false,
    }
}

/// `true` for SQLSTATE `42P01` (`undefined_table`) — the relation is missing, i.e. `migrate()`
/// has not run.
pub(crate) fn is_undefined_table(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db) => db.code().as_deref() == Some("42P01"),
        _ => false,
    }
}

/// Validates a schema name against PostgreSQL's unquoted-identifier grammar
/// (`[A-Za-z_][A-Za-z0-9_$]*`, at most 63 bytes — Postgres's own `NAMEDATALEN` limit) **before**
/// it is ever interpolated into `SET search_path`/`dangerous_set_table_name`, both of which
/// build SQL text from this value rather than binding it as data (contract §7 J4). Used by both
/// `PostgresOutboxSettings::schema` (at `connect`) and `MigrateOptions::schema` (at `migrate`),
/// so the two validate identically.
pub(crate) fn is_valid_schema_name(schema: &str) -> bool {
    let mut chars = schema.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    schema.len() <= 63 && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}
