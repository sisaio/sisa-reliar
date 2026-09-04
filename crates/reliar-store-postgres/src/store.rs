//! [`PostgresOutboxStore`]: construction, `enqueue`, and the `OutboxStore`/`OutboxDeadLetters`
//! implementations (SRS §20, §21, §24, contract §4).

use std::sync::Arc;

use bytes::Bytes;
use reliar_core::{ContentType, Message, MessageId, Serializer};
use reliar_outbox::{
    AcquiredBatch, CompletedMessage, DeadLetterPage, DeadQuery, FailedMessage, FailureOutcome,
    MessageRef, OutboxDeadLetters, OutboxStats, OutboxStore, PoisonedRow, PurgeReport,
    PurgeRequest, WorkerId,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::error::{
    EnqueueError, PostgresStoreError, is_undefined_table, map_enqueue_error, map_operational_error,
};
use crate::records::{RawRow, decode_row};
use crate::settings::PostgresOutboxSettings;

#[cfg(feature = "json")]
use reliar_core::JsonSerializer;

/// The largest `DeadQuery::limit` [`PostgresOutboxStore::list_dead`] honours — a caller-supplied
/// value above this is silently capped, never sent to the database (contract §3.3: "provider-
/// capped; default 100").
const MAX_LIST_DEAD_LIMIT: u32 = 1000;

/// Options specific to one `enqueue` call (contract §4 #9): the application-supplied
/// `ordering_key`, which is deliberately not part of `Metadata` (§22.2).
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct EnqueueOptions<'a> {
    /// The ordering-strategy key this message belongs to. `None` (the default) means
    /// unordered.
    pub ordering_key: Option<&'a str>,
}

impl<'a> EnqueueOptions<'a> {
    /// Sets [`Self::ordering_key`]. `#[non_exhaustive]` forbids struct-literal construction
    /// outside this crate, so this is the only way to set a non-default value.
    #[must_use]
    pub const fn ordering_key(mut self, key: &'a str) -> Self {
        self.ordering_key = Some(key);
        self
    }
}

/// Reliar's PostgreSQL outbox provider. Cheap to clone into an `AppState` — it wraps a
/// [`PgPool`]; no outer `Arc` required. The connection pool stays the host's: Reliar never owns
/// or reads a `DATABASE_URL`.
///
/// The default type parameter only exists behind the crate's default `json` feature (contract
/// §4, review 1 B2): under `--no-default-features` there is no default, so [`Self::connect`] is
/// the only constructor and `cargo hack --feature-powerset` compiles every combination.
#[non_exhaustive]
pub struct PostgresOutboxStore<
    #[cfg(feature = "json")] Ser = JsonSerializer,
    #[cfg(not(feature = "json"))] Ser,
> {
    pool: PgPool,
    settings: PostgresOutboxSettings,
    serializer: Arc<Ser>,
}

/// **Manual impl, never derived**: a derived `Clone` would condition on `Ser: Clone`. The
/// serializer is held as `Arc<Ser>` — stateless and cheap to share — so cloning the store never
/// requires the serializer itself to be `Clone`.
impl<Ser> Clone for PostgresOutboxStore<Ser> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            settings: self.settings.clone(),
            serializer: Arc::clone(&self.serializer),
        }
    }
}

impl<Ser> std::fmt::Debug for PostgresOutboxStore<Ser> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresOutboxStore")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// One row of the startup `search_path` verification query (ADR 0017).
struct SchemaCheck {
    resolved_schema: Option<String>,
    configured_exists: bool,
    search_path: String,
}

async fn verify_schema(pool: &PgPool, schema: &str) -> Result<SchemaCheck, PostgresStoreError> {
    let qualified = format!("{schema}.outbox");
    let row = sqlx::query!(
        r#"SELECT
             current_setting('search_path') AS "search_path!",
             (SELECT n.nspname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE c.oid = to_regclass('outbox')) AS resolved_schema,
             (to_regclass($1) IS NOT NULL) AS "configured_exists!""#,
        qualified,
    )
    .fetch_one(pool)
    .await
    .map_err(|err| {
        if is_undefined_table(&err) {
            PostgresStoreError::NotMigrated {
                schema: schema.to_owned(),
            }
        } else {
            PostgresStoreError::from(err)
        }
    })?;

    Ok(SchemaCheck {
        resolved_schema: row.resolved_schema,
        configured_exists: row.configured_exists,
        search_path: row.search_path,
    })
}

/// Rows with a `relname = 'outbox'` outside `schema`, for the same-named-table warning
/// (ADR 0017). Empty when there is no such duplicate.
async fn other_outbox_schemas(
    pool: &PgPool,
    schema: &str,
) -> Result<Vec<String>, PostgresStoreError> {
    let schemas = sqlx::query_scalar!(
        r#"SELECT n.nspname
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = 'outbox' AND n.nspname <> $1"#,
        schema,
    )
    .fetch_all(pool)
    .await?;
    Ok(schemas)
}

impl<Ser: Serializer + Send + Sync + 'static> PostgresOutboxStore<Ser> {
    /// Wraps `pool` with `settings` and `serializer`. **Verifies once at construction** that
    /// the unqualified name `outbox` resolves to `settings.schema`: fails fast with
    /// [`PostgresStoreError::SchemaResolution`] (`search_path` problem) or
    /// [`PostgresStoreError::NotMigrated`] (the relation is missing entirely) rather than
    /// surprising the first `acquire`. Logs a `tracing::warn!` when a same-named table also
    /// exists in another schema on the path.
    ///
    /// # Errors
    ///
    /// Returns [`PostgresStoreError::NotMigrated`], [`PostgresStoreError::SchemaResolution`],
    /// or [`PostgresStoreError::Database`] for a connection failure during verification.
    pub async fn connect(
        pool: PgPool,
        settings: PostgresOutboxSettings,
        serializer: Ser,
    ) -> Result<Self, PostgresStoreError> {
        if !crate::error::is_valid_schema_name(&settings.schema) {
            return Err(PostgresStoreError::InvalidSchema {
                schema: settings.schema,
            });
        }

        let check = verify_schema(&pool, &settings.schema).await?;

        let resolved_here = check.resolved_schema.as_deref() == Some(settings.schema.as_str());
        if !resolved_here {
            if !check.configured_exists {
                return Err(PostgresStoreError::NotMigrated {
                    schema: settings.schema,
                });
            }
            return Err(PostgresStoreError::SchemaResolution {
                configured: settings.schema,
                observed: check.search_path,
            });
        }

        let others = other_outbox_schemas(&pool, &settings.schema).await?;
        if !others.is_empty() {
            tracing::warn!(
                configured_schema = %settings.schema,
                other_schemas = ?others,
                "a table named `outbox` also exists outside the configured schema; \
                 an unqualified reference from another session could resolve to it"
            );
        }

        Ok(Self {
            pool,
            settings,
            serializer: Arc::new(serializer),
        })
    }

    /// The `ContentType` this store writes to every row — `Serializer::content_type()`. The
    /// only way a caller can predict the `content_type` of an envelope it will later acquire:
    /// `enqueue` writes this value, ignoring whatever `envelope.metadata.delivery.content_type`
    /// held (contract §4).
    #[must_use]
    pub fn content_type(&self) -> &ContentType {
        self.serializer.content_type()
    }

    /// Maps a `sqlx::Error` from one of this store's own operations to a typed
    /// [`PostgresStoreError`], catching SQLSTATE `42P01` on **every** call, not just startup
    /// verification (contract §7 J2).
    fn map_err(&self, err: sqlx::Error) -> PostgresStoreError {
        map_operational_error(&self.settings.schema, err)
    }

    /// Issues `SET LOCAL statement_timeout` on an already-open transaction — the shared half of
    /// every `Duration::ZERO`-vs-non-zero split below (contract §4, review 2 major 3).
    async fn set_local_timeout(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), PostgresStoreError> {
        let timeout_ms = i64::try_from(self.settings.statement_timeout.as_millis())
            .unwrap_or(i64::MAX)
            .to_string();
        sqlx::query_scalar!(
            "SELECT set_config('statement_timeout', $1, true)",
            timeout_ms
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| self.map_err(e))?;
        Ok(())
    }

    /// Stages a message in the **application's own transaction** — atomicity is visible in the
    /// signature. Plain `INSERT`, **no `ON CONFLICT`**: a reused `MessageId` aborts the
    /// caller's transaction rather than silently losing a message. Returns the id it wrote, so
    /// the caller can use it as the next message's `causation_id` in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns [`EnqueueError::Serialize`] if the configured `Serializer` rejects the body,
    /// [`EnqueueError::Duplicate`] for a reused `MessageId`, or [`EnqueueError::Database`] for
    /// any other `sqlx` failure.
    pub async fn enqueue<T: Message>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        envelope: &reliar_core::Envelope<T>,
    ) -> Result<MessageId, EnqueueError<Ser::Error>> {
        self.enqueue_with(tx, envelope, EnqueueOptions::default())
            .await
    }

    /// Same as [`Self::enqueue`], with provider-side options (currently
    /// [`EnqueueOptions::ordering_key`]).
    ///
    /// # Errors
    ///
    /// Same as [`Self::enqueue`].
    pub async fn enqueue_with<T: Message>(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        envelope: &reliar_core::Envelope<T>,
        options: EnqueueOptions<'_>,
    ) -> Result<MessageId, EnqueueError<Ser::Error>> {
        let payload = self
            .serializer
            .serialize(&envelope.body)
            .map_err(|source| EnqueueError::Serialize { source })?;

        let restore = if self.settings.enqueue_sets_search_path {
            Some(set_search_path(tx, &self.settings.schema).await?)
        } else {
            None
        };

        let result = insert_row(tx, envelope, &payload, self.content_type(), options).await;

        // Only restore on success: a failed INSERT already aborts the transaction (25P02), so
        // issuing another statement on it would mask the real error behind "current transaction
        // is aborted" instead (contract review 1, blocker 2). The transaction-local scope makes
        // skipping the restore safe — the caller's own rollback/abandonment is what actually
        // undoes it.
        if result.is_ok()
            && let Some(previous) = restore
        {
            restore_search_path(tx, &previous).await?;
        }

        result.map_err(|source| map_enqueue_error(envelope.id, source))?;
        Ok(envelope.id)
    }
}

/// Reads the caller's current `search_path`, sets it transaction-locally
/// (`set_config(.., true)` — dies with the caller's `COMMIT`/`ROLLBACK`) to put `schema` first,
/// and returns the previous value so it can be restored (contract §4).
async fn set_search_path<E>(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
) -> Result<String, EnqueueError<E>> {
    let previous: String = sqlx::query_scalar!("SELECT current_setting('search_path')")
        .fetch_one(&mut **tx)
        .await
        .map_err(|source| EnqueueError::Database { source })?
        .unwrap_or_default();
    let wanted = format!("{schema},public");
    sqlx::query_scalar!("SELECT set_config('search_path', $1, true)", wanted)
        .fetch_one(&mut **tx)
        .await
        .map_err(|source| EnqueueError::Database { source })?;
    Ok(previous)
}

async fn restore_search_path<E>(
    tx: &mut Transaction<'_, Postgres>,
    previous: &str,
) -> Result<(), EnqueueError<E>> {
    sqlx::query_scalar!("SELECT set_config('search_path', $1, true)", previous)
        .fetch_one(&mut **tx)
        .await
        .map_err(|source| EnqueueError::Database { source })?;
    Ok(())
}

async fn insert_row<T: Message>(
    tx: &mut Transaction<'_, Postgres>,
    envelope: &reliar_core::Envelope<T>,
    payload: &Bytes,
    content_type: &ContentType,
    options: EnqueueOptions<'_>,
) -> Result<(), sqlx::Error> {
    let corr = &envelope.metadata.correlation;
    let sent_at_ms = envelope
        .metadata
        .delivery
        .sent_at
        .map(crate::records::encode_epoch_millis);
    let rest = crate::records::MetadataRest {
        trace: crate::records::TraceRest {
            traceparent: envelope.metadata.trace.traceparent.clone(),
            tracestate: envelope.metadata.trace.tracestate.clone(),
        },
        routing: crate::records::RoutingRest {
            source: envelope
                .metadata
                .routing
                .source
                .as_ref()
                .map(|v| v.as_str().to_owned()),
            destination: envelope
                .metadata
                .routing
                .destination
                .as_ref()
                .map(|v| v.as_str().to_owned()),
            reply_to: envelope
                .metadata
                .routing
                .reply_to
                .as_ref()
                .map(|v| v.as_str().to_owned()),
        },
        delivery: crate::records::DeliveryRest {
            sent_at_ms,
            deduplication_id: envelope.metadata.delivery.deduplication_id.clone(),
        },
    };
    // An empty remainder is written as SQL NULL, not '{}', so pending rows stay small (§24.2).
    let metadata_json = if rest.trace.traceparent.is_none()
        && rest.trace.tracestate.is_none()
        && rest.routing.source.is_none()
        && rest.routing.destination.is_none()
        && rest.routing.reply_to.is_none()
        && rest.delivery.sent_at_ms.is_none()
        && rest.delivery.deduplication_id.is_none()
    {
        None
    } else {
        // `MetadataRest`'s fields are now all plain owned `String`/`i64`/`Option` values (no
        // RFC3339 formatting, contract §7 J5) — `serde_json::to_value` is total over this
        // shape. The fallback is unreachable in practice; kept non-panicking rather than
        // `.expect()`'d away (§19.5 forbids a panic on the enqueue path). `.ok()` rather than a
        // `Value::Null` fallback (review 2 minor): on the unreachable error branch this writes
        // SQL `NULL` — the same "no remainder" shape as the empty-check above — rather than a
        // JSON `null` a reader would then have to treat as yet another poison case.
        serde_json::to_value(&rest).ok()
    };

    let headers_json = envelope.headers().filter(|h| !h.is_empty()).map(|h| {
        let map: serde_json::Map<String, serde_json::Value> = h
            .iter()
            .map(|(k, v)| (k.to_owned(), serde_json::Value::String(v.to_owned())))
            .collect();
        serde_json::Value::Object(map)
    });

    sqlx::query!(
        r#"INSERT INTO outbox (
             id, message_type, message_version,
             correlation_id, conversation_id, causation_id, request_id,
             content_type, payload, tenant_id, expires_at, ordering_key,
             metadata, headers, available_at
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14, now())"#,
        envelope.id.as_uuid(),
        T::TYPE,
        i32::from(T::VERSION),
        corr.correlation_id
            .as_ref()
            .map(reliar_core::CorrelationId::as_str),
        corr.conversation_id.as_uuid(),
        corr.causation_id.map(|id| id.as_uuid()),
        corr.request_id.map(|id| id.as_uuid()),
        content_type.as_str(),
        &payload[..],
        envelope.metadata.tenant_id.as_deref(),
        envelope.metadata.delivery.expires_at,
        options.ordering_key,
        metadata_json,
        headers_json,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(feature = "json")]
impl PostgresOutboxStore<JsonSerializer> {
    /// Convenience over [`Self::connect`], behind the crate's default `json` feature.
    ///
    /// # Errors
    ///
    /// Same as [`Self::connect`].
    pub async fn new(pool: PgPool) -> Result<Self, PostgresStoreError> {
        Self::connect(pool, PostgresOutboxSettings::default(), JsonSerializer).await
    }

    /// Convenience over [`Self::connect`] with explicit settings, behind the crate's default
    /// `json` feature.
    ///
    /// # Errors
    ///
    /// Same as [`Self::connect`].
    pub async fn with_settings(
        pool: PgPool,
        settings: PostgresOutboxSettings,
    ) -> Result<Self, PostgresStoreError> {
        Self::connect(pool, settings, JsonSerializer).await
    }
}

/// `acquire`'s poison sweep: moves every row `decode_row` couldn't reconstruct to dead with
/// `DeadReason::Undecodable`, worker-guarded the same way every other outcome update is
/// (`o.locked_by = $3`) so a row already reclaimed by a different worker after this one's lease
/// lapsed is left alone.
async fn poison_sweep_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    poisoned_ids: &[uuid::Uuid],
    poisoned_errors: &[String],
    worker: &str,
    undecodable: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"UPDATE outbox o
              SET dead_at      = now(),
                  dead_reason  = $4,
                  last_error   = f.err,
                  locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
             FROM UNNEST($1::uuid[], $2::text[]) AS f(id, err)
            WHERE o.id = f.id AND o.locked_by = $3"#,
        poisoned_ids,
        poisoned_errors,
        worker,
        undecodable,
    )
    .execute(executor)
    .await?;
    Ok(())
}

/// `purge`'s published-row delete, bounded by `batch_size`. The outer `WHERE` repeats the
/// subselect's own predicate **in full** — not just the `IS NOT NULL` half — so `EvalPlanQual`'s
/// re-check (on a row a concurrent writer touched between the subselect's snapshot and this
/// statement's lock acquisition) can actually exclude it, rather than deleting on stale
/// information (review 3 B1, review 4 minor: the retention-age comparison needs repeating too,
/// not only nullness, since a row could in principle be re-published with a fresher timestamp
/// between the snapshot and the lock).
async fn purge_published_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    retention_ms: i64,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"DELETE FROM outbox WHERE id IN (
               SELECT id FROM outbox
                WHERE published_at IS NOT NULL
                  AND published_at < now() - ($1::bigint * interval '1 millisecond')
                LIMIT $2
           )
           AND published_at IS NOT NULL
           AND published_at < now() - ($1::bigint * interval '1 millisecond')"#,
        retention_ms,
        batch_size,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// `purge`'s dead-row delete, bounded by `batch_size`. Same full-predicate `EvalPlanQual` guard
/// as [`purge_published_rows`] — without it, a row `retry_dead` resurrects (or re-deadens with a
/// fresher `dead_at`) between the subselect's snapshot and this statement's lock acquisition
/// could still be deleted (review 3 B1, the blocker this fixes; review 4 minor extended it to
/// the retention-age comparison too).
async fn purge_dead_retention_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    retention_ms: i64,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"DELETE FROM outbox WHERE id IN (
               SELECT id FROM outbox
                WHERE dead_at IS NOT NULL
                  AND dead_at < now() - ($1::bigint * interval '1 millisecond')
                LIMIT $2
           )
           AND dead_at IS NOT NULL
           AND dead_at < now() - ($1::bigint * interval '1 millisecond')"#,
        retention_ms,
        batch_size,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// `purge`'s expired-pending-to-dead sweep, bounded by `batch_size`. The outer
/// `published_at IS NULL AND dead_at IS NULL` plus the lease clause repeat the subselect's own
/// mutable-state predicates so a lapsed-lease worker's concurrent `complete`/`fail` can't race
/// this into a `ck_outbox_terminal` violation (review 3 M1); `expires_at` itself is immutable
/// once written, so it doesn't need repeating.
async fn purge_expired_sweep_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    batch_size: i64,
    expired_reason: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox
              SET dead_at      = now(),
                  dead_reason  = $2,
                  last_error   = 'reliar: expired before publication',
                  locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
            WHERE id IN (
                SELECT id FROM outbox
                 WHERE expires_at IS NOT NULL AND expires_at < now()
                   AND published_at IS NULL AND dead_at IS NULL
                   AND (locked_until IS NULL OR locked_until < now())
                 LIMIT $1
            )
              AND published_at IS NULL AND dead_at IS NULL
              AND (locked_until IS NULL OR locked_until < now())"#,
        batch_size,
        expired_reason,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// Worker-guarded `complete`: clears the lease and sets `published_at`, only for rows this
/// worker still holds (`locked_by = $2`) — a row already reclaimed by another worker
/// contributes nothing (ADR 0008).
async fn complete_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
    worker: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox
              SET published_at = now(),
                  attempts     = attempts + 1,
                  locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
            WHERE id = ANY($1) AND locked_by = $2"#,
        ids,
        worker,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

async fn release_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
    worker: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox
              SET locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
            WHERE id = ANY($1) AND locked_by = $2"#,
        ids,
        worker,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

async fn extend_lease_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
    lease_ms: i64,
    worker: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox
              SET locked_until = now() + ($2::bigint * interval '1 millisecond'),
                  updated_at   = now()
            WHERE id = ANY($1) AND locked_by = $3"#,
        ids,
        lease_ms,
        worker,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

async fn fail_retry_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
    errors: &[String],
    delays_ms: &[i64],
    worker: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox o
              SET attempts     = o.attempts + 1,
                  last_error   = f.err,
                  locked_by    = NULL,
                  locked_until = NULL,
                  available_at = now() + (f.delay_ms * interval '1 millisecond'),
                  updated_at   = now()
             FROM UNNEST($1::uuid[], $2::text[], $3::bigint[]) AS f(id, err, delay_ms)
            WHERE o.id = f.id AND o.locked_by = $4"#,
        ids,
        errors,
        delays_ms,
        worker,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

async fn fail_dead_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
    errors: &[String],
    reasons: &[&str],
    worker: &str,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox o
              SET attempts     = o.attempts + 1,
                  last_error   = f.err,
                  dead_at      = now(),
                  dead_reason  = f.reason,
                  locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
             FROM UNNEST($1::uuid[], $2::text[], $3::text[]) AS f(id, err, reason)
            WHERE o.id = f.id AND o.locked_by = $4"#,
        ids,
        errors,
        reasons as &[&str],
        worker,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// The canonical single-statement claim (SRS §24.1, ADR 0006): a CTE
/// `SELECT … FOR UPDATE SKIP LOCKED` feeding an `UPDATE … RETURNING`, so the row lock is
/// released before the call returns and no network I/O to a publisher can ever happen while it
/// is held. Named against [`RawRow`] via `query_as!` (never `FromRow`) so both the plain-pool
/// and `statement_timeout`-wrapped-transaction call sites in [`PostgresOutboxStore::acquire`]
/// share one macro invocation instead of two structurally distinct anonymous row types (review
/// 3 minor: this doc previously sat, misplaced, above `complete_rows` instead).
async fn claim_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    batch_size: i64,
    worker: &str,
    lease_ms: i64,
) -> Result<Vec<RawRow>, sqlx::Error> {
    sqlx::query_as!(
        RawRow,
        r#"WITH claimed AS (
               SELECT id FROM outbox
                WHERE published_at IS NULL AND dead_at IS NULL
                  AND available_at <= now()
                  AND (locked_until IS NULL OR locked_until < now())
                  AND (expires_at IS NULL OR expires_at > now())
                ORDER BY available_at, sequence
                LIMIT $1
                FOR UPDATE SKIP LOCKED
           )
           UPDATE outbox o
              SET locked_by    = $2,
                  locked_until = now() + ($3::bigint * interval '1 millisecond'),
                  updated_at   = now()
             FROM claimed
            WHERE o.id = claimed.id
           RETURNING o.id, o.sequence, o.message_type, o.message_version,
                     o.correlation_id, o.conversation_id, o.causation_id, o.request_id,
                     o.content_type, o.payload, o.tenant_id, o.expires_at, o.ordering_key,
                     o.metadata, o.headers, o.metadata_version,
                     o.created_at, o.available_at,
                     o.attempts, o.locked_by, o.locked_until,
                     o.published_at, o.dead_at, o.dead_reason, o.last_error"#,
        batch_size,
        worker,
        lease_ms,
    )
    .fetch_all(executor)
    .await
}

/// `list_dead`'s query, shared by the plain-pool and `statement_timeout`-wrapped-transaction
/// call sites. Named against [`RawRow`] via `query_as!`, same as [`claim_rows`] — the `SELECT`
/// list matches its field order exactly.
async fn list_dead_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    query: &DeadQuery,
    limit: i64,
) -> Result<Vec<RawRow>, sqlx::Error> {
    sqlx::query_as!(
        RawRow,
        r#"SELECT id, sequence, message_type, message_version,
                  correlation_id, conversation_id, causation_id, request_id,
                  content_type, payload, tenant_id, expires_at, ordering_key,
                  metadata, headers, metadata_version,
                  created_at, available_at,
                  attempts, locked_by, locked_until,
                  published_at, dead_at, dead_reason, last_error
             FROM outbox
            WHERE dead_at IS NOT NULL
              AND ($1::text IS NULL OR message_type = $1)
              AND ($2::text IS NULL OR tenant_id = $2)
              AND ($3::timestamptz IS NULL OR dead_at < $3)
              AND ($4::bigint IS NULL OR sequence > $4)
            ORDER BY sequence ASC
            LIMIT $5"#,
        query.message_type,
        query.tenant_id,
        query.dead_before,
        query.after_sequence,
        limit,
    )
    .fetch_all(executor)
    .await
}

/// `retry_dead`'s query, shared by the plain-pool and `statement_timeout`-wrapped-transaction
/// call sites. Not worker-guarded — a dead row holds no lease (contract §3.4).
async fn retry_dead_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"UPDATE outbox
              SET dead_at      = NULL,
                  dead_reason  = NULL,
                  available_at = now(),
                  attempts     = 0,
                  locked_by    = NULL,
                  locked_until = NULL,
                  updated_at   = now()
            WHERE id = ANY($1) AND dead_at IS NOT NULL"#,
        ids,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

/// `purge_dead`'s query, shared by the plain-pool and `statement_timeout`-wrapped-transaction
/// call sites.
async fn purge_dead_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    ids: &[uuid::Uuid],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        "DELETE FROM outbox WHERE id = ANY($1) AND dead_at IS NOT NULL",
        ids,
    )
    .execute(executor)
    .await?;
    Ok(result.rows_affected())
}

impl<Ser: Serializer + Send + Sync + 'static> OutboxStore for PostgresOutboxStore<Ser> {
    type Error = PostgresStoreError;

    /// The canonical single-statement claim (SRS §24.1, ADR 0006): a CTE
    /// `SELECT … FOR UPDATE SKIP LOCKED` feeding an `UPDATE … RETURNING`, so the row lock is
    /// released before this future resolves and no network I/O to a publisher can ever happen
    /// while it is held.
    ///
    /// A row this call cannot decode is **excluded from `records`**, reported in `poisoned`,
    /// and **moved to dead** with `DeadReason::Undecodable` by a follow-up statement guarded by
    /// `locked_by` — the batch continues (§19.5, ADR 0008).
    async fn acquire(
        &self,
        request: reliar_outbox::AcquireRequest,
    ) -> Result<AcquiredBatch, Self::Error> {
        let batch_size = i64::from(request.batch_size);
        let lease_ms = i64::try_from(request.lease.as_millis()).unwrap_or(i64::MAX);
        let worker = request.worker.as_str();

        // `Duration::ZERO` (the default) issues nothing and runs the claim as the single
        // implicit-transaction statement ADR 0006 relies on; a non-zero `statement_timeout`
        // costs a `BEGIN`/`SET LOCAL`/statement/`COMMIT` round trip instead, which is why it is
        // opt-in (contract §4, review 1 major 4).
        let rows = if self.settings.statement_timeout.is_zero() {
            claim_rows(&self.pool, batch_size, worker, lease_ms)
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            let timeout_ms = i64::try_from(self.settings.statement_timeout.as_millis())
                .unwrap_or(i64::MAX)
                .to_string();
            sqlx::query_scalar!(
                "SELECT set_config('statement_timeout', $1, true)",
                timeout_ms
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| self.map_err(e))?;
            let rows = claim_rows(&mut *tx, batch_size, worker, lease_ms)
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            rows
        };

        let mut records = Vec::with_capacity(rows.len());
        let mut poisoned = Vec::new();
        let mut poisoned_ids = Vec::new();
        let mut poisoned_errors = Vec::new();

        for raw in rows {
            match decode_row(raw) {
                Ok(record) => records.push(record),
                Err(err) => {
                    poisoned_ids.push(err.id.as_uuid());
                    poisoned_errors.push(crate::records::truncate_last_error(err.detail.clone()));
                    poisoned.push(PoisonedRow::new(err.id, err.sequence, err.detail));
                }
            }
        }

        if !poisoned_ids.is_empty() {
            // Not an observed publish attempt, so `attempts` is untouched (ADR 0009: `attempts`
            // counts outcomes, never claims) — only the lease clears and the row goes dead. Runs
            // under the same `statement_timeout` policy as the claim itself (review 3 minor):
            // previously this always ran directly on the pool even when the claim above had
            // just gone through the `SET LOCAL` wrap, so a slow poison sweep couldn't be bounded
            // by a non-zero `statement_timeout`.
            let undecodable =
                crate::records::encode_dead_reason(reliar_outbox::DeadReason::Undecodable);
            if self.settings.statement_timeout.is_zero() {
                poison_sweep_rows(
                    &self.pool,
                    &poisoned_ids,
                    &poisoned_errors,
                    worker,
                    undecodable,
                )
                .await
                .map_err(|e| self.map_err(e))?;
            } else {
                let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
                self.set_local_timeout(&mut tx).await?;
                poison_sweep_rows(
                    &mut *tx,
                    &poisoned_ids,
                    &poisoned_errors,
                    worker,
                    undecodable,
                )
                .await
                .map_err(|e| self.map_err(e))?;
                tx.commit().await.map_err(|e| self.map_err(e))?;
            }
        }

        Ok(AcquiredBatch::new(records, poisoned))
    }

    /// Marks rows published, worker-guarded (`locked_by = $2`). A row already completed or
    /// reclaimed by another worker contributes nothing to the count — a shortfall is logged at
    /// `debug`, never an error (ADR 0008).
    async fn complete(
        &self,
        worker: &WorkerId,
        items: &[CompletedMessage],
    ) -> Result<u64, Self::Error> {
        if items.is_empty() {
            return Ok(0);
        }
        let ids: Vec<uuid::Uuid> = items.iter().map(|i| i.message.id.as_uuid()).collect();
        let affected = if self.settings.statement_timeout.is_zero() {
            complete_rows(&self.pool, &ids, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let affected = complete_rows(&mut *tx, &ids, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        log_shortfall("complete", items.len(), affected);
        Ok(affected)
    }

    /// Applies each item's [`FailureOutcome`], worker-guarded. Retry rows get
    /// `available_at = now() + delay` computed in SQL (ADR 0009); dead rows get `dead_at`/
    /// `dead_reason` set together (`ck_outbox_dead_reason`). Both increment `attempts` — on
    /// outcome, never on claim.
    async fn fail(&self, worker: &WorkerId, items: &[FailedMessage]) -> Result<u64, Self::Error> {
        if items.is_empty() {
            return Ok(0);
        }

        let mut retry_ids = Vec::new();
        let mut retry_errors = Vec::new();
        let mut retry_delays = Vec::new();
        let mut dead_ids = Vec::new();
        let mut dead_errors = Vec::new();
        let mut dead_reasons = Vec::new();

        for item in items {
            match item.outcome {
                FailureOutcome::Retry { delay } => {
                    retry_ids.push(item.message.id.as_uuid());
                    retry_errors.push(item.error.clone());
                    retry_delays.push(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
                }
                FailureOutcome::Dead { reason } => {
                    dead_ids.push(item.message.id.as_uuid());
                    dead_errors.push(item.error.clone());
                    dead_reasons.push(crate::records::encode_dead_reason(reason));
                }
                // `FailureOutcome` is `#[non_exhaustive]` from another crate; a variant this
                // build does not know how to apply is left untouched rather than guessed at —
                // it stays claimed until its lease expires and is republished, the same benign
                // outcome as any other unresolved row (ADR 0008).
                _ => tracing::error!(
                    id = %item.message.id,
                    "unrecognised FailureOutcome variant; row left as-is"
                ),
            }
        }

        let affected = if self.settings.statement_timeout.is_zero() {
            let mut affected = 0u64;
            if !retry_ids.is_empty() {
                affected += fail_retry_rows(
                    &self.pool,
                    &retry_ids,
                    &retry_errors,
                    &retry_delays,
                    worker.as_str(),
                )
                .await
                .map_err(|e| self.map_err(e))?;
            }
            if !dead_ids.is_empty() {
                affected += fail_dead_rows(
                    &self.pool,
                    &dead_ids,
                    &dead_errors,
                    &dead_reasons,
                    worker.as_str(),
                )
                .await
                .map_err(|e| self.map_err(e))?;
            }
            affected
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let mut affected = 0u64;
            if !retry_ids.is_empty() {
                affected += fail_retry_rows(
                    &mut *tx,
                    &retry_ids,
                    &retry_errors,
                    &retry_delays,
                    worker.as_str(),
                )
                .await
                .map_err(|e| self.map_err(e))?;
            }
            if !dead_ids.is_empty() {
                affected += fail_dead_rows(
                    &mut *tx,
                    &dead_ids,
                    &dead_errors,
                    &dead_reasons,
                    worker.as_str(),
                )
                .await
                .map_err(|e| self.map_err(e))?;
            }
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        log_shortfall("fail", items.len(), affected);
        Ok(affected)
    }

    /// Clears the lease for rows this worker still owns. `available_at` and `attempts` are
    /// untouched — a release is not a failure (SRS §26.1).
    async fn release(&self, worker: &WorkerId, items: &[MessageRef]) -> Result<u64, Self::Error> {
        if items.is_empty() {
            return Ok(0);
        }
        let ids: Vec<uuid::Uuid> = items.iter().map(|i| i.id.as_uuid()).collect();
        let affected = if self.settings.statement_timeout.is_zero() {
            release_rows(&self.pool, &ids, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let affected = release_rows(&mut *tx, &ids, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        log_shortfall("release", items.len(), affected);
        Ok(affected)
    }

    /// Renews `locked_until = now() + lease` for rows this worker still owns. Best-effort: a
    /// shortfall means the lease already expired (§21.1).
    async fn extend_lease(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
        lease: std::time::Duration,
    ) -> Result<u64, Self::Error> {
        if items.is_empty() {
            return Ok(0);
        }
        let ids: Vec<uuid::Uuid> = items.iter().map(|i| i.id.as_uuid()).collect();
        let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
        let affected = if self.settings.statement_timeout.is_zero() {
            extend_lease_rows(&self.pool, &ids, lease_ms, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let affected = extend_lease_rows(&mut *tx, &ids, lease_ms, worker.as_str())
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        log_shortfall("extend_lease", items.len(), affected);
        Ok(affected)
    }

    /// **One bounded pass, three statements, each capped at `request.batch_size`** (contract §7
    /// G1): published-row delete, dead-row delete, and the expired→dead sweep — none of the
    /// three is ever an unbounded `DELETE`/`UPDATE`. The sweep's predicate carries the claim's
    /// lease clause (`locked_until IS NULL OR locked_until < now()`), so it never transitions a
    /// row a live worker still owns (contract §7 G2) — that worker's own `complete`/`fail`
    /// wins, and the row becomes sweepable only once its lease lapses.
    async fn purge(&self, request: PurgeRequest) -> Result<PurgeReport, Self::Error> {
        let batch_size = i64::from(request.batch_size);
        let expired_reason = crate::records::encode_dead_reason(reliar_outbox::DeadReason::Expired);

        let (published_deleted, dead_deleted, expired_to_dead) =
            if self.settings.statement_timeout.is_zero() {
                let published_deleted = if let Some(retention) = request.published_retention {
                    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
                    purge_published_rows(&self.pool, retention_ms, batch_size)
                        .await
                        .map_err(|e| self.map_err(e))?
                } else {
                    0
                };

                let dead_deleted = if let Some(retention) = request.dead_retention {
                    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
                    purge_dead_retention_rows(&self.pool, retention_ms, batch_size)
                        .await
                        .map_err(|e| self.map_err(e))?
                } else {
                    0
                };

                let expired_to_dead =
                    purge_expired_sweep_rows(&self.pool, batch_size, expired_reason)
                        .await
                        .map_err(|e| self.map_err(e))?;

                (published_deleted, dead_deleted, expired_to_dead)
            } else {
                // One transaction, one `SET LOCAL statement_timeout`, all three statements —
                // each is individually bounded by it (contract §4/§7 ruling: `statement_timeout`
                // bounds every statement Reliar issues on its own pool, `purge` included), and
                // sharing one transaction costs one `BEGIN`/`SET LOCAL`/`COMMIT` round trip
                // instead of three.
                let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
                self.set_local_timeout(&mut tx).await?;

                let published_deleted = if let Some(retention) = request.published_retention {
                    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
                    purge_published_rows(&mut *tx, retention_ms, batch_size)
                        .await
                        .map_err(|e| self.map_err(e))?
                } else {
                    0
                };

                let dead_deleted = if let Some(retention) = request.dead_retention {
                    let retention_ms = i64::try_from(retention.as_millis()).unwrap_or(i64::MAX);
                    purge_dead_retention_rows(&mut *tx, retention_ms, batch_size)
                        .await
                        .map_err(|e| self.map_err(e))?
                } else {
                    0
                };

                let expired_to_dead =
                    purge_expired_sweep_rows(&mut *tx, batch_size, expired_reason)
                        .await
                        .map_err(|e| self.map_err(e))?;

                tx.commit().await.map_err(|e| self.map_err(e))?;
                (published_deleted, dead_deleted, expired_to_dead)
            };

        Ok(PurgeReport::new(
            published_deleted,
            dead_deleted,
            expired_to_dead,
        ))
    }
    /// One statement, four `FILTER`-qualified aggregates over a single scan of `outbox`
    /// (contract §4, S8 EXPLAIN comparison, RELIAR-17 card Log). An earlier version issued four
    /// separate statements, one per `ix_outbox_pending`/`ix_outbox_dead_at`/`ix_outbox_expires` —
    /// but `pending`'s and the `min(available_at)` row's predicates aren't a strict subset of
    /// `ix_outbox_pending` (they also filter on `available_at`/`locked_until`/`expires_at`, none
    /// of which the partial index's `WHERE` clause covers), so the planner chose a `Seq Scan`
    /// for both anyway on a realistic seeded table (20k rows, a 25/25/25/25 pending/dead/
    /// published/expired-pending mix) — meaning the four-statement form paid for that same
    /// `Seq Scan` **twice** (once for `pending`, once for the `min`/`now()` row) plus three
    /// extra round trips, for a strictly worse total (`Execution Time` 4.65 ms combined,
    /// `Buffers: shared hit` 836) than one statement computing all four aggregates from one scan
    /// (`Execution Time` 2.77 ms, `Buffers: shared hit` 412) — see the card Log for both full
    /// `EXPLAIN (ANALYZE, BUFFERS)` plans.
    async fn stats(&self) -> Result<OutboxStats, Self::Error> {
        if self.settings.statement_timeout.is_zero() {
            let row = sqlx::query!(
                r#"SELECT
                       count(*) FILTER (
                           WHERE published_at IS NULL AND dead_at IS NULL
                             AND available_at <= now()
                             AND (locked_until IS NULL OR locked_until < now())
                             AND (expires_at IS NULL OR expires_at > now())
                       ) AS "pending!",
                       count(*) FILTER (WHERE dead_at IS NOT NULL) AS "dead!",
                       count(*) FILTER (
                           WHERE published_at IS NULL AND dead_at IS NULL
                             AND expires_at IS NOT NULL AND expires_at < now()
                       ) AS "expired_pending!",
                       min(available_at) FILTER (
                           WHERE published_at IS NULL AND dead_at IS NULL
                             AND available_at <= now()
                             AND (locked_until IS NULL OR locked_until < now())
                             AND (expires_at IS NULL OR expires_at > now())
                       ) AS oldest_pending_available_at,
                       now() AS "as_of!"
                     FROM outbox"#
            )
            .fetch_one(&self.pool)
            .await
            .map_err(|e| self.map_err(e))?;

            return Ok(OutboxStats::new(
                u64::try_from(row.pending).unwrap_or(0),
                u64::try_from(row.dead).unwrap_or(0),
                u64::try_from(row.expired_pending).unwrap_or(0),
                row.oldest_pending_available_at,
                row.as_of,
            ));
        }

        // Same one statement, wrapped in a `SET LOCAL statement_timeout` transaction.
        let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
        self.set_local_timeout(&mut tx).await?;

        let row = sqlx::query!(
            r#"SELECT
                   count(*) FILTER (
                       WHERE published_at IS NULL AND dead_at IS NULL
                         AND available_at <= now()
                         AND (locked_until IS NULL OR locked_until < now())
                         AND (expires_at IS NULL OR expires_at > now())
                   ) AS "pending!",
                   count(*) FILTER (WHERE dead_at IS NOT NULL) AS "dead!",
                   count(*) FILTER (
                       WHERE published_at IS NULL AND dead_at IS NULL
                         AND expires_at IS NOT NULL AND expires_at < now()
                   ) AS "expired_pending!",
                   min(available_at) FILTER (
                       WHERE published_at IS NULL AND dead_at IS NULL
                         AND available_at <= now()
                         AND (locked_until IS NULL OR locked_until < now())
                         AND (expires_at IS NULL OR expires_at > now())
                   ) AS oldest_pending_available_at,
                   now() AS "as_of!"
                 FROM outbox"#
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| self.map_err(e))?;

        tx.commit().await.map_err(|e| self.map_err(e))?;

        Ok(OutboxStats::new(
            u64::try_from(row.pending).unwrap_or(0),
            u64::try_from(row.dead).unwrap_or(0),
            u64::try_from(row.expired_pending).unwrap_or(0),
            row.oldest_pending_available_at,
            row.as_of,
        ))
    }
}

impl<Ser: Serializer + Send + Sync + 'static> OutboxDeadLetters for PostgresOutboxStore<Ser> {
    type Error = PostgresStoreError;

    /// **`ORDER BY sequence ASC` is normative** (contract §3.4): `after_sequence` is a keyset
    /// cursor over `sequence`, the column `ix_outbox_dead` orders by; `message_type`/
    /// `tenant_id`/`dead_before` are filters only, expressed as `($n::type IS NULL OR ...)` so
    /// one static statement serves every combination. The cursor returned is the largest
    /// `sequence` **scanned**, poisoned rows included, so a poisoned tail cannot loop the
    /// caller forever.
    async fn list_dead(&self, query: DeadQuery) -> Result<DeadLetterPage, Self::Error> {
        // Provider-capped (contract §3.3 "provider-capped; default 100"): a caller-supplied
        // limit above this never reaches the database, regardless of what `DeadQuery` carries.
        let capped_limit = query.limit.min(MAX_LIST_DEAD_LIMIT);
        let limit = i64::from(capped_limit);

        let rows = if self.settings.statement_timeout.is_zero() {
            list_dead_rows(&self.pool, &query, limit)
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let rows = list_dead_rows(&mut *tx, &query, limit)
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            rows
        };

        let scanned = rows.len();
        let mut records = Vec::with_capacity(scanned);
        let mut poisoned = Vec::new();
        let mut max_sequence: Option<i64> = None;

        for raw in rows {
            max_sequence = Some(max_sequence.map_or(raw.sequence, |m| m.max(raw.sequence)));
            match decode_row(raw) {
                Ok(record) => records.push(record),
                Err(err) => poisoned.push(PoisonedRow::new(err.id, err.sequence, err.detail)),
            }
        }

        // "Full" is scanned == limit, poisoned rows included — they occupy a row in the scan,
        // so counting only decoded records would stop pagination early on a poisoned tail.
        let next_after_sequence = if scanned == capped_limit as usize {
            max_sequence
        } else {
            None
        };

        Ok(DeadLetterPage::new(records, poisoned, next_after_sequence))
    }

    /// Returns dead rows to pending: clears the lease that already isn't there, resets
    /// `attempts` to 0 (the **only** operation that does), keeps `last_error` for audit. Not
    /// worker-guarded — a dead row holds no lease (contract §3.4).
    async fn retry_dead(&self, refs: &[MessageRef]) -> Result<u64, Self::Error> {
        if refs.is_empty() {
            return Ok(0);
        }
        let ids: Vec<uuid::Uuid> = refs.iter().map(|r| r.id.as_uuid()).collect();
        let affected = if self.settings.statement_timeout.is_zero() {
            retry_dead_rows(&self.pool, &ids)
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let affected = retry_dead_rows(&mut *tx, &ids)
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        Ok(affected)
    }

    /// Deletes dead rows by reference, regardless of [`PurgeRequest::dead_retention`].
    async fn purge_dead(&self, refs: &[MessageRef]) -> Result<u64, Self::Error> {
        if refs.is_empty() {
            return Ok(0);
        }
        let ids: Vec<uuid::Uuid> = refs.iter().map(|r| r.id.as_uuid()).collect();
        let affected = if self.settings.statement_timeout.is_zero() {
            purge_dead_rows(&self.pool, &ids)
                .await
                .map_err(|e| self.map_err(e))?
        } else {
            let mut tx = self.pool.begin().await.map_err(|e| self.map_err(e))?;
            self.set_local_timeout(&mut tx).await?;
            let affected = purge_dead_rows(&mut *tx, &ids)
                .await
                .map_err(|e| self.map_err(e))?;
            tx.commit().await.map_err(|e| self.map_err(e))?;
            affected
        };
        Ok(affected)
    }
}

/// Logs a claimed-vs-affected shortfall at `debug` — never an error (ADR 0008): it means the
/// lease was lost to another worker or the row was already retried/completed, both benign.
fn log_shortfall(operation: &'static str, claimed: usize, affected: u64) {
    let claimed = claimed as u64;
    if affected < claimed {
        tracing::debug!(
            operation,
            claimed,
            affected,
            "fewer rows affected than claimed"
        );
    }
}
