//! Storage side of the outbox: the [`OutboxStore`]/[`OutboxDeadLetters`] traits and the request
//! and result types that cross their boundary (SRS §19, §19.1–§19.6, ADR 0006, ADR 0008,
//! ADR 0009, ADR 0016, ADR 0023).

use std::time::Duration;

use reliar_core::MessageId;

use crate::ordering::Ordering;
use crate::publisher::Classify;
use crate::record::{OutboxRecord, truncate_error};
use crate::worker::WorkerId;

/// What [`OutboxStore::acquire`] claims.
///
/// `#[non_exhaustive]`: build with [`Self::new`] and the builder methods, never a struct
/// literal, so a new field never breaks a caller outside this crate.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AcquireRequest {
    /// The claiming worker; every claimed row's `locked_by` is set to this value.
    pub worker: WorkerId,
    /// The maximum number of rows to claim. Default 100.
    pub batch_size: u32,
    /// How long the claim holds the lease before it may be reclaimed. Default 30 s.
    pub lease: Duration,
    /// The ordering strategy to claim under. Default [`Ordering::Unordered`].
    pub ordering: Ordering,
}

impl AcquireRequest {
    /// Starts a request for `worker` with the documented defaults (§23.1): `batch_size = 100`,
    /// `lease = 30s`, `ordering = Unordered`.
    #[must_use]
    pub fn new(worker: WorkerId) -> Self {
        Self {
            worker,
            batch_size: 100,
            lease: Duration::from_secs(30),
            ordering: Ordering::default(),
        }
    }

    /// Sets [`Self::batch_size`].
    #[must_use]
    pub const fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets [`Self::lease`].
    #[must_use]
    pub const fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Sets [`Self::ordering`].
    #[must_use]
    pub const fn ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }
}

/// The result of one [`OutboxStore::acquire`] call.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct AcquiredBatch {
    /// The rows claimed and successfully decoded.
    pub records: Vec<OutboxRecord>,
    /// Rows the store could not decode. **Already moved to dead** by the same call, with
    /// [`DeadReason::Undecodable`] (ADR 0008) — the caller only reports them.
    pub poisoned: Vec<PoisonedRow>,
}

impl AcquiredBatch {
    /// Builds an acquired batch from its claimed and poisoned rows.
    #[must_use]
    pub fn new(records: Vec<OutboxRecord>, poisoned: Vec<PoisonedRow>) -> Self {
        Self { records, poisoned }
    }

    /// `true` when there are no records **and** no poisoned rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.poisoned.is_empty()
    }
}

/// One page of [`OutboxDeadLetters::list_dead`].
///
/// Poisoned rows here are **already dead**, so unlike [`AcquiredBatch`] there is no transition to
/// make — they are reported so an operator can see them (ADR 0023).
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct DeadLetterPage {
    /// The dead rows decoded successfully.
    pub records: Vec<OutboxRecord>,
    /// Dead rows the store could not decode, reported rather than silently skipped.
    pub poisoned: Vec<PoisonedRow>,
    /// Feeds the next [`DeadQuery::after_sequence`]. `None` when the page was not full — the
    /// cursor is computed over every row scanned, including poisoned ones, so a poisoned tail
    /// never loops the caller forever.
    pub next_after_sequence: Option<i64>,
}

impl DeadLetterPage {
    /// Builds a dead-letter page from its rows and keyset cursor.
    #[must_use]
    pub fn new(
        records: Vec<OutboxRecord>,
        poisoned: Vec<PoisonedRow>,
        next_after_sequence: Option<i64>,
    ) -> Self {
        Self {
            records,
            poisoned,
            next_after_sequence,
        }
    }

    /// `true` when there are no records **and** no poisoned rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.poisoned.is_empty()
    }
}

/// A row the store could not decode (a corrupt envelope, an unknown `dead_reason`, an
/// unsupported metadata version). Reported, never silently dropped.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PoisonedRow {
    /// The row's message id.
    pub id: MessageId,
    /// The row's store-assigned sequence.
    pub sequence: i64,
    /// The decode failure. Truncated to 2 KiB at a char boundary (§17.1).
    pub error: String,
}

impl PoisonedRow {
    /// Builds a poisoned-row report, truncating `error` to 2 KiB at a char boundary with a
    /// `"…[truncated]"` marker (§17.1).
    #[must_use]
    pub fn new(id: MessageId, sequence: i64, error: impl Into<String>) -> Self {
        Self {
            id,
            sequence,
            error: truncate_error(error.into()),
        }
    }
}

/// Identifies one row for a by-id [`OutboxStore`] operation. Carries `created_at` alongside `id`
/// because a partitioned table needs it to prune to the right partition (§24.3, ADR 0016).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MessageRef {
    /// The row's message id.
    pub id: MessageId,
    /// The row's immutable creation timestamp.
    pub created_at: time::OffsetDateTime,
}

impl MessageRef {
    /// Builds a message reference.
    #[must_use]
    pub const fn new(id: MessageId, created_at: time::OffsetDateTime) -> Self {
        Self { id, created_at }
    }
}

/// One row to mark published in [`OutboxStore::complete`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CompletedMessage {
    /// The row that published successfully.
    pub message: MessageRef,
}

impl CompletedMessage {
    /// Builds a completed-message report.
    #[must_use]
    pub const fn new(message: MessageRef) -> Self {
        Self { message }
    }
}

/// One row to apply a [`FailureOutcome`] to in [`OutboxStore::fail`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FailedMessage {
    /// The row whose publish failed.
    pub message: MessageRef,
    /// The failure, already truncated and redacted (§17.1).
    pub error: String,
    /// The decided outcome. The store never re-derives retry policy — it only applies this.
    pub outcome: FailureOutcome,
}

impl FailedMessage {
    /// Builds a failed-message report, truncating `error` to 2 KiB at a char boundary with a
    /// `"…[truncated]"` marker (§17.1).
    #[must_use]
    pub fn new(message: MessageRef, error: impl Into<String>, outcome: FailureOutcome) -> Self {
        Self {
            message,
            error: truncate_error(error),
            outcome,
        }
    }
}

/// What [`OutboxStore::fail`] should do with one row, as decided by a [`crate::RetryPolicy`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailureOutcome {
    /// Retry later. The store applies it as `available_at = now() + delay` **in SQL**
    /// (ADR 0009).
    Retry {
        /// The delay before the row becomes claimable again.
        delay: Duration,
    },
    /// Terminal. The store sets `dead_at = now()` and records `reason`.
    Dead {
        /// Why the row is dead.
        reason: DeadReason,
    },
}

/// Why a row is dead. Persisted so an operator inspecting [`OutboxDeadLetters::list_dead`] can
/// tell a broker rejection from an expired message without reading `last_error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeadReason {
    /// The publisher classified the failure as [`crate::FailureKind::Permanent`].
    PermanentError,
    /// The retry policy's `max_attempts` was reached.
    AttemptsExhausted,
    /// The row's `expires_at` passed before it was published.
    Expired,
    /// The store could not decode the row (a corrupt envelope, an unknown column value).
    Undecodable,
}

/// What [`OutboxStore::purge`] should delete or sweep in one bounded pass.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PurgeRequest {
    /// How long a published row is kept before it is deleted. `None` disables published
    /// purging. Default `Some(7 days)`.
    pub published_retention: Option<Duration>,
    /// How long a dead row is kept before it is deleted. `None` (the default) keeps dead rows
    /// until an explicit purge — deleting one is always a deliberate act.
    pub dead_retention: Option<Duration>,
    /// The maximum number of rows deleted per call, for each of the published and dead passes.
    /// Default 1000.
    pub batch_size: u32,
}

/// **Hand-written, never derived**: a derived `Default` would give `None`/`None`/`0` — a
/// `purge` that deletes nothing and reports success.
impl Default for PurgeRequest {
    fn default() -> Self {
        Self {
            published_retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            dead_retention: None,
            batch_size: 1_000,
        }
    }
}

impl PurgeRequest {
    /// Sets [`Self::published_retention`].
    #[must_use]
    pub const fn published_retention(mut self, retention: Option<Duration>) -> Self {
        self.published_retention = retention;
        self
    }

    /// Sets [`Self::dead_retention`].
    #[must_use]
    pub const fn dead_retention(mut self, retention: Option<Duration>) -> Self {
        self.dead_retention = retention;
        self
    }

    /// Sets [`Self::batch_size`].
    #[must_use]
    pub const fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// What one [`OutboxStore::purge`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PurgeReport {
    /// Published rows deleted.
    pub published_deleted: u64,
    /// Dead rows deleted.
    pub dead_deleted: u64,
    /// Pending rows swept to dead because their `expires_at` had passed.
    pub expired_to_dead: u64,
}

impl PurgeReport {
    /// Builds a purge report.
    #[must_use]
    pub const fn new(published_deleted: u64, dead_deleted: u64, expired_to_dead: u64) -> Self {
        Self {
            published_deleted,
            dead_deleted,
            expired_to_dead,
        }
    }

    /// `false` when any of the three counts hit `batch_size` — the caller should call
    /// [`OutboxStore::purge`] again; one call is one bounded pass, never an internal loop
    /// (ADR 0009). The expired-to-dead sweep is bounded by the same `batch_size` as the two
    /// deletes, so it is checked here too.
    #[must_use]
    #[allow(
        clippy::cast_lossless,
        reason = "widening u32 -> u64; `u64::from` is not callable from a const fn on stable"
    )]
    pub const fn is_complete(&self, batch_size: u32) -> bool {
        self.published_deleted < batch_size as u64
            && self.dead_deleted < batch_size as u64
            && self.expired_to_dead < batch_size as u64
    }
}

/// A snapshot of the outbox's backlog, for the outbox-lag and dead-count gauges.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct OutboxStats {
    /// Claimable rows only — the same predicate `acquire` uses, so an expired row is excluded.
    pub pending: u64,
    /// Dead rows.
    pub dead: u64,
    /// Pending rows whose `expires_at` has passed but have not yet been swept to dead by
    /// `purge`. Unclaimable; counted separately so they can be alerted on without pinning
    /// [`Self::lag`].
    pub expired_pending: u64,
    /// The oldest claimable row's `available_at`, over claimable rows only, so it cannot be
    /// pinned by an expired row.
    pub oldest_pending_available_at: Option<time::OffsetDateTime>,
    /// The database's `now()` at the moment of the query, so [`Self::lag`] never compares an
    /// application clock against a database one.
    pub as_of: time::OffsetDateTime,
}

impl OutboxStats {
    /// Builds a stats snapshot.
    #[must_use]
    pub const fn new(
        pending: u64,
        dead: u64,
        expired_pending: u64,
        oldest_pending_available_at: Option<time::OffsetDateTime>,
        as_of: time::OffsetDateTime,
    ) -> Self {
        Self {
            pending,
            dead,
            expired_pending,
            oldest_pending_available_at,
            as_of,
        }
    }

    /// `as_of - oldest_pending_available_at`, clamped at zero. The "outbox lag" gauge. `None`
    /// when there is no claimable pending row.
    #[must_use]
    pub fn lag(&self) -> Option<Duration> {
        let oldest = self.oldest_pending_available_at?;
        let diff = self.as_of - oldest;
        Some(if diff.is_negative() {
            Duration::ZERO
        } else {
            diff.unsigned_abs()
        })
    }
}

/// A filtered, paginated query over dead rows.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DeadQuery {
    /// Restricts to one message type name (every version), if set.
    pub message_type: Option<String>,
    /// Restricts to one tenant, if set.
    pub tenant_id: Option<String>,
    /// Restricts to rows that went dead before this time, if set.
    pub dead_before: Option<time::OffsetDateTime>,
    /// The maximum number of rows to return. Provider-capped. Default 100.
    pub limit: u32,
    /// Keyset pagination cursor: only rows with a greater `sequence`. Feed with
    /// [`DeadLetterPage::next_after_sequence`].
    pub after_sequence: Option<i64>,
}

/// **Hand-written, never derived**: a derived `Default` would set `limit = 0` and return
/// nothing.
impl Default for DeadQuery {
    fn default() -> Self {
        Self {
            message_type: None,
            tenant_id: None,
            dead_before: None,
            limit: 100,
            after_sequence: None,
        }
    }
}

impl DeadQuery {
    /// Sets [`Self::message_type`].
    #[must_use]
    pub fn message_type(mut self, message_type: impl Into<String>) -> Self {
        self.message_type = Some(message_type.into());
        self
    }

    /// Sets [`Self::tenant_id`].
    #[must_use]
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// Sets [`Self::dead_before`].
    #[must_use]
    pub const fn dead_before(mut self, dead_before: time::OffsetDateTime) -> Self {
        self.dead_before = Some(dead_before);
        self
    }

    /// Sets [`Self::limit`].
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Sets [`Self::after_sequence`].
    #[must_use]
    pub const fn after_sequence(mut self, after_sequence: i64) -> Self {
        self.after_sequence = Some(after_sequence);
        self
    }
}

/// The dispatcher's portable side of the outbox. One provider implements this per database.
///
/// **`enqueue` is deliberately not here** — it must join the application's own transaction and
/// stays provider-inherent (ADR 0008).
///
/// Every state-changing method takes `worker` and **SHALL** match it (`AND locked_by = $worker`)
/// wherever the row carries a lease. A store that ignores it does not implement this trait.
/// Each returns the count of rows affected; a shortfall means the lease was lost to another
/// worker and is **benign, never an error**.
pub trait OutboxStore: Send + Sync {
    /// A failure of the *call* — never a property of one row's content. Must self-classify via
    /// [`crate::Classify`] so `run()` (S4) can tell a transient outage from a permanent one
    /// (ADR 0014).
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Claims up to `request.batch_size` due, unlocked, unexpired rows. **Must have committed
    /// before the future resolves** — the caller publishes outside any transaction (ADR 0006).
    fn acquire(
        &self,
        request: AcquireRequest,
    ) -> impl Future<Output = Result<AcquiredBatch, Self::Error>> + Send;

    /// Marks rows published and increments `attempts`. Idempotent under the worker guard: a
    /// row already completed or reclaimed by another worker contributes nothing to the count.
    fn complete(
        &self,
        worker: &WorkerId,
        items: &[CompletedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Applies each item's [`FailureOutcome`] and increments `attempts`.
    fn fail(
        &self,
        worker: &WorkerId,
        items: &[FailedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Hands rows back at once: clears the lease, `available_at` unchanged, **`attempts`
    /// unchanged**. Used on graceful shutdown.
    fn release(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Renews `locked_until = now() + lease` for rows this worker still owns. Best-effort: a
    /// shortfall means the lease already expired.
    fn extend_lease(
        &self,
        worker: &WorkerId,
        items: &[MessageRef],
        lease: Duration,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// **One bounded pass**: deletes at most `request.batch_size` published rows and at most
    /// `request.batch_size` dead rows, and sweeps expired pending rows to dead
    /// ([`DeadReason::Expired`]). It **does not loop internally** — unbounded work inside a
    /// trait method has no cancellation point and no progress reporting. The **caller** repeats
    /// while `!report.is_complete(request.batch_size)`. Reliar starts no maintenance timer; the
    /// host calls this from its own periodic task.
    fn purge(
        &self,
        request: PurgeRequest,
    ) -> impl Future<Output = Result<PurgeReport, Self::Error>> + Send;

    /// Feeds the outbox-lag and dead-count gauges. Polled by the dispatcher's `run` loop (S4)
    /// every `DispatcherSettings::stats_interval`, never per batch; `Duration::ZERO` there
    /// disables the tick entirely, so a host that never calls this directly gets no gauges at
    /// all rather than a zero-valued one. Also `pub`, so a host may call it directly (e.g. for an
    /// admin endpoint) independently of the dispatcher.
    fn stats(&self) -> impl Future<Output = Result<OutboxStats, Self::Error>> + Send;
}

/// The operator surface over dead rows. A separate small capability — the dispatcher never
/// calls it (SRS §34).
pub trait OutboxDeadLetters: Send + Sync {
    /// A failure of the *call*.
    type Error: std::error::Error + Send + Sync + 'static;

    /// **`ORDER BY sequence ASC` is normative, not an implementation detail.**
    /// [`DeadQuery::after_sequence`] is a keyset cursor, and a keyset cursor is only correct over
    /// the column it orders by; `sequence` is the store-assigned monotonic identity (§22.2) and
    /// is unique, so it needs no tiebreak. `message_type`, `tenant_id` and `dead_before` are
    /// filters, never part of the order.
    ///
    /// Returns a page, not a bare `Vec`: a dead row can itself be undecodable, and such a row
    /// is reported, never silently skipped (ADR 0023). The cursor is computed over every row
    /// scanned, including the poisoned ones — deriving it only from decoded rows would loop
    /// forever on a poisoned tail.
    fn list_dead(
        &self,
        query: DeadQuery,
    ) -> impl Future<Output = Result<DeadLetterPage, Self::Error>> + Send;

    /// Returns dead rows to pending: clears `dead_at`/`dead_reason`, sets `available_at =
    /// now()`, resets `attempts` to 0, keeps `last_error` for audit. Affects only rows with
    /// `dead_at IS NOT NULL`.
    ///
    /// The **only** operation in the system that resets `attempts`, and always an explicit
    /// operator action. Not worker-guarded — a dead row holds no lease.
    fn retry_dead(
        &self,
        refs: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Deletes dead rows by reference, regardless of [`PurgeRequest::dead_retention`].
    fn purge_dead(
        &self,
        refs: &[MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;
}
