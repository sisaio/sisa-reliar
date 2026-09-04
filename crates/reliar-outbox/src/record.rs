//! The outbox row: [`OutboxRecord`] and its builder (SRS §17, §17.1, ADR 0005, ADR 0016,
//! ADR 0023).

use reliar_core::SerializedEnvelope;

use crate::store::{DeadReason, MessageRef};
use crate::worker::WorkerId;

/// The maximum length, in bytes, [`OutboxRecord::last_error`] and [`crate::PoisonedRow::error`]
/// are truncated to before being persisted (§17.1).
const MAX_ERROR_LEN: usize = 2048;
const TRUNCATION_MARKER: &str = "…[truncated]";

/// Truncates `error` to [`MAX_ERROR_LEN`] bytes at a char boundary, appending
/// [`TRUNCATION_MARKER`] when it was cut. Shared by [`OutboxRecordBuilder::last_error`] and
/// [`crate::PoisonedRow::new`] so both truncate identically (§17.1).
pub(crate) fn truncate_error(error: impl Into<String>) -> String {
    let error = error.into();
    if error.len() <= MAX_ERROR_LEN {
        return error;
    }

    let budget = MAX_ERROR_LEN.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = budget.min(error.len());
    while end > 0 && !error.is_char_boundary(end) {
        end -= 1;
    }

    tracing::debug!(
        original_len = error.len(),
        truncated_len = end,
        "outbox error truncated before persisting"
    );

    let mut truncated = String::with_capacity(end + TRUNCATION_MARKER.len());
    truncated.push_str(&error[..end]);
    truncated.push_str(TRUNCATION_MARKER);
    truncated
}

/// An envelope **plus** its outbound delivery state. Distinct from
/// [`reliar_core::Envelope`] (ADR 0005): nothing here reaches the wire.
///
/// `Clone` and `PartialEq` are required: the `test-support` fakes (S3) hand records out by
/// value and the acceptance tests compare them. `Debug` is derived and is payload-safe — it
/// delegates to `Envelope`'s manual `Debug`, which elides the body for every `T`; `last_error`
/// is already truncated and redacted (§17.1), so it is safe to print in full.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct OutboxRecord {
    /// The envelope this row carries.
    pub envelope: SerializedEnvelope,

    /// Monotonic, store-assigned. Not gap-free.
    pub sequence: i64,
    /// Immutable once written (ADR 0016).
    pub created_at: time::OffsetDateTime,
    /// `None` means unordered.
    pub ordering_key: Option<String>,

    /// Publish outcomes observed, **not** claims (ADR 0009). A claimed-then-crashed row
    /// reports 0 here even though it was claimed once.
    pub attempts: u32,
    /// The time before which this row is not claimable.
    pub available_at: time::OffsetDateTime,

    /// The worker currently holding this row's lease, if any.
    pub locked_by: Option<WorkerId>,
    /// When the current lease expires, if any.
    pub locked_until: Option<time::OffsetDateTime>,

    /// When this row was marked published, if it was.
    pub published_at: Option<time::OffsetDateTime>,
    /// When this row was marked dead, if it was.
    pub dead_at: Option<time::OffsetDateTime>,
    /// Why this row is dead, if it is.
    pub dead_reason: Option<DeadReason>,

    /// The last failure's `Display` output, truncated to 2 KiB at a char boundary with a
    /// `"…[truncated]"` marker. Never payload bytes, header values, or credentials (§17.1).
    pub last_error: Option<String>,
}

impl OutboxRecord {
    /// The value the dead-letter API and by-id operations take.
    #[must_use]
    pub fn message_ref(&self) -> MessageRef {
        MessageRef::new(self.envelope.id, self.created_at)
    }

    /// **Provider entry point.** `OutboxRecord` is `#[non_exhaustive]`, so a crate other than
    /// `reliar-outbox` cannot build one with struct-literal syntax — every provider rehydrating
    /// a row goes through this builder.
    ///
    /// Defaults: `attempts = 0`, `available_at = created_at`, no lease, not published, not dead,
    /// no error, no ordering key.
    pub fn builder(
        envelope: SerializedEnvelope,
        sequence: i64,
        created_at: time::OffsetDateTime,
    ) -> OutboxRecordBuilder {
        OutboxRecordBuilder::new(envelope, sequence, created_at)
    }
}

/// Builds an [`OutboxRecord`]. Obtained from [`OutboxRecord::builder`].
#[must_use]
#[derive(Debug)]
pub struct OutboxRecordBuilder {
    envelope: SerializedEnvelope,
    sequence: i64,
    created_at: time::OffsetDateTime,
    ordering_key: Option<String>,
    attempts: u32,
    available_at: time::OffsetDateTime,
    locked_by: Option<WorkerId>,
    locked_until: Option<time::OffsetDateTime>,
    published_at: Option<time::OffsetDateTime>,
    dead_at: Option<time::OffsetDateTime>,
    dead_reason: Option<DeadReason>,
    last_error: Option<String>,
}

impl OutboxRecordBuilder {
    fn new(envelope: SerializedEnvelope, sequence: i64, created_at: time::OffsetDateTime) -> Self {
        Self {
            envelope,
            sequence,
            available_at: created_at,
            created_at,
            ordering_key: None,
            attempts: 0,
            locked_by: None,
            locked_until: None,
            published_at: None,
            dead_at: None,
            dead_reason: None,
            last_error: None,
        }
    }

    /// Sets [`OutboxRecord::ordering_key`].
    pub fn ordering_key(mut self, key: Option<String>) -> Self {
        self.ordering_key = key;
        self
    }

    /// Sets [`OutboxRecord::attempts`].
    pub const fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    /// Sets [`OutboxRecord::available_at`].
    pub const fn available_at(mut self, at: time::OffsetDateTime) -> Self {
        self.available_at = at;
        self
    }

    /// Sets [`OutboxRecord::locked_by`] and [`OutboxRecord::locked_until`] together — a lease is
    /// either held by a worker until a time, or not held at all.
    pub fn lease(mut self, by: Option<WorkerId>, until: Option<time::OffsetDateTime>) -> Self {
        self.locked_by = by;
        self.locked_until = until;
        self
    }

    /// Sets [`OutboxRecord::published_at`].
    pub const fn published_at(mut self, at: Option<time::OffsetDateTime>) -> Self {
        self.published_at = at;
        self
    }

    /// Sets [`OutboxRecord::dead_at`] and [`OutboxRecord::dead_reason`] together.
    pub const fn dead(
        mut self,
        at: Option<time::OffsetDateTime>,
        reason: Option<DeadReason>,
    ) -> Self {
        self.dead_at = at;
        self.dead_reason = reason;
        self
    }

    /// Sets [`OutboxRecord::last_error`], truncating to 2 KiB at a char boundary with a
    /// `"…[truncated]"` marker (§17.1).
    pub fn last_error(mut self, error: Option<String>) -> Self {
        self.last_error = error.map(truncate_error);
        self
    }

    /// Builds the record.
    #[must_use]
    pub fn build(self) -> OutboxRecord {
        OutboxRecord {
            envelope: self.envelope,
            sequence: self.sequence,
            created_at: self.created_at,
            ordering_key: self.ordering_key,
            attempts: self.attempts,
            available_at: self.available_at,
            locked_by: self.locked_by,
            locked_until: self.locked_until,
            published_at: self.published_at,
            dead_at: self.dead_at,
            dead_reason: self.dead_reason,
            last_error: self.last_error,
        }
    }
}
