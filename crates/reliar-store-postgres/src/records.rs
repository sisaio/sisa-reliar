//! Row ⇄ [`OutboxRecord`]/[`reliar_core::SerializedEnvelope`] mapping and the `MetadataRest`
//! JSONB contract (SRS §24.2, ADR 0012).
//!
//! A malformed row (bad JSONB, an unknown `metadata_version`, an unrecognised `dead_reason`,
//! an invalid promoted column) never panics and never fails the whole batch — it becomes a
//! [`reliar_outbox::PoisonedRow`] via [`RowError`], and the caller (`acquire`/`list_dead`)
//! reports it while continuing with the rest (ADR 0008, §19.5).

use bytes::Bytes;
use reliar_core::{
    ContentType, ConversationId, CorrelationId, EndpointAddress, Headers, MessageId, MessageType,
    Metadata, RequestId, SerializedEnvelope,
};
use reliar_outbox::{DeadReason, OutboxRecord, WorkerId};
use uuid::Uuid;

/// The shape Reliar v0.1 writes into `metadata_version = 1`'s JSONB remainder: every field
/// `Metadata` carries that is **not** a promoted column (ADR 0012). `#[serde(default)]`
/// everywhere so a blob written by an older version — missing a field this version added —
/// still deserializes; unknown fields are ignored, never rejected, so a rolling deploy is safe
/// in both directions.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct MetadataRest {
    pub(crate) trace: TraceRest,
    pub(crate) routing: RoutingRest,
    pub(crate) delivery: DeliveryRest,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct TraceRest {
    pub(crate) traceparent: Option<String>,
    pub(crate) tracestate: Option<String>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct RoutingRest {
    pub(crate) source: Option<String>,
    pub(crate) destination: Option<String>,
    pub(crate) reply_to: Option<String>,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub(crate) struct DeliveryRest {
    /// Milliseconds since the Unix epoch, UTC, negative before 1970 — **not** RFC 3339
    /// (contract §7 J5). `time`'s RFC 3339 formatter cannot represent a year outside
    /// `0000..=9999`, which `OffsetDateTime` itself accepts, so an RFC-3339-encoded `sent_at`
    /// derived from arithmetic on an untrusted value could fail to serialize — a panic on the
    /// enqueue path if `.expect()`'d away, which §19.5 forbids. Epoch milliseconds are total
    /// over the whole range, so the failure mode is deleted rather than made fallible.
    pub(crate) sent_at_ms: Option<i64>,
    pub(crate) deduplication_id: Option<String>,
}

/// Encodes an [`time::OffsetDateTime`] as epoch milliseconds, saturating rather than
/// overflowing at either end of `i64`'s range (contract §7 J5) — this can run on an
/// application-supplied `sent_at`, not just Reliar's own clock.
pub(crate) fn encode_epoch_millis(dt: time::OffsetDateTime) -> i64 {
    let millis_of_second = i64::from(dt.millisecond());
    dt.unix_timestamp()
        .checked_mul(1000)
        .and_then(|ms| ms.checked_add(millis_of_second))
        .unwrap_or(if dt.unix_timestamp() < 0 {
            i64::MIN
        } else {
            i64::MAX
        })
}

/// The inverse of [`encode_epoch_millis`]. `None` when `ms` is outside the range
/// [`time::OffsetDateTime`] can represent — treated as a poisoned row by the caller, never a
/// panic.
pub(crate) fn decode_epoch_millis(ms: i64) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).ok()
}

/// The `metadata_version` v0.1 reads and writes. A row carrying any other value is a poison
/// row (ADR 0012).
pub(crate) const METADATA_VERSION: i32 = 1;

/// Every field `outbox` carries that [`decode_row`] needs, named rather than positional so the
/// two call sites (`acquire`'s claim `RETURNING`, `list_dead`'s `SELECT`) can each destructure
/// their own `sqlx::query!`-generated anonymous row type into one shared decoder.
pub(crate) struct RawRow {
    pub(crate) id: Uuid,
    pub(crate) sequence: i64,
    pub(crate) message_type: String,
    pub(crate) message_version: i32,
    pub(crate) correlation_id: Option<String>,
    pub(crate) conversation_id: Uuid,
    pub(crate) causation_id: Option<Uuid>,
    pub(crate) request_id: Option<Uuid>,
    pub(crate) content_type: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) tenant_id: Option<String>,
    pub(crate) expires_at: Option<time::OffsetDateTime>,
    pub(crate) ordering_key: Option<String>,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) headers: Option<serde_json::Value>,
    pub(crate) metadata_version: i32,
    pub(crate) created_at: time::OffsetDateTime,
    pub(crate) available_at: time::OffsetDateTime,
    pub(crate) attempts: i32,
    pub(crate) locked_by: Option<String>,
    pub(crate) locked_until: Option<time::OffsetDateTime>,
    pub(crate) published_at: Option<time::OffsetDateTime>,
    pub(crate) dead_at: Option<time::OffsetDateTime>,
    pub(crate) dead_reason: Option<String>,
    pub(crate) last_error: Option<String>,
}

/// Why [`decode_row`] could not turn a [`RawRow`] into an [`OutboxRecord`]. Always carries the
/// row's id and sequence so the caller can build a [`reliar_outbox::PoisonedRow`] and, for
/// `acquire`, move the row to dead (ADR 0008, §19.5).
pub(crate) struct RowError {
    pub(crate) id: MessageId,
    pub(crate) sequence: i64,
    pub(crate) detail: String,
}

/// Turns the `dead_reason` codec's stable `snake_case` string into a [`DeadReason`] (ADR 0023).
/// **These strings are a public contract and are never renamed** — a new variant only ever adds
/// a new string.
fn decode_dead_reason(value: &str) -> Option<DeadReason> {
    match value {
        "permanent_error" => Some(DeadReason::PermanentError),
        "attempts_exhausted" => Some(DeadReason::AttemptsExhausted),
        "expired" => Some(DeadReason::Expired),
        "undecodable" => Some(DeadReason::Undecodable),
        _ => None,
    }
}

/// The inverse of [`decode_dead_reason`] — the value `fail`'s dead transition and `purge`'s
/// expiry sweep write. Exhaustive over every variant this crate's `reliar-outbox` dependency
/// declares today; the `#[non_exhaustive]` wildcard below can only be reached if that pinned
/// version ever adds a variant without this crate being updated in the same change, which a
/// single-workspace `path` dependency makes impossible in practice.
#[allow(
    clippy::unreachable,
    reason = "DeadReason is #[non_exhaustive] from another crate, so the match needs a \
              catch-all; the exact-pinned path dependency makes it genuinely unreachable"
)]
pub(crate) fn encode_dead_reason(reason: DeadReason) -> &'static str {
    match reason {
        DeadReason::PermanentError => "permanent_error",
        DeadReason::AttemptsExhausted => "attempts_exhausted",
        DeadReason::Expired => "expired",
        DeadReason::Undecodable => "undecodable",
        _ => unreachable!("DeadReason gained a variant reliar-store-postgres does not encode yet"),
    }
}

/// The maximum length, in bytes, [`truncate_last_error`] truncates to before persisting into the
/// `last_error` column — mirrors `reliar_outbox`'s own `OutboxRecord::last_error`/`PoisonedRow`
/// truncation (SRS §17.1, 2 KiB), so a poisoned row's persisted error is bounded the same way a
/// publish failure's is.
const MAX_LAST_ERROR_LEN: usize = 2048;
const TRUNCATION_MARKER: &str = "…[truncated]";

/// Truncates `error` to [`MAX_LAST_ERROR_LEN`] bytes at a char boundary, appending
/// [`TRUNCATION_MARKER`] when cut (SRS §17.1, contract review 1, major 6) — applied to the
/// poison sweep's `last_error`, which otherwise persists an unbounded decode-failure message
/// (potentially quoting a malformed JSONB value) straight into the row.
pub(crate) fn truncate_last_error(error: String) -> String {
    if error.len() <= MAX_LAST_ERROR_LEN {
        return error;
    }
    let budget = MAX_LAST_ERROR_LEN.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = budget.min(error.len());
    while end > 0 && !error.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = String::with_capacity(end + TRUNCATION_MARKER.len());
    truncated.push_str(&error[..end]);
    truncated.push_str(TRUNCATION_MARKER);
    truncated
}

/// Reconstructs an [`OutboxRecord`] from a claimed or listed row, merging the promoted columns
/// with the `MetadataRest` remainder (ADR 0012). Never panics: every fallible step returns
/// [`RowError`] instead.
#[allow(
    clippy::too_many_lines,
    reason = "one decode covering every promoted column plus the MetadataRest merge; splitting \
              it would scatter a single invariant (ADR 0012's promoted-column list) across \
              several functions with no reuse to justify it"
)]
pub(crate) fn decode_row(row: RawRow) -> Result<OutboxRecord, RowError> {
    let id = MessageId::from_uuid(row.id);
    let fail = |detail: String| RowError {
        id,
        sequence: row.sequence,
        detail,
    };

    if row.metadata_version != METADATA_VERSION {
        return Err(fail(format!(
            "unknown metadata_version {}",
            row.metadata_version
        )));
    }

    let message_version = u16::try_from(row.message_version).map_err(|_| {
        fail(format!(
            "message_version {} out of range for u16",
            row.message_version
        ))
    })?;
    let message_type = MessageType::from_parts(row.message_type, message_version);

    let rest: MetadataRest = match row.metadata {
        Some(value) => serde_json::from_value(value)
            .map_err(|err| fail(format!("malformed metadata: {err}")))?,
        None => MetadataRest::default(),
    };

    let correlation_id = row
        .correlation_id
        .map(CorrelationId::parse)
        .transpose()
        .map_err(|err| fail(format!("invalid correlation_id: {err}")))?;
    let content_type = ContentType::parse(row.content_type)
        .map_err(|err| fail(format!("invalid content_type: {err}")))?;
    let source = rest
        .routing
        .source
        .map(EndpointAddress::parse)
        .transpose()
        .map_err(|err| fail(format!("invalid routing.source: {err}")))?;
    let destination = rest
        .routing
        .destination
        .map(EndpointAddress::parse)
        .transpose()
        .map_err(|err| fail(format!("invalid routing.destination: {err}")))?;
    let reply_to = rest
        .routing
        .reply_to
        .map(EndpointAddress::parse)
        .transpose()
        .map_err(|err| fail(format!("invalid routing.reply_to: {err}")))?;

    // Every one of these types is `#[non_exhaustive]`, so this crate builds them by starting
    // from `Default` and assigning each public field — never a struct literal.
    let mut metadata = Metadata::default();
    metadata.correlation.correlation_id = correlation_id;
    metadata.correlation.conversation_id = ConversationId::from_uuid(row.conversation_id);
    metadata.correlation.causation_id = row.causation_id.map(MessageId::from_uuid);
    metadata.correlation.request_id = row.request_id.map(RequestId::from_uuid);
    metadata.trace.traceparent = rest.trace.traceparent;
    metadata.trace.tracestate = rest.trace.tracestate;
    metadata.routing.source = source;
    metadata.routing.destination = destination;
    metadata.routing.reply_to = reply_to;
    metadata.delivery.content_type = content_type;
    let sent_at = rest
        .delivery
        .sent_at_ms
        .map(|ms| {
            decode_epoch_millis(ms).ok_or_else(|| fail(format!("sent_at_ms {ms} out of range")))
        })
        .transpose()?;
    metadata.delivery.sent_at = sent_at;
    metadata.delivery.expires_at = row.expires_at;
    metadata.delivery.deduplication_id = rest.delivery.deduplication_id;
    metadata.tenant_id = row.tenant_id;

    let headers = match row.headers {
        Some(serde_json::Value::Object(map)) => {
            let mut headers = Headers::default();
            for (key, value) in map {
                let value = value
                    .as_str()
                    .ok_or_else(|| fail(format!("header {key:?} is not a JSON string")))?;
                headers
                    .insert(key.clone(), value)
                    .map_err(|err| fail(format!("invalid header {key:?}: {err}")))?;
            }
            Some(headers)
        }
        Some(_) => return Err(fail("headers column is not a JSON object".into())),
        None => None,
    };

    let mut envelope =
        SerializedEnvelope::from_parts(id, message_type, Bytes::from(row.payload), metadata, None);
    envelope.set_headers(headers);

    let dead_reason = row
        .dead_reason
        .as_deref()
        .map(|value| {
            decode_dead_reason(value).ok_or_else(|| fail(format!("unknown dead_reason {value:?}")))
        })
        .transpose()?;

    let locked_by = row
        .locked_by
        .map(WorkerId::parse)
        .transpose()
        .map_err(|err| fail(format!("invalid locked_by: {err}")))?;

    let attempts = u32::try_from(row.attempts)
        .map_err(|_| fail(format!("attempts {} out of range for u32", row.attempts)))?;

    Ok(
        OutboxRecord::builder(envelope, row.sequence, row.created_at)
            .ordering_key(row.ordering_key)
            .attempts(attempts)
            .available_at(row.available_at)
            .lease(locked_by, row.locked_until)
            .published_at(row.published_at)
            .dead(row.dead_at, dead_reason)
            .last_error(row.last_error)
            .build(),
    )
}
