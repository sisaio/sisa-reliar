//! The enqueue capability [`crate::OutboxPublisher`] composes with (SRS §19.6, §20, ADR 0036 §6).

use reliar_core::{Classify, MessageId, SerializedEnvelope};

/// Enqueuing a serialized message in the caller's own transaction — what
/// [`crate::OutboxPublisher::enqueue`] calls.
///
/// `Tx` is the provider's transaction type: `sqlx::Transaction<'_, Postgres>` for
/// `reliar-store-postgres`. It is a **type parameter** precisely so this crate names no storage
/// type (SRS §19.6, ADR 0036 §6), and one implementor may support several.
///
/// Deliberately **not** a method on [`crate::OutboxStore`]: enqueuing takes a transaction handle
/// the claim side never sees, `OutboxStore` is already published, and a GAT `type Tx<'a>` would
/// have to spell `&'a mut Transaction<'c, _>` and reintroduce an invariance problem.
///
/// Renamed from `OutboxStaging` in 0.4.0 (decision #34); the method was `stage`.
pub trait OutboxEnqueue<Tx>: Send + Sync {
    /// What enqueuing fails with. `Classify` is required because
    /// [`crate::OutboxPublisher::enqueue`] returns this error directly, and
    /// `reliar_core::Publisher::Error: Classify`.
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Enqueues `envelope` in `tx`. Returns the id written, for the implementor's own callers;
    /// [`crate::OutboxPublisher::enqueue`] discards it (contract §11.4) — the envelope already
    /// carries its `id`, and a *next* message's `causation_id` is the caller's business, set
    /// before this call, not derived from this method's return value.
    ///
    /// The implementation SHALL persist `envelope.metadata.delivery.content_type` **verbatim**:
    /// the caller serialized the body and is authoritative about its content type (SRS §12). It
    /// SHALL issue no network I/O other than the statement itself, and SHALL NOT commit, roll
    /// back or otherwise consume `tx` — the caller owns it.
    ///
    /// # Errors
    ///
    /// Provider-defined. An `Err` **MAY** leave `tx` unusable, and whether it does is the
    /// provider's contract — every implementor documents which. The portable rule a caller can
    /// rely on is therefore: treat any enqueue error as *abort this transaction* — issue no
    /// further statement on `tx`, roll it back, and consider every earlier write in it lost.
    /// With `reliar-store-postgres` the transaction **is** aborted: PostgreSQL rejects every
    /// subsequent statement on it, so no earlier write in that transaction can still be committed.
    fn enqueue(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
