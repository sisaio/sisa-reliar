//! The staging capability [`crate::OutboxPublisher`] composes with (SRS §19.6, §20, ADR 0033
//! Amendment D).

use reliar_core::{Classify, MessageId, SerializedEnvelope};

/// Staging a serialized message in the caller's own transaction — the routed half of
/// [`crate::OutboxPublisher`].
///
/// `Tx` is the provider's transaction type: `sqlx::Transaction<'_, Postgres>` for
/// `reliar-store-postgres`. It is a **type parameter** precisely so this crate names no storage
/// type (SRS §19.6, ADR 0033 §2 + Amendment D §2), and one implementor may support several.
///
/// Deliberately **not** a method on [`crate::OutboxStore`]: staging takes a transaction handle
/// the claim side never sees, `OutboxStore` is already published, and a GAT `type Tx<'a>` would
/// have to spell `&'a mut Transaction<'c, _>` and reintroduce the invariance ADR 0033 §2 rejected.
pub trait OutboxStaging<Tx>: Send + Sync {
    /// What staging fails with. `Classify` is required because
    /// [`crate::ScopedOutboxPublisher`]'s `Publisher::Error` is built from it, and
    /// `reliar_core::Publisher::Error: Classify`.
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Stages `envelope` in `tx`. Returns the id written, so the caller can use it as the next
    /// message's `causation_id` in the same transaction.
    ///
    /// The implementation SHALL persist `envelope.metadata.delivery.content_type` **verbatim**:
    /// the caller serialized the body and is authoritative about its content type (SRS §12). It
    /// SHALL issue no network I/O other than the statement itself, and SHALL NOT commit, roll
    /// back or otherwise consume `tx` — the caller owns it.
    ///
    /// # Errors
    ///
    /// Provider-defined. A failure has typically aborted `tx`; the caller must roll back.
    fn stage(
        &self,
        tx: &mut Tx,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send;
}
