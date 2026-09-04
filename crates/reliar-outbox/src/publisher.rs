//! Publication and failure classification (SRS §19.4, §24.1, ADR 0008).

use reliar_core::SerializedEnvelope;

/// The wire side of the outbox. One provider implements this per transport.
///
/// A publish **timeout** classifies as [`FailureKind::Transient`]. A payload the broker rejects
/// as too large classifies as [`FailureKind::Permanent`] — retrying forever cannot help
/// (SRS §24.1).
pub trait Publisher: Send + Sync {
    /// The error a publish attempt can fail with. Must self-classify via [`Classify`] so the
    /// dispatcher can decide retry vs. dead without inspecting transport internals.
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Publishes one envelope. Never retried by the publisher itself — retry is the
    /// dispatcher's and [`crate::RetryPolicy`]'s job.
    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Publishes a batch. Results are **positional** — one per envelope, in the same order, so
    /// a partial batch failure never loses a per-message verdict.
    ///
    /// The default loops over [`Self::publish`]; a transport with a native batch API overrides
    /// it and owns proving its positional results. **v0.1's dispatcher calls [`Self::publish`],
    /// not this method** — it needs a per-message outcome and a per-message timeout.
    fn publish_batch(
        &self,
        envelopes: &[SerializedEnvelope],
    ) -> impl Future<Output = Vec<Result<(), Self::Error>>> + Send {
        async move {
            let mut out = Vec::with_capacity(envelopes.len());
            for envelope in envelopes {
                out.push(self.publish(envelope).await);
            }
            out
        }
    }
}

/// Implemented by every [`Publisher::Error`] and [`crate::OutboxStore::Error`] so the dispatcher
/// can decide retry vs. dead without a downcast. Carried **by the error type**, not by the
/// publisher: the error value is what crosses a `JoinSet` boundary into the dispatcher, so it
/// must carry its own verdict (ADR 0008).
pub trait Classify {
    /// Whether the failure this error represents can succeed on retry.
    fn kind(&self) -> FailureKind;
}

/// Whether a failure is worth retrying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    /// May succeed if retried (a timeout, a connection blip, a lock conflict).
    Transient,
    /// No retry can fix it (an oversized payload, an unresolvable schema).
    Permanent,
}
