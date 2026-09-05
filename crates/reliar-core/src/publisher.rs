//! Publication (SRS §19.4, §24.1, ADR 0008, ADR 0032).

use crate::SerializedEnvelope;
use crate::failure::Classify;

/// The wire side of the outbox. One provider implements this per transport.
///
/// A publish **timeout** classifies as [`crate::FailureKind::Transient`]. A payload the broker
/// rejects as too large classifies as [`crate::FailureKind::Permanent`] — retrying forever
/// cannot help (SRS §24.1).
pub trait Publisher: Send + Sync {
    /// The error a publish attempt can fail with. Must self-classify via [`Classify`] so the
    /// dispatcher can decide retry vs. dead without inspecting transport internals.
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    /// Publishes one envelope. Never retried by the publisher itself — retry is the
    /// dispatcher's and `RetryPolicy`'s job (`reliar-outbox`).
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
