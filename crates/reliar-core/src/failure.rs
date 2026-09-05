//! Failure classification shared by [`crate::Publisher`] and `OutboxStore` (`reliar-outbox`)
//! (SRS §19.4, §23, ADR 0008, ADR 0032).

/// Implemented by every [`crate::publisher::Publisher::Error`] and `OutboxStore::Error`
/// (`reliar-outbox`) so a dispatcher can decide retry vs. dead without a downcast. Carried **by
/// the error type**, not by the publisher: the error value is what crosses a `JoinSet` boundary
/// into the dispatcher, so it must carry its own verdict (ADR 0008).
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
