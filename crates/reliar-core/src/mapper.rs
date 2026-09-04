//! Transport mapping abstraction (SRS §16, ADR 0004). Implemented in Phase 2.

use crate::SerializedEnvelope;

/// Converts a [`SerializedEnvelope`] to and from one transport's native message type `M`.
///
/// No implementation ships from `reliar-core` — a mapper's transport headers are a
/// **projection** of [`Metadata`](crate::Metadata), not a second source of truth (ADR 0004).
/// The reserved `reliar-*` header names a mapper writes are a public contract (§14).
pub trait EnvelopeMapper<M> {
    /// The mapper's own error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Encodes a canonical envelope into the transport's native message type.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if the transport's native message type cannot represent the
    /// envelope (e.g. a field it cannot carry).
    fn encode(&self, envelope: &SerializedEnvelope) -> Result<M, Self::Error>;

    /// Decodes a transport message back into a canonical envelope.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if the transport message cannot be decoded into a canonical
    /// envelope (a missing required framework header, or a malformed one).
    fn decode(&self, message: M) -> Result<SerializedEnvelope, Self::Error>;
}
