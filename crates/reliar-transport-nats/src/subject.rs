//! Subject resolution — a transport-side strategy kept out of `reliar-core` (SRS §12, §18,
//! ADR 0027, contract §3).

use reliar_core::SerializedEnvelope;

use crate::error::SubjectError;

/// Chooses the NATS subject an envelope is published to. **Pure and synchronous** — resolution
/// is a function of the envelope, which is what makes a failure permanently classifiable
/// (ADR 0027). A resolver that needs a lookup table builds it at construction; it must never
/// perform I/O.
pub trait SubjectResolver: Send + Sync {
    /// Why this resolver rejected an envelope.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Resolves the subject.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when the envelope cannot be routed — the publisher turns it into a
    /// **permanent** `NatsPublishError::Subject`, so the outbox row dead-letters instead of
    /// retrying (ADR 0030).
    fn subject(&self, envelope: &SerializedEnvelope) -> Result<async_nats::Subject, Self::Error>;
}

/// The default resolver: `<prefix>.<message_type>` — `reliar.orders.created.v1` for prefix
/// `"reliar"`, using [`MessageType`](reliar_core::MessageType)'s `Display` (a documented public
/// contract, ADR 0010). Ignores `RoutingMetadata.destination` (ADR 0027 §5) — see
/// [`DestinationSubjects`] for a resolver that honours it.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PrefixSubjects {
    prefix: String,
}

impl PrefixSubjects {
    /// The prefix used when `NatsSettings::subject_prefix` is left at its default.
    pub const DEFAULT_PREFIX: &'static str = "reliar";

    /// Validates `prefix` as one or more legal subject tokens (§3.1).
    ///
    /// # Errors
    ///
    /// Returns [`SubjectError`] for an empty, wildcard-bearing, illegally-charactered, or
    /// over-length prefix.
    pub fn new(prefix: impl Into<String>) -> Result<Self, SubjectError> {
        let prefix = prefix.into();
        validate_subject(&prefix)?;
        Ok(Self { prefix })
    }

    /// The configured prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

impl Default for PrefixSubjects {
    /// [`Self::DEFAULT_PREFIX`] is a compile-time-known legal subject token, so this is built
    /// directly rather than through the fallible [`Self::new`] (never a panic on a literal we
    /// control).
    fn default() -> Self {
        Self {
            prefix: Self::DEFAULT_PREFIX.to_string(),
        }
    }
}

impl SubjectResolver for PrefixSubjects {
    type Error = SubjectError;

    fn subject(&self, envelope: &SerializedEnvelope) -> Result<async_nats::Subject, SubjectError> {
        let resolved = format!("{}.{}", self.prefix, envelope.message_type);
        validate_subject(&resolved)?;
        Ok(async_nats::Subject::from(resolved))
    }
}

/// Opt-in: `RoutingMetadata.destination` verbatim when set, else the wrapped [`PrefixSubjects`]
/// (ADR 0027 §5). For applications that already decided routing per message.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DestinationSubjects {
    fallback: PrefixSubjects,
}

impl DestinationSubjects {
    /// Wraps `fallback`, used whenever an envelope carries no `destination`.
    #[must_use]
    pub fn new(fallback: PrefixSubjects) -> Self {
        Self { fallback }
    }
}

impl SubjectResolver for DestinationSubjects {
    type Error = SubjectError;

    fn subject(&self, envelope: &SerializedEnvelope) -> Result<async_nats::Subject, SubjectError> {
        match &envelope.metadata.routing.destination {
            Some(destination) => {
                let resolved = destination.as_str();
                validate_subject(resolved)?;
                Ok(async_nats::Subject::from(resolved.to_string()))
            }
            None => self.fallback.subject(envelope),
        }
    }
}

/// Shared subject validation for both resolvers, applied to the **resolved** subject (§3.1):
/// rejects an empty subject or token, a wildcard token or character, any byte outside ASCII
/// `0x21..=0x7E` (whitespace, control characters and non-ASCII all fall outside this range), and
/// anything over [`SubjectError::MAX_LEN`] bytes.
pub(crate) fn validate_subject(subject: &str) -> Result<(), SubjectError> {
    if subject.is_empty() {
        return Err(SubjectError::Empty);
    }
    if subject.len() > SubjectError::MAX_LEN {
        return Err(SubjectError::TooLong {
            len: subject.len(),
            limit: SubjectError::MAX_LEN,
        });
    }
    if subject.bytes().any(|b| !(0x21..=0x7E).contains(&b)) {
        return Err(SubjectError::IllegalCharacter {
            subject: subject.to_string(),
        });
    }
    for token in subject.split('.') {
        if token.is_empty() {
            return Err(SubjectError::EmptyToken {
                subject: subject.to_string(),
            });
        }
        if token.contains('*') || token.contains('>') {
            return Err(SubjectError::Wildcard {
                subject: subject.to_string(),
            });
        }
    }
    Ok(())
}
