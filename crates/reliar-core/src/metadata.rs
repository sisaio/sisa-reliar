//! Canonical, typed framework metadata (SRS §12, ADR 0003, ADR 0004).

use core::fmt;

use crate::{
    ContentType, ConversationId, CorrelationId, MessageId, RequestId,
    ids::{IdError, contains_control_char},
};

/// Canonical, typed framework metadata: the single source of truth. No value here is ever
/// duplicated into [`Headers`](crate::Headers) (ADR 0004).
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct Metadata {
    /// Correlation and conversation identity.
    pub correlation: CorrelationMetadata,
    /// W3C Trace Context, carried verbatim.
    pub trace: TraceContext,
    /// Transport-independent routing hints.
    pub routing: RoutingMetadata,
    /// Serialization and delivery hints.
    pub delivery: DeliveryMetadata,
    /// The owning tenant, if this deployment is multi-tenant.
    pub tenant_id: Option<String>,
}

/// Correlation and conversation identity for one envelope.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct CorrelationMetadata {
    /// Application/business workflow correlation, set by the caller.
    pub correlation_id: Option<CorrelationId>,
    /// Groups every message in one business conversation.
    pub conversation_id: ConversationId,
    /// The message that directly caused this one.
    pub causation_id: Option<MessageId>,
    /// The inbound request that (transitively) caused this one.
    pub request_id: Option<RequestId>,
}

/// Sets `conversation_id` to the [`ConversationId::UNSET`] sentinel (the nil UUID) — **not** a
/// fresh mint — so [`crate::EnvelopeBuilder::build`] can tell "not yet rooted" from a genuinely
/// chosen value by comparing it, not by tracking which builder setter was called.
/// `build` replaces `UNSET` with the envelope's own id, so an un-correlated message is the root
/// of its own conversation, and leaves any other value alone. A `Metadata` that never passes
/// through the builder (e.g. read straight off `Default`) keeps the placeholder verbatim.
impl Default for CorrelationMetadata {
    fn default() -> Self {
        Self {
            correlation_id: None,
            conversation_id: ConversationId::UNSET,
            causation_id: None,
            request_id: None,
        }
    }
}

/// W3C Trace Context, carried verbatim. Reliar never invents or re-derives it (ADR 0004,
/// ADR 0020): a transport mapper writes these from an active span and reads them back on
/// decode.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub struct TraceContext {
    /// The W3C `traceparent` header value.
    pub traceparent: Option<String>,
    /// The W3C `tracestate` header value.
    pub tracestate: Option<String>,
}

/// Transport-independent routing only. Kafka partition keys, `RabbitMQ` exchanges and NATS
/// subject options are transport concepts and SHALL NOT appear here (§12).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RoutingMetadata {
    /// The logical origin of this message.
    pub source: Option<EndpointAddress>,
    /// The logical destination of this message.
    pub destination: Option<EndpointAddress>,
    /// Where a reply to this message should be sent.
    pub reply_to: Option<EndpointAddress>,
}

/// An opaque, transport-interpreted address string (a queue name, a subject, a service name —
/// Reliar does not care which). Capped at [`Self::MAX_LEN`] bytes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointAddress(String);

impl EndpointAddress {
    /// Maximum length in bytes.
    pub const MAX_LEN: usize = 256;

    /// Validates and wraps an endpoint address. Returns `Err` for an empty string, one
    /// containing a control character (including CR/LF — a header-injection surface), or one
    /// over [`Self::MAX_LEN`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`IdError::Empty`], [`IdError::ControlCharacter`], or [`IdError::TooLong`].
    pub fn parse(s: impl Into<String>) -> Result<Self, IdError> {
        let s = s.into();
        if s.is_empty() {
            return Err(IdError::Empty);
        }
        if contains_control_char(&s) {
            return Err(IdError::ControlCharacter);
        }
        if s.len() > Self::MAX_LEN {
            return Err(IdError::TooLong {
                len: s.len(),
                max: Self::MAX_LEN,
            });
        }
        Ok(Self(s))
    }

    /// Returns the address as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EndpointAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for EndpointAddress {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for EndpointAddress {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Serialization and delivery hints for one envelope.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct DeliveryMetadata {
    /// **Authoritatively set by the store at enqueue** from `Serializer::content_type()`, and
    /// read back from the provider's `content_type` column on rehydration. The `Default` value
    /// below is a placeholder a call site never chooses (ADR 0010).
    pub content_type: ContentType,
    /// When the application handed this message to Reliar (app clock; never compared against a
    /// DB timestamp).
    pub sent_at: Option<time::OffsetDateTime>,
    /// The time after which this message SHALL NOT be published. Enforced in DB time by a
    /// provider's claim predicate; an expired pending row goes dead with
    /// `DeadReason::Expired` and consumes no retry attempt (§12.2).
    pub expires_at: Option<time::OffsetDateTime>,
    /// A transport mapper's broker-specific dedup key (falling back to the message id). Reliar
    /// never deduplicates on it in the database (§12.3).
    pub deduplication_id: Option<String>,
}

impl Default for DeliveryMetadata {
    fn default() -> Self {
        Self {
            content_type: ContentType::JSON,
            sent_at: None,
            expires_at: None,
            deduplication_id: None,
        }
    }
}

#[cfg(feature = "serde")]
mod serde_impls {
    //! `Serialize`/`Deserialize` for hosts that want to persist or log `Metadata` themselves.
    //! Unrelated to `reliar-store-postgres`'s own private JSONB persistence contract (ADR 0012),
    //! which defines its own `MetadataRest` shape with its own forward-compatibility rules.

    use serde::{Deserialize, Serialize};

    use super::{CorrelationMetadata, DeliveryMetadata, Metadata, RoutingMetadata, TraceContext};

    // `#[serde(default)]` on every field: a persisted blob missing a whole sub-struct (an
    // 0.2 field addition, or a value written before it existed) still deserializes, falling
    // back to that sub-struct's own `Default` (§43.A, ADR 0012's sibling contract for
    // `reliar-core`'s own optional `Metadata` serde).
    #[derive(Serialize, Deserialize)]
    #[serde(remote = "Metadata")]
    struct MetadataDef {
        #[serde(default)]
        correlation: CorrelationMetadata,
        #[serde(default)]
        trace: TraceContext,
        #[serde(default)]
        routing: RoutingMetadata,
        #[serde(default)]
        delivery: DeliveryMetadata,
        #[serde(default)]
        tenant_id: Option<String>,
    }

    impl Serialize for Metadata {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            MetadataDef::serialize(self, s)
        }
    }
    impl<'de> Deserialize<'de> for Metadata {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            MetadataDef::deserialize(d)
        }
    }

    impl Serialize for CorrelationMetadata {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            // Field names mirror `CorrelationMetadata` on purpose, for wire compatibility.
            #[derive(Serialize)]
            #[allow(clippy::struct_field_names)]
            struct Def<'a> {
                correlation_id: &'a Option<super::CorrelationId>,
                conversation_id: &'a super::ConversationId,
                causation_id: &'a Option<super::MessageId>,
                request_id: &'a Option<super::RequestId>,
            }
            Def {
                correlation_id: &self.correlation_id,
                conversation_id: &self.conversation_id,
                causation_id: &self.causation_id,
                request_id: &self.request_id,
            }
            .serialize(s)
        }
    }
    impl<'de> Deserialize<'de> for CorrelationMetadata {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            // `ConversationId`'s own `Default` mints a **fresh** `UUIDv7` (it has no notion of
            // "not yet rooted"), so a bare `#[serde(default)]` here would silently disagree with
            // `CorrelationMetadata::default()`, which uses the `UNSET` sentinel. A blob missing
            // `conversation_id` (e.g. written before it existed) must fall back to the same
            // sentinel, not a random id.
            fn default_conversation_id() -> super::ConversationId {
                super::ConversationId::UNSET
            }

            #[derive(Deserialize)]
            #[allow(clippy::struct_field_names)]
            struct Def {
                #[serde(default)]
                correlation_id: Option<super::CorrelationId>,
                #[serde(default = "default_conversation_id")]
                conversation_id: super::ConversationId,
                #[serde(default)]
                causation_id: Option<super::MessageId>,
                #[serde(default)]
                request_id: Option<super::RequestId>,
            }
            let def = Def::deserialize(d)?;
            Ok(CorrelationMetadata {
                correlation_id: def.correlation_id,
                conversation_id: def.conversation_id,
                causation_id: def.causation_id,
                request_id: def.request_id,
            })
        }
    }

    impl Serialize for RoutingMetadata {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            #[derive(Serialize)]
            struct Def<'a> {
                source: &'a Option<super::EndpointAddress>,
                destination: &'a Option<super::EndpointAddress>,
                reply_to: &'a Option<super::EndpointAddress>,
            }
            Def {
                source: &self.source,
                destination: &self.destination,
                reply_to: &self.reply_to,
            }
            .serialize(s)
        }
    }
    impl<'de> Deserialize<'de> for RoutingMetadata {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            #[derive(Deserialize)]
            struct Def {
                #[serde(default)]
                source: Option<super::EndpointAddress>,
                #[serde(default)]
                destination: Option<super::EndpointAddress>,
                #[serde(default)]
                reply_to: Option<super::EndpointAddress>,
            }
            let def = Def::deserialize(d)?;
            Ok(RoutingMetadata {
                source: def.source,
                destination: def.destination,
                reply_to: def.reply_to,
            })
        }
    }

    impl Serialize for DeliveryMetadata {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            #[derive(Serialize)]
            struct Def<'a> {
                content_type: &'a super::ContentType,
                #[serde(with = "time::serde::rfc3339::option")]
                sent_at: &'a Option<time::OffsetDateTime>,
                #[serde(with = "time::serde::rfc3339::option")]
                expires_at: &'a Option<time::OffsetDateTime>,
                deduplication_id: &'a Option<String>,
            }
            Def {
                content_type: &self.content_type,
                sent_at: &self.sent_at,
                expires_at: &self.expires_at,
                deduplication_id: &self.deduplication_id,
            }
            .serialize(s)
        }
    }
    impl<'de> Deserialize<'de> for DeliveryMetadata {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            // `ContentType` has no public `Default` (ADR 0010: a call site never chooses one),
            // so its missing-field fallback is this private fn rather than a bare
            // `#[serde(default)]` — it mirrors `DeliveryMetadata::default()`'s own placeholder.
            fn default_content_type() -> super::ContentType {
                super::ContentType::JSON
            }

            #[derive(Deserialize)]
            struct Def {
                #[serde(default = "default_content_type")]
                content_type: super::ContentType,
                #[serde(default, with = "time::serde::rfc3339::option")]
                sent_at: Option<time::OffsetDateTime>,
                #[serde(default, with = "time::serde::rfc3339::option")]
                expires_at: Option<time::OffsetDateTime>,
                #[serde(default)]
                deduplication_id: Option<String>,
            }
            let def = Def::deserialize(d)?;
            Ok(DeliveryMetadata {
                content_type: def.content_type,
                sent_at: def.sent_at,
                expires_at: def.expires_at,
                deduplication_id: def.deduplication_id,
            })
        }
    }
}
