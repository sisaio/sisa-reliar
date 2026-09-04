//! Identity newtypes shared by every envelope (SRS §11, ADR 0011, ADR 0015).

use core::fmt;

use uuid::Uuid;

/// Validation failures shared by every capped string identity newtype in `reliar-core`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdError {
    /// The value was empty.
    Empty,
    /// The value exceeded its type's maximum length.
    TooLong {
        /// The value's actual length in bytes.
        len: usize,
        /// The maximum allowed length in bytes.
        max: usize,
    },
    /// The value contained a control character (including CR/LF) — a header-injection
    /// surface once a mapper writes this value onto the wire.
    ControlCharacter,
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("value must not be empty"),
            Self::TooLong { len, max } => {
                write!(f, "value length {len} exceeds the maximum of {max}")
            }
            Self::ControlCharacter => f.write_str("value must not contain a control character"),
        }
    }
}

impl std::error::Error for IdError {}

/// Shared by every capped string identity newtype (and [`crate::Headers`]): `true` if `s`
/// contains a control character (including CR/LF), which would let a value smuggle extra
/// header/line-oriented content onto the wire once a transport mapper writes it verbatim.
///
/// `char::is_control` matches Unicode category `Cc` (`U+0000..=U+001F`, `U+007F`,
/// `U+0080..=U+009F`) — exactly the code points a line-oriented wire format (an HTTP-style
/// header, a CSV row) treats specially. It is deliberately not a wider "non-printable" or
/// "non-ASCII" check: rejecting e.g. combining marks or emoji would reject legitimate
/// human-readable data this type has no reason to forbid.
pub(crate) fn contains_control_char(s: &str) -> bool {
    s.chars().any(char::is_control)
}

/// Declares a UUID-backed identity newtype: `Clone + Copy + Debug + Eq + Hash + Ord`, a
/// `new()` that mints a fresh `UUIDv7`, and a `Display` that renders the inner UUID verbatim.
/// Every one Reliar generates is `UUIDv7` (ADR 0015); applications may supply any UUID and
/// Reliar SHALL NOT inspect or reject its version.
macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a fresh `UUIDv7` id.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps an existing UUID without inspecting or rejecting its version.
            #[must_use]
            pub const fn from_uuid(id: Uuid) -> Self {
                Self(id)
            }

            /// Returns the inner UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    };
}

uuid_id!(
    /// Uniquely identifies one envelope end-to-end: enqueue, storage row, wire message, and,
    /// if it fails permanently, the dead entry.
    MessageId
);
uuid_id!(
    /// Groups every message in one business conversation. Defaults to the id of the message
    /// that starts it (see [`crate::EnvelopeBuilder::build`]), so an un-correlated message is
    /// the root of its own conversation.
    ConversationId
);
uuid_id!(
    /// Correlates an envelope back to the inbound request (HTTP call, RPC, CLI invocation) that
    /// caused it, so an outbound message can be traced to its trigger.
    RequestId
);

impl ConversationId {
    /// The reserved "not yet rooted" sentinel: the **nil** UUID. [`CorrelationMetadata`]'s
    /// default uses it, and [`EnvelopeBuilder::build`] replaces it with the envelope's own id —
    /// conversation rooting is decided by *this value*, not by which builder setter was called.
    /// [`Self::new`]/[`Self::default`] mint a fresh `UUIDv7` and are therefore never `UNSET`. An
    /// application SHALL NOT use the nil UUID as a real conversation id.
    ///
    /// [`CorrelationMetadata`]: crate::CorrelationMetadata
    /// [`EnvelopeBuilder::build`]: crate::EnvelopeBuilder::build
    pub const UNSET: Self = Self::from_uuid(Uuid::nil());

    /// `true` when this id is [`Self::UNSET`].
    #[must_use]
    pub const fn is_unset(&self) -> bool {
        self.0.is_nil()
    }
}

/// Application/business workflow correlation id — distinct from [`ConversationId`] (Reliar's own
/// conversation root) and a `causation_id` (the direct parent message). Capped at
/// [`Self::MAX_LEN`] bytes: it lands in a `text` column read on every claim (§11).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Maximum length in bytes.
    pub const MAX_LEN: usize = 256;

    /// Validates and wraps a correlation id. Returns `Err` for an empty string, one containing
    /// a control character (including CR/LF — a header-injection surface), or one over
    /// [`Self::MAX_LEN`] bytes.
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

    /// Returns the correlation id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "serde")]
mod serde_impls {
    use serde::{Deserialize, Serialize, de::Error as _};
    use uuid::Uuid;

    use super::{ConversationId, CorrelationId, MessageId, RequestId};

    // Serialized as the canonical hyphenated UUID string rather than via `uuid`'s own `serde`
    // feature, so enabling `reliar-core/serde` never has to unify `uuid`'s feature set.
    macro_rules! uuid_id_serde {
        ($name:ident) => {
            impl Serialize for $name {
                fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                    s.collect_str(&self.0)
                }
            }
            impl<'de> Deserialize<'de> for $name {
                fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                    let raw = String::deserialize(d)?;
                    Uuid::parse_str(&raw).map(Self).map_err(D::Error::custom)
                }
            }
        };
    }
    uuid_id_serde!(MessageId);
    uuid_id_serde!(ConversationId);
    uuid_id_serde!(RequestId);

    impl Serialize for CorrelationId {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.collect_str(&self.0)
        }
    }
    impl<'de> Deserialize<'de> for CorrelationId {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let raw = String::deserialize(d)?;
            Self::parse(raw).map_err(D::Error::custom)
        }
    }
}
