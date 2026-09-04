//! Validating custom-header newtype (SRS §13, §13.1, §14, ADR 0004, ADR 0011).

use core::fmt;
use std::collections::HashMap;

use crate::ids::contains_control_char;

/// Application-defined metadata Reliar does not understand: a validating newtype, never a
/// `HashMap` alias and never exposed through `Deref` (ADR 0011). Reserves the entire `reliar-`
/// prefix (case-insensitive) so framework metadata is never duplicated here — see
/// [`Metadata`](crate::Metadata) for the one canonical source of truth (ADR 0004, §14).
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Headers(HashMap<String, String>);

/// **Never derived.** Header values are application-supplied and may be secrets (SRS §33,
/// conventions §9): every value prints as `<redacted>`, keys print verbatim so the shape of a
/// header set is still debuggable.
impl fmt::Debug for Headers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.keys().map(|k| (k, "<redacted>")))
            .finish()
    }
}

impl Headers {
    /// Case-insensitively reserved prefix; [`Self::insert`] rejects any key starting with it.
    pub const RESERVED_PREFIX: &'static str = "reliar-";
    /// Maximum key length in bytes.
    pub const MAX_KEY_LEN: usize = 128;
    /// Maximum value length in bytes.
    pub const MAX_VALUE_LEN: usize = 1024;
    /// Maximum number of distinct headers on one envelope.
    pub const MAX_COUNT: usize = 32;

    /// Inserts a header, returning the previous value if the key was already present.
    ///
    /// Returns `Err` — never a silent drop or overwrite — for: the reserved `reliar-` prefix
    /// (matched case-insensitively, so `Reliar-Correlation-Id` is rejected too), an empty key, a
    /// key or value containing a control character (including CR/LF — a header-injection surface
    /// once a mapper writes the value onto the wire, same rule as [`crate::CorrelationId`],
    /// [`crate::EndpointAddress`] and [`crate::ContentType`]), a key over [`Self::MAX_KEY_LEN`],
    /// or a value over [`Self::MAX_VALUE_LEN`]. Replacing a key that is already present never
    /// counts against [`Self::MAX_COUNT`]; adding a genuinely new key while already at the cap
    /// does.
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError`] for a reserved, empty, control-character-containing, or
    /// over-length key, a control-character-containing or over-length value, or a new key that
    /// would exceed [`Self::MAX_COUNT`].
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Option<String>, HeaderError> {
        let key = key.into();
        let value = value.into();

        if Self::has_reserved_prefix(&key) {
            return Err(HeaderError::Reserved { key });
        }
        if key.is_empty() {
            return Err(HeaderError::EmptyKey);
        }
        if contains_control_char(&key) {
            return Err(HeaderError::ControlCharacterInKey { key });
        }
        if key.len() > Self::MAX_KEY_LEN {
            return Err(HeaderError::KeyTooLong { len: key.len() });
        }
        if contains_control_char(&value) {
            return Err(HeaderError::ControlCharacterInValue { key });
        }
        if value.len() > Self::MAX_VALUE_LEN {
            return Err(HeaderError::ValueTooLong { len: value.len() });
        }
        if !self.0.contains_key(&key) && self.0.len() >= Self::MAX_COUNT {
            return Err(HeaderError::TooManyHeaders {
                limit: Self::MAX_COUNT,
            });
        }

        Ok(self.0.insert(key, value))
    }

    /// Looks up a header by exact key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Removes a header, returning its value if present.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// The number of headers stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if no headers are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over every stored header as borrowed string slices.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn has_reserved_prefix(key: &str) -> bool {
        key.len() >= Self::RESERVED_PREFIX.len()
            && key.as_bytes()[..Self::RESERVED_PREFIX.len()]
                .eq_ignore_ascii_case(Self::RESERVED_PREFIX.as_bytes())
    }
}

/// [`Headers::insert`] failures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HeaderError {
    /// The key starts with the reserved `reliar-` prefix (case-insensitive).
    Reserved {
        /// The rejected key.
        key: String,
    },
    /// The key was empty.
    EmptyKey,
    /// The key contained a control character (including CR/LF — a header-injection surface).
    ControlCharacterInKey {
        /// The rejected key.
        key: String,
    },
    /// The value for this key contained a control character (including CR/LF). The value
    /// itself is never carried on the error — it may be a secret (SRS §33); the key is, so the
    /// caller can tell which header was rejected.
    ControlCharacterInValue {
        /// The key whose value was rejected.
        key: String,
    },
    /// The key exceeded [`Headers::MAX_KEY_LEN`].
    KeyTooLong {
        /// The key's actual length in bytes.
        len: usize,
    },
    /// The value exceeded [`Headers::MAX_VALUE_LEN`].
    ValueTooLong {
        /// The value's actual length in bytes.
        len: usize,
    },
    /// Inserting a new key would exceed [`Headers::MAX_COUNT`].
    TooManyHeaders {
        /// The configured limit.
        limit: usize,
    },
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Reserved { key } => {
                write!(f, "header key {key:?} uses the reserved `reliar-` prefix")
            }
            Self::EmptyKey => f.write_str("header key must not be empty"),
            Self::ControlCharacterInKey { key } => {
                write!(f, "header key {key:?} contains a control character")
            }
            Self::ControlCharacterInValue { key } => {
                write!(
                    f,
                    "header value for key {key:?} contains a control character"
                )
            }
            Self::KeyTooLong { len } => write!(
                f,
                "header key length {len} exceeds the maximum of {}",
                Headers::MAX_KEY_LEN
            ),
            Self::ValueTooLong { len } => write!(
                f,
                "header value length {len} exceeds the maximum of {}",
                Headers::MAX_VALUE_LEN
            ),
            Self::TooManyHeaders { limit } => {
                write!(f, "header count would exceed the maximum of {limit}")
            }
        }
    }
}

impl std::error::Error for HeaderError {}
