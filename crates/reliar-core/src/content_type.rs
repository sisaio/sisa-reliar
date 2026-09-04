//! Validated MIME content type (SRS §12.1, ADR 0010).

use core::fmt;
use std::borrow::Cow;

use crate::ids::contains_control_char;

/// A validated MIME type. Owned by the [`Serializer`](crate::Serializer) that produced a
/// payload — never chosen at the call site (ADR 0010).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContentType(Cow<'static, str>);

impl ContentType {
    /// `"application/json"` — [`crate::JsonSerializer`]'s content type.
    pub const JSON: Self = Self(Cow::Borrowed("application/json"));

    /// Maximum length in bytes — it lands in a `content_type` column read back on every
    /// rehydration, same reasoning as [`crate::CorrelationId::MAX_LEN`].
    pub const MAX_LEN: usize = 256;

    /// The number of leading characters of a rejected value [`ContentTypeError::Malformed`]'s
    /// `Display` **and** `Debug` echo before truncating with `…`. Shorter than [`Self::MAX_LEN`]
    /// so neither ever dumps most of a near-cap-length malformed value into a log line.
    const MALFORMED_DISPLAY_LEN: usize = 64;

    /// Validates and wraps a content type string. Returns `Err` for an empty value, one over
    /// [`Self::MAX_LEN`] bytes, one containing a control character (including CR/LF — a
    /// header-injection surface), or one that is not a `type/subtype` MIME string with both
    /// halves non-empty.
    ///
    /// # Errors
    ///
    /// Returns [`ContentTypeError::Empty`], [`ContentTypeError::TooLong`], or
    /// [`ContentTypeError::Malformed`].
    pub fn parse(s: impl Into<Cow<'static, str>>) -> Result<Self, ContentTypeError> {
        let s = s.into();
        if s.is_empty() {
            return Err(ContentTypeError::Empty);
        }
        if s.len() > Self::MAX_LEN {
            return Err(ContentTypeError::TooLong {
                len: s.len(),
                max: Self::MAX_LEN,
            });
        }
        let is_valid = !contains_control_char(&s)
            && matches!(s.split_once('/'), Some((ty, subtype)) if !ty.is_empty() && !subtype.is_empty());
        if !is_valid {
            return Err(ContentTypeError::Malformed {
                value: s.into_owned(),
            });
        }
        Ok(Self(s))
    }

    /// Returns the content type as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// [`ContentType::parse`] failures.
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContentTypeError {
    /// The value was empty.
    Empty,
    /// The value exceeded [`ContentType::MAX_LEN`].
    TooLong {
        /// The value's actual length in bytes.
        len: usize,
        /// The maximum allowed length in bytes.
        max: usize,
    },
    /// The value was not a `type/subtype` MIME string.
    Malformed {
        /// The rejected value.
        value: String,
    },
}

/// Truncates `value` to [`ContentType::MALFORMED_DISPLAY_LEN`] characters, returning the shown
/// prefix and whether it was actually truncated. Shared by `Display` and `Debug` so **both**
/// never echo more of a rejected value than the other.
fn truncated_for_display(value: &str) -> (&str, bool) {
    match value.char_indices().nth(ContentType::MALFORMED_DISPLAY_LEN) {
        Some((byte_idx, _)) => (&value[..byte_idx], true),
        None => (value, false),
    }
}

impl fmt::Display for ContentTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("content type must not be empty"),
            Self::TooLong { len, max } => {
                write!(f, "content type length {len} exceeds the maximum of {max}")
            }
            // Truncated to a fixed prefix regardless of the (already `MAX_LEN`-capped) value's
            // real length, so this is the only place a malformed value is echoed and it never
            // dumps a near-256-byte string into a log line.
            Self::Malformed { value } => {
                let (shown, truncated) = truncated_for_display(value);
                write!(f, "malformed content type: {shown:?}")?;
                if truncated {
                    f.write_str("…")?;
                }
                Ok(())
            }
        }
    }
}

/// **Never derived.** A derived `Debug` on `Malformed` would print its `value` field in full,
/// undermining the same truncation `Display` applies — so `Debug` reuses the identical
/// truncation and renders it as the usual derive-shaped output.
impl fmt::Debug for ContentTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::TooLong { len, max } => f
                .debug_struct("TooLong")
                .field("len", len)
                .field("max", max)
                .finish(),
            Self::Malformed { value } => {
                let (shown, truncated) = truncated_for_display(value);
                let shown = if truncated {
                    format!("{shown}…")
                } else {
                    shown.to_string()
                };
                f.debug_struct("Malformed").field("value", &shown).finish()
            }
        }
    }
}

impl std::error::Error for ContentTypeError {}

#[cfg(feature = "serde")]
impl serde::Serialize for ContentType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ContentType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}
