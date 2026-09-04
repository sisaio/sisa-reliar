//! Body ⇄ bytes conversion (SRS §12.1, ADR 0010).

use crate::{ContentType, Message};

/// Converts a typed [`Message`] body to and from bytes. Lives in `reliar-core`: it touches
/// neither storage nor transport (ADR 0010).
///
/// Stateless and cheap; implementations SHALL NOT be placed behind a `dyn Serializer` on the
/// enqueue path (ADR 0001).
pub trait Serializer: Send + Sync {
    /// The serializer's own error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The content type this serializer produces. Populates both
    /// [`DeliveryMetadata::content_type`](crate::DeliveryMetadata::content_type) and a
    /// provider's `content_type` column — one value, chosen by the serializer, never by the
    /// call site.
    fn content_type(&self) -> &ContentType;

    /// Serializes a message body to bytes.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if `body` cannot be represented in this serializer's format.
    fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error>;

    /// Deserializes bytes back into a message body.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` if `bytes` is not a valid encoding of `T` in this serializer's
    /// format.
    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error>;
}

#[cfg(feature = "json")]
mod json {
    use core::fmt;

    use bytes::Bytes;

    use super::Serializer;
    use crate::{ContentType, Message};

    /// The default [`Serializer`]: JSON via `serde_json`. Ships behind the default `json`
    /// feature; disable it to supply a different wire format (ADR 0010).
    ///
    /// ```
    /// use reliar_core::{JsonSerializer, Serializer};
    ///
    /// #[derive(serde::Serialize, serde::Deserialize)]
    /// struct Ping;
    /// impl reliar_core::Message for Ping {
    ///     const TYPE: &'static str = "ping";
    ///     const VERSION: u16 = 1;
    /// }
    ///
    /// let serializer = JsonSerializer;
    /// let bytes = serializer.serialize(&Ping)?;
    /// let _: Ping = serializer.deserialize(&bytes)?;
    /// assert_eq!(serializer.content_type().as_str(), "application/json");
    /// # Ok::<(), reliar_core::JsonError>(())
    /// ```
    #[derive(Clone, Debug, Default)]
    pub struct JsonSerializer;

    impl Serializer for JsonSerializer {
        type Error = JsonError;

        fn content_type(&self) -> &ContentType {
            &ContentType::JSON
        }

        fn serialize<T: Message>(&self, body: &T) -> Result<Bytes, Self::Error> {
            serde_json::to_vec(body)
                .map(Bytes::from)
                .map_err(|source| JsonError::Serialize { source })
        }

        fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
            serde_json::from_slice(bytes).map_err(|source| JsonError::Deserialize { source })
        }
    }

    /// [`JsonSerializer`] failures. `Display` names the operation, the error class
    /// (`serde_json::error::Category`), and the line/column — **never `serde_json::Error`'s own
    /// message**, which for a data error embeds a fragment of the value it rejected (e.g.
    /// `invalid type: string "sk-live-…", expected u64`). The full underlying error, message
    /// included, is still reachable via [`std::error::Error::source`] for a caller that
    /// deliberately wants it — that caller's own logging is then responsible for §33.
    ///
    /// **`Debug` is a manual impl, never derived**: `serde_json::Error`'s own `Debug` embeds its
    /// `Display` message (the same payload fragment `Display` above must avoid), so deriving
    /// here would leak through `{:?}` even though `Display` is safe.
    #[non_exhaustive]
    pub enum JsonError {
        /// Serializing a body to JSON failed.
        Serialize {
            /// The underlying `serde_json` error.
            source: serde_json::Error,
        },
        /// Deserializing bytes into a body failed.
        Deserialize {
            /// The underlying `serde_json` error.
            source: serde_json::Error,
        },
    }

    /// Renders a `serde_json::Error` as its classification and position only — never its
    /// `Display`, which embeds a fragment of the offending payload for data errors.
    fn describe(source: &serde_json::Error) -> String {
        let category = match source.classify() {
            serde_json::error::Category::Io => "io",
            serde_json::error::Category::Syntax => "syntax",
            serde_json::error::Category::Data => "data",
            serde_json::error::Category::Eof => "eof",
        };
        format!(
            "{category} error at line {}, column {}",
            source.line(),
            source.column()
        )
    }

    impl fmt::Display for JsonError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Serialize { source } => {
                    write!(f, "failed to serialize to JSON: {}", describe(source))
                }
                Self::Deserialize { source } => {
                    write!(f, "failed to deserialize from JSON: {}", describe(source))
                }
            }
        }
    }

    impl fmt::Debug for JsonError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let (variant, source) = match self {
                Self::Serialize { source } => ("Serialize", source),
                Self::Deserialize { source } => ("Deserialize", source),
            };
            f.debug_struct(variant)
                .field("classification", &describe(source))
                .finish()
        }
    }

    impl std::error::Error for JsonError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Serialize { source } | Self::Deserialize { source } => Some(source),
            }
        }
    }
}

#[cfg(feature = "json")]
pub use json::{JsonError, JsonSerializer};
