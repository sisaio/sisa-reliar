//! `reliar-core` is the pure envelope/message model every Reliar crate builds on: identity
//! newtypes, the [`Message`] contract, a validated [`Headers`] map, typed [`Metadata`], and the
//! [`Envelope`]/[`SerializedEnvelope`] pair that carries them (SRS §9–§17).
//!
//! # Guarantees
//!
//! - **Pure.** No storage or transport dependency: no sqlx, no broker client, no routing
//!   concept (a Kafka partition key, a `RabbitMQ` exchange, a NATS subject). Every other Reliar
//!   crate depends on this one; this one depends on nothing Reliar-specific (ADR 0002).
//! - **One source of truth for framework metadata.** [`Envelope::metadata`] is canonical for
//!   every concept Reliar understands. A value here is never duplicated into
//!   [`Envelope::headers`], and Reliar never reads a framework value back out of headers
//!   (ADR 0004). [`Headers`] reserves the entire `reliar-` prefix, case-insensitively, so a
//!   custom header can never collide with — or be mistaken for — a framework one.
//! - **Message identity never depends on `std::any::type_name::<T>()` or a module path.** A
//!   [`Message`]'s [`MessageType`] comes from its own `TYPE`/`VERSION` constants, so renaming or
//!   moving the Rust type is safe and two distinct types sharing them render identically
//!   (ADR 0010).
//! - **One envelope type for both sides of a serialization boundary.** [`Envelope<T>`] and
//!   [`SerializedEnvelope`] (`= Envelope<bytes::Bytes>`) are the same generic type; converting
//!   between them ([`Envelope::map_body`]) can never drop or duplicate a field (ADR 0003).
//!
//! Every public error is a hand-rolled, `#[non_exhaustive]` enum with a wired
//! [`std::error::Error::source`] — no `thiserror`, no `anyhow`. `Debug` on payload-bearing types
//! elides the bytes; no `Display` here ever prints a payload, a header value, or a credential.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod content_type;
mod envelope;
mod headers;
mod ids;
mod mapper;
mod message;
mod metadata;
mod serializer;

pub use content_type::{ContentType, ContentTypeError};
pub use envelope::{Envelope, EnvelopeBuilder, SerializedEnvelope};
pub use headers::{HeaderError, Headers};
pub use ids::{ConversationId, CorrelationId, IdError, MessageId, RequestId};
pub use mapper::EnvelopeMapper;
pub use message::{Message, MessageType};
pub use metadata::{
    CorrelationMetadata, DeliveryMetadata, EndpointAddress, Metadata, RoutingMetadata, TraceContext,
};
pub use serializer::Serializer;

#[cfg(feature = "json")]
pub use serializer::{JsonError, JsonSerializer};
