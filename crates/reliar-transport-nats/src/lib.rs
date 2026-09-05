// The crate doc is the crate README, verbatim — one source of truth instead of two documents
// that drift apart, and it makes the README's own example a real doctest `cargo test --doc` runs
// (review n1). Its `# Ok::<…>` hidden-line syntax only takes effect once rustdoc processes this
// file as a doctest; a plain-Markdown renderer (GitHub, crates.io) shows the same code fenced and
// ignores the doc-comment mechanics entirely.
#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod mapper;
mod publisher;
mod settings;
mod subject;

pub mod headers;

pub use error::{NatsConfigError, NatsMapError, NatsPublishError, SubjectError};
pub use mapper::{NatsEnvelopeMapper, NatsWireMessage};
pub use publisher::NatsPublisher;
/// Reused rather than duplicated: a host configuring Reliar from the environment matches one
/// error type, not two, whether it is calling `NatsSettings::from_env` or an outbox provider's
/// own `from_env` (ADR 0032, contract §4.1 "Decided here" #10).
pub use reliar_core::SettingsError;
pub use settings::NatsSettings;
pub use subject::{DestinationSubjects, PrefixSubjects, SubjectResolver};
