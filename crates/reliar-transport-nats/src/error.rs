//! Hand-rolled error enums for this crate (SRS §23, ADR 0026, 0027, 0028, 0030).
//!
//! No `thiserror`, no `anyhow`. Every `Display` is payload/credential-free: a header value, a
//! subject's contents beyond routing, and the `async-nats` error's own `Display` (which can carry
//! a credentialed server URL) are never printed (§17.1, ADR 0030 Amendment B). Classification is
//! wired only where a value crosses [`Publisher`](reliar_core::Publisher)'s `Classify` boundary —
//! [`NatsPublishError`] implements it directly; [`NatsMapError`] and [`SubjectError`] are always
//! permanent by construction and are folded into `NatsPublishError`'s own table instead of
//! duplicating it.

use core::fmt;

use async_nats::Subject;
use reliar_core::{Classify, FailureKind, HeaderError};

/// Why an envelope could not be expressed as a NATS message, or a NATS message as an envelope.
/// Every variant is **permanent** (ADR 0030): the same bytes fail identically on every retry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NatsMapError {
    /// A required framework header was absent (§2.5).
    MissingHeader {
        /// The absent header's name.
        header: &'static str,
    },
    /// A framework header was present but could not be parsed (bad UUID, bad RFC 3339, a
    /// `content_type` `ContentType::parse` rejects, …). The **value is never included** (§17.1).
    MalformedHeader {
        /// The header whose value could not be parsed.
        header: &'static str,
    },
    /// A framework header appeared more than once, or under two casings (§2.5).
    DuplicateHeader {
        /// The header that appeared more than once.
        header: &'static str,
    },
    /// A custom header key NATS cannot express: not ASCII-graphic, or containing `:` (§2.4).
    UnsupportedHeaderName {
        /// The rejected key.
        key: String,
    },
    /// A custom header key inside NATS's reserved `Nats-` namespace (§2.4).
    ReservedHeaderName {
        /// The rejected key.
        key: String,
    },
    /// A value carried `\r` or `\n` — the header-injection surface core's unvalidated `String`
    /// fields (`MessageType::name`, `tenant_id`, `traceparent`, `tracestate`, …) leave open
    /// (§2.4). Names the header — a `headers::*` constant for a framework field, the caller's key
    /// for a custom one — **never** the value (ADR 0026 Amendment B).
    InvalidHeaderValue {
        /// The header whose value was rejected.
        header: String,
    },
    /// `Headers::insert` refused a decoded custom header (over-length, over-count).
    RejectedHeader {
        /// The rejected key.
        key: String,
        /// Why [`Headers`](reliar_core::Headers)`::insert` refused it.
        source: HeaderError,
    },
}

impl fmt::Display for NatsMapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHeader { header } => write!(f, "missing required header {header:?}"),
            Self::MalformedHeader { header } => {
                write!(f, "header {header:?} could not be parsed")
            }
            Self::DuplicateHeader { header } => {
                write!(f, "header {header:?} appeared more than once")
            }
            Self::UnsupportedHeaderName { key } => {
                write!(f, "header key {key:?} is not a legal NATS header name")
            }
            Self::ReservedHeaderName { key } => {
                write!(f, "header key {key:?} uses the reserved `Nats-` prefix")
            }
            Self::InvalidHeaderValue { header } => write!(
                f,
                "the value for header {header:?} is not a legal NATS header value"
            ),
            Self::RejectedHeader { key, .. } => {
                write!(f, "custom header {key:?} was rejected")
            }
        }
    }
}

impl std::error::Error for NatsMapError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RejectedHeader { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Why an envelope could not be turned into a legal NATS subject.
/// Always **permanent** — the same envelope resolves the same way every time. A subject is
/// routing configuration, not user data, so including it in [`Display`](fmt::Display) is
/// intended: it is what makes a dead row actionable (ADR 0030).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubjectError {
    /// The resolved subject, or the configured prefix, was empty.
    Empty,
    /// A token between two `.`s was empty (`a..b`, `.a`, `a.`).
    EmptyToken {
        /// The rejected subject.
        subject: String,
    },
    /// A wildcard (`*` or `>`) would publish to a subject the caller did not choose.
    Wildcard {
        /// The rejected subject.
        subject: String,
    },
    /// Whitespace, a control character, or a non-printable-ASCII byte.
    IllegalCharacter {
        /// The rejected subject.
        subject: String,
    },
    /// Over [`SubjectError::MAX_LEN`] bytes.
    TooLong {
        /// The subject's actual length in bytes.
        len: usize,
        /// The maximum allowed length in bytes.
        limit: usize,
    },
}

impl SubjectError {
    /// The maximum legal subject length in bytes (§3.1).
    pub const MAX_LEN: usize = 255;
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("subject must not be empty"),
            Self::EmptyToken { subject } => {
                write!(f, "subject {subject:?} has an empty token between two `.`s")
            }
            Self::Wildcard { subject } => {
                write!(f, "subject {subject:?} contains a wildcard (`*` or `>`)")
            }
            Self::IllegalCharacter { subject } => write!(
                f,
                "subject {subject:?} contains whitespace, a control character, or a non-ASCII byte"
            ),
            Self::TooLong { len, limit } => {
                write!(f, "subject length {len} exceeds the maximum of {limit}")
            }
        }
    }
}

impl std::error::Error for SubjectError {}

/// Why a publish failed. Every variant's [`Classify`] verdict is fixed and asserted by test
/// (ADR 0030). No [`Display`](fmt::Display) here prints payload bytes, a header value, or a
/// server address (§17.1).
#[derive(Debug)]
#[non_exhaustive]
pub enum NatsPublishError {
    /// The envelope could not be expressed as a NATS message (ADR 0026 §3).
    Map(NatsMapError),
    /// The [`SubjectResolver`](crate::SubjectResolver) rejected the envelope (ADR 0027).
    Subject {
        /// Why the resolver rejected it.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The pre-flight [`NatsSettings::max_payload`](crate::NatsSettings::max_payload) guard
    /// rejected the message before any I/O.
    PayloadTooLarge {
        /// The wire length ([`NatsWireMessage::wire_len`](crate::NatsWireMessage::wire_len))
        /// that was rejected.
        len: usize,
        /// The configured [`NatsSettings::max_payload`](crate::NatsSettings::max_payload) limit.
        limit: usize,
    },
    /// The server's own `max_payload` rejected the message
    /// (`PublishErrorKind::MaxPayloadExceeded`).
    MaxPayloadExceeded {
        /// The subject the message was published to.
        subject: Subject,
    },
    /// A publish precondition failed (`WrongLastMessageId`/`WrongLastSequence`). Unreachable by
    /// construction today — this publisher never sets a last-message expectation — and mapped
    /// rather than folded into [`Self::Broker`] so a future precondition failure is never hidden
    /// (ADR 0030).
    WrongLastMessage {
        /// The subject the message was published to.
        subject: Subject,
    },
    /// The publish did not ack within
    /// [`NatsSettings::publish_timeout`](crate::NatsSettings::publish_timeout) (or the host
    /// `Context`'s own, possibly shorter, timeout — contract §4.1), or the server itself reported
    /// `PublishErrorKind::TimedOut`.
    Timeout {
        /// The subject the message was published to.
        subject: Subject,
        /// The **measured** elapsed time, in milliseconds — never the configured setting
        /// (ADR 0028 Amendment A, review m1).
        after_ms: u64,
    },
    /// The connection failed while sending (`PublishErrorKind::BrokenPipe`).
    Connection {
        /// The subject the message was published to.
        subject: Subject,
    },
    /// No stream captures `subject` (ADR 0029 §3) — a provisioning gap, not a payload problem.
    StreamNotFound {
        /// The subject the message was published to.
        subject: Subject,
    },
    /// The server is applying publish back-pressure (`PublishErrorKind::MaxAckPending`).
    MaxAckPending {
        /// The subject the message was published to.
        subject: Subject,
    },
    /// `PublishErrorKind::Other`, or any broker-reported kind this crate does not otherwise map.
    /// Logged once at `warn` with the subject and a bounded kind name (`"other"` or
    /// `"unrecognised"`) only — **never** the `async-nats` error's own `Display`, which can carry
    /// a credentialed server URL and is neither logged nor persisted anywhere in this crate
    /// (ADR 0030 Amendment B, §17.1).
    Broker {
        /// The subject the message was published to.
        subject: Subject,
    },
}

impl fmt::Display for NatsPublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Map(err) => write!(f, "envelope could not be mapped to a NATS message: {err}"),
            Self::Subject { source } => write!(f, "subject could not be resolved: {source}"),
            Self::PayloadTooLarge { len, limit } => write!(
                f,
                "message of {len} bytes exceeds the configured max_payload of {limit} bytes"
            ),
            Self::MaxPayloadExceeded { subject } => {
                write!(
                    f,
                    "the server rejected the message to {subject} as too large"
                )
            }
            Self::WrongLastMessage { subject } => {
                write!(f, "publish to {subject} failed a last-message precondition")
            }
            Self::Timeout { subject, after_ms } => {
                write!(f, "publish to {subject} did not ack within {after_ms}ms")
            }
            Self::Connection { subject } => write!(
                f,
                "no connection to the NATS server while publishing to {subject}"
            ),
            Self::StreamNotFound { subject } => {
                write!(f, "no stream is bound to subject {subject}")
            }
            Self::MaxAckPending { subject } => {
                write!(
                    f,
                    "the server is applying publish back-pressure for {subject}"
                )
            }
            Self::Broker { subject } => {
                write!(f, "the NATS server rejected the publish to {subject}")
            }
        }
    }
}

impl std::error::Error for NatsPublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Map(err) => Some(err),
            Self::Subject { source } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl Classify for NatsPublishError {
    fn kind(&self) -> FailureKind {
        match self {
            Self::Map(_)
            | Self::Subject { .. }
            | Self::PayloadTooLarge { .. }
            | Self::MaxPayloadExceeded { .. }
            | Self::WrongLastMessage { .. } => FailureKind::Permanent,
            Self::Timeout { .. }
            | Self::Connection { .. }
            | Self::StreamNotFound { .. }
            | Self::MaxAckPending { .. }
            | Self::Broker { .. } => FailureKind::Transient,
        }
    }
}

/// A publisher configuration that cannot be started — **never a panic**, mirroring
/// `reliar-outbox`'s `ConfigError` role for the dispatcher (a code span, not a link: this crate
/// no longer depends on that crate — ADR 0032).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NatsConfigError {
    /// `batch_pipeline_depth` was zero: `publish_batch` could never send anything.
    ZeroBatchPipelineDepth,
    /// `publish_timeout` was zero: every publish would time out immediately.
    ZeroPublishTimeout,
    /// `max_payload` was `Some(0)`: **every** message — including one with an empty body —
    /// exceeds a zero limit, so the pre-flight guard (§4.2) would turn the whole outbox into dead
    /// rows without a single byte leaving the process. The one payload limit that is unusable for
    /// every possible envelope is therefore a construction error, not a runtime verdict
    /// (ADR 0030 Amendment A).
    ZeroMaxPayload,
    /// `subject_prefix` is not a legal subject prefix.
    Subject(SubjectError),
}

impl fmt::Display for NatsConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBatchPipelineDepth => {
                f.write_str("batch_pipeline_depth must be greater than zero")
            }
            Self::ZeroPublishTimeout => f.write_str("publish_timeout must be greater than zero"),
            Self::ZeroMaxPayload => {
                f.write_str("max_payload must not be Some(0) — every message would be rejected")
            }
            Self::Subject(err) => write!(f, "subject_prefix is not a legal subject: {err}"),
        }
    }
}

impl std::error::Error for NatsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Subject(err) => Some(err),
            Self::ZeroBatchPipelineDepth | Self::ZeroPublishTimeout | Self::ZeroMaxPayload => None,
        }
    }
}

impl From<SubjectError> for NatsConfigError {
    fn from(err: SubjectError) -> Self {
        Self::Subject(err)
    }
}
