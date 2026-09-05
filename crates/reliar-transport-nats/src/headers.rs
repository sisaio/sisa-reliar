//! The `reliar-*` header names this transport writes — SRS §14's closed list, in the projection
//! table's order (contract §2.3). Public so tests and a future Phase 3 decoder never spell one by
//! hand.

/// `envelope.id` — always written.
pub const MESSAGE_ID: &str = "reliar-message-id";
/// `envelope.message_type.name()` — always written.
pub const MESSAGE_TYPE: &str = "reliar-message-type";
/// `envelope.message_type.version()` — always written.
pub const MESSAGE_VERSION: &str = "reliar-message-version";
/// `metadata.delivery.content_type` — always written.
pub const CONTENT_TYPE: &str = "reliar-content-type";
/// `metadata.correlation.correlation_id` — written when `Some`.
pub const CORRELATION_ID: &str = "reliar-correlation-id";
/// `metadata.correlation.conversation_id` — written when not
/// `reliar_core::ConversationId::UNSET`.
pub const CONVERSATION_ID: &str = "reliar-conversation-id";
/// `metadata.correlation.causation_id` — written when `Some`.
pub const CAUSATION_ID: &str = "reliar-causation-id";
/// `metadata.correlation.request_id` — written when `Some`.
pub const REQUEST_ID: &str = "reliar-request-id";
/// `metadata.tenant_id` — written when `Some`.
pub const TENANT_ID: &str = "reliar-tenant-id";
/// `metadata.delivery.sent_at`, RFC 3339 UTC — written when `Some`.
pub const SENT_AT: &str = "reliar-sent-at";
/// `metadata.delivery.expires_at`, RFC 3339 UTC — written when `Some`.
pub const EXPIRES_AT: &str = "reliar-expires-at";
/// `metadata.routing.source` — written when `Some`.
pub const SOURCE: &str = "reliar-source";
/// `metadata.routing.destination` — written when `Some`.
pub const DESTINATION: &str = "reliar-destination";
/// `metadata.routing.reply_to` — written when `Some`.
pub const REPLY_TO: &str = "reliar-reply-to";
/// W3C Trace Context — deliberately **not** `reliar-` prefixed (SRS §14).
pub const TRACEPARENT: &str = "traceparent";
/// W3C Trace Context — deliberately **not** `reliar-` prefixed (SRS §14).
pub const TRACESTATE: &str = "tracestate";
/// `JetStream`'s duplicate-suppression key (SRS §12.3). Unlike every other constant here this is
/// **not** lowercase: it must match the exact casing the NATS server and `async-nats`'s own
/// standard-header table expect.
pub const NATS_MSG_ID: &str = "Nats-Msg-Id";
/// The prefix [`reliar_core::Headers`] reserves, and the one `decode` skips on an otherwise
/// unrecognised header (contract §2.5).
pub const RELIAR_PREFIX: &str = "reliar-";
/// NATS's own reserved prefix: rejected on `encode`, skipped on `decode` (contract §2.4, §2.5).
pub const NATS_PREFIX: &str = "Nats-";
