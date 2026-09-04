//! `Metadata`'s optional `serde` feature round-trips through JSON. This is a convenience for a
//! host that wants to persist or log `Metadata` itself; it is unrelated to
//! `reliar-store-postgres`'s own private `MetadataRest` JSONB contract (ADR 0012), which lives
//! in that provider crate and is proven there by a `pg`-layer property test (§43.A.4).
//!
//! Requires the `serde` feature — see the `required-features` entry in `Cargo.toml`.

use reliar_core::{CorrelationId, EndpointAddress, Metadata};
use time::OffsetDateTime;

#[test]
fn round_trips_a_fully_populated_metadata() {
    // Every relevant type is `#[non_exhaustive]`, so it is built from `Default` and mutated
    // field by field — the same pattern an application uses (ADR 0022).
    let mut metadata = Metadata::default();
    metadata.correlation.correlation_id = Some(CorrelationId::parse("checkout-1").unwrap());
    metadata.trace.traceparent = Some("00-4bf9-1-01".to_string());
    metadata.tenant_id = Some("acme".to_string());

    let json = serde_json::to_string(&metadata).unwrap();
    let round_tripped: Metadata = serde_json::from_str(&json).unwrap();

    assert_eq!(round_tripped, metadata);
}

/// Review 2, minor 7 — no existing serde test populated `delivery.sent_at`/`expires_at`, the
/// `rfc3339::option` path, or `routing`'s `EndpointAddress` fields.
#[test]
fn round_trips_sent_at_expires_at_and_routing_addresses() {
    let sent_at = OffsetDateTime::from_unix_timestamp(1_757_000_000).unwrap();
    let expires_at = OffsetDateTime::from_unix_timestamp(1_757_003_600).unwrap();

    let mut metadata = Metadata::default();
    metadata.delivery.sent_at = Some(sent_at);
    metadata.delivery.expires_at = Some(expires_at);
    metadata.routing.source = Some(EndpointAddress::parse("orders-service").unwrap());
    metadata.routing.destination = Some(EndpointAddress::parse("billing-service").unwrap());

    let json = serde_json::to_string(&metadata).unwrap();
    let round_tripped: Metadata = serde_json::from_str(&json).unwrap();

    assert_eq!(round_tripped, metadata);
    assert_eq!(round_tripped.delivery.sent_at, Some(sent_at));
    assert_eq!(round_tripped.delivery.expires_at, Some(expires_at));
}

#[test]
fn round_trips_the_default_metadata() {
    let metadata = Metadata::default();

    let json = serde_json::to_string(&metadata).unwrap();
    let round_tripped: Metadata = serde_json::from_str(&json).unwrap();

    assert_eq!(round_tripped, metadata);
}
