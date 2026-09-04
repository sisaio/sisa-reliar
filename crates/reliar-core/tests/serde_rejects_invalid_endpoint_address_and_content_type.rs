//! Review 2, minor 4 — `EndpointAddress` and `ContentType`'s hand-rolled `Deserialize` impls
//! reuse their own `parse`, so a value rehydrated from a persisted blob is validated exactly like
//! one built directly, rather than trusting the wire verbatim. Requires the `serde` feature.

use reliar_core::{ContentType, EndpointAddress};

#[test]
fn endpoint_address_deserialize_rejects_an_overlong_value() {
    let overlong = "x".repeat(EndpointAddress::MAX_LEN + 1);
    let json = serde_json::to_string(&overlong).unwrap();

    let result: Result<EndpointAddress, _> = serde_json::from_str(&json);
    assert!(result.is_err(), "an overlong address must not deserialize");
}

#[test]
fn endpoint_address_deserialize_rejects_embedded_crlf() {
    let json = serde_json::to_string("orders\r\nX-Injected: true").unwrap();

    let result: Result<EndpointAddress, _> = serde_json::from_str(&json);
    assert!(
        result.is_err(),
        "an address containing CR/LF must not deserialize"
    );
}

#[test]
fn endpoint_address_deserialize_accepts_a_valid_value() {
    let json = serde_json::to_string("orders-service").unwrap();

    let parsed: EndpointAddress = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.as_str(), "orders-service");
}

#[test]
fn content_type_deserialize_rejects_an_overlong_value() {
    let overlong = "x".repeat(ContentType::MAX_LEN + 1);
    let json = serde_json::to_string(&overlong).unwrap();

    let result: Result<ContentType, _> = serde_json::from_str(&json);
    assert!(
        result.is_err(),
        "an overlong content type must not deserialize"
    );
}

#[test]
fn content_type_deserialize_rejects_embedded_crlf() {
    let json = serde_json::to_string("application/json\r\nX-Injected: true").unwrap();

    let result: Result<ContentType, _> = serde_json::from_str(&json);
    assert!(
        result.is_err(),
        "a content type containing CR/LF must not deserialize"
    );
}

#[test]
fn content_type_deserialize_accepts_a_valid_value() {
    let json = serde_json::to_string("application/json").unwrap();

    let parsed: ContentType = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.as_str(), "application/json");
}
