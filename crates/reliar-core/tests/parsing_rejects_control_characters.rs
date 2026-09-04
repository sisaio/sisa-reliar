//! Review 1, major 4 — every capped string identity newtype rejects a control character
//! (including CR/LF, a header-injection surface once a mapper writes the value onto the wire),
//! and `ContentType::parse` additionally requires a non-empty `type` and `subtype`.

use reliar_core::{ContentType, ContentTypeError, CorrelationId, EndpointAddress, IdError};

#[test]
fn correlation_id_rejects_a_bare_control_character() {
    assert!(matches!(
        CorrelationId::parse("checkout-\u{0007}-42"),
        Err(IdError::ControlCharacter)
    ));
}

#[test]
fn correlation_id_rejects_embedded_crlf() {
    assert!(matches!(
        CorrelationId::parse("checkout-42\r\nX-Injected: true"),
        Err(IdError::ControlCharacter)
    ));
}

#[test]
fn endpoint_address_rejects_embedded_crlf() {
    assert!(matches!(
        EndpointAddress::parse("orders\r\nX-Injected: true"),
        Err(IdError::ControlCharacter)
    ));
}

#[test]
fn endpoint_address_rejects_a_bare_control_character() {
    assert!(matches!(
        EndpointAddress::parse("orders\u{0007}service"),
        Err(IdError::ControlCharacter)
    ));
}

#[test]
fn content_type_rejects_embedded_crlf() {
    assert!(matches!(
        ContentType::parse("application/json\r\nX-Injected: true"),
        Err(ContentTypeError::Malformed { .. })
    ));
}

#[test]
fn content_type_requires_a_non_empty_type_and_subtype() {
    assert!(matches!(
        ContentType::parse("application/"),
        Err(ContentTypeError::Malformed { .. })
    ));
    assert!(matches!(
        ContentType::parse("/json"),
        Err(ContentTypeError::Malformed { .. })
    ));
    assert!(matches!(
        ContentType::parse("application-json"),
        Err(ContentTypeError::Malformed { .. })
    ));
}
