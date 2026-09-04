//! Review 1, minor 7 — untested `Empty`/`TooLong` edges of the capped string identity newtypes,
//! plus `ContentType::parse`'s `Empty` case.

use reliar_core::{ContentType, ContentTypeError, CorrelationId, EndpointAddress, IdError};

#[test]
fn correlation_id_rejects_empty() {
    assert!(matches!(CorrelationId::parse(""), Err(IdError::Empty)));
}

#[test]
fn correlation_id_rejects_over_the_length_cap() {
    let overlong = "x".repeat(CorrelationId::MAX_LEN + 1);
    assert!(matches!(
        CorrelationId::parse(overlong),
        Err(IdError::TooLong { len, max }) if len == CorrelationId::MAX_LEN + 1 && max == CorrelationId::MAX_LEN
    ));
}

#[test]
fn correlation_id_accepts_exactly_the_length_cap() {
    let at_cap = "x".repeat(CorrelationId::MAX_LEN);
    assert!(CorrelationId::parse(at_cap).is_ok());
}

#[test]
fn endpoint_address_rejects_empty() {
    assert!(matches!(EndpointAddress::parse(""), Err(IdError::Empty)));
}

#[test]
fn endpoint_address_rejects_over_the_length_cap() {
    let overlong = "x".repeat(EndpointAddress::MAX_LEN + 1);
    assert!(matches!(
        EndpointAddress::parse(overlong),
        Err(IdError::TooLong { len, max })
            if len == EndpointAddress::MAX_LEN + 1 && max == EndpointAddress::MAX_LEN
    ));
}

#[test]
fn content_type_rejects_empty() {
    assert!(matches!(
        ContentType::parse(""),
        Err(ContentTypeError::Empty)
    ));
}

#[test]
fn content_type_rejects_over_the_length_cap() {
    let overlong = "x".repeat(ContentType::MAX_LEN + 1);
    assert!(matches!(
        ContentType::parse(overlong),
        Err(ContentTypeError::TooLong { len, max })
            if len == ContentType::MAX_LEN + 1 && max == ContentType::MAX_LEN
    ));
}

#[test]
fn content_type_accepts_exactly_the_length_cap() {
    // A valid `type/subtype` string of exactly `MAX_LEN` bytes.
    let subtype = "x".repeat(ContentType::MAX_LEN - "type/".len());
    let at_cap = format!("type/{subtype}");
    assert_eq!(at_cap.len(), ContentType::MAX_LEN);
    assert!(ContentType::parse(at_cap).is_ok());
}

/// Review 2, minor 2 — `ContentTypeError::Malformed`'s `Display` truncates a long rejected value
/// rather than echoing it in full.
#[test]
fn malformed_display_truncates_a_long_rejected_value() {
    // No `/` at all, so this is `Malformed`, not `TooLong` (well under `MAX_LEN`).
    let no_slash = "x".repeat(200);
    let error = ContentType::parse(no_slash).unwrap_err();

    let rendered = error.to_string();
    assert!(
        rendered.contains(&"x".repeat(64)),
        "expected the 64-char shown prefix: {rendered}"
    );
    assert!(
        !rendered.contains(&"x".repeat(65)),
        "must not echo more than the truncation length: {rendered}"
    );
    assert!(
        rendered.ends_with('…'),
        "expected the truncation marker: {rendered}"
    );
}

/// Review 2, minor 3 — `ContentTypeError`'s `Debug` must apply the same truncation as `Display`,
/// not derive-print the rejected value in full.
#[test]
fn malformed_debug_truncates_the_same_as_display() {
    let no_slash = "y".repeat(200);
    let error = ContentType::parse(no_slash).unwrap_err();

    let rendered = format!("{error:?}");
    assert!(
        rendered.contains(&"y".repeat(64)),
        "expected the 64-char shown prefix: {rendered}"
    );
    assert!(
        !rendered.contains(&"y".repeat(65)),
        "Debug must not echo more than the truncation length: {rendered}"
    );
    assert!(
        rendered.contains('…'),
        "expected the truncation marker: {rendered}"
    );
}

#[test]
fn non_malformed_variants_debug_normally() {
    assert_eq!(format!("{:?}", ContentTypeError::Empty), "Empty");
    let rendered = format!(
        "{:?}",
        ContentTypeError::TooLong {
            len: 300,
            max: ContentType::MAX_LEN
        }
    );
    assert!(rendered.contains("300"));
    assert!(rendered.contains(&ContentType::MAX_LEN.to_string()));
}

/// Caps are enforced in **bytes**, not `char`s: a multi-byte UTF-8 string can breach a byte cap
/// while its character count stays well under it.
#[test]
fn correlation_id_cap_is_enforced_in_bytes_not_characters() {
    // 129 two-byte characters = 258 bytes: over `MAX_LEN` (256) in bytes, but only 129 in chars.
    let multi_byte = "é".repeat(129);
    assert_eq!(multi_byte.chars().count(), 129);
    assert!(multi_byte.len() > CorrelationId::MAX_LEN);

    assert!(matches!(
        CorrelationId::parse(multi_byte),
        Err(IdError::TooLong { .. })
    ));
}
