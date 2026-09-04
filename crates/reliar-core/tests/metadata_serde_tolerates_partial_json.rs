//! Review 1, major 2 — `Metadata`'s serde impl must tolerate a persisted blob from *before* a
//! field/sub-struct existed (an 0.2 addition) and from *after* one is removed or renamed
//! (forward compat: unknown fields ignored, not rejected). Requires the `serde` feature — see
//! the `required-features` entry in `Cargo.toml`.

use reliar_core::Metadata;

#[test]
fn empty_object_deserializes_to_the_default() {
    let metadata: Metadata = serde_json::from_str("{}").unwrap();
    assert_eq!(metadata, Metadata::default());
}

#[test]
fn missing_a_whole_sub_struct_falls_back_to_its_default() {
    // No `routing` key at all — as if it were added in a later release.
    let json = r#"{"correlation":{},"trace":{},"delivery":{}}"#;
    let metadata: Metadata = serde_json::from_str(json).unwrap();
    assert_eq!(metadata.routing, reliar_core::RoutingMetadata::default());
}

#[test]
fn unknown_top_level_and_nested_fields_are_tolerated() {
    let json = r#"{
        "correlation": {"a_future_field": "x"},
        "trace": {},
        "routing": {},
        "delivery": {},
        "tenant_id": null,
        "a_field_from_a_newer_release": 123
    }"#;
    let metadata: Metadata = serde_json::from_str(json).unwrap();
    assert_eq!(metadata, Metadata::default());
}
