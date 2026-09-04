//! Review 2, minor 8 — untested surface: `ConversationId::UNSET`/`is_unset`, `MessageType::new`,
//! and `EndpointAddress`'s `Display` impl.

use reliar_core::{ConversationId, EndpointAddress, MessageType};

#[test]
fn unset_is_the_nil_uuid_and_reports_itself_as_unset() {
    assert!(ConversationId::UNSET.is_unset());
    assert_eq!(ConversationId::UNSET.as_uuid(), uuid::Uuid::nil());
}

#[test]
fn a_freshly_minted_conversation_id_is_never_unset() {
    assert!(!ConversationId::new().is_unset());
    assert!(!ConversationId::default().is_unset());
}

#[test]
fn message_type_new_builds_from_a_static_name_and_version() {
    let message_type = MessageType::new("orders.created", 1);

    assert_eq!(message_type.name(), "orders.created");
    assert_eq!(message_type.version(), 1);
    assert_eq!(message_type.to_string(), "orders.created.v1");
}

#[test]
fn endpoint_address_display_renders_the_raw_string() {
    let address = EndpointAddress::parse("orders-service").unwrap();
    assert_eq!(address.to_string(), "orders-service");
    assert_eq!(address.as_str(), "orders-service");
}
