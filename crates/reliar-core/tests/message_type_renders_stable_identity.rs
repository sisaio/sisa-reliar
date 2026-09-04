//! §43.A.3 — `MessageType` renders `<name>.v<version>` from `Message::TYPE`/`VERSION` and never
//! from `type_name::<T>()`; two distinct Rust types with the same `TYPE`/`VERSION` render
//! identically (§10.1, ADR 0010).

mod common;

use common::{OrderCreated, OrderCreatedAgain};
use reliar_core::MessageType;

#[test]
fn renders_name_dot_v_version() {
    let message_type = MessageType::of::<OrderCreated>();

    assert_eq!(message_type.to_string(), "orders.created.v1");
    assert_eq!(message_type.name(), "orders.created");
    assert_eq!(message_type.version(), 1);
}

#[test]
fn distinct_types_sharing_type_and_version_render_identically() {
    let a = MessageType::of::<OrderCreated>();
    let b = MessageType::of::<OrderCreatedAgain>();

    // If this were derived from `std::any::type_name::<T>()`, `a` and `b` would differ because
    // the Rust types are distinct — that is exactly the failure mode ADR 0010 rules out.
    assert_eq!(a, b);
    assert_eq!(a.to_string(), b.to_string());
}

#[test]
fn from_parts_round_trips_the_rendered_form() {
    let message_type = MessageType::from_parts("orders.created".to_string(), 1);

    assert_eq!(message_type, MessageType::of::<OrderCreated>());
    assert_eq!(message_type.to_string(), "orders.created.v1");
}
