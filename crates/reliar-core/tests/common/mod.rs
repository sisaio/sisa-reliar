#![allow(dead_code)]
//! Shared test fixtures for `reliar-core`'s public-API tests.

use bytes::Bytes;
use reliar_core::{Envelope, Message, SerializedEnvelope};
use serde::{Deserialize, Serialize};

/// A minimal message body used across scenario files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// A second, distinct Rust type declaring the **same** `TYPE`/`VERSION` as [`OrderCreated`], so
/// tests can assert `MessageType` renders identically regardless of the Rust type behind it
/// (§43.A.3).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrderCreatedAgain {
    pub order_id: u64,
}

impl Message for OrderCreatedAgain {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// A minimal serialized envelope for tests that only need *an* envelope, not a specific body.
pub(crate) fn serialized_envelope() -> SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"))
}
