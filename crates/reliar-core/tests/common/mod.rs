#![allow(dead_code)]
//! Shared test fixtures for `reliar-core`'s public-API tests.

use reliar_core::Message;
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
