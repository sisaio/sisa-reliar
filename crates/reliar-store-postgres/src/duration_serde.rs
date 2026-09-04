//! `serde` (de)serialization for [`std::time::Duration`] as integer milliseconds, matching the
//! `*_MS` environment variables `PostgresOutboxSettings::from_env` reads (§7.2). Only compiled
//! behind the `serde` feature. Mirrors `reliar-outbox`'s private module of the same name so both
//! crates' settings serialize durations identically.

#![cfg(feature = "serde")]

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer, ser::Error as _};

pub(crate) fn serialize<S: Serializer>(value: &Duration, s: S) -> Result<S::Ok, S::Error> {
    let ms = u64::try_from(value.as_millis()).map_err(S::Error::custom)?;
    s.serialize_u64(ms)
}

pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    Ok(Duration::from_millis(u64::deserialize(d)?))
}
