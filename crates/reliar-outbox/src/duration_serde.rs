//! `serde` (de)serialization for [`std::time::Duration`] as integer milliseconds, matching the
//! `*_ms` environment variables `from_env` reads (§7.2). Only compiled behind the `serde`
//! feature.

#![cfg(feature = "serde")]

/// `#[serde(with = "duration_serde::millis")]` on a required `Duration` field.
pub(crate) mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer, ser::Error as _};

    pub(crate) fn serialize<S: Serializer>(value: &Duration, s: S) -> Result<S::Ok, S::Error> {
        let ms = u64::try_from(value.as_millis()).map_err(S::Error::custom)?;
        s.serialize_u64(ms)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_millis(u64::deserialize(d)?))
    }
}

/// `#[serde(with = "duration_serde::optional_millis")]` on an `Option<Duration>` field.
pub(crate) mod optional_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};

    #[allow(
        clippy::ref_option,
        reason = "serde's `with` derive calls this as `&self.field`; the field is `Option<Duration>`"
    )]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<Duration>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => {
                let ms = u64::try_from(value.as_millis()).map_err(S::Error::custom)?;
                Some(ms).serialize(s)
            }
            None => None::<u64>.serialize(s),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(d)?.map(Duration::from_millis))
    }
}
