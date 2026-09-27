//! Inspect a command's outer keys without buffering or parsing its payloads.
//! Variant decoding remains authoritative, including malformed mixed wrappers.
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

#[derive(Clone, Copy, Default)]
pub(super) struct Fields(u16);

impl Fields {
    pub(super) const PARTITION: u16 = 1;
    pub(super) const CONTROL: u16 = 1 << 3;
    pub(super) const RETENTION: u16 = 1 << 4;
    pub(super) const BATCH: u16 = 1 << 5;
    pub(super) const SCOPED: u16 = Self::PARTITION | (1 << 1) | (1 << 2);
    pub(super) const FENCED: u16 = (1 << 6) | (1 << 7);
    pub(super) const RECOVERY: u16 = 1 << 8;
    const ALL: Self = Self(u16::MAX);

    pub(super) fn inspect(raw: &serde_json::value::RawValue) -> Self {
        serde_json::from_str(raw.get()).unwrap_or(Self::ALL)
    }

    pub(super) fn has(self, fields: u16) -> bool {
        self.0 & fields == fields
    }
}

struct Field(u16);

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Key;
        impl Visitor<'_> for Key {
            type Value = Field;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a command field")
            }

            fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Field, E> {
                Ok(Field(match key {
                    "partition" => Fields::PARTITION,
                    "epoch" => 1 << 1,
                    "command" => 1 << 2,
                    "partition_control" => Fields::CONTROL,
                    "retention" => Fields::RETENTION,
                    "batch" => Fields::BATCH,
                    "leader_id" => 1 << 6,
                    "commit" => 1 << 7,
                    "recovery_barrier" => Fields::RECOVERY,
                    _ => 0,
                }))
            }
        }
        deserializer.deserialize_identifier(Key)
    }
}

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Keys;
        impl<'de> Visitor<'de> for Keys {
            type Value = Fields;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a command object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Fields, A::Error> {
                let mut fields = Fields::default();
                while let Some(Field(field)) = map.next_key()? {
                    fields.0 |= field;
                    // serde_json's iterative skip preserves independent payload
                    // depth limits without constructing strings, arrays or maps.
                    // Actual variant/per-value decoders still validate the data.
                    map.next_value::<IgnoredAny>()?;
                }
                Ok(fields)
            }
        }
        deserializer.deserialize_map(Keys)
    }
}
