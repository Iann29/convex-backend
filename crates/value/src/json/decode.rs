//! One-pass decoder from internal JSON text to [`ConvexValue`].
//!
//! This is observably equivalent to parsing into a `serde_json::Value` and
//! then converting with `impl TryFrom<JsonValue> for ConvexValue`, but skips
//! the intermediate tree (and its `IndexMap` per object).
//!
//! Two properties of the two-step path shape this decoder:
//!
//! 1) `serde_json::Value` keeps the *last* value of a repeated object key, and
//!    the conversion only validates what survives. A value that fails Convex
//!    validation can therefore be rescued by a later duplicate key. Syntax
//!    errors abort parsing immediately (serde errors), while Convex-level
//!    errors are carried as [`Decoded`] values until the enclosing object knows
//!    whether they were overwritten.
//! 2) With serde_json's `raw_value` feature, an object whose first key is the
//!    private raw-value token is replaced by the JSON parsed from its (string)
//!    value, with a fresh recursion limit. This is mirrored exactly.

use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt,
    num::FpCategory,
};

use anyhow::anyhow;
use serde::de::{
    DeserializeSeed,
    Deserializer,
    Error as _,
    MapAccess,
    SeqAccess,
    Visitor,
};

use crate::{
    json::{
        bytes::JsonBytes,
        float::JsonFloat,
        integer::JsonInteger,
    },
    numeric::is_negative_zero,
    ConvexArray,
    ConvexObject,
    ConvexValue,
    FieldName,
};

/// Convex-level outcome of decoding one syntactically valid JSON value.
type Decoded = anyhow::Result<ConvexValue>;

/// `serde_json::raw::TOKEN`: the first-key marker `serde_json::Value` treats
/// as an embedded raw value when the `raw_value` feature is enabled.
const RAW_VALUE_TOKEN: &str = "$serde_json::private::RawValue";

pub(super) fn decode_str(s: &str) -> anyhow::Result<ConvexValue> {
    decode_str_with(s, ValueSeed)?
}

pub(super) fn decode_slice(s: &[u8]) -> anyhow::Result<ConvexValue> {
    let mut de = serde_json::Deserializer::from_slice(s);
    let decoded = ValueSeed.deserialize(&mut de)?;
    de.end()?;
    decoded
}

/// Equivalent of `serde_json::from_str` with a seed: default recursion limit,
/// only trailing whitespace allowed.
fn decode_str_with<'a, S>(s: &'a str, seed: S) -> Result<Decoded, serde_json::Error>
where
    S: DeserializeSeed<'a, Value = Decoded>,
{
    let mut de = serde_json::Deserializer::from_str(s);
    let decoded = seed.deserialize(&mut de)?;
    de.end()?;
    Ok(decoded)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Special {
    Bytes,
    Integer,
    Float,
}

impl Special {
    fn key(self) -> &'static str {
        match self {
            Special::Bytes => "$bytes",
            Special::Integer => "$integer",
            Special::Float => "$float",
        }
    }

    fn decode(self, s: &str) -> Decoded {
        match self {
            Special::Bytes => JsonBytes::decode_str(s).map(ConvexValue::Bytes),
            Special::Integer => JsonInteger::decode_str(s).map(ConvexValue::from),
            Special::Float => {
                let n = JsonFloat::decode_str(s)?;
                // Float64s encoded as a $float object must not fit into a regular
                // `number`.
                if !is_negative_zero(n)
                    && let FpCategory::Normal | FpCategory::Subnormal = n.classify()
                {
                    anyhow::bail!("Float64 {} should be encoded as a number", n);
                }
                Ok(ConvexValue::from(n))
            },
        }
    }

    fn not_a_string(self) -> anyhow::Error {
        anyhow!("{} value must be a string", self.key())
    }
}

fn reserved_key(key: &str) -> anyhow::Error {
    anyhow!("Field name {key} starts with '$', which is reserved.")
}

/// Classification of the first key of an object.
enum FirstKey {
    RawValue,
    Special(Special),
    Field(anyhow::Result<FieldName>),
}

impl FirstKey {
    fn classify(s: &str) -> Self {
        match s {
            RAW_VALUE_TOKEN => FirstKey::RawValue,
            "$bytes" => FirstKey::Special(Special::Bytes),
            "$integer" => FirstKey::Special(Special::Integer),
            "$float" => FirstKey::Special(Special::Float),
            _ => FirstKey::Field(s.parse()),
        }
    }
}

struct FirstKeySeed;

impl<'de> DeserializeSeed<'de> for FirstKeySeed {
    type Value = FirstKey;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<FirstKey, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for FirstKeySeed {
    type Value = FirstKey;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<FirstKey, E> {
        Ok(FirstKey::classify(s))
    }
}

/// Any key after the first one: a plain field name (every `$`-prefixed key,
/// special or not, is an invalid field name there).
struct FieldKeySeed;

impl<'de> DeserializeSeed<'de> for FieldKeySeed {
    type Value = anyhow::Result<FieldName>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for FieldKeySeed {
    type Value = anyhow::Result<FieldName>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
        Ok(s.parse())
    }
}

/// The string payload of a raw-value object (`serde_json::raw::BoxedFromString`
/// uses `deserialize_str` too).
struct RawStrSeed;

impl<'de> DeserializeSeed<'de> for RawStrSeed {
    type Value = Cow<'de, str>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for RawStrSeed {
    type Value = Cow<'de, str>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("raw value")
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, s: &'de str) -> Result<Self::Value, E> {
        Ok(Cow::Borrowed(s))
    }

    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
        Ok(Cow::Owned(s.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Self::Value, E> {
        Ok(Cow::Owned(s))
    }
}

/// Decodes any JSON value the way `serde_json::Value` + `TryFrom` would.
#[derive(Clone, Copy)]
struct ValueSeed;

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = Decoded;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Decoded, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for ValueSeed {
    type Value = Decoded;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, b: bool) -> Result<Decoded, E> {
        Ok(Ok(ConvexValue::from(b)))
    }

    fn visit_i64<E>(self, n: i64) -> Result<Decoded, E> {
        Ok(Ok(ConvexValue::from(n as f64)))
    }

    fn visit_u64<E>(self, n: u64) -> Result<Decoded, E> {
        Ok(Ok(ConvexValue::from(n as f64)))
    }

    fn visit_f64<E>(self, n: f64) -> Result<Decoded, E> {
        // `serde_json::Number::from_f64` rejects non-finite values and
        // `serde_json::Value` turns them into `Null`.
        Ok(Ok(if n.is_finite() {
            ConvexValue::from(n)
        } else {
            ConvexValue::Null
        }))
    }

    fn visit_str<E>(self, s: &str) -> Result<Decoded, E> {
        Ok(ConvexValue::try_from(s.to_owned()))
    }

    fn visit_string<E>(self, s: String) -> Result<Decoded, E> {
        Ok(ConvexValue::try_from(s))
    }

    fn visit_unit<E>(self) -> Result<Decoded, E> {
        Ok(Ok(ConvexValue::Null))
    }

    fn visit_none<E>(self) -> Result<Decoded, E> {
        Ok(Ok(ConvexValue::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Decoded, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Decoded, A::Error> {
        let mut items = Vec::new();
        let mut error = None;
        while let Some(item) = seq.next_element_seed(ValueSeed)? {
            match item {
                Ok(v) => {
                    if error.is_none() {
                        items.push(v);
                    }
                },
                Err(e) => {
                    // Keep consuming: the rest must still be valid JSON.
                    if error.is_none() {
                        error = Some(e);
                        items = Vec::new();
                    }
                },
            }
        }
        Ok(match error {
            Some(e) => Err(e),
            None => ConvexArray::try_from(items).map(ConvexValue::Array),
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Decoded, A::Error> {
        match map.next_key_seed(FirstKeySeed)? {
            None => Ok(ConvexObject::try_from(BTreeMap::new()).map(ConvexValue::Object)),
            Some(first) => decode_object(map, first),
        }
    }
}

fn decode_object<'de, A: MapAccess<'de>>(mut map: A, first: FirstKey) -> Result<Decoded, A::Error> {
    match first {
        FirstKey::RawValue => {
            let raw = map.next_value_seed(RawStrSeed)?;
            decode_str_with(&raw, ValueSeed).map_err(A::Error::custom)
        },
        FirstKey::Special(kind) => decode_special_object(map, kind),
        FirstKey::Field(key) => decode_field_object(map, key),
    }
}

/// An object whose first key is `$bytes`, `$integer` or `$float`. It is a
/// special encoding only if every key is that same key (the last value wins);
/// any other key makes the object invalid, since the special key is then an
/// invalid field name.
fn decode_special_object<'de, A: MapAccess<'de>>(
    mut map: A,
    kind: Special,
) -> Result<Decoded, A::Error> {
    let mut value = map.next_value_seed(SpecialValueSeed(kind))?;
    let mut other_key = false;
    while let Some(key) = map.next_key_seed(FirstKeySeed)? {
        if !other_key && matches!(key, FirstKey::Special(k) if k == kind) {
            value = map.next_value_seed(SpecialValueSeed(kind))?;
        } else {
            other_key = true;
            let _ = map.next_value_seed(ValueSeed)?;
        }
    }
    if other_key {
        return Ok(Err(reserved_key(kind.key())));
    }
    Ok(value)
}

/// A regular object. Values that fail Convex validation are remembered per
/// key, because a later duplicate key replaces them.
fn decode_field_object<'de, A: MapAccess<'de>>(
    mut map: A,
    first_key: anyhow::Result<FieldName>,
) -> Result<Decoded, A::Error> {
    let mut fields: BTreeMap<FieldName, ConvexValue> = BTreeMap::new();
    let mut overwritable_errors: Vec<(FieldName, anyhow::Error)> = Vec::new();
    let mut invalid_key: Option<anyhow::Error> = None;
    let mut key = first_key;
    loop {
        let value = map.next_value_seed(ValueSeed)?;
        match key {
            Err(e) => {
                invalid_key.get_or_insert(e);
            },
            Ok(_) if invalid_key.is_some() => {},
            Ok(name) => match value {
                Ok(v) => {
                    if !overwritable_errors.is_empty() {
                        overwritable_errors.retain(|(n, _)| *n != name);
                    }
                    fields.insert(name, v);
                },
                Err(e) => {
                    fields.remove(&name);
                    match overwritable_errors.iter_mut().find(|(n, _)| *n == name) {
                        Some(slot) => slot.1 = e,
                        None => overwritable_errors.push((name, e)),
                    }
                },
            },
        }
        match map.next_key_seed(FieldKeySeed)? {
            Some(next) => key = next,
            None => break,
        }
    }
    if let Some(e) = invalid_key {
        return Ok(Err(e));
    }
    if let Some((_, e)) = overwritable_errors.into_iter().next() {
        return Ok(Err(e));
    }
    Ok(ConvexObject::try_from(fields).map(ConvexValue::Object))
}

/// The value of a `$bytes`/`$integer`/`$float` key. The reference path
/// requires it to be a JSON string (`serde_json::from_value::<String>`); any
/// other value is still fully parsed (it must be valid JSON), then rejected.
/// A raw-value object counts as whatever JSON its payload parses to.
#[derive(Clone, Copy)]
struct SpecialValueSeed(Special);

impl<'de> DeserializeSeed<'de> for SpecialValueSeed {
    type Value = Decoded;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Decoded, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for SpecialValueSeed {
    type Value = Decoded;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_str<E>(self, s: &str) -> Result<Decoded, E> {
        Ok(self.0.decode(s))
    }

    fn visit_bool<E>(self, _: bool) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_i64<E>(self, _: i64) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_u64<E>(self, _: u64) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_f64<E>(self, _: f64) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_unit<E>(self) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_none<E>(self) -> Result<Decoded, E> {
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Decoded, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Decoded, A::Error> {
        let _ = ValueSeed.visit_seq(seq)?;
        Ok(Err(self.0.not_a_string()))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Decoded, A::Error> {
        match map.next_key_seed(FirstKeySeed)? {
            None => Ok(Err(self.0.not_a_string())),
            Some(FirstKey::RawValue) => {
                let raw = map.next_value_seed(RawStrSeed)?;
                decode_str_with(&raw, self).map_err(A::Error::custom)
            },
            Some(first) => {
                let _ = decode_object(map, first)?;
                Ok(Err(self.0.not_a_string()))
            },
        }
    }
}
