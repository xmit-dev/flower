//! The binary value encoding of GUEST_ABI.md: a tag byte, then little-endian
//! fixed-width fields. Decoding accepts every string representation and key
//! reference the host sends; encoding writes UTF-8 strings and plain keys.
use crate::{Failure, Map, Value};
use alloc::{string::String, vec::Vec};

pub const NULL: u8 = 0x00;
pub const FALSE: u8 = 0x01;
pub const TRUE: u8 = 0x02;
pub const INT: u8 = 0x03;
pub const FLOAT: u8 = 0x04;
pub const UTF8: u8 = 0x05;
pub const LATIN1: u8 = 0x06;
pub const UTF16: u8 = 0x07;
pub const ARRAY: u8 = 0x08;
pub const MAP: u8 = 0x09;
pub const KEY: u8 = 0x0a;

pub const SUCCESS: u8 = 0x00;
pub const FAILURE: u8 = 0x01;

pub const MAX_DEPTH: usize = 128;

/// A malformed message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invalid(pub &'static str);

/// Writes values into one message.
#[derive(Default)]
pub struct Encoder {
    pub out: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self::with_capacity(64)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Encoder {
            out: Vec::with_capacity(capacity),
        }
    }
    fn head(&mut self, tag: u8, count: usize) {
        self.out.push(tag);
        self.out.extend_from_slice(&(count as u32).to_le_bytes());
    }
    pub fn null(&mut self) {
        self.out.push(NULL);
    }
    pub fn bool(&mut self, value: bool) {
        self.out.push(if value { TRUE } else { FALSE });
    }
    pub fn number(&mut self, value: f64) {
        // Integral values in i32 range travel as integers, like QuickJS's.
        if (-2_147_483_648.0..=2_147_483_647.0).contains(&value) && value as i32 as f64 == value {
            self.out.push(INT);
            self.out.extend_from_slice(&(value as i32).to_le_bytes());
        } else {
            self.out.push(FLOAT);
            self.out.extend_from_slice(&value.to_le_bytes());
        }
    }
    pub fn str(&mut self, text: &str) {
        self.head(UTF8, text.len());
        self.out.extend_from_slice(text.as_bytes());
    }
    pub fn array(&mut self, count: usize) {
        self.head(ARRAY, count);
    }
    pub fn map(&mut self, count: usize) {
        self.head(MAP, count);
    }
    /// A map key; follow it with the member's value.
    pub fn key(&mut self, key: &str) {
        self.str(key);
    }
    pub fn value(&mut self, value: &Value) {
        match value {
            Value::Null => self.null(),
            Value::Bool(value) => self.bool(*value),
            Value::Number(value) => self.number(*value),
            Value::String(text) => self.str(text),
            Value::Array(items) => {
                self.array(items.len());
                for item in items {
                    self.value(item);
                }
            }
            Value::Object(map) => {
                self.map(map.len());
                for (key, item) in map.iter() {
                    self.key(key);
                    self.value(item);
                }
            }
        }
    }
}

/// A success outcome: status byte, then the value.
pub fn success(value: &Value) -> Vec<u8> {
    let mut encoder = Encoder::new();
    encoder.out.push(SUCCESS);
    encoder.value(value);
    encoder.out
}

/// A failure outcome. Details are omitted when `details` is false.
pub fn failure(failure: &Failure, details: bool) -> Vec<u8> {
    let details = failure.details.as_ref().filter(|_| details);
    let mut encoder = Encoder::with_capacity(32 + failure.code.len() + failure.message.len());
    encoder.out.push(FAILURE);
    encoder.map(2 + details.is_some() as usize);
    encoder.key("code");
    encoder.str(&failure.code);
    encoder.key("message");
    encoder.str(&failure.message);
    if let Some(details) = details {
        encoder.key("details");
        encoder.value(details);
    }
    encoder.out
}

/// Reads one message; key references resolve against the keys it has read.
pub struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
    keys: Vec<String>,
}

impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Decoder {
            bytes,
            offset: 0,
            keys: Vec::new(),
        }
    }
    pub fn finish(&self) -> Result<(), Invalid> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Invalid("trailing bytes"))
        }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], Invalid> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Invalid("truncated"))?;
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }
    pub fn byte(&mut self) -> Result<u8, Invalid> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, Invalid> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn count(&mut self) -> Result<usize, Invalid> {
        let count = self.u32()? as usize;
        if count > self.bytes.len() - self.offset {
            return Err(Invalid("truncated"));
        }
        Ok(count)
    }
    fn string(&mut self, tag: u8) -> Result<String, Invalid> {
        let length = self.u32()? as usize;
        match tag {
            UTF8 => core::str::from_utf8(self.take(length)?)
                .map(String::from)
                .map_err(|_| Invalid("string is not UTF-8")),
            LATIN1 => {
                let bytes = self.take(length)?;
                Ok(match core::str::from_utf8(bytes) {
                    Ok(ascii) if bytes.is_ascii() => String::from(ascii),
                    _ => bytes.iter().map(|&byte| byte as char).collect(),
                })
            }
            _ => {
                let bytes = self.take(length.checked_mul(2).ok_or(Invalid("truncated"))?)?;
                let units = bytes
                    .chunks_exact(2)
                    .map(|unit| u16::from_le_bytes([unit[0], unit[1]]));
                char::decode_utf16(units)
                    .collect::<Result<String, _>>()
                    .map_err(|_| Invalid("string has a lone surrogate"))
            }
        }
    }
    fn key(&mut self) -> Result<String, Invalid> {
        match self.byte()? {
            KEY => {
                let index = self.u32()? as usize;
                self.keys
                    .get(index)
                    .cloned()
                    .ok_or(Invalid("unknown key reference"))
            }
            tag @ (UTF8 | LATIN1 | UTF16) => {
                let key = self.string(tag)?;
                self.keys.push(key.clone());
                Ok(key)
            }
            _ => Err(Invalid("map key is not a string")),
        }
    }
    /// One root value.
    pub fn value(&mut self) -> Result<Value, Invalid> {
        self.nested(1)
    }
    fn nested(&mut self, depth: usize) -> Result<Value, Invalid> {
        if depth > MAX_DEPTH {
            return Err(Invalid("nesting exceeds 128 levels"));
        }
        Ok(match self.byte()? {
            NULL => Value::Null,
            FALSE => Value::Bool(false),
            TRUE => Value::Bool(true),
            INT => Value::Number(i32::from_le_bytes(self.take(4)?.try_into().unwrap()) as f64),
            FLOAT => {
                let value = f64::from_le_bytes(self.take(8)?.try_into().unwrap());
                if !value.is_finite() {
                    return Err(Invalid("number is not finite"));
                }
                Value::Number(value)
            }
            tag @ (UTF8 | LATIN1 | UTF16) => Value::String(self.string(tag)?),
            ARRAY => {
                let count = self.count()?;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(self.nested(depth + 1)?);
                }
                Value::Array(items)
            }
            MAP => {
                let count = self.count()?;
                let mut map = Map::with_capacity(count);
                for _ in 0..count {
                    let key = self.key()?;
                    let item = self.nested(depth + 1)?;
                    if map.contains_key(&key) {
                        return Err(Invalid("duplicate map key"));
                    }
                    map.push_unique(key, item);
                }
                Value::Object(map)
            }
            _ => return Err(Invalid("unknown tag")),
        })
    }
}

/// Decode a message holding exactly one value.
pub fn decode(bytes: &[u8]) -> Result<Value, Invalid> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.value()?;
    decoder.finish()?;
    Ok(value)
}

/// Decode an outcome: the callback's value or its failure.
pub fn outcome(bytes: &[u8]) -> Result<Result<Value, Failure>, Invalid> {
    let mut decoder = Decoder::new(bytes);
    let result = match decoder.byte()? {
        SUCCESS => Ok(decoder.value()?),
        FAILURE => {
            let Value::Object(mut map) = decoder.value()? else {
                return Err(Invalid("failure is not a map"));
            };
            let (Some(Value::String(code)), Some(Value::String(message))) =
                (map.remove("code"), map.remove("message"))
            else {
                return Err(Invalid("failure needs a string code and message"));
            };
            let details = map.remove("details");
            if !map.is_empty() {
                return Err(Invalid("failure has unknown fields"));
            }
            Err(Failure {
                code,
                message,
                details,
            })
        }
        _ => return Err(Invalid("unknown outcome status")),
    };
    decoder.finish()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array, object};

    #[test]
    fn values_round_trip() {
        for value in [
            Value::Null,
            Value::Bool(true),
            Value::Number(-7.0),
            Value::Number(1_790_000_000_123.0),
            Value::Number(0.5),
            Value::from("é🌸\0"),
            array![1, "two", Value::Null, array![]],
            object! {"a" => object! {"b" => array![true]}, "c" => 3},
        ] {
            let mut encoder = Encoder::new();
            encoder.value(&value);
            assert_eq!(decode(&encoder.out).unwrap(), value);
        }
    }

    #[test]
    fn host_representations_decode() {
        // {"ab": "é" as Latin-1, "x": [{"ab": "🌸" as UTF-16}]} with a key reference.
        let mut bytes = alloc::vec![
            MAP, 2, 0, 0, 0, LATIN1, 2, 0, 0, 0, b'a', b'b', LATIN1, 1, 0, 0, 0, 0xe9
        ];
        bytes.extend([
            UTF8, 1, 0, 0, 0, b'x', ARRAY, 1, 0, 0, 0, MAP, 1, 0, 0, 0, KEY, 0, 0, 0, 0,
        ]);
        bytes.extend([UTF16, 2, 0, 0, 0, 0x3c, 0xd8, 0x38, 0xdf]);
        assert_eq!(
            decode(&bytes).unwrap(),
            object! {"ab" => "é", "x" => array![object! {"ab" => "🌸"}]}
        );
        assert_eq!(
            decode(&[MAP, 1, 0, 0, 0, KEY, 0, 0, 0, 0, NULL]),
            Err(Invalid("unknown key reference"))
        );
        assert_eq!(
            decode(&[ARRAY, 9, 0, 0, 0, NULL]),
            Err(Invalid("truncated"))
        );
        assert_eq!(decode(&[NULL, NULL]), Err(Invalid("trailing bytes")));
    }

    #[test]
    fn outcomes_carry_values_or_failures() {
        let failed = Failure {
            code: "NOPE".into(),
            message: "no".into(),
            details: Some(array![1]),
        };
        assert_eq!(
            outcome(&success(&Value::from(2))).unwrap(),
            Ok(Value::from(2))
        );
        assert_eq!(
            outcome(&failure(&failed, true)).unwrap(),
            Err(failed.clone())
        );
        let bare = outcome(&failure(&failed, false)).unwrap().unwrap_err();
        assert_eq!(bare.details, None);
    }
}
