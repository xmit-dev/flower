//! Guest ABI values (GUEST_ABI.md): one tag byte, then little-endian
//! fixed-width fields. Strings travel in whichever representation the sender
//! already holds, and repeated map keys refer back to their first occurrence,
//! so neither side transcodes or re-interns what it has already seen.
use rustc_hash::FxHashMap;
use serde_json::{Map, Number, Value};
use std::fmt;

pub(crate) const MAX_DEPTH: usize = 128;

pub(crate) const NULL: u8 = 0;
pub(crate) const FALSE: u8 = 1;
pub(crate) const TRUE: u8 = 2;
pub(crate) const INT: u8 = 3;
pub(crate) const FLOAT: u8 = 4;
pub(crate) const UTF8: u8 = 5;
pub(crate) const LATIN1: u8 = 6;
pub(crate) const UTF16: u8 = 7;
pub(crate) const ARRAY: u8 = 8;
pub(crate) const MAP: u8 = 9;
pub(crate) const KEY: u8 = 10;

pub(crate) const SUCCESS: u8 = 0;
pub(crate) const FAILURE: u8 = 1;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Invalid(pub(crate) &'static str);

impl fmt::Display for Invalid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "INVALID_VALUE: {}", self.0)
    }
}

impl std::error::Error for Invalid {}

const NESTING: Invalid = Invalid("nesting exceeds 128 levels");

/// Writes one message. Keys are interned per message: the decoder numbers
/// every key string it reads, in order, and `KEY n` repeats the nth.
#[derive(Default)]
pub(crate) struct Encoder<'a> {
    pub(crate) out: Vec<u8>,
    keys: FxHashMap<&'a str, u32>,
    next_key: u32,
}

impl<'a> Encoder<'a> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            out: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    fn head(&mut self, tag: u8, length: usize) {
        self.out.push(tag);
        self.out
            .extend_from_slice(&u32::try_from(length).unwrap_or(u32::MAX).to_le_bytes());
    }

    pub(crate) fn text(&mut self, value: &str) {
        // Every ASCII string is also Latin-1, which QuickJS copies verbatim.
        self.head(if value.is_ascii() { LATIN1 } else { UTF8 }, value.len());
        self.out.extend_from_slice(value.as_bytes());
    }

    pub(crate) fn key(&mut self, key: &'a str) {
        if let Some(index) = self.keys.get(key) {
            self.out.push(KEY);
            self.out.extend_from_slice(&index.to_le_bytes());
        } else {
            self.keys.insert(key, self.next_key);
            self.next_key += 1;
            self.text(key);
        }
    }

    pub(crate) fn number(&mut self, number: &Number) {
        if let Some(value) = number.as_i64().and_then(|value| i32::try_from(value).ok()) {
            self.out.push(INT);
            self.out.extend_from_slice(&value.to_le_bytes());
        } else {
            // Integers beyond 2^53 round exactly as JSON.parse rounds them.
            self.out.push(FLOAT);
            let value = number.as_f64().unwrap_or(0.0);
            self.out.extend_from_slice(&value.to_le_bytes());
        }
    }

    pub(crate) fn map_head(&mut self, count: usize) {
        self.head(MAP, count);
    }

    pub(crate) fn array_head(&mut self, count: usize) {
        self.head(ARRAY, count);
    }

    /// Append one value. Maps are written in JavaScript's canonical order:
    /// UTF-16 code unit order, which equals byte order for ASCII keys.
    pub(crate) fn value(&mut self, value: &'a Value) -> Result<(), Invalid> {
        self.nested(value, 1)
    }

    fn nested(&mut self, value: &'a Value, depth: usize) -> Result<(), Invalid> {
        if depth > MAX_DEPTH {
            return Err(NESTING);
        }
        match value {
            Value::Null => self.out.push(NULL),
            Value::Bool(false) => self.out.push(FALSE),
            Value::Bool(true) => self.out.push(TRUE),
            Value::Number(number) => self.number(number),
            Value::String(text) => self.text(text),
            Value::Array(items) => {
                self.array_head(items.len());
                for item in items {
                    self.nested(item, depth + 1)?;
                }
            }
            Value::Object(entries) => {
                self.map_head(entries.len());
                if entries.keys().all(|key| key.is_ascii()) {
                    for (key, item) in entries {
                        self.key(key);
                        self.nested(item, depth + 1)?;
                    }
                } else {
                    let mut sorted: Vec<_> = entries.iter().collect();
                    sorted.sort_unstable_by(|(left, _), (right, _)| {
                        left.encode_utf16().cmp(right.encode_utf16())
                    });
                    for (key, item) in sorted {
                        self.key(key);
                        self.nested(item, depth + 1)?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn encode(value: &Value) -> Result<Vec<u8>, Invalid> {
    let mut encoder = Encoder::default();
    encoder.value(value)?;
    Ok(encoder.out)
}

/// A success outcome: status byte, then the value.
pub(crate) fn success(value: &Value) -> Result<Vec<u8>, Invalid> {
    let mut encoder = Encoder::with_capacity(64);
    encoder.out.push(SUCCESS);
    encoder.value(value)?;
    Ok(encoder.out)
}

/// A failure outcome: status byte, then `{code, message, details?}` with keys
/// in canonical order. The map is transport: `details` is a root value.
pub(crate) fn failure(code: &str, message: &str, details: Option<&Value>) -> Vec<u8> {
    // Unrepresentable details are dropped, never the failure itself.
    let details = details.filter(|details| within_depth(details, 1));
    let mut encoder = Encoder::with_capacity(32 + code.len() + message.len());
    encoder.out.push(FAILURE);
    encoder.map_head(2 + details.is_some() as usize);
    encoder.key("code");
    encoder.text(code);
    if let Some(details) = details {
        encoder.key("details");
        let _ = encoder.value(details);
    }
    encoder.key("message");
    encoder.text(message);
    encoder.out
}

fn within_depth(value: &Value, depth: usize) -> bool {
    depth <= MAX_DEPTH
        && match value {
            Value::Array(items) => items.iter().all(|item| within_depth(item, depth + 1)),
            Value::Object(entries) => entries.values().all(|item| within_depth(item, depth + 1)),
            _ => true,
        }
}

/// Reads one message. Integral numbers become integers exactly where
/// `serde_json` would parse JavaScript's rendering of them as integers.
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
    keys: Vec<String>,
}

impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            offset: 0,
            keys: Vec::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    pub(crate) fn finish(&self) -> Result<(), Invalid> {
        if self.is_empty() {
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

    pub(crate) fn byte(&mut self) -> Result<u8, Invalid> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, Invalid> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Every element occupies at least one byte, so a count larger than the
    /// remaining input is malformed. This bounds preallocation by input size.
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
            UTF8 => std::str::from_utf8(self.take(length)?)
                .map(str::to_owned)
                .map_err(|_| Invalid("string is not UTF-8")),
            LATIN1 => {
                let bytes = self.take(length)?;
                if bytes.is_ascii() {
                    // SAFETY: ASCII is valid UTF-8.
                    Ok(unsafe { String::from_utf8_unchecked(bytes.to_vec()) })
                } else {
                    Ok(bytes.iter().map(|&byte| byte as char).collect())
                }
            }
            _ => {
                let bytes = self.take(length.checked_mul(2).ok_or(Invalid("truncated"))?)?;
                let units = bytes
                    .chunks_exact(2)
                    .map(|unit| u16::from_le_bytes([unit[0], unit[1]]));
                let mut text = String::with_capacity(length);
                for unit in char::decode_utf16(units) {
                    text.push(unit.map_err(|_| Invalid("string has a lone surrogate"))?);
                }
                Ok(text)
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

    /// Decode one value at the root of the value model.
    pub(crate) fn value(&mut self) -> Result<Value, Invalid> {
        self.nested(1)
    }

    fn nested(&mut self, depth: usize) -> Result<Value, Invalid> {
        if depth > MAX_DEPTH {
            return Err(NESTING);
        }
        Ok(match self.byte()? {
            NULL => Value::Null,
            FALSE => Value::Bool(false),
            TRUE => Value::Bool(true),
            INT => Value::Number(i32::from_le_bytes(self.take(4)?.try_into().unwrap()).into()),
            FLOAT => number(f64::from_le_bytes(self.take(8)?.try_into().unwrap()))?,
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
                let mut entries = Map::new();
                for _ in 0..count {
                    let key = self.key()?;
                    let item = self.nested(depth + 1)?;
                    if entries.insert(key, item).is_some() {
                        return Err(Invalid("duplicate map key"));
                    }
                }
                Value::Object(entries)
            }
            _ => return Err(Invalid("unknown tag")),
        })
    }
}

/// JavaScript prints integral numbers below 1e21 as digits, which serde_json
/// parses as integers when they fit 64 bits. Keep exactly that representation
/// so values compare equal no matter which boundary they crossed.
fn number(value: f64) -> Result<Value, Invalid> {
    if !value.is_finite() {
        return Err(Invalid("number is not finite"));
    }
    if value.fract() == 0.0 {
        if (0.0..18_446_744_073_709_551_616.0).contains(&value) {
            return Ok(Value::Number((value as u64).into()));
        }
        if (-9_223_372_036_854_775_808.0..0.0).contains(&value) {
            return Ok(Value::Number((value as i64).into()));
        }
    }
    Ok(Number::from_f64(value).map_or(Value::Null, Value::Number))
}

#[cfg(test)]
pub(crate) fn decode(bytes: &[u8]) -> Result<Value, Invalid> {
    let mut decoder = Decoder::new(bytes);
    let value = decoder.value()?;
    decoder.finish()?;
    Ok(value)
}

/// A guest callback's or host call's result.
#[derive(Debug)]
pub(crate) enum Outcome {
    Success(Value),
    Failure {
        code: String,
        message: String,
        details: Option<Value>,
    },
}

pub(crate) fn outcome(bytes: &[u8]) -> Result<Outcome, Invalid> {
    let mut decoder = Decoder::new(bytes);
    let outcome = match decoder.byte()? {
        SUCCESS => Outcome::Success(decoder.value()?),
        FAILURE => {
            if decoder.byte()? != MAP {
                return Err(Invalid("failure is not a map"));
            }
            let (mut code, mut message, mut details) = (None, None, None);
            for _ in 0..decoder.count()? {
                let key = decoder.key()?;
                let slot = match key.as_str() {
                    "code" => &mut code,
                    "message" => &mut message,
                    "details" => &mut details,
                    _ => return Err(Invalid("failure has unknown fields")),
                };
                if slot.replace(decoder.value()?).is_some() {
                    return Err(Invalid("duplicate map key"));
                }
            }
            let (Some(Value::String(code)), Some(Value::String(message))) = (code, message) else {
                return Err(Invalid("failure needs a string code and message"));
            };
            Outcome::Failure {
                code,
                message,
                details,
            }
        }
        _ => return Err(Invalid("unknown outcome status")),
    };
    decoder.finish()?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(value: Value) {
        assert_eq!(decode(&encode(&value).unwrap()).unwrap(), value, "{value}");
    }

    #[test]
    fn values_round_trip() {
        for value in [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(i32::MAX),
            json!(i32::MIN),
            json!(2_147_483_648_u64),
            json!(-2_147_483_649_i64),
            json!(1_700_000_000_000_u64),
            json!(9_007_199_254_740_991_u64),
            json!(-9_007_199_254_740_991_i64),
            json!(1.5),
            json!(-0.25),
            json!(1e300),
            json!(""),
            json!("ascii"),
            json!("🌺 goblins\u{0} é"),
            json!([1, [2, [3]], {"a": {"b": []}}]),
            json!({"z": 1, "a": 2, "é": null, "😀": {"\u{e000}": 1}}),
            json!([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"name": "c", "id": 3}]),
        ] {
            round_trip(value);
        }
    }

    #[test]
    fn layout_is_fixed_width_little_endian_with_interned_keys() {
        assert_eq!(encode(&json!(null)).unwrap(), [NULL]);
        assert_eq!(encode(&json!(-2)).unwrap(), [INT, 0xfe, 0xff, 0xff, 0xff]);
        assert_eq!(
            encode(&json!(0.5)).unwrap(),
            [FLOAT, 0, 0, 0, 0, 0, 0, 0xe0, 0x3f]
        );
        assert_eq!(
            encode(&json!("hi")).unwrap(),
            [LATIN1, 2, 0, 0, 0, b'h', b'i']
        );
        assert_eq!(encode(&json!("é")).unwrap(), [UTF8, 2, 0, 0, 0, 0xc3, 0xa9]);
        assert_eq!(
            encode(&json!([{"a": true}, {"a": false}])).unwrap(),
            [
                ARRAY, 2, 0, 0, 0, //
                MAP, 1, 0, 0, 0, LATIN1, 1, 0, 0, 0, b'a', TRUE, //
                MAP, 1, 0, 0, 0, KEY, 0, 0, 0, 0, FALSE,
            ]
        );
        // Keys follow JavaScript's UTF-16 order, not UTF-8 byte order.
        let bytes = encode(&json!({"\u{ff61}": 1, "😀": 2})).unwrap();
        assert_eq!(&bytes[5..10], [UTF8, 4, 0, 0, 0]);
    }

    #[test]
    fn guest_representations_decode_to_canonical_values() {
        // Latin-1 and UTF-16 strings, as QuickJS stores them.
        assert_eq!(
            decode(&[LATIN1, 2, 0, 0, 0, b'a', 0xe9]).unwrap(),
            json!("aé")
        );
        assert_eq!(
            decode(&[UTF16, 3, 0, 0, 0, 0x61, 0, 0x3d, 0xd8, 0x00, 0xde]).unwrap(),
            json!("a😀")
        );
        // Integral floats land where serde_json puts JavaScript's rendering.
        let float = |value: f64| {
            let mut bytes = vec![FLOAT];
            bytes.extend_from_slice(&value.to_le_bytes());
            decode(&bytes).unwrap()
        };
        assert_eq!(float(2.0), json!(2));
        assert_eq!(float(-0.0), json!(0));
        assert_eq!(float(-3.0), json!(-3));
        assert_eq!(
            float(9_007_199_254_740_992.0),
            json!(9_007_199_254_740_992_u64)
        );
        assert_eq!(float(1e19), json!(10_000_000_000_000_000_000_u64));
        assert_eq!(float(-9.3e18), json!(-9.3e18));
        assert_eq!(float(1e21), json!(1e21));
        assert_eq!(float(0.1), json!(0.1));
    }

    #[test]
    fn decoding_rejects_everything_outside_the_value_model() {
        let nan = [&[FLOAT][..], &f64::NAN.to_le_bytes()].concat();
        let infinity = [&[FLOAT][..], &f64::INFINITY.to_le_bytes()].concat();
        for (bytes, reason) in [
            (&[][..], "truncated"),
            (&[NULL, NULL][..], "trailing bytes"),
            (&[INT, 1, 0][..], "truncated"),
            (&[11][..], "unknown tag"),
            (&[KEY, 0, 0, 0, 0][..], "unknown tag"),
            (&nan[..], "number is not finite"),
            (&infinity[..], "number is not finite"),
            (&[UTF8, 2, 0, 0, 0, 0xff, 0xfe][..], "string is not UTF-8"),
            (
                &[UTF16, 1, 0, 0, 0, 0x00, 0xd8][..],
                "string has a lone surrogate",
            ),
            (&[UTF16, 0xff, 0xff, 0xff, 0xff][..], "truncated"),
            (&[ARRAY, 0xff, 0xff, 0xff, 0xff][..], "truncated"),
            (
                &[MAP, 1, 0, 0, 0, NULL, NULL][..],
                "map key is not a string",
            ),
            (
                &[MAP, 1, 0, 0, 0, KEY, 0, 0, 0, 0, NULL][..],
                "unknown key reference",
            ),
            (
                &[
                    MAP, 2, 0, 0, 0, LATIN1, 1, 0, 0, 0, b'a', NULL, KEY, 0, 0, 0, 0, NULL,
                ][..],
                "duplicate map key",
            ),
        ] {
            assert_eq!(decode(bytes), Err(Invalid(reason)), "{bytes:02x?}");
        }
        let nest = |levels: usize| {
            let mut bytes = [ARRAY, 1, 0, 0, 0].repeat(levels);
            bytes.push(NULL);
            bytes
        };
        assert!(decode(&nest(MAX_DEPTH - 1)).is_ok());
        assert_eq!(decode(&nest(MAX_DEPTH)), Err(NESTING));
        let mut deep = json!(null);
        for _ in 0..MAX_DEPTH {
            deep = json!([deep]);
        }
        assert_eq!(encode(&deep), Err(NESTING));
    }

    #[test]
    fn outcomes_carry_values_and_failures() {
        let Outcome::Success(value) = outcome(&success(&json!({"a": [1]})).unwrap()).unwrap()
        else {
            panic!("expected success");
        };
        assert_eq!(value, json!({"a": [1]}));
        let failed = failure("NOPE", "no", Some(&json!({"why": "because"})));
        let Outcome::Failure {
            code,
            message,
            details,
        } = outcome(&failed).unwrap()
        else {
            panic!("expected failure");
        };
        assert_eq!(
            (code.as_str(), message.as_str(), details),
            ("NOPE", "no", Some(json!({"why": "because"})))
        );
        let mut deep = json!(null);
        for _ in 1..MAX_DEPTH {
            deep = json!([deep]);
        }
        let Outcome::Failure { details, .. } = outcome(&failure("E", "m", Some(&deep))).unwrap()
        else {
            panic!("expected failure");
        };
        assert_eq!(details, Some(deep.clone()), "details are root values");
        deep = json!([deep]);
        assert_eq!(
            failure("NOPE", "no", Some(&deep)),
            failure("NOPE", "no", None),
            "unrepresentable details are dropped"
        );
        assert_eq!(
            outcome(&[FAILURE, NULL]).err(),
            Some(Invalid("failure is not a map"))
        );
        assert_eq!(
            outcome(&[2, NULL]).err(),
            Some(Invalid("unknown outcome status"))
        );
    }
}
