//! JavaScript-exact JSON: `canonicalJson` (byte-identical to `sdk/json.ts`), `JSON.stringify` number
//! spelling, UTF-16 key order, byte counts and nesting checks.
//!
//! Modelled on the server's `src/evaluator/rust_engine/json.rs`, except that object keys are always
//! sorted explicitly: a crate that depends on this one may enable serde_json's `preserve_order`, and
//! then `Map` iterates in insertion order.

use std::cmp::Ordering;
use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::FlowerError;

/// Flower's nesting limit: `canonicalJson` throws beyond it, watches reject values beyond it.
pub const MAX_DEPTH: usize = 128;

/// The deepest bracket nesting [`from_slice_deep`] parses; deeper input is rejected before
/// parsing, so hostile input cannot overflow the stack.
pub const MAX_PARSE_DEPTH: usize = 512;

/// Compare strings by UTF-16 code units, like JavaScript's default `sort()` and `<`.
pub fn compare_utf16(left: &str, right: &str) -> Ordering {
    if left.is_ascii() && right.is_ascii() {
        left.cmp(right)
    } else {
        left.encode_utf16().cmp(right.encode_utf16())
    }
}

/// `JSON.stringify(number)` for a finite double: shortest round-trip digits, JS exponent rules,
/// `-0` as `0`.
pub fn format_number(value: f64) -> String {
    let mut output = String::new();
    push_number(value, &mut output);
    output
}

fn push_number(value: f64, output: &mut String) {
    if value == 0.0 {
        output.push('0');
    } else if value.is_finite() {
        output.push_str(ryu_js::Buffer::new().format_finite(value));
    } else {
        output.push_str("null");
    }
}

fn number_f64(number: &serde_json::Number) -> f64 {
    number.as_f64().unwrap_or(f64::NAN)
}

/// `canonicalJson`: `JSON.stringify` spelling with object keys sorted by UTF-16 code units at every
/// level, arrays in order. Integers beyond 2^53 print like the JavaScript doubles they become.
/// Does not check nesting; see [`canonical_json_checked`].
pub fn canonical_json(value: &Value) -> String {
    let mut output = String::new();
    append_canonical(value, &mut output);
    output
}

/// [`canonical_json`] that also rejects nesting beyond 128 like the TypeScript
/// (`TypeError: Flower JSON nesting exceeds 128`).
pub fn canonical_json_checked(value: &Value) -> Result<String, FlowerError> {
    check_depth(value, "Flower JSON nesting exceeds 128")?;
    Ok(canonical_json(value))
}

/// Canonical JSON of any serializable value.
pub fn canonical_json_of<T: Serialize + ?Sized>(value: &T) -> Result<String, FlowerError> {
    let value = serde_json::to_value(value).map_err(|error| {
        FlowerError::invalid(format!("Flower values must be JSON values: {error}"))
    })?;
    canonical_json_checked(&value)
}

fn append_canonical(value: &Value, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(number) => push_number(number_f64(number), output),
        Value::String(value) => push_string(value, output),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                append_canonical(value, output);
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut entries: Vec<(&String, &Value)> = values.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| compare_utf16(left, right));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                push_string(key, output);
                output.push(':');
                append_canonical(value, output);
            }
            output.push('}');
        }
    }
}

/// `JSON.stringify(string)`: `"`, `\` and C0 controls escaped (`\b \f \n \r \t`, else `\u00xx`),
/// everything else (U+2028, U+007F, astral characters) verbatim.
pub fn push_string(value: &str, output: &mut String) {
    if value
        .bytes()
        .all(|byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
    {
        output.push('"');
        output.push_str(value);
        output.push('"');
    } else {
        output.push_str(&serde_json::to_string(value).expect("strings encode"));
    }
}

fn string_len(value: &str) -> usize {
    2 + value.len()
        + value
            .bytes()
            .map(|byte| match byte {
                b'"' | b'\\' | b'\x08' | b'\x0c' | b'\n' | b'\r' | b'\t' => 1,
                0..=0x1f => 5,
                _ => 0,
            })
            .sum::<usize>()
}

/// UTF-8 byte length of `JSON.stringify(value)`, without building it.
pub fn stringify_len(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(value) => {
            if *value {
                4
            } else {
                5
            }
        }
        Value::Number(number) => {
            let value = number_f64(number);
            if value == 0.0 {
                1
            } else if value.is_finite() {
                ryu_js::Buffer::new().format_finite(value).len()
            } else {
                4
            }
        }
        Value::String(value) => string_len(value),
        Value::Array(values) => {
            2 + values.len().saturating_sub(1) + values.iter().map(stringify_len).sum::<usize>()
        }
        Value::Object(values) => {
            2 + values.len().saturating_sub(1)
                + values
                    .iter()
                    .map(|(key, value)| string_len(key) + 1 + stringify_len(value))
                    .sum::<usize>()
        }
    }
}

/// JavaScript truthiness of a JSON value (the TS default predicate `Boolean`).
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => {
            let value = number_f64(number);
            value != 0.0 && !value.is_nan()
        }
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Reject a value whose items nest deeper than [`MAX_DEPTH`] (the root is depth 0).
pub(crate) fn check_depth(value: &Value, message: &str) -> Result<(), FlowerError> {
    fn deepest(value: &Value, depth: usize) -> bool {
        if depth > MAX_DEPTH {
            return false;
        }
        match value {
            Value::Array(values) => values.iter().all(|value| deepest(value, depth + 1)),
            Value::Object(values) => values.values().all(|value| deepest(value, depth + 1)),
            _ => true,
        }
    }
    if deepest(value, 0) {
        Ok(())
    } else {
        Err(FlowerError::invalid(message))
    }
}

/// The deepest bracket nesting in JSON text, ignoring brackets inside strings.
pub fn nesting(bytes: &[u8]) -> usize {
    let (mut depth, mut deepest, mut string, mut escaped) = (0usize, 0usize, false, false);
    for &byte in bytes {
        if string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                string = false;
            }
            continue;
        }
        match byte {
            b'"' => string = true,
            b'[' | b'{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// Parse JSON like `JSON.parse` does for nesting: beyond serde_json's default 128 levels (Flower
/// values may nest 128 deep inside a reply envelope), up to [`MAX_PARSE_DEPTH`].
pub fn from_slice_deep<T: DeserializeOwned>(bytes: &[u8]) -> serde_json::Result<T> {
    if nesting(bytes) > MAX_PARSE_DEPTH {
        return Err(serde::de::Error::custom(format!(
            "JSON nesting exceeds {MAX_PARSE_DEPTH}"
        )));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    deserializer.disable_recursion_limit();
    let value = T::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

/// serde_json formatting with `JSON.stringify` numbers: `1.0` → `1`, `1e21` → `1e+21`, `-0` → `0`,
/// integers beyond 2^53 rounded like JavaScript doubles.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsFormatter;

const SAFE: u64 = (1 << 53) - 1;

impl serde_json::ser::Formatter for JsFormatter {
    fn write_f32<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f32) -> io::Result<()> {
        self.write_f64(writer, f64::from(value))
    }

    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(format_number(value).as_bytes())
    }

    fn write_u64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: u64) -> io::Result<()> {
        if value > SAFE {
            self.write_f64(writer, value as f64)
        } else {
            writer.write_all(value.to_string().as_bytes())
        }
    }

    fn write_i64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: i64) -> io::Result<()> {
        if value.unsigned_abs() > SAFE {
            self.write_f64(writer, value as f64)
        } else {
            writer.write_all(value.to_string().as_bytes())
        }
    }

    fn write_u128<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: u128) -> io::Result<()> {
        self.write_f64(writer, value as f64)
    }

    fn write_i128<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: i128) -> io::Result<()> {
        self.write_f64(writer, value as f64)
    }
}

/// `JSON.stringify` of a serializable value: its own field/map order, JavaScript number spelling.
/// Non-finite floats become `null` (serde_json never shows them to the formatter).
pub fn to_js_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, FlowerError> {
    let mut output = Vec::new();
    write_js(&mut output, value)?;
    Ok(output)
}

/// [`to_js_vec`] as a `String`.
pub fn to_js_string<T: Serialize + ?Sized>(value: &T) -> Result<String, FlowerError> {
    Ok(String::from_utf8(to_js_vec(value)?).expect("serde_json writes UTF-8"))
}

pub(crate) fn write_js<T: Serialize + ?Sized>(
    output: &mut Vec<u8>,
    value: &T,
) -> Result<(), FlowerError> {
    let mut serializer = serde_json::Serializer::with_formatter(output, JsFormatter);
    value.serialize(&mut serializer).map_err(|error| {
        FlowerError::invalid(format!("Flower values must be JSON values: {error}"))
    })
}
