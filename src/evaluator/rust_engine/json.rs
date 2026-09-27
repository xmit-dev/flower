//! ECMAScript-compatible JSON identities without entering an application VM.

use std::cmp::Ordering;

use serde_json::{Map, Value};

use super::{EngineError, EngineResult};

pub(super) fn compare(left: &str, right: &str) -> Ordering {
    if left.is_ascii() && right.is_ascii() {
        left.cmp(right)
    } else {
        left.encode_utf16().cmp(right.encode_utf16())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Key(pub String);

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// JSON.stringify-compatible numbers and UTF-16-sorted object properties.
/// Integer-looking keys deliberately retain lexical order for persisted IDs.
pub fn canonical_json(value: &Value) -> String {
    fn append(value: &Value, output: &mut String) {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(number) => {
                let value = number.as_f64().expect("JSON number is finite");
                if value == 0.0 {
                    output.push('0');
                } else {
                    output.push_str(ryu_js::Buffer::new().format_finite(value));
                }
            }
            Value::String(value) => append_string(value, output),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    append(value, output);
                }
                output.push(']');
            }
            Value::Object(values) => {
                output.push('{');
                let mut member = |index: usize, key: &str, value: &Value| {
                    if index != 0 {
                        output.push(',');
                    }
                    append_string(key, output);
                    output.push(':');
                    append(value, output);
                };
                if values.keys().all(|key| key.is_ascii()) {
                    for (index, (key, value)) in values.iter().enumerate() {
                        member(index, key, value);
                    }
                } else {
                    let mut entries: Vec<_> = values.iter().collect();
                    entries.sort_unstable_by(|(left, _), (right, _)| compare(left, right));
                    for (index, (key, value)) in entries.into_iter().enumerate() {
                        member(index, key, value);
                    }
                }
                output.push('}');
            }
        }
    }
    let mut output = String::new();
    append(value, &mut output);
    output
}

fn append_string(value: &str, output: &mut String) {
    // UTF-8 bytes outside ASCII need no JSON escaping. Ordinary names and key
    // components can append directly without allocating a temporary encoding.
    if value
        .bytes()
        .all(|byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
    {
        output.push('"');
        output.push_str(value);
        output.push('"');
    } else {
        output.push_str(&serde_json::to_string(value).expect("string encodes"));
    }
}

pub(super) fn string_len(value: &str) -> usize {
    // Count the original UTF-8 bytes once, then only the extra ASCII escaping.
    // Decoding every Unicode scalar adds work without changing its byte count.
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

/// Exact JSON.stringify byte count without allocating or cloning output JSON.
pub(super) fn encoded_len(value: &Value) -> usize {
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
            let value = number.as_f64().expect("JSON number");
            if value == 0.0 {
                1
            } else {
                ryu_js::Buffer::new().format_finite(value).len()
            }
        }
        Value::String(value) => string_len(value),
        Value::Array(values) => {
            2 + values.len().saturating_sub(1) + values.iter().map(encoded_len).sum::<usize>()
        }
        Value::Object(values) => {
            2 + values.len().saturating_sub(1)
                + values
                    .iter()
                    .map(|(key, value)| string_len(key) + 1 + encoded_len(value))
                    .sum::<usize>()
        }
    }
}

/// Conservative retained JSON allocation estimate, including structural costs
/// that are much larger than JSON text for arrays of small scalar values.
pub(super) fn allocation_cost(value: &Value) -> usize {
    match value {
        Value::String(value) => 64 + value.capacity(),
        Value::Array(values) => 64 + values.iter().map(allocation_cost).sum::<usize>(),
        Value::Object(values) => {
            64 + values
                .iter()
                .map(|(key, value)| 64 + key.capacity() + allocation_cost(value))
                .sum::<usize>()
        }
        _ => 64,
    }
}

pub(super) fn equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| equal(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .all(|(key, value)| right.get(key).is_some_and(|other| equal(value, other)))
        }
        _ => left == right,
    }
}

pub(super) fn depth(value: &Value, initial: usize, code: &str) -> EngineResult<()> {
    let mut pending = vec![(value, initial)];
    while let Some((value, depth)) = pending.pop() {
        if depth > 128 {
            return Err(EngineError::new(code, "JSON nesting exceeds 128 levels"));
        }
        match value {
            Value::Array(values) => pending.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => {
                pending.extend(values.values().map(|value| (value, depth + 1)))
            }
            _ => {}
        }
    }
    Ok(())
}

/// JSON entering from Rust has the same numeric domain as JSON.parse in QuickJS.
pub(super) fn normalize(mut value: Value, code: &str) -> EngineResult<Value> {
    depth(&value, 0, code)?;
    fn visit(value: &mut Value) {
        match value {
            Value::Number(number) => {
                // Small integer JSON numbers already have precisely the form
                // produced by JSON.parse(JSON.stringify(number)). Keep floats,
                // including -0 and 1.0, on the existing normalization path.
                if number.as_i64().is_some_and(|integer| {
                    (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&integer)
                }) {
                    return;
                }
                let number = number.as_f64().expect("JSON number");
                let mut buffer = ryu_js::Buffer::new();
                let encoded = if number == 0.0 {
                    "0"
                } else {
                    buffer.format_finite(number)
                };
                *value = serde_json::from_str(encoded).expect("finite ECMAScript JSON number");
            }
            Value::Array(values) => values.iter_mut().for_each(visit),
            Value::Object(values) => values.values_mut().for_each(visit),
            _ => {}
        }
    }
    visit(&mut value);
    Ok(value)
}

pub(super) fn record<'a>(
    value: &'a Value,
    label: &str,
    code: &str,
) -> EngineResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| EngineError::new(code, format!("{label} must be an object")))
}

pub(super) fn string<'a>(value: &'a Value, label: &str, code: &str) -> EngineResult<&'a str> {
    value
        .as_str()
        .ok_or_else(|| EngineError::new(code, format!("{label} must be a string")))
}

pub(super) fn source_id(collection: &str, key: &str) -> String {
    format!(
        "source:{}",
        serde_json::to_string(&[collection, key]).expect("source identity")
    )
}

pub(super) fn collection_id(collection: &str) -> String {
    format!(
        "collection:{}",
        serde_json::to_string(collection).expect("collection identity")
    )
}

pub(super) fn cell_id(name: &str, args: &Value) -> String {
    format!(
        "cell:[{},{}]",
        serde_json::to_string(name).expect("name encodes"),
        canonical_json(args)
    )
}

pub(super) fn root_id(name: &str, args: &Value) -> String {
    format!(
        "root:[{},{}]",
        serde_json::to_string(name).expect("name encodes"),
        canonical_json(args)
    )
}

pub(super) fn source_pair(id: &str) -> EngineResult<(String, String)> {
    let pair: (String, String) = serde_json::from_str(&id[7..])
        .map_err(|_| EngineError::new("INPUT_INVALID", "Malformed stored source key"))?;
    if source_id(&pair.0, &pair.1) != id {
        return Err(EngineError::new(
            "INPUT_INVALID",
            "Malformed stored source key",
        ));
    }
    Ok(pair)
}

#[cfg(test)]
mod normalization_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_encoding_and_byte_counts_match_json_for_controls_and_unicode() {
        let all_controls: String = (0u8..=127).map(char::from).collect();
        for value in [
            "",
            "pizza.shopSummary",
            "tenant-17",
            "quote\"slash\\",
            "é中文🌸",
            "\u{2028}\u{2029}\u{d7ff}\u{e000}\u{10ffff}",
            &all_controls,
        ] {
            let expected = serde_json::to_string(value).unwrap();
            assert_eq!(string_len(value), expected.len());
            let mut actual = String::from("prefix:");
            append_string(value, &mut actual);
            assert_eq!(&actual[7..], expected);
            assert_eq!(
                canonical_json(&json!([value, {value: value}])),
                serde_json::to_string(&json!([value, {value: value}])).unwrap()
            );
        }
        // Include every valid scalar, with multibyte sequences crossing many
        // positions in the encoder's scan, without constructing invalid UTF-8.
        let unicode: String = (0..=0x10ffff).filter_map(char::from_u32).collect();
        let expected = serde_json::to_string(&unicode).unwrap();
        assert_eq!(string_len(&unicode), expected.len());
        let mut actual = String::new();
        append_string(&unicode, &mut actual);
        assert_eq!(actual, expected);
    }

    #[test]
    fn ascii_comparisons_and_canonical_maps_keep_utf16_identity() {
        let keys = [
            "", "\0", "1", "10", "2", "A", "a", "aa", "a\0", "\u{7f}", "é", "\u{d7ff}", "𐀀", "😀",
            "\u{e000}", "\u{ffff}",
        ];
        for left in keys {
            for right in keys {
                assert_eq!(
                    compare(left, right),
                    left.encode_utf16().cmp(right.encode_utf16())
                );
            }
        }
        assert_eq!(
            canonical_json(&json!({"2": 1e21, "10": {"z": -0.0, "a": "🌸"}, "1": [true, null]})),
            r#"{"1":[true,null],"10":{"a":"🌸","z":0},"2":1e+21}"#
        );
    }

    #[test]
    fn numbers_keep_ecmascript_rounding_and_json_representation() {
        for (input, expected) in [
            ("0", "0"),
            ("-1", "-1"),
            ("1.0", "1"),
            ("-0.0", "0"),
            ("9007199254740991", "9007199254740991"),
            ("-9007199254740991", "-9007199254740991"),
            ("9007199254740992", "9007199254740992"),
            ("9007199254740993", "9007199254740992"),
            ("-9007199254740993", "-9007199254740992"),
            ("1000000000000000128", "1000000000000000100"),
            ("18446744073709551615", "18446744073709552000"),
            ("9223372036854775807", "9223372036854776000"),
            ("-9223372036854775808", "-9223372036854776000"),
            ("1.25", "1.25"),
            ("1e-7", "1e-7"),
            ("1e-6", "0.000001"),
            ("1e20", "100000000000000000000"),
            ("1e21", "1e21"),
            ("5e-324", "5e-324"),
        ] {
            let actual = normalize(serde_json::from_str(input).unwrap(), "INPUT_INVALID").unwrap();
            let expected: Value = serde_json::from_str(expected).unwrap();
            assert_eq!(actual, expected, "normalizing {input}");
            assert_eq!(
                serde_json::to_string(&actual).unwrap(),
                serde_json::to_string(&expected).unwrap(),
                "numeric representation after normalizing {input}"
            );
        }
    }

    #[test]
    fn nested_values_normalize_without_rebuilding_container_storage() {
        let mut array = Vec::with_capacity(64);
        array.extend([json!(1.0), json!(-0.0), json!(9_007_199_254_740_993_u64)]);
        let value = json!({
            "array": Value::Array(array),
            "object": {"number": 1.0, "plain": true, "nothing": null, "text": "flower 🌸"}
        });
        let array = value["array"].as_array().unwrap();
        let array_storage = array.as_ptr();
        let array_capacity = array.capacity();
        let property_storage = &value["object"]["number"] as *const Value;
        let actual = normalize(value, "INVALID_VALUE").unwrap();
        assert_eq!(
            actual,
            json!({
                "array": [1, 0, 9_007_199_254_740_992_u64],
                "object": {"number": 1, "plain": true, "nothing": null, "text": "flower 🌸"}
            })
        );
        assert_eq!(actual["array"].as_array().unwrap().as_ptr(), array_storage);
        assert_eq!(
            actual["array"].as_array().unwrap().capacity(),
            array_capacity
        );
        assert_eq!(
            &actual["object"]["number"] as *const Value,
            property_storage
        );
    }

    #[test]
    fn normalization_retains_the_depth_boundary_and_error_code() {
        let mut value = json!(1.0);
        for _ in 0..128 {
            value = Value::Array(vec![value]);
        }
        let normalized = normalize(value.clone(), "INPUT_INVALID").unwrap();
        let mut leaf = &normalized;
        for _ in 0..128 {
            leaf = &leaf[0];
        }
        assert_eq!(*leaf, json!(1));
        let error = normalize(Value::Array(vec![value]), "INVALID_VALUE").unwrap_err();
        assert_eq!(error.code, "INVALID_VALUE");
        assert_eq!(error.message, "JSON nesting exceeds 128 levels");
    }
}
