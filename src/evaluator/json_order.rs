//! Serialize trusted snapshots for the JavaScript reference oracle with the
//! same UTF-16 key ordering used by the JavaScript canonical encoder. Values
//! stay borrowed; only key references are sorted. JSON.parse still applies
//! JavaScript's numeric-index enumeration rules.

use std::collections::BTreeMap;

use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::Value;

/// The caller validates snapshot depth before serialization. Omitting only the
/// top-level bundle keeps application data and nested fields named bundle intact.
pub(super) fn ordered_snapshot(
    data: &BTreeMap<String, Value>,
    omit_bundle: bool,
) -> serde_json::Result<String> {
    serde_json::to_string(&Snapshot { data, omit_bundle })
}

struct Snapshot<'a> {
    data: &'a BTreeMap<String, Value>,
    omit_bundle: bool,
}

fn serialize_object<'a, S: Serializer>(
    entries: impl Iterator<Item = (&'a String, &'a Value)> + Clone,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    // Maps already iterate in UTF-8 order. ASCII keys have exactly the same
    // UTF-16 order, so ordinary application records need no temporary vector.
    if entries.clone().all(|(key, _)| key.is_ascii()) {
        let mut output = serializer.serialize_map(None)?;
        for (key, value) in entries {
            output.serialize_entry(key, &OrderedValue(value))?;
        }
        return output.end();
    }
    let mut entries: Vec<_> = entries.collect();
    entries.sort_unstable_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
    let mut output = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        output.serialize_entry(key, &OrderedValue(value))?;
    }
    output.end()
}

impl Serialize for Snapshot<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialize_object(
            self.data
                .iter()
                .filter(|(key, _)| !self.omit_bundle || key.as_str() != "bundle"),
            serializer,
        )
    }
}

struct OrderedValue<'a>(&'a Value);

pub(super) fn ordered_value(value: &Value) -> serde_json::Result<String> {
    serde_json::to_string(&OrderedValue(value))
}

impl Serialize for OrderedValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(values) => serialize_object(values.iter(), serializer),
            Value::Array(values) => {
                let mut output = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    output.serialize_element(&OrderedValue(value))?;
                }
                output.end()
            }
            value => value.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ascii_fast_path_preserves_nested_unicode_and_numeric_key_order() {
        let value = json!({
            "2": ["\0\"\\", {"\u{e000}": 1, "😀": 2}],
            "10": {"z": null, "a": true},
            "1": false,
            "": "first",
        });
        assert_eq!(
            ordered_value(&value).unwrap(),
            "{\"\":\"first\",\"1\":false,\"10\":{\"a\":true,\"z\":null},\"2\":[\"\\u0000\\\"\\\\\",{\"😀\":2,\"\u{e000}\":1}]}"
        );
    }

    #[test]
    fn every_object_uses_utf16_order_without_reordering_arrays() {
        let data = BTreeMap::from([
            ("\u{e000}".into(), json!(0)),
            (
                "😀".into(),
                json!({"\u{e000}":1,"😀":2,"10":10,"2":2,"1":1}),
            ),
            (
                "array".into(),
                json!([{"\u{e000}":3,"😀":4},false,null,0,"a\n\"\\b"]),
            ),
        ]);
        let encoded = ordered_snapshot(&data, false).unwrap();
        assert_eq!(
            encoded,
            r#"{"array":[{"😀":4,"":3},false,null,0,"a\n\"\\b"],"😀":{"1":1,"10":10,"2":2,"😀":2,"":1},"":0}"#
        );
        assert_eq!(
            serde_json::from_str::<BTreeMap<String, Value>>(&encoded).unwrap(),
            data
        );
    }

    #[test]
    fn omits_only_the_top_level_bundle() {
        let data = BTreeMap::from([
            ("bundle".into(), json!({"javascript":"code"})),
            ("source".into(), json!({"bundle":{"😀":1,"\u{e000}":2}})),
        ]);
        let omitted: BTreeMap<String, Value> =
            serde_json::from_str(&ordered_snapshot(&data, true).unwrap()).unwrap();
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted["source"], data["source"]);
        assert_eq!(
            serde_json::from_str::<BTreeMap<String, Value>>(
                &ordered_snapshot(&data, false).unwrap()
            )
            .unwrap(),
            data
        );
        assert_eq!(ordered_snapshot(&BTreeMap::new(), true).unwrap(), "{}");
    }

    #[test]
    fn parsed_key_enumeration_matches_the_existing_canonical_copy() {
        let data = BTreeMap::from([(
            "record".into(),
            json!({
                "\u{e000}":0,"😀":1,"2":2,"10":10,"1":1,"01":3,"__proto__":4,
                "nested":[{"\u{ffff}":5,"𐀀":6,"a":7}]
            }),
        )]);
        let script = format!(
            r#"
            const ordered={ordered}, original={original};
            function canonical(value) {{
                if (value === null || typeof value !== 'object') return JSON.stringify(value);
                if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
                return '{{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}}';
            }}
            const actual=JSON.parse(ordered), expected=JSON.parse(canonical(JSON.parse(original)));
            JSON.stringify(actual)===JSON.stringify(expected) &&
            JSON.stringify(Object.keys(actual.record))===JSON.stringify(['1','2','10','01','__proto__','nested','😀','\ue000']) &&
            Object.hasOwn(actual.record,'__proto__');
        "#,
            ordered = serde_json::to_string(&ordered_snapshot(&data, true).unwrap()).unwrap(),
            original = serde_json::to_string(&serde_json::to_string(&data).unwrap()).unwrap()
        );
        let shared = super::super::wasm::Limits::new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            128 * 1024 * 1024,
        );
        assert_eq!(
            super::super::wasm::reference_script(&script, shared).unwrap(),
            "true"
        );
    }

    #[test]
    fn preserves_deep_values_and_numeric_representations() {
        let mut value =
            json!({"😀": [null, true, -0.0, -12.5, 9_007_199_254_740_991_u64], "\u{e000}": "end"});
        for _ in 0..123 {
            value = json!({"child":value});
        }
        let data = BTreeMap::from([("record".into(), value)]);
        let encoded = ordered_snapshot(&data, false).unwrap();
        assert_eq!(
            serde_json::from_str::<BTreeMap<String, Value>>(&encoded).unwrap(),
            data
        );
        assert_eq!(encoded.len(), serde_json::to_string(&data).unwrap().len());
        assert!(encoded.contains("9007199254740991"));
    }
}
