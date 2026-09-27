//! Canonical JSON parity with `sdk/json.ts` (goldens from `parity/canonical.ts`) and ports of
//! `sdk/json.test.ts`.

use flower_client::json::{
    canonical_json, canonical_json_checked, canonical_json_of, compare_utf16, format_number, from_slice_deep, nesting,
    stringify_len, to_js_string, truthy,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::cmp::Ordering;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    input: String,
    canonical: String,
    stringify_bytes: usize,
    bits: Option<String>,
}

#[test]
fn canonical_json_matches_the_typescript_goldens() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("vectors/canonical.json")).unwrap();
    assert!(cases.len() > 400);
    let (mut failures, mut misparsed) = (Vec::new(), 0);
    for case in &cases {
        let parsed: Value = from_slice_deep(case.input.as_bytes()).unwrap_or_else(|error| panic!("{}: {error}", case.input));
        // For numbers, format the double JSON.parse chose: serde_json parses decimals exactly only
        // with float_roundtrip, which this crate must not unify into the Flower server.
        let value = match &case.bits {
            Some(bits) => json!(f64::from_bits(u64::from_str_radix(bits, 16).unwrap())),
            None => parsed.clone(),
        };
        if canonical_json(&parsed) != case.canonical {
            misparsed += 1;
        }
        let canonical = canonical_json(&value);
        if canonical != case.canonical {
            failures.push(format!("{} => {canonical}, TS {}", case.input, case.canonical));
        }
        if stringify_len(&value) != case.stringify_bytes {
            failures.push(format!("{}: stringify length {} vs TS {}", case.input, stringify_len(&value), case.stringify_bytes));
        }
    }
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));
    // Informational: without float_roundtrip, serde_json misrounds many long decimals by an ulp.
    eprintln!("{misparsed} of {} inputs parse differently from JSON.parse in this build", cases.len());
}

#[test]
fn canonical_primitive_keys_retain_json_escaping_number_spelling_and_depth() {
    for (value, expected) in [
        (json!(null), "null"),
        (json!(true), "true"),
        (json!(-0.0), "0"),
        (json!("a\0b🌸"), r#""a\u0000b🌸""#),
        (json!(["tenant-a", "store-1"]), r#"["tenant-a","store-1"]"#),
        (json!([null, true, false, -0.0, 1e-7, 1e21, "\"\\"]), r#"[null,true,false,0,1e-7,1e+21,"\"\\"]"#),
    ] {
        assert_eq!(canonical_json(&value), expected);
    }
    let shared = json!({ "\u{e000}": 1, "😀": 2, "10": 10, "2": 2 });
    assert_eq!(
        canonical_json(&json!([shared, shared])),
        "[{\"10\":10,\"2\":2,\"😀\":2,\"\u{e000}\":1},{\"10\":10,\"2\":2,\"😀\":2,\"\u{e000}\":1}]"
    );
    let mut boundary = json!(["key"]);
    for _ in 0..127 {
        boundary = json!([boundary]);
    }
    assert!(canonical_json_checked(&boundary).is_ok());
    let error = canonical_json_checked(&json!([boundary])).unwrap_err();
    assert!(error.message.contains("nesting exceeds"), "{error}");
    assert!(!error.is_transient());
}

#[test]
fn keys_sort_by_utf16_code_units_not_utf8() {
    assert_eq!(compare_utf16("😀", "\u{ffff}"), Ordering::Less);
    assert_eq!("😀".cmp("\u{ffff}"), Ordering::Greater, "UTF-8 order differs");
    assert_eq!(compare_utf16("10", "2"), Ordering::Less);
    assert_eq!(compare_utf16("", "a"), Ordering::Less);
    assert_eq!(
        canonical_json(&json!({"\u{ffff}": 1, "😀": 2, "\u{e000}": 3, "~": 4})),
        "{\"~\":4,\"😀\":2,\"\u{e000}\":3,\"\u{ffff}\":1}"
    );
}

#[test]
fn canonical_json_of_structs_sorts_fields() {
    #[derive(serde::Serialize)]
    struct Args {
        zeta: f64,
        alpha: Option<u64>,
        big: u64,
    }
    let args = Args {
        zeta: 1.0,
        alpha: None,
        big: u64::MAX,
    };
    assert_eq!(canonical_json_of(&args).unwrap(), r#"{"alpha":null,"big":18446744073709552000,"zeta":1}"#);
    // JSON.stringify spelling keeps the field order.
    assert_eq!(to_js_string(&args).unwrap(), r#"{"zeta":1,"alpha":null,"big":18446744073709552000}"#);
}

#[test]
fn js_number_spelling() {
    for (value, expected) in [
        (0.0, "0"),
        (-0.0, "0"),
        (1.0, "1"),
        (1.5, "1.5"),
        (1e21, "1e+21"),
        (1e20, "100000000000000000000"),
        (1e-7, "1e-7"),
        (1e-6, "0.000001"),
        (5e-324, "5e-324"),
        (-1.7976931348623157e308, "-1.7976931348623157e+308"),
        (0.1 + 0.2, "0.30000000000000004"),
    ] {
        assert_eq!(format_number(value), expected);
        assert_eq!(to_js_string(&value).unwrap(), expected);
    }
    assert_eq!(to_js_string(&i64::MIN).unwrap(), "-9223372036854776000");
    assert_eq!(to_js_string(&9_007_199_254_740_993u64).unwrap(), "9007199254740992");
    assert_eq!(to_js_string(&9_007_199_254_740_991u64).unwrap(), "9007199254740991");
    assert_eq!(to_js_string(&f64::NAN).unwrap(), "null", "serde_json hides non-finite floats as null");
}

#[test]
fn truthiness_is_javascripts() {
    for value in [json!(null), json!(false), json!(0), json!(-0.0), json!(0.0), json!("")] {
        assert!(!truthy(&value), "{value}");
    }
    for value in [json!(true), json!(1), json!(-1), json!("0"), json!([]), json!({})] {
        assert!(truthy(&value), "{value}");
    }
}

#[test]
fn deep_parsing_goes_beyond_serde_defaults_but_stays_bounded() {
    let deep = format!("{}1{}", "[".repeat(200), "]".repeat(200));
    assert!(serde_json::from_str::<Value>(&deep).is_err(), "serde_json's default limit is 128");
    assert!(from_slice_deep::<Value>(deep.as_bytes()).is_ok());
    let hostile = "[".repeat(100_000);
    assert!(from_slice_deep::<Value>(hostile.as_bytes()).is_err());
    assert_eq!(nesting(br#"{"a":"[[[","b":[{"c":"\"]"}]}"#), 3);
}
