//! Feature-unification guards: this crate is built inside Flower's workspace, where serde_json's
//! `preserve_order` would break the server's sorted canonical JSON and `arbitrary_precision` its
//! number handling. Neither may ever be enabled here.

use serde_json::{Map, Value, json};

#[test]
fn serde_json_maps_stay_sorted() {
    let mut map = Map::new();
    map.insert("b".into(), json!(1));
    map.insert("a".into(), json!(2));
    let keys: Vec<&str> = map.keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        ["a", "b"],
        "serde_json preserve_order is enabled in this build"
    );
}

#[test]
fn serde_json_numbers_are_plain() {
    let value: Value = serde_json::from_str("1.10").unwrap();
    assert_eq!(
        value.to_string(),
        "1.1",
        "serde_json arbitrary_precision is enabled in this build"
    );
    assert!(value.is_f64());
}
