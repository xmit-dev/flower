//! Runtime validators with the TypeScript SDK's `v` semantics and messages.
//! Schemas are constant data, so a module declares them as `const`s.
use crate::{Value, json};
use alloc::{format, string::String, vec::Vec};

#[derive(Clone, Copy)]
pub struct Pattern {
    /// How the pattern prints in messages, like a RegExp: `/^[a-z]+$/`.
    pub source: &'static str,
    pub test: fn(&str) -> bool,
}

#[derive(Clone, Copy)]
pub struct Field {
    pub name: &'static str,
    pub schema: Schema,
    pub optional: bool,
}

#[derive(Clone, Copy)]
pub enum Schema {
    String {
        min: Option<f64>,
        max: Option<f64>,
        pattern: Option<Pattern>,
    },
    Number {
        min: Option<f64>,
        max: Option<f64>,
        integer: bool,
    },
    Boolean,
    Null,
    /// Any JSON value.
    Json,
    Array {
        item: &'static Schema,
        min: Option<f64>,
        max: Option<f64>,
    },
    Tuple(&'static [Schema]),
    /// Named members; any other member is an error.
    Object(&'static [Field]),
    Nullable(&'static Schema),
}

/// Constructors mirroring the TypeScript SDK's `v`.
pub mod v {
    use super::*;
    pub const fn string() -> Schema {
        Schema::String {
            min: None,
            max: None,
            pattern: None,
        }
    }
    pub const fn number() -> Schema {
        Schema::Number {
            min: None,
            max: None,
            integer: false,
        }
    }
    pub const fn int() -> Schema {
        Schema::Number {
            min: None,
            max: None,
            integer: true,
        }
    }
    pub const fn boolean() -> Schema {
        Schema::Boolean
    }
    pub const fn null() -> Schema {
        Schema::Null
    }
    pub const fn json() -> Schema {
        Schema::Json
    }
    pub const fn array(item: &'static Schema) -> Schema {
        Schema::Array {
            item,
            min: None,
            max: None,
        }
    }
    pub const fn tuple(items: &'static [Schema]) -> Schema {
        Schema::Tuple(items)
    }
    pub const fn object(fields: &'static [Field]) -> Schema {
        Schema::Object(fields)
    }
    pub const fn nullable(inner: &'static Schema) -> Schema {
        Schema::Nullable(inner)
    }
    pub const fn field(name: &'static str, schema: Schema) -> Field {
        Field {
            name,
            schema,
            optional: false,
        }
    }
    pub const fn optional(name: &'static str, schema: Schema) -> Field {
        Field {
            name,
            schema,
            optional: true,
        }
    }
}

impl Schema {
    /// Minimum length, item count or value.
    pub const fn min(self, bound: f64) -> Schema {
        match self {
            Schema::String { max, pattern, .. } => Schema::String {
                min: Some(bound),
                max,
                pattern,
            },
            Schema::Number { max, integer, .. } => Schema::Number {
                min: Some(bound),
                max,
                integer,
            },
            Schema::Array { item, max, .. } => Schema::Array {
                item,
                min: Some(bound),
                max,
            },
            _ => panic!("min applies to strings, numbers and arrays"),
        }
    }
    /// Maximum length, item count or value.
    pub const fn max(self, bound: f64) -> Schema {
        match self {
            Schema::String { min, pattern, .. } => Schema::String {
                min,
                max: Some(bound),
                pattern,
            },
            Schema::Number { min, integer, .. } => Schema::Number {
                min,
                max: Some(bound),
                integer,
            },
            Schema::Array { item, min, .. } => Schema::Array {
                item,
                min,
                max: Some(bound),
            },
            _ => panic!("max applies to strings, numbers and arrays"),
        }
    }
    pub const fn pattern(self, pattern: Pattern) -> Schema {
        match self {
            Schema::String { min, max, .. } => Schema::String {
                min,
                max,
                pattern: Some(pattern),
            },
            _ => panic!("pattern applies to strings"),
        }
    }

    /// Returns Ok when the value matches.
    pub fn parse(&self, value: &Value) -> Result<(), ValidationError> {
        self.check(value, &mut Vec::new())
    }

    pub fn is(&self, value: &Value) -> bool {
        self.parse(value).is_ok()
    }

    fn check(&self, value: &Value, path: &mut Vec<PathPart>) -> Result<(), ValidationError> {
        let fail = |path: &[PathPart], reason: String| {
            Err(ValidationError {
                reason,
                path: path.to_vec(),
            })
        };
        match self {
            Schema::String { min, max, pattern } => {
                let Some(text) = value.as_str() else {
                    return fail(path, "must be a string".into());
                };
                let length = json::length(text) as f64;
                if let Some(min) = *min
                    && length < min
                {
                    return fail(
                        path,
                        if min == 1.0 {
                            "must not be empty".into()
                        } else {
                            format!("must contain at least {} characters", json::number(min))
                        },
                    );
                }
                if let Some(max) = *max
                    && length > max
                {
                    return fail(
                        path,
                        format!("must contain at most {} characters", json::number(max)),
                    );
                }
                if let Some(pattern) = pattern
                    && !(pattern.test)(text)
                {
                    return fail(path, format!("must match {}", pattern.source));
                }
            }
            Schema::Number { min, max, integer } => {
                let Some(number) = value.as_f64() else {
                    return fail(path, "must be a finite number".into());
                };
                if *integer && !is_safe_integer(number) {
                    return fail(path, "must be a safe integer".into());
                }
                if let Some(min) = *min
                    && number < min
                {
                    return fail(path, format!("must be at least {}", json::number(min)));
                }
                if let Some(max) = *max
                    && number > max
                {
                    return fail(path, format!("must be at most {}", json::number(max)));
                }
            }
            Schema::Boolean => {
                if value.as_bool().is_none() {
                    return fail(path, "must be a boolean".into());
                }
            }
            Schema::Null => {
                if !value.is_null() {
                    return fail(path, "must be null".into());
                }
            }
            Schema::Json => {}
            Schema::Array { item, min, max } => {
                let Some(items) = value.as_array() else {
                    return fail(path, "must be an array".into());
                };
                let count = items.len() as f64;
                if let Some(min) = *min
                    && count < min
                {
                    return fail(
                        path,
                        format!("must contain at least {} items", json::number(min)),
                    );
                }
                if let Some(max) = *max
                    && count > max
                {
                    return fail(
                        path,
                        format!("must contain at most {} items", json::number(max)),
                    );
                }
                for (index, element) in items.iter().enumerate() {
                    path.push(PathPart::Index(index));
                    item.check(element, path)?;
                    path.pop();
                }
            }
            Schema::Tuple(schemas) => {
                let items = value
                    .as_array()
                    .filter(|items| items.len() == schemas.len());
                let Some(items) = items else {
                    return fail(path, format!("must be an array of {} items", schemas.len()));
                };
                for (index, (schema, element)) in schemas.iter().zip(items).enumerate() {
                    path.push(PathPart::Index(index));
                    schema.check(element, path)?;
                    path.pop();
                }
            }
            Schema::Object(fields) => {
                let Some(map) = value.as_object() else {
                    return fail(path, "must be an object".into());
                };
                for field in fields.iter() {
                    let Some(member) = map.get(field.name) else {
                        if field.optional {
                            continue;
                        }
                        return fail(path, format!("is missing {}", json::string(field.name)));
                    };
                    path.push(PathPart::Key(field.name.into()));
                    field.schema.check(member, path)?;
                    path.pop();
                }
                for key in map.keys() {
                    if !fields.iter().any(|field| field.name == key) {
                        return fail(
                            path,
                            format!("has unexpected property {}", json::string(key)),
                        );
                    }
                }
            }
            Schema::Nullable(inner) => {
                if !value.is_null() {
                    inner.check(value, path)?;
                }
            }
        }
        Ok(())
    }
}

/// `Number.isSafeInteger`.
pub fn is_safe_integer(value: f64) -> bool {
    value.abs() <= 9_007_199_254_740_991.0 && is_integral(value)
}

/// A finite number without a fractional part.
pub fn is_integral(value: f64) -> bool {
    // Every f64 of magnitude 2^52 or more is an integer.
    if value.abs() >= 4_503_599_627_370_496.0 {
        value.is_finite()
    } else {
        value as i64 as f64 == value
    }
}

/// `2 ** exponent` for integral exponents.
pub fn pow2(exponent: f64) -> f64 {
    let exponent = exponent.clamp(-1_074.0, 1_023.0) as i32;
    let mut value = 1.0;
    for _ in 0..exponent.unsigned_abs() {
        value = if exponent < 0 {
            value / 2.0
        } else {
            value * 2.0
        };
    }
    value
}

#[derive(Clone, Debug, PartialEq)]
pub enum PathPart {
    Key(String),
    Index(usize),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidationError {
    pub reason: String,
    pub path: Vec<PathPart>,
}

impl ValidationError {
    /// `path: reason`, with the path written like a JavaScript accessor.
    pub fn message(&self) -> String {
        let mut text = String::new();
        for part in &self.path {
            match part {
                PathPart::Index(index) => text.push_str(&format!("[{index}]")),
                PathPart::Key(key) if identifier(key) => {
                    if !text.is_empty() {
                        text.push('.');
                    }
                    text.push_str(key);
                }
                PathPart::Key(key) => text.push_str(&format!("[{}]", json::string(key))),
            }
        }
        if text.is_empty() {
            self.reason.clone()
        } else {
            format!("{text}: {}", self.reason)
        }
    }

    /// The path as JSON, for failure details.
    pub fn path_value(&self) -> Value {
        Value::Array(
            self.path
                .iter()
                .map(|part| match part {
                    PathPart::Key(key) => Value::from(key),
                    PathPart::Index(index) => Value::from(*index),
                })
                .collect(),
        )
    }
}

/// `/^[A-Za-z_$][\w$]*$/`
fn identifier(key: &str) -> bool {
    let mut bytes = key.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'$')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'$')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array, object};

    const ID: Schema = v::string().pattern(Pattern {
        source: "/^[a-z]+$/",
        test: |text| !text.is_empty() && text.bytes().all(|b| b.is_ascii_lowercase()),
    });
    const PAIR: Schema = v::tuple(&[ID, ID]);
    const ORDER: Schema = v::object(&[
        v::field("shop", PAIR),
        v::field("quantity", v::int().min(1.0).max(4.0)),
        v::optional("note", v::string().min(1.0)),
    ]);

    fn message(schema: &Schema, value: Value) -> String {
        schema.parse(&value).unwrap_err().message()
    }

    #[test]
    fn messages_and_paths_match_the_typescript_sdk() {
        assert!(ORDER.is(&object! {"shop" => array!["a", "b"], "quantity" => 4}));
        assert_eq!(
            message(
                &ORDER,
                object! {"shop" => array!["a", "b"], "quantity" => 5}
            ),
            "quantity: must be at most 4"
        );
        assert_eq!(
            message(
                &ORDER,
                object! {"shop" => array!["a", "B"], "quantity" => 1}
            ),
            "shop[1]: must match /^[a-z]+$/"
        );
        assert_eq!(
            message(&ORDER, object! {"shop" => array!["a"], "quantity" => 1}),
            "shop: must be an array of 2 items"
        );
        assert_eq!(
            message(&ORDER, object! {"quantity" => 1}),
            "is missing \"shop\""
        );
        assert_eq!(
            message(
                &ORDER,
                object! {"shop" => array!["a", "b"], "quantity" => 1, "x-y" => 1}
            ),
            "has unexpected property \"x-y\""
        );
        assert_eq!(
            message(
                &ORDER,
                object! {"shop" => array!["a", "b"], "quantity" => 1.5}
            ),
            "quantity: must be a safe integer"
        );
        assert_eq!(
            message(
                &ORDER,
                object! {"shop" => array!["a", "b"], "quantity" => 1, "note" => ""}
            ),
            "note: must not be empty"
        );
        assert_eq!(message(&ORDER, Value::Null), "must be an object");
        let error = ORDER
            .parse(&object! {"shop" => array!["a", 1], "quantity" => 1})
            .unwrap_err();
        assert_eq!(error.path_value(), array!["shop", 1]);
        const NESTED: Schema = v::object(&[v::field("a-b", v::array(&v::null()).max(1.0))]);
        assert_eq!(
            message(&NESTED, object! {"a-b" => array![Value::Null, Value::Null]}),
            "[\"a-b\"]: must contain at most 1 items"
        );
        assert_eq!(
            message(&NESTED, object! {"a-b" => array![true]}),
            "[\"a-b\"][0]: must be null"
        );
    }
}
