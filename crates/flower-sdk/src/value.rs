//! Plain JSON data, as JavaScript sees it: numbers are f64 and objects keep
//! their insertion order. The host stores records with canonical key order, so
//! order only matters for enumeration inside a callback.
use alloc::{string::String, vec::Vec};

#[derive(Clone, Debug, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(Map),
}

pub static NULL: Value = Value::Null;

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(value) => Some(*value),
            _ => None,
        }
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(value) => Some(*value),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(value) => Some(value),
            _ => None,
        }
    }
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Value>> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }
    pub fn as_object(&self) -> Option<&Map> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }
    pub fn as_object_mut(&mut self) -> Option<&mut Map> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }
    pub fn into_array(self) -> Option<Vec<Value>> {
        match self {
            Value::Array(items) => Some(items),
            _ => None,
        }
    }
    pub fn into_object(self) -> Option<Map> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// `value[key]`: a member of an object, or null (JavaScript's undefined)
    /// for other values and missing members. Use `has` to tell them apart.
    pub fn get(&self, key: &str) -> &Value {
        self.as_object()
            .and_then(|map| map.get(key))
            .unwrap_or(&NULL)
    }
    /// `Object.hasOwn(value, key)`.
    pub fn has(&self, key: &str) -> bool {
        self.as_object().is_some_and(|map| map.contains_key(key))
    }
    /// `value[index]` of an array, or null.
    pub fn at(&self, index: usize) -> &Value {
        self.as_array()
            .and_then(|items| items.get(index))
            .unwrap_or(&NULL)
    }
    /// A number member, when present.
    pub fn number(&self, key: &str) -> Option<f64> {
        self.get(key).as_f64()
    }
    /// A string member, when present.
    pub fn text(&self, key: &str) -> Option<&str> {
        self.get(key).as_str()
    }
    /// Set a member of an object; other values are left unchanged.
    pub fn set(&mut self, key: &str, value: impl Into<Value>) {
        if let Value::Object(map) = self {
            map.insert(key, value);
        }
    }
    /// `{...self, ...other}` for objects: other's members win.
    pub fn with(mut self, other: &Value) -> Value {
        if let (Value::Object(map), Value::Object(extra)) = (&mut self, other) {
            for (key, value) in extra.iter() {
                map.insert(key, value.clone());
            }
        }
        self
    }
}

/// JavaScript's sameValue for plain data: -0 equals 0 and member order is ignored.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(left), Value::Bool(right)) => left == right,
            (Value::Number(left), Value::Number(right)) => left == right,
            (Value::String(left), Value::String(right)) => left == right,
            (Value::Array(left), Value::Array(right)) => left == right,
            (Value::Object(left), Value::Object(right)) => left == right,
            _ => false,
        }
    }
}

/// Object members in insertion order. Keys are unique.
#[derive(Clone, Debug, Default)]
pub struct Map(Vec<(String, Value)>);

impl Map {
    pub const fn new() -> Self {
        Map(Vec::new())
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Map(Vec::with_capacity(capacity))
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn position(&self, key: &str) -> Option<usize> {
        self.0.iter().position(|(name, _)| name == key)
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.0
            .iter_mut()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value)
    }
    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_some()
    }
    /// Assign a member: an existing key keeps its position.
    pub fn insert(&mut self, key: impl Into<String> + AsRef<str>, value: impl Into<Value>) {
        match self.position(key.as_ref()) {
            Some(index) => self.0[index].1 = value.into(),
            None => self.0.push((key.into(), value.into())),
        }
    }
    /// Append a member known not to exist yet, as decoders do.
    pub(crate) fn push_unique(&mut self, key: String, value: Value) {
        self.0.push((key, value));
    }
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.position(key).map(|index| self.0.remove(index).1)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(key, _)| key.as_str())
    }
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.0.iter().map(|(_, value)| value)
    }
}

impl PartialEq for Map {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key).is_some_and(|other| other == value))
    }
}

impl IntoIterator for Map {
    type Item = (String, Value);
    type IntoIter = alloc::vec::IntoIter<(String, Value)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<K: Into<String> + AsRef<str>, V: Into<Value>> FromIterator<(K, V)> for Map {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        let mut map = Map::new();
        for (key, value) in entries {
            map.insert(key, value);
        }
        map
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}
impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Number(value)
    }
}
macro_rules! numbers {
    ($($type:ty),*) => {$(
        impl From<$type> for Value {
            fn from(value: $type) -> Self {
                Value::Number(value as f64)
            }
        }
    )*};
}
numbers!(i32, u32, i64, u64, usize);
impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.into())
    }
}
impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}
impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Value::String(value.clone())
    }
}
impl From<Vec<Value>> for Value {
    fn from(items: Vec<Value>) -> Self {
        Value::Array(items)
    }
}
impl From<Map> for Value {
    fn from(map: Map) -> Self {
        Value::Object(map)
    }
}
impl From<&Value> for Value {
    fn from(value: &Value) -> Self {
        value.clone()
    }
}
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        value.map_or(Value::Null, Into::into)
    }
}

/// `object! { "key" => value, … }` builds an object in the order written.
#[macro_export]
macro_rules! object {
    ($($key:expr => $value:expr),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut map = $crate::Map::new();
        $(map.insert($key, $value);)*
        $crate::Value::Object(map)
    }};
}

/// `array![a, b, …]` builds an array of values.
#[macro_export]
macro_rules! array {
    ($($value:expr),* $(,)?) => {
        $crate::Value::Array($crate::__vec![$($crate::Value::from($value)),*])
    };
}
