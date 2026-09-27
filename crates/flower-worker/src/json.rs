//! JSON helpers: arguments serialized in the TS SDK's key order, and `storable()`.

use serde::Serialize;
use serde::ser;
use serde_json::value::RawValue;

/// Serialize request arguments, keeping the field order of `value`'s type.
pub(crate) fn raw<T: Serialize + ?Sized>(value: &T) -> Box<RawValue> {
    to_js_raw(value).expect("worker arguments serialize")
}

/// `JSON.stringify`: serde_json's output with JS number spelling (`1e+21`, `-0` as `0`), so the
/// bytes a worker sends equal the TS SDK's. Keys keep the order they serialize in.
pub fn to_js_raw<T: Serialize + ?Sized>(value: &T) -> Result<Box<RawValue>, serde_json::Error> {
    let mut out = Vec::with_capacity(128);
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut out,
        JsFormatter,
    ))?;
    let text = String::from_utf8(out).expect("serde_json writes UTF-8");
    RawValue::from_string(text)
}

/// Numbers as `JSON.stringify` spells them.
struct JsFormatter;

impl serde_json::ser::Formatter for JsFormatter {
    fn write_f64<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f64,
    ) -> std::io::Result<()> {
        if !value.is_finite() {
            return writer.write_all(b"null");
        }
        if value == 0.0 {
            return writer.write_all(b"0");
        }
        writer.write_all(ryu_js::Buffer::new().format_finite(value).as_bytes())
    }

    fn write_f32<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        if !value.is_finite() {
            return writer.write_all(b"null");
        }
        if value == 0.0 {
            return writer.write_all(b"0");
        }
        writer.write_all(ryu_js::Buffer::new().format_finite(value).as_bytes())
    }
}

/// `storable(result)`: the result as Flower will store it. One it cannot store fails the attempt
/// with the reason. `canonicalJson`'s checks that a Rust value can fail are finite numbers and
/// nesting of at most 128.
pub(crate) fn storable<R: Serialize + ?Sized>(result: &R) -> Result<Box<RawValue>, String> {
    checked_raw(result).map_err(|reason| format!("The result cannot be stored: {reason}"))
}

/// Serialize a value `canonicalJson` accepts, or say why it doesn't.
pub(crate) fn checked_raw<R: Serialize + ?Sized>(value: &R) -> Result<Box<RawValue>, String> {
    value
        .serialize(Check { depth: 0 })
        .and_then(|()| to_js_raw(value).map_err(|error| Invalid(error.to_string())))
        .map_err(|Invalid(reason)| reason)
}

#[derive(Debug)]
struct Invalid(String);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Invalid {}

impl ser::Error for Invalid {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Invalid(message.to_string())
    }
}

const NESTING: &str = "Flower JSON nesting exceeds 128";
const FINITE: &str = "Flower values require finite numbers";

/// Walks a value the way `canonicalJson` encodes it, without output: every value sits at a depth,
/// and one deeper than 128 is refused.
#[derive(Clone, Copy)]
struct Check {
    depth: usize,
}

impl Check {
    fn at(self) -> Result<(), Invalid> {
        if self.depth > 128 {
            Err(Invalid(NESTING.into()))
        } else {
            Ok(())
        }
    }

    fn child(self) -> Check {
        Check {
            depth: self.depth + 1,
        }
    }

    /// An enum variant's content sits inside `{"Variant": …}`.
    fn variant(self) -> Result<Check, Invalid> {
        self.at()?;
        let inner = self.child();
        inner.at()?;
        Ok(inner)
    }
}

impl ser::Serializer for Check {
    type Ok = ();
    type Error = Invalid;
    type SerializeSeq = Check;
    type SerializeTuple = Check;
    type SerializeTupleStruct = Check;
    type SerializeTupleVariant = Check;
    type SerializeMap = Check;
    type SerializeStruct = Check;
    type SerializeStructVariant = Check;

    fn serialize_bool(self, _: bool) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_i8(self, _: i8) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_i16(self, _: i16) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_i32(self, _: i32) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_i64(self, _: i64) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_i128(self, _: i128) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_u8(self, _: u8) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_u16(self, _: u16) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_u32(self, _: u32) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_u64(self, _: u64) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_u128(self, _: u128) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_f32(self, value: f32) -> Result<(), Invalid> {
        self.serialize_f64(value as f64)
    }
    fn serialize_f64(self, value: f64) -> Result<(), Invalid> {
        self.at()?;
        if value.is_finite() {
            Ok(())
        } else {
            Err(Invalid(FINITE.into()))
        }
    }
    fn serialize_char(self, _: char) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_str(self, _: &str) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_bytes(self, _: &[u8]) -> Result<(), Invalid> {
        // serde_json writes bytes as an array of numbers.
        self.at()?;
        self.child().at()
    }
    fn serialize_none(self) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Invalid> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), Invalid> {
        self.at()
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Invalid> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), Invalid> {
        self.at()?;
        value.serialize(self.child())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Check, Invalid> {
        self.at()?;
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Check, Invalid> {
        self.at()?;
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Check, Invalid> {
        self.at()?;
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Check, Invalid> {
        self.variant()
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Check, Invalid> {
        self.at()?;
        Ok(self)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Check, Invalid> {
        self.at()?;
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Check, Invalid> {
        self.variant()
    }
}

impl ser::SerializeSeq for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeTuple for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeMap for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, _: &T) -> Result<(), Invalid> {
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeStruct for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for Check {
    type Ok = ();
    type Error = Invalid;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Invalid> {
        value.serialize(self.child())
    }
    fn end(self) -> Result<(), Invalid> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn storable_accepts_json_and_keeps_field_order() {
        #[derive(Serialize)]
        struct Outcome {
            zeta: u32,
            alpha: &'static str,
        }
        let raw = storable(&Outcome {
            zeta: 1,
            alpha: "a",
        })
        .unwrap();
        assert_eq!(raw.get(), r#"{"zeta":1,"alpha":"a"}"#);
        assert_eq!(
            storable(&json!({"b": [1, 2.5, null]})).unwrap().get(),
            r#"{"b":[1,2.5,null]}"#
        );
    }

    #[test]
    fn numbers_are_spelled_as_json_stringify_spells_them() {
        let numbers = [
            1e21,
            1e-7,
            -0.0,
            0.1 + 0.2,
            123456789012345680000.0,
            5e-324,
            f64::MAX,
            100.0,
            1.5,
            1e20,
            0.000001,
            -2.5e-8,
        ];
        // JSON.stringify of the same array, from Node.
        let expected = "[1e+21,1e-7,0,0.30000000000000004,123456789012345680000,5e-324,1.7976931348623157e+308,100,1.5,100000000000000000000,0.000001,-2.5e-8]";
        assert_eq!(raw(&numbers).get(), expected);
        assert_eq!(
            raw(&json!({"n": 1e21, "i": -3, "u": u64::MAX})).get(),
            r#"{"i":-3,"n":1e+21,"u":18446744073709551615}"#
        );
    }

    #[test]
    fn storable_refuses_what_canonical_json_refuses() {
        assert_eq!(
            storable(&f64::NAN).unwrap_err(),
            "The result cannot be stored: Flower values require finite numbers"
        );
        assert_eq!(
            storable(&vec![1.0, f64::INFINITY]).unwrap_err(),
            "The result cannot be stored: Flower values require finite numbers"
        );
        let mut deep = json!(1);
        for _ in 0..128 {
            deep = json!([deep]);
        }
        assert!(storable(&deep).is_ok(), "a value at depth 128 is stored");
        let deeper = json!([deep]);
        assert_eq!(
            storable(&deeper).unwrap_err(),
            "The result cannot be stored: Flower JSON nesting exceeds 128"
        );
    }
}
