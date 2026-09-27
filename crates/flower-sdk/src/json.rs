//! JSON text exactly as JavaScript writes it: `canonical` is the SDK's
//! canonicalJson (keys sorted by UTF-16 code units), `parse` is JSON.parse.
use crate::{Map, Value};
use alloc::{string::String, vec::Vec};
use core::cmp::Ordering;
use core::fmt::Write;

/// Compare strings as JavaScript does: by UTF-16 code units.
pub fn compare(left: &str, right: &str) -> Ordering {
    if left.is_ascii() && right.is_ascii() {
        return left.cmp(right);
    }
    left.encode_utf16().cmp(right.encode_utf16())
}

/// The number of UTF-16 code units, JavaScript's `string.length`.
pub fn length(text: &str) -> usize {
    if text.is_ascii() {
        text.len()
    } else {
        text.encode_utf16().count()
    }
}

/// Stable JSON for identities: object keys sorted, no whitespace.
pub fn canonical(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(&mut out, value);
    out
}

pub fn write_canonical(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => write_number(out, *value),
        Value::String(text) => write_string(out, text),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|(left, _), (right, _)| compare(left, right));
            out.push('{');
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_string(out, key);
                out.push(':');
                write_canonical(out, item);
            }
            out.push('}');
        }
    }
}

/// JSON.stringify of a string.
pub fn write_string(out: &mut String, text: &str) {
    out.push('"');
    let mut start = 0;
    for (index, byte) in text.bytes().enumerate() {
        let escape = match byte {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\x08' => "\\b",
            b'\x0c' => "\\f",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0..0x20 => "",
            _ => continue,
        };
        out.push_str(&text[start..index]);
        if escape.is_empty() {
            let _ = write!(out, "\\u{byte:04x}");
        } else {
            out.push_str(escape);
        }
        start = index + 1;
    }
    out.push_str(&text[start..]);
    out.push('"');
}

/// JSON.stringify of a string, as a new string.
pub fn string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    write_string(&mut out, text);
    out
}

/// JavaScript's Number.prototype.toString for finite numbers.
pub fn write_number(out: &mut String, value: f64) {
    if value == 0.0 {
        out.push('0');
        return;
    }
    if value.abs() < 9_007_199_254_740_992.0 && value as i64 as f64 == value {
        let _ = write!(out, "{}", value as i64);
        return;
    }
    if value < 0.0 {
        out.push('-');
    }
    // Rust's shortest round-trip digits, reformatted with JavaScript's rules.
    let mut scientific = String::new();
    let _ = write!(scientific, "{:e}", value.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i32;
    let n = exponent.parse::<i32>().unwrap_or(0) + 1;
    if k <= n && n <= 21 {
        out.push_str(&digits);
        out.extend(core::iter::repeat_n('0', (n - k) as usize));
    } else if 0 < n && n <= 21 {
        out.push_str(&digits[..n as usize]);
        out.push('.');
        out.push_str(&digits[n as usize..]);
    } else if -6 < n && n <= 0 {
        out.push_str("0.");
        out.extend(core::iter::repeat_n('0', (-n) as usize));
        out.push_str(&digits);
    } else {
        out.push_str(&digits[..1]);
        if k > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        let _ = write!(out, "e{}{}", if n > 0 { '+' } else { '-' }, (n - 1).abs());
    }
}

/// A number as JavaScript prints it.
pub fn number(value: f64) -> String {
    let mut out = String::new();
    write_number(&mut out, value);
    out
}

/// JSON.parse. Returns None for malformed text or lone surrogates.
pub fn parse(text: &str) -> Option<Value> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        offset: 0,
    };
    let value = parser.value(0)?;
    parser.whitespace();
    (parser.offset == parser.bytes.len()).then_some(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    offset: usize,
}

impl Parser<'_> {
    fn whitespace(&mut self) {
        while matches!(
            self.bytes.get(self.offset),
            Some(b' ' | b'\t' | b'\n' | b'\r')
        ) {
            self.offset += 1;
        }
    }
    fn eat(&mut self, byte: u8) -> bool {
        self.whitespace();
        if self.bytes.get(self.offset) == Some(&byte) {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn literal(&mut self, word: &str, value: Value) -> Option<Value> {
        if self.text[self.offset..].starts_with(word) {
            self.offset += word.len();
            Some(value)
        } else {
            None
        }
    }
    fn value(&mut self, depth: usize) -> Option<Value> {
        if depth > 512 {
            return None;
        }
        self.whitespace();
        match *self.bytes.get(self.offset)? {
            b'n' => self.literal("null", Value::Null),
            b't' => self.literal("true", Value::Bool(true)),
            b'f' => self.literal("false", Value::Bool(false)),
            b'"' => self.string().map(Value::String),
            b'[' => {
                self.offset += 1;
                let mut items = Vec::new();
                if self.eat(b']') {
                    return Some(Value::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    if self.eat(b']') {
                        return Some(Value::Array(items));
                    }
                    if !self.eat(b',') {
                        return None;
                    }
                }
            }
            b'{' => {
                self.offset += 1;
                let mut map = Map::new();
                if self.eat(b'}') {
                    return Some(Value::Object(map));
                }
                loop {
                    self.whitespace();
                    let key = self.string()?;
                    if !self.eat(b':') {
                        return None;
                    }
                    // JSON.parse keeps the last duplicate.
                    let item = self.value(depth + 1)?;
                    map.insert(key, item);
                    if self.eat(b'}') {
                        return Some(Value::Object(map));
                    }
                    if !self.eat(b',') {
                        return None;
                    }
                }
            }
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }
    fn number(&mut self) -> Option<Value> {
        let start = self.offset;
        let digits = |parser: &mut Self| {
            let begin = parser.offset;
            while matches!(parser.bytes.get(parser.offset), Some(b'0'..=b'9')) {
                parser.offset += 1;
            }
            parser.offset - begin
        };
        if self.bytes[self.offset] == b'-' {
            self.offset += 1;
        }
        let integer = self.offset;
        if digits(self) == 0 || (self.bytes[integer] == b'0' && self.offset - integer > 1) {
            return None;
        }
        if self.bytes.get(self.offset) == Some(&b'.') {
            self.offset += 1;
            if digits(self) == 0 {
                return None;
            }
        }
        if matches!(self.bytes.get(self.offset), Some(b'e' | b'E')) {
            self.offset += 1;
            if matches!(self.bytes.get(self.offset), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            if digits(self) == 0 {
                return None;
            }
        }
        let value: f64 = self.text[start..self.offset].parse().ok()?;
        value.is_finite().then_some(Value::Number(value))
    }
    fn hex(&mut self) -> Option<u16> {
        let digits = self.text.get(self.offset..self.offset + 4)?;
        self.offset += 4;
        u16::from_str_radix(digits, 16)
            .ok()
            .filter(|_| digits.bytes().all(|b| b.is_ascii_hexdigit()))
    }
    fn string(&mut self) -> Option<String> {
        if self.bytes.get(self.offset) != Some(&b'"') {
            return None;
        }
        self.offset += 1;
        let mut out = String::new();
        loop {
            let start = self.offset;
            while !matches!(self.bytes.get(self.offset), Some(b'"' | b'\\') | None) {
                if self.bytes[self.offset] < 0x20 {
                    return None;
                }
                self.offset += 1;
            }
            out.push_str(&self.text[start..self.offset]);
            match *self.bytes.get(self.offset)? {
                b'"' => {
                    self.offset += 1;
                    return Some(out);
                }
                _ => {
                    self.offset += 1;
                    let escape = *self.bytes.get(self.offset)?;
                    self.offset += 1;
                    out.push(match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\x08',
                        b'f' => '\x0c',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let unit = self.hex()?;
                            if (0xd800..0xdc00).contains(&unit) {
                                if !self.text[self.offset..].starts_with("\\u") {
                                    return None;
                                }
                                self.offset += 2;
                                let low = self.hex()?;
                                char::decode_utf16([unit, low]).next()?.ok()?
                            } else {
                                char::from_u32(unit as u32)?
                            }
                        }
                        _ => return None,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{array, object};

    #[test]
    fn numbers_print_like_javascript() {
        for (value, text) in [
            (0.0, "0"),
            (-0.0, "0"),
            (7.0, "7"),
            (-42.0, "-42"),
            (1_790_000_000_123.0, "1790000000123"),
            (0.5, "0.5"),
            (-1.25, "-1.25"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e21, "1e+21"),
            (1.5e21, "1.5e+21"),
            (123456789012345680000.0, "123456789012345680000"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (1.2345e-7, "1.2345e-7"),
            (9_007_199_254_740_993.0, "9007199254740992"),
            (f64::MAX, "1.7976931348623157e+308"),
            (5e-324, "5e-324"),
        ] {
            assert_eq!(number(value), text, "{value:e}");
        }
    }

    #[test]
    fn canonical_json_sorts_keys_by_code_units() {
        let value = object! {"b" => 1, "a" => array!["x\"\\\n\u{1}\u{7f}é"], "\u{ffff}" => 0, "😀" => Value::Null};
        assert_eq!(
            canonical(&value),
            "{\"a\":[\"x\\\"\\\\\\n\\u0001\u{7f}é\"],\"b\":1,\"😀\":null,\"\u{ffff}\":0}"
        );
    }

    #[test]
    fn parse_reads_what_canonical_writes() {
        let value = object! {"k" => array!["t0", "[\"a\",\"b\"]", 1.5, -3, true, Value::Null], "e" => object! {}};
        assert_eq!(parse(&canonical(&value)), Some(value));
        assert_eq!(
            parse(" [1, \"\\ud83d\\ude00\\u00e9\"] "),
            Some(array![1, "😀é"])
        );
        for bad in [
            "",
            "[1,]",
            "01",
            "\"\\ud800\"",
            "{\"a\" 1}",
            "[1] x",
            "1.",
            "-",
        ] {
            assert_eq!(parse(bad), None, "{bad}");
        }
    }
}
