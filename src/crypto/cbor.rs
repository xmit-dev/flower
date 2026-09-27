//! Strict decoding of the CTAP2 canonical CBOR subset that WebAuthn uses:
//! definite lengths, integers, byte and text strings, arrays, maps, booleans
//! and null. Tags, floats, indefinite lengths and repeated map keys are
//! rejected. Containers are small and shallow, which bounds the work a native
//! call spends on hostile input; it cannot be interrupted midway.
use anyhow::{bail, ensure, Context, Result};

const MAX_DEPTH: usize = 16;
const MAX_ENTRIES: u64 = 64;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Value<'a> {
    Unsigned(u64),
    /// The integer `-1 - n`.
    Negative(u64),
    Bytes(&'a [u8]),
    Text(&'a str),
    Array(Vec<Value<'a>>),
    Map(Vec<(Value<'a>, Value<'a>)>),
    Bool(bool),
    Null,
}

impl<'a> Value<'a> {
    pub(super) fn integer(&self) -> Option<i64> {
        match *self {
            Self::Unsigned(n) => i64::try_from(n).ok(),
            Self::Negative(n) => i64::try_from(n).ok().map(|n| -1 - n),
            _ => None,
        }
    }

    pub(super) fn bytes(&self) -> Option<&'a [u8]> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub(super) fn text(&self) -> Option<&'a str> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }

    /// A map's value under an integer key, as COSE uses.
    pub(super) fn label(&self, label: i64) -> Option<&Value<'a>> {
        self.entry(|key| key.integer() == Some(label))
    }

    /// A map's value under a text key.
    pub(super) fn field(&self, name: &str) -> Option<&Value<'a>> {
        self.entry(|key| key.text() == Some(name))
    }

    fn entry(&self, matches: impl Fn(&Value<'a>) -> bool) -> Option<&Value<'a>> {
        match self {
            Self::Map(entries) => entries
                .iter()
                .find(|(key, _)| matches(key))
                .map(|(_, value)| value),
            _ => None,
        }
    }
}

/// Decode exactly one item spanning all of `input`.
pub(super) fn decode(input: &[u8]) -> Result<Value<'_>> {
    let mut position = 0;
    let value = item(input, &mut position, 0)?;
    ensure!(position == input.len(), "CBOR has trailing bytes");
    Ok(value)
}

/// Decode the item at `position` and advance past it.
pub(super) fn item<'a>(input: &'a [u8], position: &mut usize, depth: usize) -> Result<Value<'a>> {
    let initial = *input.get(*position).context("CBOR is truncated")?;
    *position += 1;
    let (major, info) = (initial >> 5, initial & 0x1f);
    if major == 7 {
        return Ok(match info {
            20 => Value::Bool(false),
            21 => Value::Bool(true),
            22 => Value::Null,
            _ => bail!("CBOR floats and other simple values are not accepted"),
        });
    }
    let argument = match info {
        0..=23 => u64::from(info),
        24..=27 => take(input, position, 1 << (info - 24))?
            .iter()
            .fold(0, |value, byte| (value << 8) | u64::from(*byte)),
        31 => bail!("indefinite-length CBOR is not accepted"),
        _ => bail!("CBOR uses a reserved length encoding"),
    };
    Ok(match major {
        0 => Value::Unsigned(argument),
        1 => Value::Negative(argument),
        2 => Value::Bytes(take(input, position, length(argument)?)?),
        3 => Value::Text(
            std::str::from_utf8(take(input, position, length(argument)?)?)
                .context("CBOR text is not UTF-8")?,
        ),
        4 | 5 => {
            ensure!(depth < MAX_DEPTH, "CBOR nests too deeply");
            ensure!(argument <= MAX_ENTRIES, "CBOR container is too large");
            if major == 4 {
                let mut values = Vec::with_capacity(argument as usize);
                for _ in 0..argument {
                    values.push(item(input, position, depth + 1)?);
                }
                Value::Array(values)
            } else {
                let mut entries: Vec<(Value<'a>, Value<'a>)> =
                    Vec::with_capacity(argument as usize);
                for _ in 0..argument {
                    let key = item(input, position, depth + 1)?;
                    ensure!(
                        entries.iter().all(|(existing, _)| *existing != key),
                        "CBOR map repeats a key"
                    );
                    entries.push((key, item(input, position, depth + 1)?));
                }
                Value::Map(entries)
            }
        }
        _ => bail!("CBOR tags are not accepted"),
    })
}

fn length(argument: u64) -> Result<usize> {
    usize::try_from(argument).context("CBOR length exceeds memory")
}

fn take<'a>(input: &'a [u8], position: &mut usize, size: usize) -> Result<&'a [u8]> {
    let end = position
        .checked_add(size)
        .filter(|end| *end <= input.len())
        .context("CBOR is truncated")?;
    let bytes = &input[*position..end];
    *position = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_the_webauthn_subset() {
        // {1: 2, 3: -7, -1: 1, "fmt": "none", "x": h'0102', "ok": [true, false, null]}
        let input = [
            0xa6, 0x01, 0x02, 0x03, 0x26, 0x20, 0x01, 0x63, b'f', b'm', b't', 0x64, b'n', b'o',
            b'n', b'e', 0x61, b'x', 0x42, 1, 2, 0x62, b'o', b'k', 0x83, 0xf5, 0xf4, 0xf6,
        ];
        let value = decode(&input).unwrap();
        assert_eq!(value.label(1).and_then(Value::integer), Some(2));
        assert_eq!(value.label(3).and_then(Value::integer), Some(-7));
        assert_eq!(value.label(-1).and_then(Value::integer), Some(1));
        assert_eq!(value.field("fmt").and_then(Value::text), Some("none"));
        assert_eq!(value.field("x").and_then(Value::bytes), Some(&[1u8, 2][..]));
        assert_eq!(
            value.field("ok"),
            Some(&Value::Array(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Null
            ]))
        );
        // Multi-byte arguments, including the largest negative integer.
        assert_eq!(decode(&[0x19, 0x01, 0x00]).unwrap().integer(), Some(256));
        assert_eq!(
            decode(&[0x3b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])
                .unwrap()
                .integer(),
            Some(i64::MIN)
        );
        assert_eq!(
            decode(&[0x3b, 0x80, 0, 0, 0, 0, 0, 0, 0])
                .unwrap()
                .integer(),
            None
        );
    }

    #[test]
    fn rejects_everything_outside_the_subset() {
        for (input, reason) in [
            (&[][..], "truncated"),
            (&[0x42, 1], "truncated"),
            (&[0x01, 0x02], "trailing"),
            (&[0x5f, 0xff], "indefinite"),
            (&[0x9f, 0xff], "indefinite"),
            (&[0xc0, 0x01], "tags"),
            (&[0xf9, 0x3c, 0x00], "floats"),
            (&[0xf7], "simple"),
            (&[0x1c], "reserved"),
            (&[0x62, 0xff, 0xfe], "UTF-8"),
            (&[0xa2, 0x01, 0x01, 0x01, 0x02], "repeats"),
            (&[0x98, 65], "too large"),
            (
                &[0x5b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                "truncated",
            ),
        ] {
            let error = format!("{:#}", decode(input).unwrap_err());
            assert!(error.contains(reason), "{input:?}: {error}");
        }
        let deep = [vec![0x81; 17], vec![0x00]].concat();
        assert!(format!("{:#}", decode(&deep).unwrap_err()).contains("nests too deeply"));
        assert!(decode(&[vec![0x81; 16], vec![0x00]].concat()).is_ok());
    }
}
