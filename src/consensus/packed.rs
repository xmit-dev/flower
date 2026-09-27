//! Stored JSON, LZ4-compressed when that saves enough to repay reading it.
//! A packed value is its JSON as it is, or a zero byte, the JSON's length (four
//! little-endian bytes) and its LZ4 block. No JSON text starts with a zero
//! byte, so values written before packing existed read unchanged.
use std::borrow::Cow;

use anyhow::{Context, ensure};

const PACKED: u8 = 0;
// Below this, the header and the decompression cost more than they save.
const MIN_PACKED_BYTES: usize = 128;
// LZ4 cannot expand a byte into more than 255.
const MAX_RATIO: usize = 255;

/// `json` as stored.
pub(crate) fn pack(json: &[u8]) -> Cow<'_, [u8]> {
    if json.len() < MIN_PACKED_BYTES || u32::try_from(json.len()).is_err() {
        return Cow::Borrowed(json);
    }
    let mut packed = Vec::with_capacity(5 + lz4_flex::block::get_maximum_output_size(json.len()));
    packed.push(PACKED);
    packed.extend_from_slice(&(json.len() as u32).to_le_bytes());
    packed.resize(packed.capacity(), 0);
    let length = lz4_flex::block::compress_into(json, &mut packed[5..])
        .expect("LZ4 maximum output reservation");
    // Keep it only if it saves at least an eighth.
    if 5 + length > json.len() - json.len() / 8 {
        return Cow::Borrowed(json);
    }
    packed.truncate(5 + length);
    Cow::Owned(packed)
}

/// The JSON of a stored value.
pub(crate) fn unpack(bytes: &[u8]) -> anyhow::Result<Cow<'_, [u8]>> {
    let Some((&PACKED, rest)) = bytes.split_first() else {
        return Ok(Cow::Borrowed(bytes));
    };
    let (length, block) = rest.split_at_checked(4).context("truncated packed value")?;
    let length = u32::from_le_bytes(length.try_into().expect("four bytes")) as usize;
    ensure!(
        length <= block.len().saturating_mul(MAX_RATIO),
        "packed value length exceeds its block"
    );
    let mut json = vec![0; length];
    let written =
        lz4_flex::block::decompress_into(block, &mut json).context("invalid packed value")?;
    ensure!(written == length, "packed value length mismatch");
    Ok(Cow::Owned(json))
}

/// The length of a stored value's JSON, without unpacking it.
pub(crate) fn json_len(bytes: &[u8]) -> usize {
    match bytes {
        [PACKED, a, b, c, d, ..] => u32::from_le_bytes([*a, *b, *c, *d]) as usize,
        _ => bytes.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_only_what_it_shrinks_and_reads_both_forms() {
        let small = br#"{"a":1}"#;
        assert!(matches!(pack(small), Cow::Borrowed(_)));
        let repetitive =
            serde_json::to_vec(&vec!["index-entry:[\"jobs\",[\"state\"]]"; 64]).unwrap();
        let packed = pack(&repetitive);
        assert!(packed.len() < repetitive.len() / 4);
        assert_eq!(packed[0], PACKED);
        assert_eq!(json_len(&packed), repetitive.len());
        assert_eq!(unpack(&packed).unwrap().as_ref(), repetitive.as_slice());
        // Unpacked JSON, including values stored before packing, reads as is.
        assert_eq!(unpack(&repetitive).unwrap().as_ref(), repetitive.as_slice());
        let random: Vec<u8> = (0..4096u32)
            .map(|i| b"0123456789abcdef"[(i.wrapping_mul(2_654_435_761) >> 13) as usize % 16])
            .collect();
        let random = serde_json::to_vec(&String::from_utf8(random).unwrap()).unwrap();
        assert!(pack(&random).len() <= random.len());
        assert_eq!(unpack(&pack(&random)).unwrap().as_ref(), random.as_slice());
    }

    #[test]
    fn corrupt_packed_values_fail_instead_of_allocating() {
        assert!(unpack(&[PACKED, 1]).is_err());
        assert!(unpack(&[PACKED, 0xff, 0xff, 0xff, 0x7f, 0]).is_err());
        let packed = pack(&[b'7'; 512]).into_owned();
        let mut wrong = packed.clone();
        wrong[1] = wrong[1].wrapping_add(1);
        assert!(unpack(&wrong).is_err());
    }
}
