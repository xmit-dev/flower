//! Nonrecursive framing preflight for JWT key parsers.
//!
//! jsonwebtoken's PEM adapter uses simple_asn1, whose generic decoder and key
//! classification recurse without a depth bound. Validate the complete encoded
//! tree before calling that adapter. Key semantics remain the responsibility of
//! the maintained ASN.1/cryptography providers; this is not a new key decoder.
use anyhow::{ensure, Context, Result};
use zeroize::Zeroizing;

// Match Flower's existing 128-level input nesting boundary. This protects the
// native parser stack (including recursive classification and destruction), not
// a workload-size limit. Ordinary PKCS#8/SPKI/PKCS#1 key trees are much shallower.
const MAX_DEPTH: usize = 128;

/// Use the same PEM major version as jsonwebtoken so block selection and Base64
/// interpretation cannot disagree with the parser this function protects.
pub(super) fn validate_pem(input: &[u8]) -> Result<()> {
    let parsed = pem::parse(input).context("Invalid JWT PEM key")?;
    let supported = matches!(
        parsed.tag(),
        "PRIVATE KEY" | "PUBLIC KEY" | "RSA PRIVATE KEY" | "RSA PUBLIC KEY"
    );
    let der = Zeroizing::new(parsed.into_contents());
    ensure!(
        supported,
        "JWT PEM requires a PKCS#8, SPKI, or RSA PKCS#1 key"
    );
    validate_der(&der)
}

/// Check canonical tag/length framing and primitive encodings relevant to keys.
/// No input-controlled recursion, arbitrary-precision identifiers, or heap
/// allocations occur here. Opaque primitive OCTET/BIT STRING values stay opaque,
/// exactly as in the generic parser being protected; their key data is checked
/// subsequently by the algorithm-specific provider.
pub(super) fn validate_der(input: &[u8]) -> Result<()> {
    let mut root_position = 0;
    let root = element(input, &mut root_position, input.len())?;
    ensure!(
        root.class == 0 && root.tag == 16 && root.constructed && root.end == input.len(),
        "JWT DER requires exactly one key SEQUENCE"
    );

    let mut ends = [0usize; MAX_DEPTH];
    ends[0] = input.len();
    let mut depth = 1;
    let mut position = 0;
    while depth > 0 {
        if position == ends[depth - 1] {
            depth -= 1;
            continue;
        }
        let value = element(input, &mut position, ends[depth - 1])?;
        if value.constructed && position < value.end {
            ensure!(depth < MAX_DEPTH, "JWT DER nesting exceeds 128 levels");
            ends[depth] = value.end;
            depth += 1;
        } else {
            position = value.end;
        }
    }
    Ok(())
}

struct Element {
    class: u8,
    tag: u64,
    constructed: bool,
    end: usize,
}

fn byte(input: &[u8], position: &mut usize, end: usize) -> Result<u8> {
    ensure!(*position < end, "Truncated JWT DER key");
    let value = input[*position];
    *position += 1;
    Ok(value)
}

// Every identifier in the supported key formats fits u64. Bounding the
// representation also prevents simple_asn1's repeatedly growing BigUint tag/OID
// decoder from doing quadratic work on a malicious multi-megabyte identifier.
fn identifier(input: &[u8], position: &mut usize, end: usize) -> Result<u64> {
    let mut value = 0u64;
    let mut first = true;
    loop {
        let next = byte(input, position, end)?;
        ensure!(!first || next != 0x80, "Noncanonical JWT DER identifier");
        first = false;
        value = value
            .checked_mul(128)
            .and_then(|value| value.checked_add(u64::from(next & 0x7f)))
            .context("Unsupported oversized JWT DER identifier")?;
        if next & 0x80 == 0 {
            return Ok(value);
        }
    }
}

fn element(input: &[u8], position: &mut usize, parent_end: usize) -> Result<Element> {
    let first = byte(input, position, parent_end)?;
    let class = first >> 6;
    let constructed = first & 0x20 != 0;
    let mut tag = u64::from(first & 0x1f);
    if tag == 31 {
        tag = identifier(input, position, parent_end)?;
        ensure!(tag >= 31, "Noncanonical JWT DER tag");
    }
    if class == 0 {
        ensure!(tag != 0, "DER does not permit end-of-contents markers");
        // SEQUENCE and SET must be constructed: simple_asn1 recurses into them
        // even when an attacker clears the constructed bit on their tag.
        let container = matches!(tag, 8 | 11 | 16 | 17 | 29);
        ensure!(constructed == container, "Invalid JWT DER constructed tag");
    }

    let first_length = byte(input, position, parent_end)?;
    let length = if first_length & 0x80 == 0 {
        usize::from(first_length)
    } else {
        let octets = usize::from(first_length & 0x7f);
        ensure!(octets > 0, "DER does not permit indefinite lengths");
        ensure!(octets <= size_of::<usize>(), "JWT DER length overflow");
        let mut length = 0usize;
        for index in 0..octets {
            let next = byte(input, position, parent_end)?;
            ensure!(index != 0 || next != 0, "Noncanonical JWT DER length");
            length = length
                .checked_mul(256)
                .and_then(|length| length.checked_add(usize::from(next)))
                .context("JWT DER length overflow")?;
        }
        ensure!(length >= 128, "Noncanonical JWT DER length");
        length
    };
    let end = position
        .checked_add(length)
        .context("JWT DER length overflow")?;
    ensure!(end <= parent_end, "JWT DER value exceeds its parent");
    let body = &input[*position..end];
    if class == 0 && !constructed {
        match tag {
            1 => ensure!(body == [0] || body == [0xff], "Invalid DER boolean"),
            2 => {
                ensure!(!body.is_empty(), "Empty DER integer");
                ensure!(
                    body.len() < 2
                        || !((body[0] == 0 && body[1] & 0x80 == 0)
                            || (body[0] == 0xff && body[1] & 0x80 != 0)),
                    "Noncanonical DER integer"
                );
            }
            3 => {
                ensure!(!body.is_empty() && body[0] <= 7, "Invalid DER bit string");
                let unused = body[0];
                ensure!(
                    body.len() != 1 || unused == 0,
                    "Invalid empty DER bit string"
                );
                if unused > 0 {
                    ensure!(
                        body[body.len() - 1] & ((1 << unused) - 1) == 0,
                        "Noncanonical DER bit string"
                    );
                }
            }
            5 => ensure!(body.is_empty(), "Invalid DER NULL"),
            6 => {
                ensure!(!body.is_empty(), "Empty DER object identifier");
                let mut index = *position;
                while index < end {
                    identifier(input, &mut index, end)?;
                }
            }
            _ => (),
        }
    }
    Ok(Element {
        class,
        tag,
        constructed,
        end,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wrapped(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut output = vec![tag];
        if body.len() < 128 {
            output.push(body.len() as u8);
        } else {
            let length = body.len().to_be_bytes();
            let first = length.iter().position(|byte| *byte != 0).unwrap();
            output.push(0x80 | (length.len() - first) as u8);
            output.extend_from_slice(&length[first..]);
        }
        output.extend_from_slice(body);
        output
    }

    #[test]
    fn rejects_deep_sequences_sets_and_explicit_tags_without_recursion() {
        for tag in [0x30, 0x31, 0xa0] {
            // This is a flat byte vector, including on drop; constructing the
            // fixture never creates a recursive Rust object or invokes ASN.1.
            let mut encoded = vec![5, 0];
            for _ in 0..10_000 {
                encoded = wrapped(tag, &encoded);
            }
            let encoded = wrapped(0x30, &encoded);
            assert!(validate_der(&encoded)
                .unwrap_err()
                .to_string()
                .contains("nesting"));
        }
    }

    #[test]
    fn depth_boundary_is_bounded_and_primitives_stay_opaque() {
        let mut encoded = vec![5, 0];
        for _ in 1..MAX_DEPTH {
            encoded = wrapped(0x30, &encoded);
        }
        validate_der(&encoded).unwrap();
        assert!(validate_der(&wrapped(0x30, &encoded)).is_err());
        // An OCTET STRING holds private-key bytes, not another envelope tree.
        validate_der(&wrapped(0x30, &wrapped(4, &encoded))).unwrap();
    }

    #[test]
    fn rejects_noncanonical_framing_and_primitive_container_bypass() {
        for invalid in [
            vec![0x30, 0x80, 5, 0, 0, 0], // BER indefinite length.
            vec![0x30, 0x81, 2, 5, 0],    // Overlong length.
            vec![0x30, 2, 5],             // Truncated body.
            vec![0x30, 2, 5, 0, 5, 0],    // A second root value.
            vec![0x30, 2, 0x10, 0],       // SEQUENCE with constructed bit cleared.
            vec![0x30, 2, 0x11, 0],       // SET with constructed bit cleared.
            vec![0x30, 3, 0x1f, 0x10, 0], // High-tag encoding of small tag16.
            vec![0x30, 4, 2, 2, 0, 1],    // Noncanonical integer.
            vec![0x30, 4, 3, 2, 1, 1],    // Nonzero unused bit.
            vec![0x30, 4, 6, 2, 0x80, 1], // Overlong OID arc.
            vec![0x30, 2, 0, 0],          // End-of-contents is BER-only.
        ] {
            assert!(validate_der(&invalid).is_err(), "accepted {invalid:?}");
        }
        assert!(validate_der(&[]).is_err());
    }

    #[test]
    fn oversized_identifiers_fail_before_arbitrary_precision_work() {
        let oid = wrapped(0x30, &wrapped(6, &[0xff; 20_000]));
        assert!(validate_der(&oid)
            .unwrap_err()
            .to_string()
            .contains("identifier"));
        let tag = wrapped(0x30, &[&[0xbf][..], &[0xff; 20_000], &[0]].concat());
        assert!(validate_der(&tag)
            .unwrap_err()
            .to_string()
            .contains("identifier"));
    }

    #[test]
    fn pem_preflight_accepts_key_envelopes_and_rejects_certificate_wrappers() {
        let tree = vec![0x30, 2, 5, 0];
        for label in [
            "PRIVATE KEY",
            "PUBLIC KEY",
            "RSA PRIVATE KEY",
            "RSA PUBLIC KEY",
        ] {
            validate_pem(pem::encode(&pem::Pem::new(label, tree.clone())).as_bytes()).unwrap();
        }
        assert!(validate_pem(pem::encode(&pem::Pem::new("CERTIFICATE", tree)).as_bytes()).is_err());
        assert!(validate_pem(b"not a PEM key").is_err());
    }
}
