//! Native implementations of TweetNaCl's high-level binary operations.
//!
//! Wire layouts match NaCl: secretbox is `tag || ciphertext`, a signed message
//! is `signature || message`, and an Ed25519 secret key is `seed || public_key`.
//! We deliberately use strict Ed25519 verification and validate the public half
//! of signing keys; legacy TweetNaCl's malleable-signature and mismatched-key
//! behavior is not supported. X25519 retains TweetNaCl/RFC 7748 semantics,
//! including an all-zero result for low-order inputs. Callers implementing a
//! key-exchange protocol must authenticate peer keys and handle that result.
//!
//! This module is deterministic. The bridge owns OS randomness, its mutation
//! restriction, and resource admission. Inputs are borrowed bytes, never JSON.

use anyhow::{anyhow, bail, ensure, Context, Result};
use crypto_secretbox::{
    aead::{AeadInPlace, KeyInit},
    Kdf, Key, XSalsa20Poly1305,
};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use x25519_dalek::{x25519, X25519_BASEPOINT_BYTES};
use zeroize::{Zeroize, Zeroizing};

/// The bridge must erase a byte result after copying it into guest memory:
/// outputs may contain secret keys, shared secrets, or plaintext.
#[derive(PartialEq, Eq)]
pub enum Output {
    Bytes(Vec<u8>),
    Bool(bool),
    Null,
}

// Avoid accidentally logging keys or plaintext through diagnostic formatting.
impl std::fmt::Debug for Output {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bytes(bytes) => f.debug_struct("Bytes").field("len", &bytes.len()).finish(),
            Self::Bool(value) => f.debug_tuple("Bool").field(value).finish(),
            Self::Null => f.write_str("Null"),
        }
    }
}

/// Validate arity and fixed input lengths and return the largest byte result.
/// Authentication failure can instead produce Null; boolean results need no
/// byte buffer. This does no cryptographic work or message-sized allocation.
pub fn output_len(op: u32, args: &[&[u8]]) -> Result<usize> {
    let lengths: &[Option<usize>] = match op {
        1 | 2 => &[None, Some(24), Some(32)],
        3 | 5 => &[Some(32), Some(32)],
        4 | 12 | 14 => &[Some(32)],
        6 | 7 => &[None, Some(24), Some(32), Some(32)],
        8 | 10 => &[None, Some(64)],
        9 => &[None, Some(32)],
        11 => &[None, Some(64), Some(32)],
        13 => &[Some(64)],
        15 | 17 => &[None],
        16 => &[None, None],
        _ => bail!("unknown native NaCl operation"),
    };
    ensure!(args.len() == lengths.len(), "invalid NaCl argument count");
    for (input, expected) in args.iter().zip(lengths) {
        if let Some(length) = expected {
            ensure!(input.len() == *length, "invalid NaCl argument length");
        }
    }
    match op {
        1 | 6 => args[0]
            .len()
            .checked_add(16)
            .context("NaCl output too large"),
        2 | 7 => Ok(args[0].len().saturating_sub(16)),
        3..=5 | 17 => Ok(32),
        8 => args[0]
            .len()
            .checked_add(64)
            .context("NaCl output too large"),
        9 => Ok(args[0].len().saturating_sub(64)),
        10 | 14 | 15 => Ok(64),
        11 | 16 => Ok(0),
        12 | 13 => Ok(96),
        _ => unreachable!("operation was validated above"),
    }
}

/// Binary operation IDs (arguments in parentheses):
/// 1 secretbox(message, nonce, key); 2 secretbox.open(box, nonce, key);
/// 3 scalarMult(secret, public); 4 scalarMult.base(secret);
/// 5 box.before(public, secret); 6 box(message, nonce, public, secret);
/// 7 box.open(box, nonce, public, secret); 8 sign(message, secret);
/// 9 sign.open(signed_message, public); 10 sign.detached(message, secret);
/// 11 sign.detached.verify(message, signature, public);
/// 12 sign.keyPair.fromSeed(seed); 13 sign.keyPair.fromSecretKey(secret);
/// 14 box.keyPair.fromSecretKey(secret); 15 hash(message); 16 verify(a, b);
/// 17 SHA-256(message), which is not NaCl but shares its binary shape.
/// Key-pair results are `public_key || secret_key` (96/64 bytes respectively).
/// `box.after`/`box.open.after` are aliases for 1/2. Random key-pair wrappers
/// obtain OS random bytes separately and use 12/14.
pub fn execute(op: u32, args: &[&[u8]]) -> Result<Output> {
    let capacity = output_len(op, args)?;
    match op {
        1 => seal(args[0], fixed(args[1])?, fixed(args[2])?, capacity),
        2 => open(args[0], fixed(args[1])?, fixed(args[2])?),
        3 => {
            let shared = Zeroizing::new(x25519(*fixed(args[0])?, *fixed(args[1])?));
            Ok(Output::Bytes(shared.to_vec()))
        }
        4 => Ok(Output::Bytes(
            x25519(*fixed(args[0])?, X25519_BASEPOINT_BYTES).to_vec(),
        )),
        5 => Ok(Output::Bytes(
            before(fixed(args[0])?, fixed(args[1])?).to_vec(),
        )),
        6 | 7 => {
            let key = before(fixed(args[2])?, fixed(args[3])?);
            if op == 6 {
                seal(args[0], fixed(args[1])?, fixed(&key)?, capacity)
            } else {
                open(args[0], fixed(args[1])?, fixed(&key)?)
            }
        }
        8 | 10 => {
            let signing_key = signing_key(args[1])?;
            let signature = signing_key.sign(args[0]).to_bytes();
            let mut bytes = buffer(capacity)?;
            bytes.extend_from_slice(&signature);
            if op == 8 {
                bytes.extend_from_slice(args[0]);
            }
            Ok(Output::Bytes(bytes))
        }
        9 => {
            if args[0].len() < 64 {
                return Ok(Output::Null);
            }
            let (signature, message) = args[0].split_at(64);
            if verify_signature(message, fixed(signature)?, fixed(args[1])?) {
                let mut bytes = buffer(capacity)?;
                bytes.extend_from_slice(message);
                Ok(Output::Bytes(bytes))
            } else {
                Ok(Output::Null)
            }
        }
        11 => Ok(Output::Bool(verify_signature(
            args[0],
            fixed(args[1])?,
            fixed(args[2])?,
        ))),
        12 | 13 => {
            let signing_key = if op == 12 {
                SigningKey::from_bytes(fixed(args[0])?)
            } else {
                signing_key(args[0])?
            };
            let secret = Zeroizing::new(signing_key.to_keypair_bytes());
            let mut bytes = buffer(capacity)?;
            bytes.extend_from_slice(&signing_key.verifying_key().to_bytes());
            bytes.extend_from_slice(secret.as_ref());
            Ok(Output::Bytes(bytes))
        }
        14 => {
            let mut bytes = buffer(capacity)?;
            bytes.extend_from_slice(&x25519(*fixed(args[0])?, X25519_BASEPOINT_BYTES));
            bytes.extend_from_slice(args[0]);
            Ok(Output::Bytes(bytes))
        }
        15 => Ok(Output::Bytes(Sha512::digest(args[0]).to_vec())),
        17 => Ok(Output::Bytes(Sha256::digest(args[0]).to_vec())),
        16 => Ok(Output::Bool(
            !args[0].is_empty() && bool::from(args[0].ct_eq(args[1])),
        )),
        _ => unreachable!("operation was validated above"),
    }
}

fn fixed<const N: usize>(bytes: &[u8]) -> Result<&[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| anyhow!("invalid NaCl argument length"))
}

fn buffer(capacity: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .context("allocate NaCl result")?;
    Ok(bytes)
}

fn seal(message: &[u8], nonce: &[u8; 24], key: &[u8; 32], capacity: usize) -> Result<Output> {
    let cipher = XSalsa20Poly1305::new(key.into());
    let mut bytes = Zeroizing::new(buffer(capacity)?);
    bytes.resize(16, 0);
    bytes.extend_from_slice(message);
    let tag = cipher
        .encrypt_in_place_detached(nonce.into(), b"", &mut bytes[16..])
        .map_err(|_| anyhow!("NaCl secretbox encryption failed"))?;
    bytes[..16].copy_from_slice(&tag);
    Ok(Output::Bytes(std::mem::take(&mut *bytes)))
}

fn open(ciphertext: &[u8], nonce: &[u8; 24], key: &[u8; 32]) -> Result<Output> {
    if ciphertext.len() < 16 {
        return Ok(Output::Null);
    }
    let cipher = XSalsa20Poly1305::new(key.into());
    let mut bytes = buffer(ciphertext.len() - 16)?;
    bytes.extend_from_slice(&ciphertext[16..]);
    if cipher
        .decrypt_in_place_detached(
            nonce.into(),
            b"",
            &mut bytes,
            fixed::<16>(&ciphertext[..16])?.into(),
        )
        .is_ok()
    {
        Ok(Output::Bytes(bytes))
    } else {
        bytes.zeroize();
        Ok(Output::Null)
    }
}

fn before(public: &[u8; 32], secret: &[u8; 32]) -> Zeroizing<Key> {
    // This is the original NaCl crypto_box composition used by RustCrypto's
    // crypto_box crate: HSalsa20(X25519(secret, public), zero16), not a hash.
    let shared = Zeroizing::new(x25519(*secret, *public));
    Zeroizing::new(<XSalsa20Poly1305 as Kdf>::kdf(
        (&*shared).into(),
        &Default::default(),
    ))
}

fn signing_key(secret: &[u8]) -> Result<SigningKey> {
    SigningKey::from_keypair_bytes(fixed(secret)?)
        .map_err(|_| anyhow!("Ed25519 secret key has an inconsistent public key"))
}

fn verify_signature(message: &[u8], signature: &[u8; 64], public: &[u8; 32]) -> bool {
    VerifyingKey::from_bytes(public).is_ok_and(|key| {
        key.verify_strict(message, &Signature::from_bytes(signature))
            .is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(value: &str) -> Vec<u8> {
        assert!(value.len().is_multiple_of(2));
        (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
            .collect()
    }

    fn bytes(op: u32, args: &[&[u8]]) -> Vec<u8> {
        let expected = output_len(op, args).unwrap();
        let Output::Bytes(bytes) = execute(op, args).unwrap() else {
            panic!("expected bytes")
        };
        assert_eq!(bytes.len(), expected);
        bytes
    }

    #[test]
    fn tweetnacl_secretbox_vectors_and_rejected_tampering() {
        // TweetNaCl's upstream test/data/secretbox.random.js vectors 0, 1, 33.
        // https://github.com/dchest/tweetnacl-js/blob/master/test/data/secretbox.random.js
        for (key, nonce, message, ciphertext) in [
            (
                "822bca3c7e05fde0dc204519730b35f81216a9c9f1df9525e2a900ec89718f57",
                "72b90208d2800e36ad16c730941a038d7c3ad9d87030d329",
                "",
                "79b34551ed224fa17cb6460ccb90a0d9",
            ),
            (
                "847310e59f2328dcdd00332dfa3c61605e78dfb3a7dad65ca2474d5f88799f12",
                "a9e9b4b8374bd894f639b323a27c34514a8e7d64cc83b95e",
                "d5",
                "54956256e80184a79a3af7d2947913cd44",
            ),
            (
                "37335116543dd189af40828a1b70e5a010d301096ed7d254673dd76a45667e00",
                "6005067d6241d12fc56ec37d416b8822c345204dfc5b5ecd",
                "cef3f99515508ed21df43e10708af6697441b0b05944745b29e5fa172802d14682",
                "60559b13a9ee14723f424e6befb8b50cab905cbf20cbcdfb1fd6328e905e11e0b051b1c012318f7bf40c0590e6fd9d7019",
            ),
        ] {
            let (key, nonce, message, ciphertext) =
                (hex(key), hex(nonce), hex(message), hex(ciphertext));
            assert_eq!(bytes(1, &[&message, &nonce, &key]), ciphertext);
            assert_eq!(bytes(2, &[&ciphertext, &nonce, &key]), message);
            for index in 0..ciphertext.len() {
                let mut altered = ciphertext.clone();
                altered[index] ^= 1;
                assert_eq!(execute(2, &[&altered, &nonce, &key]).unwrap(), Output::Null);
            }
            assert_eq!(
                execute(2, &[&ciphertext[..15], &nonce, &key]).unwrap(),
                Output::Null
            );
        }
    }

    #[test]
    fn tweetnacl_box_vectors_and_precomputation() {
        // TweetNaCl's test/data/box.random.js vectors 0, 33 use nonce zero24.
        // https://github.com/dchest/tweetnacl-js/blob/master/test/06-box.js
        for (public, secret, message, ciphertext) in [
            (
                "bd2c0c8857832d571e605e0b5f30c1e39c3b3ba8cccd3176f32898a5edbb4a4f",
                "7d153ed407c9f3016a65c44a970cc900c9850f8d9da4b14e1d4ac215151e7ca1",
                "",
                "058054c4e9397aea943b08fd55ca52dd",
            ),
            (
                "74a3d94520567e842c57ba1b443e5599020c7bf7addf1dabb85436152e834e5d",
                "c0cb69d681fda0d35375b5e12577263646c08d4b5dd894624dfcfe155a122fc7",
                "dd93fda35f25b18f282e397913349650ecd32a0f9a0de7acb3daa433fa078ff947",
                "a87ba5b2600e50ec85c3f68882f702201176350ad9d59bcfa1be0498721f96952c6e25899b714de5b1bc65804ab0a4f220",
            ),
        ] {
            let (public, secret, message, ciphertext) =
                (hex(public), hex(secret), hex(message), hex(ciphertext));
            let nonce = [0; 24];
            assert_eq!(bytes(6, &[&message, &nonce, &public, &secret]), ciphertext);
            assert_eq!(bytes(7, &[&ciphertext, &nonce, &public, &secret]), message);
            let shared = bytes(5, &[&public, &secret]);
            assert_eq!(bytes(1, &[&message, &nonce, &shared]), ciphertext);
            assert_eq!(bytes(2, &[&ciphertext, &nonce, &shared]), message);
        }
    }

    #[test]
    fn rfc7748_x25519_keypairs_and_low_order_compatibility() {
        // https://www.rfc-editor.org/rfc/rfc7748#section-6.1
        let alice = hex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
        let alice_public = hex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
        let bob = hex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
        let bob_public = hex("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
        let shared = hex("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");
        assert_eq!(bytes(4, &[&alice]), alice_public);
        assert_eq!(bytes(4, &[&bob]), bob_public);
        assert_eq!(bytes(3, &[&alice, &bob_public]), shared);
        assert_eq!(bytes(3, &[&bob, &alice_public]), shared);
        assert_eq!(
            bytes(14, &[&alice]),
            [alice_public.as_slice(), alice.as_slice()].concat()
        );
        assert_eq!(bytes(3, &[&alice, &[0; 32]]), vec![0; 32]);
        // X25519 masks the top bit of its input u coordinate.
        let mut equivalent = bob_public.clone();
        equivalent[31] ^= 128;
        assert_eq!(bytes(3, &[&alice, &equivalent]), shared);
        assert_eq!(
            bytes(5, &[&bob_public, &alice]),
            bytes(5, &[&alice_public, &bob])
        );
    }

    fn rfc8032_keys() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        // https://www.rfc-editor.org/rfc/rfc8032#section-7.1, test 1.
        (
            hex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"),
            hex("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"),
            hex(
                "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
            ),
        )
    }

    #[test]
    fn rfc8032_signatures_keys_and_signed_message_layout() {
        let (seed, public, signature) = rfc8032_keys();
        let secret = [seed.as_slice(), public.as_slice()].concat();
        let pair = [public.as_slice(), secret.as_slice()].concat();
        assert_eq!(bytes(12, &[&seed]), pair);
        assert_eq!(bytes(13, &[&secret]), pair);
        assert_eq!(bytes(8, &[b"", &secret]), signature);
        assert_eq!(bytes(10, &[b"", &secret]), signature);
        assert_eq!(bytes(9, &[&signature, &public]), b"");
        assert_eq!(
            execute(11, &[b"", &signature, &public]).unwrap(),
            Output::Bool(true)
        );
        let message = b"binary\0payload\xff";
        let signed = bytes(8, &[message, &secret]);
        assert_eq!(&signed[64..], message);
        assert_eq!(&signed[..64], bytes(10, &[message, &secret]));
        assert_eq!(bytes(9, &[&signed, &public]), message);
        assert_eq!(
            execute(11, &[b"altered", &signature, &public]).unwrap(),
            Output::Bool(false)
        );
        assert_eq!(execute(9, &[&signed[..63], &public]).unwrap(), Output::Null);
    }

    #[test]
    fn strict_ed25519_rejects_malleability_small_order_and_inconsistent_keys() {
        let (seed, public, mut signature) = rfc8032_keys();
        // Legacy TweetNaCl accepts S + L. Strict RFC 8032 verification must not.
        let order = hex("edd3f55c1a631258d69cf7a2def9de1400000000000000000000000000000010");
        let mut carry = 0u16;
        for (byte, addend) in signature[32..].iter_mut().zip(order) {
            carry += u16::from(*byte) + u16::from(addend);
            *byte = carry as u8;
            carry >>= 8;
        }
        assert_eq!(carry, 0);
        assert_eq!(
            execute(11, &[b"", &signature, &public]).unwrap(),
            Output::Bool(false)
        );
        assert_eq!(execute(9, &[&signature, &public]).unwrap(), Output::Null);
        // Identity public key/R with S=0 is not an acceptable signing identity.
        let mut identity = [0; 32];
        identity[0] = 1;
        let mut weak_signature = [0; 64];
        weak_signature[0] = 1;
        assert_eq!(
            execute(11, &[b"", &weak_signature, &identity]).unwrap(),
            Output::Bool(false)
        );
        let mut secret = [seed.as_slice(), public.as_slice()].concat();
        secret[32] ^= 1;
        for op in [8, 10] {
            assert!(execute(op, &[b"message", &secret]).is_err());
        }
        assert!(execute(13, &[&secret]).is_err());
    }

    #[test]
    fn sha512_and_verify_semantics() {
        // SHA-512's standard empty/abc vectors, also TweetNaCl hash.spec.js.
        assert_eq!(
            bytes(15, &[b""]),
            hex(
                "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
            )
        );
        assert_eq!(
            bytes(15, &[b"abc"]),
            hex(
                "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
            )
        );
        // FIPS 180-2's SHA-256 vectors.
        assert_eq!(
            bytes(17, &[b""]),
            hex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        assert_eq!(
            bytes(17, &[b"abc"]),
            hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        for (a, b, expected) in [
            (b"".as_slice(), b"".as_slice(), false),
            (b"x", b"x", true),
            (b"x", b"xy", false),
            (b"ab", b"ac", false),
        ] {
            assert_eq!(execute(16, &[a, b]).unwrap(), Output::Bool(expected));
        }
    }

    #[test]
    fn input_validation_precedes_crypto_and_result_allocation() {
        assert!(output_len(0, &[]).is_err());
        assert!(execute(u32::MAX, &[]).is_err());
        for op in 1..=16 {
            assert!(execute(op, &[]).is_err());
        }
        assert!(output_len(1, &[b"", &[0; 23], &[0; 32]]).is_err());
        assert!(execute(2, &[b"", &[0; 24], &[0; 31]]).is_err());
        assert!(execute(11, &[b"", &[0; 63], &[0; 32]]).is_err());
        assert!(execute(12, &[&[0; 31]]).is_err());
        assert!(execute(13, &[&[0; 63]]).is_err());
        assert!(execute(16, &[b"", b"", b""]).is_err());
    }
}
