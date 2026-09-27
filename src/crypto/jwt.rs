//! Compact JOSE with explicit key types, algorithms and invocation time.
//!
//! Crypto primitives come from AWS-LC; deterministic ES256 uses RustCrypto's
//! RFC 6979 implementation. This module never obtains a clock or a nonce.
use std::fmt;

use anyhow::{Context, Result, bail, ensure};
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use jsonwebtoken::{DecodingKey, EncodingKey};
use p256::{
    ecdsa::{Signature, SigningKey, signature::Signer},
    pkcs8::DecodePrivateKey,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value, json};
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Algorithm {
    HS256,
    RS256,
    ES256,
    EdDSA,
}

impl Algorithm {
    fn jwt(self) -> jsonwebtoken::Algorithm {
        match self {
            Self::HS256 => jsonwebtoken::Algorithm::HS256,
            Self::RS256 => jsonwebtoken::Algorithm::RS256,
            Self::ES256 => jsonwebtoken::Algorithm::ES256,
            Self::EdDSA => jsonwebtoken::Algorithm::EdDSA,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KeyFormat {
    Raw,
    Pem,
    Der,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignOptions {
    pub algorithm: Algorithm,
    pub key_format: KeyFormat,
    pub kid: Option<String>,
    pub typ: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerifyOptions {
    pub algorithms: Vec<Algorithm>,
    pub key_format: KeyFormat,
    pub issuer: Option<String>,
    pub audience: Option<Vec<String>>,
    pub subject: Option<String>,
    #[serde(default)]
    pub clock_tolerance_seconds: f64,
    #[serde(default = "require_expiration")]
    pub require_expiration: bool,
    pub typ: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptOptions {
    pub kid: Option<String>,
    pub typ: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DecryptOptions {
    pub issuer: Option<String>,
    pub audience: Option<Vec<String>>,
    pub subject: Option<String>,
    #[serde(default)]
    pub clock_tolerance_seconds: f64,
    #[serde(default = "require_expiration")]
    pub require_expiration: bool,
    pub typ: Option<String>,
}

fn require_expiration() -> bool {
    true
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Verified {
    pub claims: Value,
    pub protected_header: Value,
}

/// Parse JSON without accepting ambiguous duplicate member names, at any depth.
/// serde_json's ordinary recursion limit remains in effect.
pub fn parse_json(bytes: &[u8]) -> Result<Value> {
    struct Strict(Value);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: Deserializer<'de>>(
            deserializer: D,
        ) -> std::result::Result<Self, D::Error> {
            struct StrictVisitor;
            impl<'de> Visitor<'de> for StrictVisitor {
                type Value = Strict;
                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("unambiguous JSON")
                }
                fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Bool(value)))
                }
                fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Number(value.into())))
                }
                fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Number(value.into())))
                }
                fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Strict, E> {
                    Number::from_f64(value)
                        .map(|n| Strict(Value::Number(n)))
                        .ok_or_else(|| E::custom("non-finite JSON number"))
                }
                fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::String(value.into())))
                }
                fn visit_string<E: de::Error>(
                    self,
                    value: String,
                ) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::String(value)))
                }
                fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_seq<A: SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut values = Vec::new();
                    while let Some(Strict(value)) = seq.next_element()? {
                        values.push(value);
                    }
                    Ok(Strict(Value::Array(values)))
                }
                fn visit_map<A: MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut values = Map::new();
                    while let Some((key, Strict(value))) = map.next_entry::<String, Strict>()? {
                        if values.insert(key, value).is_some() {
                            return Err(de::Error::custom("duplicate JSON member"));
                        }
                    }
                    Ok(Strict(Value::Object(values)))
                }
            }
            deserializer.deserialize_any(StrictVisitor)
        }
    }
    Ok(serde_json::from_slice::<Strict>(bytes)
        .context("Invalid JWT JSON")?
        .0)
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .context("JWT claims must be a JSON object")
}

fn decode_segment(value: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(value)
        .context("Invalid JOSE base64url")
}

fn header(segment: &str, encrypted: bool) -> Result<Value> {
    let value = parse_json(&decode_segment(segment)?)?;
    let fields = value
        .as_object()
        .context("JWT protected header must be an object")?;
    for (name, value) in fields {
        ensure!(
            matches!(name.as_str(), "alg" | "typ" | "kid") || (encrypted && name == "enc"),
            "Unsupported JWT protected header: {name}"
        );
        ensure!(value.is_string(), "JWT header {name} must be a string");
    }
    Ok(value)
}

/// Only supplies an untrusted selector; callers must restrict it to an already
/// authorized managed key, then verify the complete token and claims normally.
pub(crate) fn managed_header(token: &str, encrypted: bool) -> Result<Value> {
    ensure!(
        token.split('.').count() == if encrypted { 5 } else { 3 },
        "Invalid compact JWT segment count"
    );
    header(
        token.split('.').next().context("Missing JWT header")?,
        encrypted,
    )
}

fn pem(key: &[u8], format: KeyFormat, private: bool) -> Result<Zeroizing<Vec<u8>>> {
    match format {
        KeyFormat::Pem => {
            super::der::validate_pem(key)?;
            std::str::from_utf8(key).context("JWT PEM must be valid UTF-8")?;
            Ok(Zeroizing::new(key.to_vec()))
        }
        KeyFormat::Der => {
            super::der::validate_der(key)?;
            // DER has one interoperable meaning here: PKCS#8 private / SPKI public.
            // Existing JOSE PEM parsing validates the ASN.1 and algorithm identifier.
            let label = if private { "PRIVATE KEY" } else { "PUBLIC KEY" };
            Ok(Zeroizing::new(
                format!(
                    "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
                    STANDARD.encode(key)
                )
                .into_bytes(),
            ))
        }
        KeyFormat::Raw => bail!("Asymmetric JWT keys require PEM or DER"),
    }
}

fn hmac_key(key: &[u8], format: KeyFormat) -> Result<()> {
    ensure!(
        format == KeyFormat::Raw,
        "HS256 requires an explicitly raw secret key"
    );
    ensure!(key.len() >= 32, "HS256 requires at least 32 secret bytes");
    let start = key
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(key.len());
    ensure!(
        !key[start..].starts_with(b"-----BEGIN "),
        "PEM key material cannot be used as an HS256 secret"
    );
    Ok(())
}

fn signing_key(key: &[u8], algorithm: Algorithm, format: KeyFormat) -> Result<EncodingKey> {
    if algorithm == Algorithm::HS256 {
        hmac_key(key, format)?;
        return Ok(EncodingKey::from_secret(key));
    }
    let key = pem(key, format, true)?;
    Ok(match algorithm {
        Algorithm::RS256 => EncodingKey::from_rsa_pem(&key)?,
        Algorithm::ES256 => EncodingKey::from_ec_pem(&key)?,
        Algorithm::EdDSA => EncodingKey::from_ed_pem(&key)?,
        Algorithm::HS256 => unreachable!(),
    })
}

fn verifying_key(key: &[u8], algorithm: Algorithm, format: KeyFormat) -> Result<DecodingKey> {
    if algorithm == Algorithm::HS256 {
        hmac_key(key, format)?;
        return Ok(DecodingKey::from_secret(key));
    }
    let key = pem(key, format, false)?;
    Ok(match algorithm {
        Algorithm::RS256 => DecodingKey::from_rsa_pem(&key)?,
        Algorithm::ES256 => DecodingKey::from_ec_pem(&key)?,
        Algorithm::EdDSA => DecodingKey::from_ed_pem(&key)?,
        Algorithm::HS256 => unreachable!(),
    })
}

fn protected(algorithm: &str, kid: &Option<String>, typ: &Option<String>) -> Value {
    let mut value = json!({"alg":algorithm,"typ":typ.as_deref().unwrap_or("JWT")});
    if let Some(kid) = kid {
        value["kid"] = Value::String(kid.clone());
    }
    value
}

pub fn sign(claims: &Value, key: &[u8], options: &SignOptions) -> Result<String> {
    let key = signing_key(key, options.algorithm, options.key_format)?;
    sign_encoded(claims, options, |message| {
        if options.algorithm == Algorithm::ES256 {
            // RFC 6979 preserves query purity without user-selected ECDSA nonces.
            let key = SigningKey::from_pkcs8_der(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("Invalid ES256 private key"))?;
            let signature: Signature = key
                .try_sign(message)
                .map_err(|_| anyhow::anyhow!("ES256 signing failed"))?;
            Ok(URL_SAFE_NO_PAD.encode(signature.to_bytes()))
        } else {
            Ok(jsonwebtoken::crypto::sign(
                message,
                &key,
                options.algorithm.jwt(),
            )?)
        }
    })
}

pub(crate) fn sign_with(
    claims: &Value,
    options: &SignOptions,
    sign: impl FnOnce(&[u8]) -> Result<Vec<u8>>,
) -> Result<String> {
    sign_encoded(claims, options, |message| {
        Ok(URL_SAFE_NO_PAD.encode(sign(message)?))
    })
}

fn sign_encoded(
    claims: &Value,
    options: &SignOptions,
    sign: impl FnOnce(&[u8]) -> Result<String>,
) -> Result<String> {
    object(claims)?;
    let mut protected = protected("", &options.kid, &options.typ);
    protected["alg"] = serde_json::to_value(options.algorithm)?;
    let mut message = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&protected)?),
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims)?)
    );
    let signature = sign(message.as_bytes())?;
    message.push('.');
    message.push_str(&signature);
    Ok(message)
}

struct Validation<'a> {
    issuer: &'a Option<String>,
    audience: &'a Option<Vec<String>>,
    subject: &'a Option<String>,
    tolerance: f64,
    require_expiration: bool,
    typ: &'a Option<String>,
}

fn validate(claims: &Value, protected: &Value, options: Validation<'_>, now_ms: u64) -> Result<()> {
    let claims = object(claims)?;
    ensure!(
        options.tolerance.is_finite() && options.tolerance >= 0.0,
        "JWT clock tolerance must be finite and nonnegative"
    );
    if let Some(typ) = options.typ {
        ensure!(
            protected.get("typ").and_then(Value::as_str) == Some(typ),
            "JWT type mismatch"
        );
    }
    let numeric_date = |name: &str| -> Result<Option<f64>> {
        claims
            .get(name)
            .map(|v| {
                v.as_f64()
                    .filter(|v| v.is_finite())
                    .with_context(|| format!("JWT {name} must be a finite NumericDate"))
            })
            .transpose()
    };
    let exp = numeric_date("exp")?;
    let nbf = numeric_date("nbf")?;
    numeric_date("iat")?;
    let now = now_ms as f64 / 1000.0;
    ensure!(
        !options.require_expiration || exp.is_some(),
        "JWT expiration is required"
    );
    if let Some(exp) = exp {
        ensure!(now - options.tolerance < exp, "JWT expired");
    }
    if let Some(nbf) = nbf {
        ensure!(now + options.tolerance >= nbf, "JWT is not active yet");
    }
    for (name, expected) in [("iss", options.issuer), ("sub", options.subject)] {
        let actual = claims
            .get(name)
            .map(|v| {
                v.as_str()
                    .with_context(|| format!("JWT {name} must be a string"))
            })
            .transpose()?;
        if let Some(expected) = expected {
            ensure!(actual == Some(expected.as_str()), "JWT {name} mismatch");
        }
    }
    let audiences = match claims.get("aud") {
        None => None,
        Some(Value::String(value)) => Some(vec![value.as_str()]),
        Some(Value::Array(values)) => Some(
            values
                .iter()
                .map(|v| v.as_str().context("JWT audience entries must be strings"))
                .collect::<Result<Vec<_>>>()?,
        ),
        Some(_) => bail!("JWT audience must be a string or string array"),
    };
    match (audiences, options.audience) {
        (None, None) => (),
        (Some(actual), Some(expected)) => ensure!(
            actual.iter().any(|a| expected.iter().any(|e| a == e)),
            "JWT audience mismatch"
        ),
        (None, Some(_)) => bail!("JWT audience is required"),
        (Some(_), None) => bail!("JWT audience must be explicitly validated"),
    }
    Ok(())
}

/// When time alone next changes the outcome of verifying token at now_ms: a
/// token that is valid expires at exp + tolerance, and one not active yet
/// activates at nbf - tolerance. Reads the claims without verifying them: a
/// forged token's outcome never changes, so a bogus time costs one extra wake.
pub fn changes_at(token: &str, tolerance_seconds: f64, now_ms: u64) -> Option<u64> {
    let claims = parse_json(&decode_segment(token.split('.').nth(1)?).ok()?).ok()?;
    claims_change(&claims, tolerance_seconds, now_ms)
}

/// The same, for claims already read, such as those of a decrypted token.
pub fn claims_change(claims: &Value, tolerance_seconds: f64, now_ms: u64) -> Option<u64> {
    let claim = |name: &str| claims.get(name)?.as_f64().filter(|value| value.is_finite());
    let tolerance = if tolerance_seconds.is_finite() {
        tolerance_seconds.max(0.0)
    } else {
        0.0
    };
    let now = now_ms as f64 / 1000.0;
    let boundary = match (claim("nbf"), claim("exp")) {
        (Some(nbf), _) if now + tolerance < nbf => nbf - tolerance,
        (_, Some(exp)) if now - tolerance < exp => exp + tolerance,
        _ => return None,
    };
    let ms = (boundary * 1000.0).ceil();
    (ms > now_ms as f64).then(|| ms.min(9_007_199_254_740_991.0) as u64)
}

/// Signatures verified before, by a digest of all that verification read,
/// so a token presented on every call costs its signature check once. Only
/// successes are kept; claims, and time against them, are checked each call.
struct Verifications {
    known: std::collections::HashSet<[u8; 32]>,
    order: std::collections::VecDeque<[u8; 32]>,
}

const VERIFICATIONS: usize = 65_536;

static VERIFIED: std::sync::LazyLock<std::sync::Mutex<Verifications>> =
    std::sync::LazyLock::new(|| {
        std::sync::Mutex::new(Verifications {
            known: std::collections::HashSet::new(),
            order: std::collections::VecDeque::new(),
        })
    });

fn verification(
    key: &[u8],
    format: KeyFormat,
    algorithm: Algorithm,
    message: &[u8],
    signature: &[u8],
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    for part in [key, message, signature] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part);
    }
    digest.update([format as u8, algorithm as u8]);
    digest.finalize().into()
}

pub fn verify(token: &str, key: &[u8], options: &VerifyOptions, now_ms: u64) -> Result<Verified> {
    verify_with(token, options, now_ms, |algorithm, message, signature| {
        let verification = verification(key, options.key_format, algorithm, message, signature);
        let verified = || VERIFIED.lock().unwrap_or_else(|error| error.into_inner());
        if verified().known.contains(&verification) {
            return Ok(true);
        }
        let key = verifying_key(key, algorithm, options.key_format)?;
        let valid = jsonwebtoken::crypto::verify(
            token.rsplit('.').next().unwrap_or_default(),
            message,
            &key,
            algorithm.jwt(),
        )
        .unwrap_or(false);
        if valid {
            let mut verified = verified();
            if verified.known.insert(verification) {
                verified.order.push_back(verification);
                if verified.order.len() > VERIFICATIONS
                    && let Some(oldest) = verified.order.pop_front()
                {
                    verified.known.remove(&oldest);
                }
            }
        }
        Ok(valid)
    })
}

pub(crate) fn verify_with(
    token: &str,
    options: &VerifyOptions,
    now_ms: u64,
    verify: impl FnOnce(Algorithm, &[u8], &[u8]) -> Result<bool>,
) -> Result<Verified> {
    ensure!(
        !options.algorithms.is_empty(),
        "JWT algorithm allowlist must not be empty"
    );
    ensure!(
        options
            .algorithms
            .iter()
            .all(|algorithm| (*algorithm == Algorithm::HS256)
                == (options.key_format == KeyFormat::Raw)),
        "JWT algorithm allowlist must match the explicit key format"
    );
    let mut parts = token.split('.');
    let (Some(protected), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("JWS compact form requires three segments")
    };
    let protected_header = header(protected, false)?;
    let algorithm: Algorithm = serde_json::from_value(
        protected_header
            .get("alg")
            .cloned()
            .context("JWT algorithm is required")?,
    )
    .context("Unsupported JWT algorithm")?;
    ensure!(
        options.algorithms.contains(&algorithm),
        "JWT algorithm is not allowed"
    );
    // Validate canonical base64url before a provider accepts an alternate encoding.
    let signature_bytes = decode_segment(signature)?;
    let length_valid = match algorithm {
        Algorithm::HS256 => signature_bytes.len() == 32,
        Algorithm::ES256 | Algorithm::EdDSA => signature_bytes.len() == 64,
        Algorithm::RS256 => (256..=1024).contains(&signature_bytes.len()),
    };
    ensure!(length_valid, "Invalid JWT signature length");
    let message = &token[..protected.len() + 1 + payload.len()];
    ensure!(
        verify(algorithm, message.as_bytes(), &signature_bytes)?,
        "JWT signature verification failed"
    );
    let claims = parse_json(&decode_segment(payload)?)?;
    validate(
        &claims,
        &protected_header,
        Validation {
            issuer: &options.issuer,
            audience: &options.audience,
            subject: &options.subject,
            tolerance: options.clock_tolerance_seconds,
            require_expiration: options.require_expiration,
            typ: &options.typ,
        },
        now_ms,
    )?;
    Ok(Verified {
        claims,
        protected_header,
    })
}

fn cipher(key: &[u8]) -> Result<LessSafeKey> {
    ensure!(key.len() == 32, "A256GCM requires exactly 32 key bytes");
    Ok(LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, key).map_err(|_| anyhow::anyhow!("Invalid A256GCM key"))?,
    ))
}

pub fn encrypt(
    claims: &Value,
    key: &[u8],
    nonce: &[u8],
    options: &EncryptOptions,
) -> Result<String> {
    encrypt_with(claims, &cipher(key)?, nonce, options)
}

pub(crate) fn encrypt_with(
    claims: &Value,
    cipher: &LessSafeKey,
    nonce: &[u8],
    options: &EncryptOptions,
) -> Result<String> {
    object(claims)?;
    let iv: [u8; 12] = nonce
        .try_into()
        .context("A256GCM requires exactly 12 nonce bytes")?;
    let mut protected = protected("dir", &options.kid, &options.typ);
    protected["enc"] = json!("A256GCM");
    let protected = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&protected)?);
    let mut ciphertext = Zeroizing::new(serde_json::to_vec(claims)?);
    cipher
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(iv),
            Aad::from(protected.as_bytes()),
            &mut *ciphertext,
        )
        .map_err(|_| anyhow::anyhow!("JWT encryption failed"))?;
    let (ciphertext, tag) = ciphertext.split_at(ciphertext.len() - 16);
    Ok(format!(
        "{protected}..{}.{}.{}",
        URL_SAFE_NO_PAD.encode(iv),
        URL_SAFE_NO_PAD.encode(ciphertext),
        URL_SAFE_NO_PAD.encode(tag)
    ))
}

pub fn decrypt(token: &str, key: &[u8], options: &DecryptOptions, now_ms: u64) -> Result<Verified> {
    decrypt_with(token, &cipher(key)?, options, now_ms)
}

pub(crate) fn decrypt_with(
    token: &str,
    cipher: &LessSafeKey,
    options: &DecryptOptions,
    now_ms: u64,
) -> Result<Verified> {
    let mut parts = token.split('.');
    let (Some(protected), Some(encrypted_key), Some(iv), Some(ciphertext), Some(tag), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        bail!("JWE compact form requires five segments")
    };
    ensure!(
        encrypted_key.is_empty(),
        "Direct JWE encrypted-key segment must be empty"
    );
    let protected_header = header(protected, true)?;
    ensure!(
        protected_header["alg"] == "dir" && protected_header["enc"] == "A256GCM",
        "Only dir/A256GCM JWE is supported"
    );
    let nonce: [u8; 12] = decode_segment(iv)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid A256GCM nonce length"))?;
    let mut ciphertext = Zeroizing::new(decode_segment(ciphertext)?);
    let tag = decode_segment(tag)?;
    ensure!(tag.len() == 16, "Invalid A256GCM tag length");
    ciphertext.extend_from_slice(&tag);
    let plaintext = cipher
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(protected.as_bytes()),
            &mut ciphertext,
        )
        .map_err(|_| anyhow::anyhow!("JWT decryption failed"))?;
    let claims = parse_json(plaintext)?;
    validate(
        &claims,
        &protected_header,
        Validation {
            issuer: &options.issuer,
            audience: &options.audience,
            subject: &options.subject,
            tolerance: options.clock_tolerance_seconds,
            require_expiration: options.require_expiration,
            typ: &options.typ,
        },
        now_ms,
    )?;
    Ok(Verified {
        claims,
        protected_header,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn outcomes_change_at_expiry_or_activation_only() {
        let token = |claims: serde_json::Value| {
            format!(
                "e30.{}.sig",
                super::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
            )
        };
        let window = token(serde_json::json!({"nbf": 10, "exp": 20}));
        assert_eq!(
            super::changes_at(&window, 0.0, 5_000),
            Some(10_000),
            "activates at nbf"
        );
        assert_eq!(
            super::changes_at(&window, 2.0, 5_000),
            Some(8_000),
            "tolerance moves activation earlier"
        );
        assert_eq!(
            super::changes_at(&window, 0.0, 10_000),
            Some(20_000),
            "a valid token expires"
        );
        assert_eq!(
            super::changes_at(&window, 1.5, 19_000),
            Some(21_500),
            "tolerance moves expiry later"
        );
        assert_eq!(
            super::changes_at(&window, 0.0, 20_000),
            None,
            "an expired token stays expired"
        );
        assert_eq!(
            super::changes_at(&token(serde_json::json!({"sub": "a"})), 0.0, 0),
            None
        );
        assert_eq!(
            super::changes_at(&token(serde_json::json!({"exp": 1.0005})), 0.0, 1_000),
            Some(1_001)
        );
        assert_eq!(super::changes_at("not a token", 0.0, 0), None);
    }

    use super::*;
    // Independent fixtures generated with Node's built-in OpenSSL-backed crypto.
    // These private keys are public test material, never production secrets.
    const HS: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZmxvd2VyIiwib3RoZXIiXSwiZXhwIjoyMDAwLCJpYXQiOjkwMCwiaXNzIjoiaXNzdWVyIiwibmJmIjo5NTAsInN1YiI6ImFsaWNlIiwidW5pY29kZSI6IvCfjLcifQ.VCFdMMwoC4QkRbBiiElYMwrHtiIGSYejrRATpc0qrFs";
    const JWE: &str = "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIn0..AwMDAwMDAwMDAwMD.XtzCdj4KZBlYJi8znDKNLsEej5vc8OZScvKl1JuVYoKDl00St9nMIbqqcTy9_stnTldg5rOgVARfXUvRHACqBpWFGUKWk0Q9a5XC5fat5lKRw6oQKE9sWII4t7gXZQM45zKvKxtBXQ.2pnTKtx3ycKJ8YlGWh9few";
    const KEYS: &[(&str, &str, &str, &str, &str, &str)] = &[
        (
            "RS256",
            "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDEV9PTYpuF3MxQ\nfUWjGjioQ2apH1Upyeq/SwqK8/txdQvcU7Z6mKrRkVOnn+Ts/6qkGyHnRuSw2+ME\nGT8xRsuR7GKqPoLuhu6rqKwlap4ZBtpICm8TE+OzqKwWH/mWPdSjrbKIDaX5wSlH\n0JyKKhtoomAISRLZeOZ1nfwwGiICjMdJZZgMfoH1G/qyB5GuG4qOpXyR0zUYQDLt\newQ5YF4e6fk5L0Up7jnOfYXP6O44j1mJMfVsKj/XuOL+D5NMiWxt+bHV6AJWkjnx\n+ifG7c/HeQlcNDFALE0P2h6DaKewF5YEh2gSnFbgMcnNPHKNfzs3Af3u12UoKW3l\nwPmIoc5TAgMBAAECggEACne8PVrWe86HvgrPuaBeQOpHAOFAwxeWwlgX2cykLSpW\nVYrJAcQ95ypeUWN+6vu+dz1TE2d+LcerVL6b1d62X7NAl1750Am1k8VMWDpU73Sk\nEo8r2NKIoz1s30kZH19whMFv8Tz5ClW4A7IlhmA0UeHGSOrMbHe7oa2okk/yXDxZ\nF9q+4DeRT4UCaQREJQN0TK5duv9/5ulZJ47m692CJ7C5NmVFdxi60iNDMY26oB+f\n1Q/C4RsrLtPcVYcUC6Grg6jU78z4oOK0iTj0lYlSHhwevNdnb/NmPG/wgTaTDvoj\nY8o97cHUgdKOdIJf64OhgwjH9/5/V0uR8jHhmWZnUQKBgQD8tM3X/BRZR2IZjBOR\nL+n72CmUpseepwhqShc4z18Si9nHW/mdhsL+j/BZlVyDVihTJ4ySiDj//SguGhWD\ny70sTx07N0frH54PDmcb9dKiTH1Lcagl8lUoKE4vByVYm50aOjpr8lAqwx04EHtI\nKKFwZ7usVTUZ0MHeffqZ2YD+NQKBgQDG5vVVdhQtJEBCTOHkjqKWeCg0BHPbI1Cw\nr7HdFr2X+pm6D7q4dlB1NZtKD/w2SdbV+Vdu18a7h/0PDjj0yAkgOYFrlUD0734Z\n5V6/JFiaGyF8fYNgyEiyUxeEIsvchV7oYR08BC4c5jKWybxMZhJS5MtNACz3iZmV\nuUMXfnpLZwKBgQDC3/8ZPyjGDHlHMDFqtiNfdjviiZbI7xBbPxWXVrt/Rt/DkFb3\nNpQq0P9NZhQ4p/li3s3Vtj0Wk7gnjS/oOfaBM+Vb4+6PEAvImpfDBRfQ1uGMi3Jb\nCPzIggSA2abgJOjK7/pbgjp2L47ZzEP1yndsgmJErFTNuqG2nTni6MtDvQKBgFLI\nFRt4hXU0PTpa3TlO1ARkBfeAUufFjvO6bABkUoxKVGjH2yKiu2HM6dCtTn8ZxDxS\nBj2vuJqcQopdlP7rskCjLmYkPGC0vHryp7hN3EJnQEybwG4rbXYqdwMbqFUjfRii\nMpSj+L02YZ+4XpI9eSre5m4pwI1Vy4IxFOdWUHfJAoGASqXa2vSaiA+WCCfqcdWB\nseDiH5fnUUZjq5ti+M2FuzfrfItaWfExPK5u2pCb+ZFrcCZjwYAHYrqxiyjl5be8\nv1Evmr6Iv3Dl3F7y4lE4RyVEFIHE3b/4h69/ak7kLPNglt5L9PiGlledwaQsJGaU\nEoUQYgE8pxsDWp3gKnOn0XM=\n-----END PRIVATE KEY-----\n",
            "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxFfT02KbhdzMUH1Foxo4\nqENmqR9VKcnqv0sKivP7cXUL3FO2epiq0ZFTp5/k7P+qpBsh50bksNvjBBk/MUbL\nkexiqj6C7obuq6isJWqeGQbaSApvExPjs6isFh/5lj3Uo62yiA2l+cEpR9Cciiob\naKJgCEkS2XjmdZ38MBoiAozHSWWYDH6B9Rv6sgeRrhuKjqV8kdM1GEAy7XsEOWBe\nHun5OS9FKe45zn2Fz+juOI9ZiTH1bCo/17ji/g+TTIlsbfmx1egCVpI58fonxu3P\nx3kJXDQxQCxND9oeg2insBeWBIdoEpxW4DHJzTxyjX87NwH97tdlKClt5cD5iKHO\nUwIDAQAB\n-----END PUBLIC KEY-----\n",
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDEV9PTYpuF3MxQfUWjGjioQ2apH1Upyeq_SwqK8_txdQvcU7Z6mKrRkVOnn-Ts_6qkGyHnRuSw2-MEGT8xRsuR7GKqPoLuhu6rqKwlap4ZBtpICm8TE-OzqKwWH_mWPdSjrbKIDaX5wSlH0JyKKhtoomAISRLZeOZ1nfwwGiICjMdJZZgMfoH1G_qyB5GuG4qOpXyR0zUYQDLtewQ5YF4e6fk5L0Up7jnOfYXP6O44j1mJMfVsKj_XuOL-D5NMiWxt-bHV6AJWkjnx-ifG7c_HeQlcNDFALE0P2h6DaKewF5YEh2gSnFbgMcnNPHKNfzs3Af3u12UoKW3lwPmIoc5TAgMBAAECggEACne8PVrWe86HvgrPuaBeQOpHAOFAwxeWwlgX2cykLSpWVYrJAcQ95ypeUWN-6vu-dz1TE2d-LcerVL6b1d62X7NAl1750Am1k8VMWDpU73SkEo8r2NKIoz1s30kZH19whMFv8Tz5ClW4A7IlhmA0UeHGSOrMbHe7oa2okk_yXDxZF9q-4DeRT4UCaQREJQN0TK5duv9_5ulZJ47m692CJ7C5NmVFdxi60iNDMY26oB-f1Q_C4RsrLtPcVYcUC6Grg6jU78z4oOK0iTj0lYlSHhwevNdnb_NmPG_wgTaTDvojY8o97cHUgdKOdIJf64OhgwjH9_5_V0uR8jHhmWZnUQKBgQD8tM3X_BRZR2IZjBORL-n72CmUpseepwhqShc4z18Si9nHW_mdhsL-j_BZlVyDVihTJ4ySiDj__SguGhWDy70sTx07N0frH54PDmcb9dKiTH1Lcagl8lUoKE4vByVYm50aOjpr8lAqwx04EHtIKKFwZ7usVTUZ0MHeffqZ2YD-NQKBgQDG5vVVdhQtJEBCTOHkjqKWeCg0BHPbI1Cwr7HdFr2X-pm6D7q4dlB1NZtKD_w2SdbV-Vdu18a7h_0PDjj0yAkgOYFrlUD0734Z5V6_JFiaGyF8fYNgyEiyUxeEIsvchV7oYR08BC4c5jKWybxMZhJS5MtNACz3iZmVuUMXfnpLZwKBgQDC3_8ZPyjGDHlHMDFqtiNfdjviiZbI7xBbPxWXVrt_Rt_DkFb3NpQq0P9NZhQ4p_li3s3Vtj0Wk7gnjS_oOfaBM-Vb4-6PEAvImpfDBRfQ1uGMi3JbCPzIggSA2abgJOjK7_pbgjp2L47ZzEP1yndsgmJErFTNuqG2nTni6MtDvQKBgFLIFRt4hXU0PTpa3TlO1ARkBfeAUufFjvO6bABkUoxKVGjH2yKiu2HM6dCtTn8ZxDxSBj2vuJqcQopdlP7rskCjLmYkPGC0vHryp7hN3EJnQEybwG4rbXYqdwMbqFUjfRiiMpSj-L02YZ-4XpI9eSre5m4pwI1Vy4IxFOdWUHfJAoGASqXa2vSaiA-WCCfqcdWBseDiH5fnUUZjq5ti-M2FuzfrfItaWfExPK5u2pCb-ZFrcCZjwYAHYrqxiyjl5be8v1Evmr6Iv3Dl3F7y4lE4RyVEFIHE3b_4h69_ak7kLPNglt5L9PiGlledwaQsJGaUEoUQYgE8pxsDWp3gKnOn0XM",
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxFfT02KbhdzMUH1Foxo4qENmqR9VKcnqv0sKivP7cXUL3FO2epiq0ZFTp5_k7P-qpBsh50bksNvjBBk_MUbLkexiqj6C7obuq6isJWqeGQbaSApvExPjs6isFh_5lj3Uo62yiA2l-cEpR9CciiobaKJgCEkS2XjmdZ38MBoiAozHSWWYDH6B9Rv6sgeRrhuKjqV8kdM1GEAy7XsEOWBeHun5OS9FKe45zn2Fz-juOI9ZiTH1bCo_17ji_g-TTIlsbfmx1egCVpI58fonxu3Px3kJXDQxQCxND9oeg2insBeWBIdoEpxW4DHJzTxyjX87NwH97tdlKClt5cD5iKHOUwIDAQAB",
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZmxvd2VyIiwib3RoZXIiXSwiZXhwIjoyMDAwLCJpYXQiOjkwMCwiaXNzIjoiaXNzdWVyIiwibmJmIjo5NTAsInN1YiI6ImFsaWNlIiwidW5pY29kZSI6IvCfjLcifQ.wsMk5sfTxO80c82b4C04lpNqEjx1K84wnf20pU4cgwmdNx-dONqsxFlRl4JJsqWCY9UMSt6MU-4orniMUHZJ-0n-SCI3LZLYSy6PMnd8b1vrmcbYv4ZbB9ZsVzgDBXH5JVwFpJfeB13SBXJ7x-3c4uEwUYwQD0lNft29L1s5-bUmAx5TMcDtWGOcg4hodardysBbhe1FR_iDYa5YfoiWfBDhFFOGN-9R22TC3uwm6c3aQGlWMNID8U51boEqkaJffsEblAIvginMmIrP1yttG6reNd-ektZTHCOIT05ZQXjcyl4Wmb4jOpoYSZ4PWa-nGvry7-GNF0KpdeViVw-94g",
        ),
        (
            "ES256",
            "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgbmnP12MioT1SBbk4\n94+J8fwFna8vCI7eBZzBuvGyA0KhRANCAASGlIEziNUdd60p3/u5NzttW1UdCcUp\n/lYofyeQ+SpZurIL8BwNsn5ZW1DGDtqltxBHDBY59QfxXptmEdBU8reC\n-----END PRIVATE KEY-----\n",
            "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEhpSBM4jVHXetKd/7uTc7bVtVHQnF\nKf5WKH8nkPkqWbqyC/AcDbJ+WVtQxg7apbcQRwwWOfUH8V6bZhHQVPK3gg==\n-----END PUBLIC KEY-----\n",
            "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgbmnP12MioT1SBbk494-J8fwFna8vCI7eBZzBuvGyA0KhRANCAASGlIEziNUdd60p3_u5NzttW1UdCcUp_lYofyeQ-SpZurIL8BwNsn5ZW1DGDtqltxBHDBY59QfxXptmEdBU8reC",
            "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEhpSBM4jVHXetKd_7uTc7bVtVHQnFKf5WKH8nkPkqWbqyC_AcDbJ-WVtQxg7apbcQRwwWOfUH8V6bZhHQVPK3gg",
            "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZmxvd2VyIiwib3RoZXIiXSwiZXhwIjoyMDAwLCJpYXQiOjkwMCwiaXNzIjoiaXNzdWVyIiwibmJmIjo5NTAsInN1YiI6ImFsaWNlIiwidW5pY29kZSI6IvCfjLcifQ.kh5ywYjfyqmq7dyvn4JVqa2uDzdWOwZjS3mg5JrL3iMs6KI1q62BPOJOlEbE-1KgMLJVZ1CjDwpNbwRiCf-rnw",
        ),
        (
            "EdDSA",
            "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIFJBFjdgOgsFacZKjmURjC6eWw36X5TctZhpiuxmP4G+\n-----END PRIVATE KEY-----\n",
            "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAQxL1YoHWx9xtfzkKiZvYm29eqAveGtn7ayRjUPF2Z9o=\n-----END PUBLIC KEY-----\n",
            "MC4CAQAwBQYDK2VwBCIEIFJBFjdgOgsFacZKjmURjC6eWw36X5TctZhpiuxmP4G-",
            "MCowBQYDK2VwAyEAQxL1YoHWx9xtfzkKiZvYm29eqAveGtn7ayRjUPF2Z9o",
            "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZmxvd2VyIiwib3RoZXIiXSwiZXhwIjoyMDAwLCJpYXQiOjkwMCwiaXNzIjoiaXNzdWVyIiwibmJmIjo5NTAsInN1YiI6ImFsaWNlIiwidW5pY29kZSI6IvCfjLcifQ.2YiqHI6204fOQJJkq9CBsYC396NMgulfyAXnyKNJQfpISR3BOBE9243460F6aaCk1SwKaxFV5TX9RMoQCJ6EAg",
        ),
    ];

    fn claims() -> Value {
        json!({"aud":["flower","other"],"exp":2000,"iat":900,"iss":"issuer","nbf":950,"sub":"alice","unicode":"🌷"})
    }
    fn options(algorithm: Algorithm, format: KeyFormat) -> VerifyOptions {
        VerifyOptions {
            algorithms: vec![algorithm],
            key_format: format,
            issuer: Some("issuer".into()),
            audience: Some(vec!["flower".into()]),
            subject: Some("alice".into()),
            clock_tolerance_seconds: 0.0,
            require_expiration: true,
            typ: Some("JWT".into()),
        }
    }
    fn decryption() -> DecryptOptions {
        DecryptOptions {
            issuer: Some("issuer".into()),
            audience: Some(vec!["flower".into()]),
            subject: Some("alice".into()),
            clock_tolerance_seconds: 0.0,
            require_expiration: true,
            typ: Some("JWT".into()),
        }
    }
    fn hs_options() -> SignOptions {
        SignOptions {
            algorithm: Algorithm::HS256,
            key_format: KeyFormat::Raw,
            kid: None,
            typ: None,
        }
    }
    fn signed(claims: &Value) -> String {
        sign(claims, &[7; 32], &hs_options()).unwrap()
    }
    fn hs_raw(header: &str, claims: &str) -> String {
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header),
            URL_SAFE_NO_PAD.encode(claims)
        );
        let sig = jsonwebtoken::crypto::sign(
            message.as_bytes(),
            &EncodingKey::from_secret(&[7; 32]),
            jsonwebtoken::Algorithm::HS256,
        )
        .unwrap();
        format!("{message}.{sig}")
    }

    #[test]
    fn interoperable_signatures_and_deterministic_output_for_all_algorithms() {
        assert_eq!(signed(&claims()), HS);
        assert_eq!(
            verify(
                HS,
                &[7; 32],
                &options(Algorithm::HS256, KeyFormat::Raw),
                1_000_000
            )
            .unwrap()
            .claims,
            claims()
        );
        for (algorithm, private, public, private_der, public_der, token) in KEYS {
            let algorithm: Algorithm = serde_json::from_value(json!(algorithm)).unwrap();
            let verify_options = options(algorithm, KeyFormat::Pem);
            assert_eq!(
                verify(token, public.as_bytes(), &verify_options, 1_000_000)
                    .unwrap()
                    .claims,
                claims()
            );
            let sign_options = SignOptions {
                algorithm,
                key_format: KeyFormat::Pem,
                kid: None,
                typ: None,
            };
            let native = sign(&claims(), private.as_bytes(), &sign_options).unwrap();
            assert_eq!(
                native,
                sign(&claims(), private.as_bytes(), &sign_options).unwrap()
            );
            assert_eq!(
                verify(&native, public.as_bytes(), &verify_options, 1_000_000)
                    .unwrap()
                    .claims,
                claims()
            );
            // RSA PKCS#1 v1.5 and Ed25519 match the independently generated bytes;
            // ECDSA uses RFC 6979 while OpenSSL's fixture uses a random nonce.
            if algorithm != Algorithm::ES256 {
                assert_eq!(&native, token);
            }
            let mut sign_der = sign_options.clone();
            sign_der.key_format = KeyFormat::Der;
            assert_eq!(
                native,
                sign(&claims(), &decode_segment(private_der).unwrap(), &sign_der).unwrap()
            );
            let mut verify_der = verify_options.clone();
            verify_der.key_format = KeyFormat::Der;
            assert!(
                verify(
                    token,
                    &decode_segment(public_der).unwrap(),
                    &verify_der,
                    1_000_000
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn jwe_matches_independent_aes_gcm_fixture_and_binds_every_segment() {
        let token = encrypt(&claims(), &[7; 32], &[3; 12], &EncryptOptions::default()).unwrap();
        assert_eq!(token, JWE);
        let decoded = decrypt(JWE, &[7; 32], &decryption(), 1_000_000).unwrap();
        assert_eq!(decoded.claims, claims());
        assert_eq!(
            decoded.protected_header,
            json!({"alg":"dir","enc":"A256GCM","typ":"JWT"})
        );
        for index in [0, 2, 3, 4] {
            let mut parts: Vec<String> = JWE.split('.').map(str::to_owned).collect();
            if index == 0 {
                parts[index] =
                    URL_SAFE_NO_PAD.encode(br#"{"alg":"dir","enc":"A256GCM","typ":"other"}"#);
            } else {
                let mut bytes = decode_segment(&parts[index]).unwrap();
                bytes[0] ^= 1;
                parts[index] = URL_SAFE_NO_PAD.encode(bytes);
            }
            assert!(decrypt(&parts.join("."), &[7; 32], &decryption(), 1_000_000).is_err());
        }
        assert!(decrypt(JWE, &[8; 32], &decryption(), 1_000_000).is_err());
        assert!(
            decrypt(
                &JWE.replacen("..", ".AA.", 1),
                &[7; 32],
                &decryption(),
                1_000_000
            )
            .is_err()
        );
        assert!(encrypt(&claims(), &[7; 31], &[3; 12], &EncryptOptions::default()).is_err());
        assert!(encrypt(&claims(), &[7; 32], &[3; 11], &EncryptOptions::default()).is_err());
        assert!(decrypt(JWE, &[7; 32], &decryption(), 2_000_000).is_err());
    }

    #[test]
    fn time_is_explicit_with_precise_expiration_not_before_and_tolerance_boundaries() {
        let mut opts = options(Algorithm::HS256, KeyFormat::Raw);
        assert!(verify(HS, &[7; 32], &opts, 949_999).is_err());
        assert!(verify(HS, &[7; 32], &opts, 950_000).is_ok());
        assert!(verify(HS, &[7; 32], &opts, 1_999_999).is_ok());
        assert!(verify(HS, &[7; 32], &opts, 2_000_000).is_err());
        opts.clock_tolerance_seconds = 2.5;
        assert!(verify(HS, &[7; 32], &opts, 947_500).is_ok());
        assert!(verify(HS, &[7; 32], &opts, 2_002_499).is_ok());
        assert!(verify(HS, &[7; 32], &opts, 2_002_500).is_err());
        for tolerance in [-1.0, f64::INFINITY, f64::NAN] {
            opts.clock_tolerance_seconds = tolerance;
            assert!(verify(HS, &[7; 32], &opts, 1_000_000).is_err());
        }
        let mut value = claims();
        value.as_object_mut().unwrap().remove("exp");
        opts.clock_tolerance_seconds = 0.0;
        assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
        opts.require_expiration = false;
        assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_ok());
        for name in ["exp", "nbf", "iat"] {
            let mut value = claims();
            value[name] = json!("1000");
            assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
        }
        let mut fraction = claims();
        fraction["exp"] = json!(1000.5);
        assert!(verify(&signed(&fraction), &[7; 32], &opts, 1_000_499).is_ok());
        assert!(verify(&signed(&fraction), &[7; 32], &opts, 1_000_500).is_err());
    }

    #[test]
    fn remembered_signatures_still_check_their_claims_and_never_vouch_for_others() {
        let opts = options(Algorithm::HS256, KeyFormat::Raw);
        assert!(verify(HS, &[7; 32], &opts, 1_000_000).is_ok());
        // The same token, now expired, and under another key.
        assert!(verify(HS, &[7; 32], &opts, 2_000_000).is_err());
        assert!(verify(HS, &[8; 32], &opts, 1_000_000).is_err());
        let (message, _) = HS.rsplit_once('.').unwrap();
        let forged = format!("{message}.{}", "A".repeat(43));
        assert!(verify(&forged, &[7; 32], &opts, 1_000_000).is_err());
        let mut value = claims();
        value["unicode"] = json!("🌼");
        let other = signed(&value);
        let (_, signature) = HS.rsplit_once('.').unwrap();
        let (message, _) = other.rsplit_once('.').unwrap();
        assert!(
            verify(
                &format!("{message}.{signature}"),
                &[7; 32],
                &opts,
                1_000_000
            )
            .is_err()
        );
        assert!(verify(&other, &[7; 32], &opts, 1_000_000).is_ok());
    }

    #[test]
    fn explicit_audience_issuer_subject_and_type_checks() {
        let opts = options(Algorithm::HS256, KeyFormat::Raw);
        for name in ["aud", "iss", "sub"] {
            let mut value = claims();
            value[name] = json!("wrong");
            assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
            value[name] = Value::Null;
            assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
            value.as_object_mut().unwrap().remove(name);
            assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
        }
        let mut value = claims();
        value["aud"] = json!("flower");
        assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_ok());
        value["aud"] = json!(["flower", 2]);
        assert!(verify(&signed(&value), &[7; 32], &opts, 1_000_000).is_err());
        let mut no_audience = opts.clone();
        no_audience.audience = None;
        assert!(verify(HS, &[7; 32], &no_audience, 1_000_000).is_err());
        let mut wrong_type = opts.clone();
        wrong_type.typ = Some("other".into());
        assert!(verify(HS, &[7; 32], &wrong_type, 1_000_000).is_err());
    }

    #[test]
    fn algorithm_and_key_confusion_tampering_and_unsupported_headers_are_rejected() {
        let mut opts = options(Algorithm::HS256, KeyFormat::Raw);
        assert!(verify(HS, &[8; 32], &opts, 1_000_000).is_err());
        opts.algorithms.clear();
        assert!(verify(HS, &[7; 32], &opts, 1_000_000).is_err());
        opts.algorithms = vec![Algorithm::RS256];
        assert!(verify(HS, &[7; 32], &opts, 1_000_000).is_err());
        opts.algorithms = vec![Algorithm::HS256, Algorithm::RS256];
        opts.key_format = KeyFormat::Pem;
        let public = KEYS[0].2.as_bytes();
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims()).unwrap())
        );
        let signature = jsonwebtoken::crypto::sign(
            message.as_bytes(),
            &EncodingKey::from_secret(public),
            jsonwebtoken::Algorithm::HS256,
        )
        .unwrap();
        let forged = format!("{message}.{signature}");
        assert!(sign(&claims(), public, &hs_options()).is_err());
        assert!(verify(&forged, public, &opts, 1_000_000).is_err());
        opts.key_format = KeyFormat::Raw;
        assert!(verify(HS, &[7; 32], &opts, 1_000_000).is_err());
        opts.algorithms = vec![Algorithm::HS256];
        for header in [
            r#"{"alg":"none"}"#,
            r#"{"alg":"HS512"}"#,
            r#"{"alg":"HS256","crit":[]}"#,
            r#"{"alg":"HS256","b64":false}"#,
            r#"{"alg":"HS256","jwk":{}}"#,
            r#"{"alg":"HS256","typ":3}"#,
        ] {
            assert!(
                verify(
                    &hs_raw(header, &claims().to_string()),
                    &[7; 32],
                    &opts,
                    1_000_000
                )
                .is_err()
            );
        }
        assert!(sign(&claims(), &[7; 31], &hs_options()).is_err());
        let mut invalid_pem = KEYS[0].1.as_bytes().to_vec();
        invalid_pem.push(0xff);
        assert!(
            sign(
                &claims(),
                &invalid_pem,
                &SignOptions {
                    algorithm: Algorithm::RS256,
                    key_format: KeyFormat::Pem,
                    kid: None,
                    typ: None
                }
            )
            .is_err()
        );
        let mutated = HS.replacen("eyJhdWQi", "eyJhdWQj", 1);
        assert!(verify(&mutated, &[7; 32], &opts, 1_000_000).is_err());
        assert!(verify(&format!("{HS}.extra"), &[7; 32], &opts, 1_000_000).is_err());
    }

    #[test]
    fn duplicate_json_non_object_payload_and_noncanonical_encoding_are_rejected() {
        for bytes in [
            br#"{"alg":"HS256","alg":"HS256"}"#.as_slice(),
            br#"{"x":{"a":1,"a":2}}"#.as_slice(),
            br#"{"x":[{"a":1,"a":2}]}"#.as_slice(),
            b"{} {}",
            b"\xff",
        ] {
            assert!(parse_json(bytes).is_err());
        }
        let opts = options(Algorithm::HS256, KeyFormat::Raw);
        for payload in [
            r#"{"exp":2000,"exp":1}"#,
            r#"{"exp":2000,"nested":{"a":1,"a":2}}"#,
            "[]",
            "null",
            "7",
            "\"text\"",
        ] {
            assert!(
                verify(
                    &hs_raw(r#"{"alg":"HS256","typ":"JWT"}"#, payload),
                    &[7; 32],
                    &opts,
                    1_000_000
                )
                .is_err()
            );
        }
        assert!(
            verify(
                &hs_raw(r#"{"alg":"HS256","alg":"HS256"}"#, &claims().to_string()),
                &[7; 32],
                &opts,
                1_000_000
            )
            .is_err()
        );
        assert!(verify(&format!("{HS}="), &[7; 32], &opts, 1_000_000).is_err());
        assert!(sign(&json!([]), &[7; 32], &hs_options()).is_err());
        assert!(encrypt(&Value::Null, &[7; 32], &[3; 12], &EncryptOptions::default()).is_err());
    }

    #[test]
    fn strict_options_have_explicit_algorithms_and_key_formats() {
        assert!(serde_json::from_value::<VerifyOptions>(json!({"keyFormat":"raw"})).is_err());
        assert!(serde_json::from_value::<SignOptions>(json!({"algorithm":"HS256"})).is_err());
        assert!(
            serde_json::from_value::<SignOptions>(json!({"algorithm":"none","keyFormat":"raw"}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<DecryptOptions>(json!({"ignoreExpiration":true})).is_err()
        );
        let opts: DecryptOptions = serde_json::from_value(json!({})).unwrap();
        assert!(opts.require_expiration);
    }
    #[test]
    fn asymmetric_entrypoints_guard_recursive_der_before_key_parsers() {
        fn sequence(body: Vec<u8>) -> Vec<u8> {
            let mut result = vec![0x30];
            if body.len() < 128 {
                result.push(body.len() as u8);
            } else {
                let bytes = body.len().to_be_bytes();
                let start = bytes.iter().position(|byte| *byte != 0).unwrap();
                result.push(0x80 | (bytes.len() - start) as u8);
                result.extend_from_slice(&bytes[start..]);
            }
            result.extend_from_slice(&body);
            result
        }
        for depth in [127, 10_000] {
            let mut encoded = vec![5, 0];
            for _ in 0..depth {
                encoded = sequence(encoded);
            }
            for format in [KeyFormat::Der, KeyFormat::Pem] {
                let private = if format == KeyFormat::Pem {
                    format!(
                        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----",
                        STANDARD.encode(&encoded)
                    )
                    .into_bytes()
                } else {
                    encoded.clone()
                };
                let public = if format == KeyFormat::Pem {
                    format!(
                        "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
                        STANDARD.encode(&encoded)
                    )
                    .into_bytes()
                } else {
                    encoded.clone()
                };
                for (algorithm, _, _, _, _, token) in KEYS {
                    let algorithm = serde_json::from_value(json!(algorithm)).unwrap();
                    let signed = sign(
                        &claims(),
                        &private,
                        &SignOptions {
                            algorithm,
                            key_format: format,
                            kid: None,
                            typ: None,
                        },
                    );
                    let verified = verify(token, &public, &options(algorithm, format), 1_000_000);
                    assert!(signed.is_err());
                    assert!(verified.is_err());
                    if depth == 10_000 {
                        assert!(signed.unwrap_err().to_string().contains("nesting"));
                        assert!(verified.unwrap_err().to_string().contains("nesting"));
                    }
                }
            }
        }
    }
}
