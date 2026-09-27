//! WebAuthn relying-party verification for passkeys. Responses arrive in the
//! JSON form browsers produce with `PublicKeyCredential.toJSON()`: base64url
//! strings for every binary field.
//!
//! Attestation statements are not evaluated. Passkeys use "none" attestation,
//! so trust rests on the ceremony itself: the challenge and origin in the
//! client data, and the RP ID hash, flags and counter in authenticator data.
use anyhow::{bail, ensure, Context, Result};
use aws_lc_rs::signature::{self, ParsedPublicKey, RsaPublicKeyComponents};
use base64::{
    alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use ed25519_dalek::VerifyingKey;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{cbor, jwt};

/// Unpadded on output; padding is optional on input.
const BASE64URL: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

const USER_PRESENT: u8 = 0x01;
const USER_VERIFIED: u8 = 0x04;
const BACKUP_ELIGIBLE: u8 = 0x08;
const BACKUP_STATE: u8 = 0x10;
const ATTESTED: u8 = 0x40;
const EXTENSIONS: u8 = 0x80;

/// COSE algorithms with verifiers: EdDSA, ES256, ES384, ES512, PS256, RS256.
const ALGORITHMS: [i64; 6] = [-8, -7, -35, -36, -37, -257];

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum UserVerification {
    Required,
    Preferred,
    Discouraged,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistrationOptions {
    pub challenge: String,
    pub origins: Vec<String>,
    pub rp_id: String,
    pub user_verification: UserVerification,
    pub algorithms: Vec<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthenticationOptions {
    pub challenge: String,
    pub origins: Vec<String>,
    pub rp_id: String,
    pub user_verification: UserVerification,
    pub credential: StoredCredential,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredCredential {
    pub id: String,
    pub public_key: String,
    pub sign_count: u32,
    #[serde(default)]
    pub user_handle: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Registration {
    pub credential: Credential,
    pub user_verified: bool,
    pub attestation: Attestation,
}

/// What a relying party stores: the COSE key, algorithm and counter verify
/// later assertions; the rest informs the account's passkey list.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credential {
    pub id: String,
    pub public_key: String,
    pub algorithm: i64,
    pub sign_count: u32,
    pub transports: Vec<String>,
    pub backup_eligible: bool,
    pub backup_state: bool,
    pub aaguid: String,
}

#[derive(Debug, Serialize)]
pub struct Attestation {
    pub format: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Authentication {
    pub credential_id: String,
    pub sign_count: u32,
    pub user_verified: bool,
    pub backup_eligible: bool,
    pub backup_state: bool,
    pub user_handle: Option<String>,
}

pub fn verify_registration(response: &[u8], options: &RegistrationOptions) -> Result<Registration> {
    expectations(&options.challenge, &options.origins, &options.rp_id)?;
    ensure!(
        !options.algorithms.is_empty()
            && options
                .algorithms
                .iter()
                .all(|alg| ALGORITHMS.contains(alg)),
        "WebAuthn algorithms must be a nonempty list of supported COSE identifiers"
    );
    let root = jwt::parse_json(response).context("WebAuthn response is not valid JSON")?;
    let (id, fields) = envelope(&root)?;
    client_data(
        &binary(fields, "clientDataJSON")?,
        "webauthn.create",
        &options.challenge,
        &options.origins,
    )?;
    let object = binary(fields, "attestationObject")?;
    let object = cbor::decode(&object).context("attestationObject is not valid CBOR")?;
    let format = object
        .field("fmt")
        .and_then(cbor::Value::text)
        .filter(|format| (1..=32).contains(&format.len()))
        .context("attestationObject has no fmt")?;
    match object.field("attStmt") {
        Some(cbor::Value::Map(statement)) => {
            ensure!(
                format != "none" || statement.is_empty(),
                "none attestation carries a statement"
            )
        }
        _ => bail!("attestationObject has no attStmt map"),
    }
    let bytes = object
        .field("authData")
        .and_then(cbor::Value::bytes)
        .context("attestationObject has no authData")?;
    let data = authenticator_data(bytes, &options.rp_id, options.user_verification)?;
    let credential = data
        .credential
        .context("registration authenticatorData has no attested credential")?;
    ensure!(
        credential.id == id.as_slice(),
        "credential ID differs from the response id"
    );
    let key = cose_key(credential.public_key)?;
    ensure!(
        options.algorithms.contains(&key.algorithm),
        "the credential's algorithm is not allowed"
    );
    Ok(Registration {
        credential: Credential {
            id: BASE64URL.encode(credential.id),
            public_key: BASE64URL.encode(credential.public_key),
            algorithm: key.algorithm,
            sign_count: data.sign_count,
            transports: transports(fields.get("transports"))?,
            backup_eligible: data.flags & BACKUP_ELIGIBLE != 0,
            backup_state: data.flags & BACKUP_STATE != 0,
            aaguid: uuid(credential.aaguid),
        },
        user_verified: data.flags & USER_VERIFIED != 0,
        attestation: Attestation {
            format: format.into(),
        },
    })
}

pub fn verify_authentication(
    response: &[u8],
    options: &AuthenticationOptions,
) -> Result<Authentication> {
    expectations(&options.challenge, &options.origins, &options.rp_id)?;
    let stored = &options.credential;
    let root = jwt::parse_json(response).context("WebAuthn response is not valid JSON")?;
    let (id, fields) = envelope(&root)?;
    ensure!(
        decode(&stored.id).context("stored credential id is not base64url")? == id,
        "the response is for a different credential"
    );
    let client_data_json = binary(fields, "clientDataJSON")?;
    client_data(
        &client_data_json,
        "webauthn.get",
        &options.challenge,
        &options.origins,
    )?;
    let bytes = binary(fields, "authenticatorData")?;
    let data = authenticator_data(&bytes, &options.rp_id, options.user_verification)?;
    ensure!(
        data.credential.is_none(),
        "assertions carry no attested credential data"
    );
    let key = cose_key(&decode(&stored.public_key).context("stored public key is not base64url")?)?;
    let mut message = bytes.clone();
    message.extend_from_slice(&Sha256::digest(&client_data_json));
    key.verify(&message, &binary(fields, "signature")?)?;
    let user_handle = match fields.get("userHandle") {
        None | Some(Value::Null) => None,
        Some(Value::String(handle)) => Some(decode(handle).context("userHandle is not base64url")?),
        Some(_) => bail!("userHandle must be a base64url string"),
    };
    if let (Some(actual), Some(expected)) = (&user_handle, &stored.user_handle) {
        ensure!(
            *actual == decode(expected).context("stored user handle is not base64url")?,
            "the passkey belongs to a different user"
        );
    }
    // Only a genuine assertion may report a regressed counter.
    if data.sign_count != 0 || stored.sign_count != 0 {
        ensure!(
            data.sign_count > stored.sign_count,
            "WEBAUTHN_COUNTER: the signature counter did not increase, so the authenticator may be cloned"
        );
    }
    Ok(Authentication {
        credential_id: BASE64URL.encode(&id),
        sign_count: data.sign_count,
        user_verified: data.flags & USER_VERIFIED != 0,
        backup_eligible: data.flags & BACKUP_ELIGIBLE != 0,
        backup_state: data.flags & BACKUP_STATE != 0,
        user_handle: user_handle.map(|handle| BASE64URL.encode(handle)),
    })
}

fn decode(value: &str) -> Result<Vec<u8>> {
    Ok(BASE64URL.decode(value)?)
}

fn expectations(challenge: &str, origins: &[String], rp_id: &str) -> Result<()> {
    ensure!(
        decode(challenge).is_ok_and(|bytes| bytes.len() >= 16),
        "WebAuthn challenges must be base64url with at least 16 bytes"
    );
    ensure!(
        !origins.is_empty() && origins.iter().all(|origin| !origin.is_empty()),
        "WebAuthn verification needs at least one allowed origin"
    );
    ensure!(!rp_id.is_empty(), "WebAuthn verification needs an RP ID");
    Ok(())
}

fn envelope(root: &Value) -> Result<(Vec<u8>, &Map<String, Value>)> {
    let root = root
        .as_object()
        .context("WebAuthn response must be an object")?;
    ensure!(
        root.get("type").and_then(Value::as_str) == Some("public-key"),
        "WebAuthn response type must be public-key"
    );
    let id = binary(root, "id")?;
    ensure!(binary(root, "rawId")? == id, "WebAuthn id and rawId differ");
    ensure!(
        (1..=1023).contains(&id.len()),
        "credential IDs hold 1 to 1023 bytes"
    );
    let fields = root
        .get("response")
        .and_then(Value::as_object)
        .context("WebAuthn response has no response object")?;
    Ok((id, fields))
}

fn binary(object: &Map<String, Value>, name: &str) -> Result<Vec<u8>> {
    let text = object
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("WebAuthn response has no {name}"))?;
    decode(text).with_context(|| format!("WebAuthn {name} is not base64url"))
}

fn client_data(bytes: &[u8], kind: &str, challenge: &str, origins: &[String]) -> Result<()> {
    let value = jwt::parse_json(bytes).context("clientDataJSON is not valid JSON")?;
    let data = value
        .as_object()
        .context("clientDataJSON must be an object")?;
    ensure!(
        data.get("type").and_then(Value::as_str) == Some(kind),
        "clientDataJSON is not a {kind} ceremony"
    );
    let actual = data
        .get("challenge")
        .and_then(Value::as_str)
        .context("clientDataJSON has no challenge")?;
    ensure!(
        decode(actual).ok() == Some(decode(challenge)?),
        "WebAuthn challenge does not match"
    );
    let origin = data
        .get("origin")
        .and_then(Value::as_str)
        .context("clientDataJSON has no origin")?;
    ensure!(
        origins.iter().any(|allowed| allowed == origin),
        "WebAuthn origin is not allowed"
    );
    ensure!(
        matches!(data.get("crossOrigin"), None | Some(Value::Bool(false))),
        "cross-origin WebAuthn ceremonies are not accepted"
    );
    Ok(())
}

struct AuthenticatorData<'a> {
    flags: u8,
    sign_count: u32,
    credential: Option<AttestedCredential<'a>>,
}

struct AttestedCredential<'a> {
    aaguid: &'a [u8],
    id: &'a [u8],
    public_key: &'a [u8],
}

fn authenticator_data<'a>(
    bytes: &'a [u8],
    rp_id: &str,
    user_verification: UserVerification,
) -> Result<AuthenticatorData<'a>> {
    ensure!(bytes.len() >= 37, "authenticatorData is truncated");
    ensure!(
        bytes[..32] == Sha256::digest(rp_id.as_bytes())[..],
        "authenticatorData is for a different RP ID"
    );
    let flags = bytes[32];
    ensure!(flags & USER_PRESENT != 0, "the user was not present");
    ensure!(
        user_verification != UserVerification::Required || flags & USER_VERIFIED != 0,
        "the user was not verified"
    );
    ensure!(
        flags & BACKUP_STATE == 0 || flags & BACKUP_ELIGIBLE != 0,
        "authenticatorData reports a backup without backup eligibility"
    );
    let sign_count = u32::from_be_bytes([bytes[33], bytes[34], bytes[35], bytes[36]]);
    let mut position = 37;
    let credential = if flags & ATTESTED != 0 {
        let header = bytes
            .get(position..position + 18)
            .context("attested credential data is truncated")?;
        let length = usize::from(u16::from_be_bytes([header[16], header[17]]));
        ensure!(
            (1..=1023).contains(&length),
            "credential IDs hold 1 to 1023 bytes"
        );
        position += 18;
        let id = bytes
            .get(position..position + length)
            .context("credential ID is truncated")?;
        position += length;
        let start = position;
        cbor::item(bytes, &mut position, 0).context("credential public key is not valid CBOR")?;
        Some(AttestedCredential {
            aaguid: &header[..16],
            id,
            public_key: &bytes[start..position],
        })
    } else {
        None
    };
    if flags & EXTENSIONS != 0 {
        let extensions = cbor::item(bytes, &mut position, 0)
            .context("authenticator extensions are not valid CBOR")?;
        ensure!(
            matches!(extensions, cbor::Value::Map(_)),
            "authenticator extensions must be a CBOR map"
        );
    }
    ensure!(
        position == bytes.len(),
        "authenticatorData has trailing bytes"
    );
    Ok(AuthenticatorData {
        flags,
        sign_count,
        credential,
    })
}

enum PublicKey {
    Ed25519(VerifyingKey),
    Parsed(ParsedPublicKey),
}

struct CoseKey {
    algorithm: i64,
    key: PublicKey,
}

impl CoseKey {
    fn verify(&self, message: &[u8], signature: &[u8]) -> Result<()> {
        let valid = match &self.key {
            PublicKey::Ed25519(key) => ed25519_dalek::Signature::from_slice(signature)
                .is_ok_and(|signature| key.verify_strict(message, &signature).is_ok()),
            PublicKey::Parsed(key) => key.verify_sig(message, signature).is_ok(),
        };
        ensure!(valid, "WebAuthn signature is invalid");
        Ok(())
    }
}

/// Parse and validate a credential public key. COSE labels: kty 1, alg 3;
/// EC2/OKP crv -1, x -2, y -3, d -4; RSA n -1, e -2, private -3 through -12.
fn cose_key(bytes: &[u8]) -> Result<CoseKey> {
    let map = cbor::decode(bytes).context("credential public key is not valid CBOR")?;
    ensure!(
        matches!(map, cbor::Value::Map(_)),
        "credential public key must be a COSE key map"
    );
    let integer = |label| map.label(label).and_then(cbor::Value::integer);
    let bytes = |label| map.label(label).and_then(cbor::Value::bytes);
    let kty = integer(1).context("COSE key has no kty")?;
    let algorithm = integer(3).context("COSE key has no alg")?;
    let private = |labels: std::ops::RangeInclusive<i64>| {
        labels.into_iter().any(|label| map.label(label).is_some())
    };
    let key = match (kty, algorithm) {
        (1, -8) => {
            ensure!(integer(-1) == Some(6), "EdDSA COSE keys must use Ed25519");
            ensure!(!private(-4..=-4), "COSE key contains private material");
            let x: &[u8; 32] = bytes(-2)
                .and_then(|x| x.try_into().ok())
                .context("Ed25519 COSE keys need a 32-byte x")?;
            PublicKey::Ed25519(VerifyingKey::from_bytes(x).context("invalid Ed25519 public key")?)
        }
        (2, -7 | -35 | -36) => {
            let (curve, size, verification) = match algorithm {
                -7 => (1, 32, &signature::ECDSA_P256_SHA256_ASN1),
                -35 => (2, 48, &signature::ECDSA_P384_SHA384_ASN1),
                _ => (3, 66, &signature::ECDSA_P521_SHA512_ASN1),
            };
            ensure!(
                integer(-1) == Some(curve),
                "COSE key curve does not match its algorithm"
            );
            ensure!(!private(-4..=-4), "COSE key contains private material");
            let (Some(x), Some(y)) = (bytes(-2), bytes(-3)) else {
                bail!("EC2 COSE keys need uncompressed x and y coordinates")
            };
            ensure!(
                x.len() == size && y.len() == size,
                "EC2 COSE coordinates have the wrong length"
            );
            let point = [&[4][..], x, y].concat();
            PublicKey::Parsed(
                ParsedPublicKey::new(verification, point).context("invalid EC2 public key")?,
            )
        }
        (3, -257 | -37) => {
            ensure!(!private(-12..=-3), "COSE key contains private material");
            let (Some(n), Some(e)) = (bytes(-1), bytes(-2)) else {
                bail!("RSA COSE keys need n and e")
            };
            let modulus = n
                .iter()
                .position(|byte| *byte != 0)
                .map_or(0, |start| n.len() - start);
            ensure!(
                (256..=1024).contains(&modulus),
                "RSA keys must have 2048 to 8192 bits"
            );
            let parameters = if algorithm == -257 {
                &signature::RSA_PKCS1_2048_8192_SHA256
            } else {
                &signature::RSA_PSS_2048_8192_SHA256
            };
            PublicKey::Parsed(
                RsaPublicKeyComponents { n, e }
                    .to_parsed_public_key(parameters)
                    .context("invalid RSA public key")?,
            )
        }
        _ => bail!("unsupported COSE key type {kty} with algorithm {algorithm}"),
    };
    Ok(CoseKey { algorithm, key })
}

fn transports(value: Option<&Value>) -> Result<Vec<String>> {
    let values = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::Array(values)) if values.len() <= 16 => values,
        _ => bail!("transports must be an array of at most 16 strings"),
    };
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|transport| (1..=32).contains(&transport.len()))
                .map(str::to_owned)
                .context("transports must be short strings")
        })
        .collect()
}

fn uuid(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests;
