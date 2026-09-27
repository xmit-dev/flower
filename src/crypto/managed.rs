//! Named, non-exportable cryptographic capabilities and node-local native reuse.
//!
//! Only the encrypted catalog is replicated. Authorization always precedes cache
//! lookup; contexts neither authorize callers nor cache JWT validation outcomes.
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use aws_lc_rs::{
    aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey},
    encoding::{AsBigEndian, AsDer, Pkcs8V1Der, PublicKeyX509Der},
    hmac,
    signature::{self, Ed25519KeyPair, KeyPair, ParsedPublicKey, RsaKeyPair},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::{
    ecdsa::{Signature, SigningKey, signature::Signer},
    pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::{jwt, nacl};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Algorithm {
    Ed25519,
    P256,
    #[serde(rename = "RSA")]
    Rsa,
    HS256,
    A256GCM,
    XSalsa20Poly1305,
    X25519,
}

impl Algorithm {
    fn usages(self) -> &'static [&'static str] {
        match self {
            Self::Ed25519 | Self::P256 | Self::Rsa => &["sign", "verify", "publicKey"],
            Self::HS256 => &["sign", "verify"],
            Self::A256GCM | Self::XSalsa20Poly1305 => &["encrypt", "decrypt"],
            Self::X25519 => &["encrypt", "decrypt", "derive", "publicKey"],
        }
    }
    fn jwt(self) -> Result<jwt::Algorithm> {
        Ok(match self {
            Self::Ed25519 => jwt::Algorithm::EdDSA,
            Self::P256 => jwt::Algorithm::ES256,
            Self::Rsa => jwt::Algorithm::RS256,
            Self::HS256 => jwt::Algorithm::HS256,
            _ => bail!("Key algorithm cannot sign a JWT"),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Envelope {
    provider: String,
    wrapping_id: String,
    wrapped_dek: String,
    wrapping_nonce: String,
    ciphertext: String,
    nonce: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Version {
    envelope: Option<Envelope>,
    #[serde(default)]
    revoked: bool,
    #[serde(default)]
    retired: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyRecord {
    id: String,
    algorithm: Algorithm,
    active_version: u64,
    versions: BTreeMap<u64, Version>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Binding {
    key: String,
    usages: BTreeSet<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Catalog {
    domain: String,
    revision: u64,
    keys: BTreeMap<String, KeyRecord>,
    bindings: BTreeMap<String, Binding>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Resolved {
    domain: String,
    id: String,
    version: u64,
    algorithm: Algorithm,
    envelope: Envelope,
    usages: BTreeSet<String>,
}

/// Mount providers can be replaced without giving Raft application code I/O.
/// Wrapping interfaces deliberately handle an opaque DEK, never application data.
trait WrappingProvider: Send + Sync {
    fn id(&self) -> &str;
    fn wrap(&self, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>>;
    fn unwrap(&self, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>>;
}
struct MountedKey {
    id: String,
    cipher: LessSafeKey,
}
impl MountedKey {
    fn read(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path).context("Read wrapping-key file metadata")?;
        ensure!(
            metadata.is_file(),
            "Wrapping-key path must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "Wrapping-key file must not be accessible by group or other users (use chmod 600)"
            );
        }
        let bytes = Zeroizing::new(std::fs::read(path).context("Read wrapping-key file")?);
        ensure!(
            bytes.len() == 32,
            "Wrapping-key file must contain exactly 32 raw bytes"
        );
        let id = URL_SAFE_NO_PAD.encode(Sha256::digest(&*bytes));
        Ok(Self {
            id,
            cipher: cipher(&bytes)?,
        })
    }
}
impl WrappingProvider for MountedKey {
    fn id(&self) -> &str {
        &self.id
    }
    fn wrap(&self, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
        seal(&self.cipher, nonce, aad, bytes)
    }
    fn unwrap(&self, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        open(&self.cipher, nonce, aad, bytes)
    }
}
fn cipher(bytes: &[u8]) -> Result<LessSafeKey> {
    Ok(LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, bytes)
            .map_err(|_| anyhow!("Invalid AES-256 wrapping key"))?,
    ))
}
fn seal(key: &LessSafeKey, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
    let mut output = Zeroizing::new(bytes.to_vec());
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(aad),
        &mut *output,
    )
    .map_err(|_| anyhow!("Key envelope encryption failed"))?;
    Ok(std::mem::take(&mut *output))
}
fn open(
    key: &LessSafeKey,
    nonce: [u8; 12],
    aad: &[u8],
    bytes: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    let mut output = Zeroizing::new(bytes.to_vec());
    let len = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut output,
        )
        .map_err(|_| anyhow!("Key envelope authentication failed"))?
        .len();
    output.truncate(len);
    Ok(output)
}
fn random<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| anyhow!("OS randomness unavailable"))?;
    Ok(bytes)
}
fn decode(s: &str) -> Result<Vec<u8>> {
    URL_SAFE_NO_PAD
        .decode(s)
        .context("Invalid key envelope base64url")
}
fn nonce(s: &str) -> Result<[u8; 12]> {
    decode(s)?
        .try_into()
        .map_err(|_| anyhow!("Invalid key envelope nonce"))
}
fn identity() -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(random::<16>()?))
}
fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .with_context(|| format!("Missing or invalid {name}"))
}
fn aad(
    domain: &str,
    id: &str,
    version: u64,
    algorithm: Algorithm,
    purpose: &str,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&json!([
        "flower-key-v1",
        domain,
        id,
        version,
        algorithm,
        purpose
    ]))?)
}

/// Native contexts are deliberately not Debug/Serialize and have no private-byte accessor.
pub struct PreparedKey {
    algorithm: Algorithm,
    inner: Material,
    retained_bytes: usize,
    created_at: Instant,
}
enum Material {
    Ed {
        sign: Ed25519KeyPair,
        strict: ed25519_dalek::VerifyingKey,
        verify: ParsedPublicKey,
    },
    P256 {
        sign: SigningKey,
        verify: ParsedPublicKey,
    },
    Rsa {
        sign: RsaKeyPair,
        verify: ParsedPublicKey,
    },
    Hmac(Box<hmac::Key>),
    Aes(LessSafeKey),
    Secret(Zeroizing<[u8; 32]>),
}
impl PreparedKey {
    fn prepare(algorithm: Algorithm, bytes: &[u8]) -> Result<Self> {
        let inner = match algorithm {
            Algorithm::Ed25519 => {
                let sign = Ed25519KeyPair::from_seed_unchecked(bytes)
                    .map_err(|_| anyhow!("Invalid Ed25519 seed"))?;
                let public: [u8; 32] = sign
                    .public_key()
                    .as_ref()
                    .try_into()
                    .expect("Ed25519 public length");
                let strict = ed25519_dalek::VerifyingKey::from_bytes(&public)
                    .map_err(|_| anyhow!("Invalid Ed25519 public key"))?;
                let verify = ParsedPublicKey::new(&signature::ED25519, public)
                    .map_err(|_| anyhow!("Invalid Ed25519 public key"))?;
                Material::Ed {
                    sign,
                    strict,
                    verify,
                }
            }
            Algorithm::P256 => {
                let sign = SigningKey::from_pkcs8_der(bytes)
                    .map_err(|_| anyhow!("Invalid P256 private key"))?;
                let public = sign.verifying_key().to_sec1_point(false);
                let verify =
                    ParsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public.as_bytes())
                        .map_err(|_| anyhow!("Invalid P256 public key"))?;
                Material::P256 { sign, verify }
            }
            Algorithm::Rsa => {
                let sign = RsaKeyPair::from_pkcs8(bytes)
                    .map_err(|_| anyhow!("Invalid RSA PKCS8 private key"))?;
                ensure!(
                    (256..=1024).contains(&sign.public_modulus_len()),
                    "RSA key must be 2048–8192 bits"
                );
                let verify = ParsedPublicKey::new(
                    &signature::RSA_PKCS1_2048_8192_SHA256,
                    sign.public_key().as_ref(),
                )
                .map_err(|_| anyhow!("Invalid RSA public key"))?;
                Material::Rsa { sign, verify }
            }
            Algorithm::HS256 => {
                ensure!(bytes.len() >= 32, "HS256 requires at least 32 bytes");
                Material::Hmac(Box::new(hmac::Key::new(hmac::HMAC_SHA256, bytes)))
            }
            Algorithm::A256GCM => Material::Aes(cipher(bytes)?),
            Algorithm::XSalsa20Poly1305 | Algorithm::X25519 => Material::Secret(Zeroizing::new(
                bytes
                    .try_into()
                    .map_err(|_| anyhow!("Key requires exactly 32 bytes"))?,
            )),
        };
        Ok(Self {
            algorithm,
            inner,
            retained_bytes: bytes.len().saturating_mul(8).saturating_add(8192),
            created_at: Instant::now(),
        })
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        Ok(match &self.inner {
            Material::Ed { sign, .. } => sign
                .try_sign(message)
                .map_err(|_| anyhow!("Ed25519 signing failed"))?
                .as_ref()
                .to_vec(),
            Material::P256 { sign, .. } => {
                let signature: Signature = sign
                    .try_sign(message)
                    .map_err(|_| anyhow!("P256 signing failed"))?;
                signature.to_bytes().to_vec()
            }
            Material::Rsa { sign, .. } => {
                let mut signature = vec![0; sign.public_modulus_len()];
                sign.sign(
                    &signature::RSA_PKCS1_SHA256,
                    &aws_lc_rs::rand::SystemRandom::new(),
                    message,
                    &mut signature,
                )
                .map_err(|_| anyhow!("RSA signing failed"))?;
                signature
            }
            Material::Hmac(key) => hmac::sign(key, message).as_ref().to_vec(),
            _ => bail!("Key cannot sign"),
        })
    }
    fn verify(&self, message: &[u8], signature: &[u8], strict: bool) -> bool {
        match &self.inner {
            Material::Ed { strict: key, .. } if strict => <&[u8; 64]>::try_from(signature)
                .ok()
                .is_some_and(|signature| {
                    key.verify_strict(message, &ed25519_dalek::Signature::from_bytes(signature))
                        .is_ok()
                }),
            Material::Ed { verify, .. }
            | Material::P256 { verify, .. }
            | Material::Rsa { verify, .. } => verify.verify_sig(message, signature).is_ok(),
            Material::Hmac(key) => hmac::verify(key, message, signature).is_ok(),
            _ => false,
        }
    }
    fn public(&self) -> Result<Vec<u8>> {
        Ok(match &self.inner {
            Material::Ed { sign, .. } => sign.public_key().as_ref().to_vec(),
            Material::P256 { sign, .. } => sign
                .verifying_key()
                .to_public_key_der()
                .map_err(|_| anyhow!("Encode P256 public key"))?
                .as_bytes()
                .to_vec(),
            Material::Rsa { sign, .. } => AsDer::<PublicKeyX509Der>::as_der(sign.public_key())
                .map_err(|_| anyhow!("Encode RSA public key"))?
                .as_ref()
                .to_vec(),
            Material::Secret(secret) if self.algorithm == Algorithm::X25519 => {
                x25519_dalek::x25519(**secret, x25519_dalek::X25519_BASEPOINT_BYTES).to_vec()
            }
            _ => bail!("This key has no public component"),
        })
    }
}

struct CacheEntry {
    value: Arc<PreparedKey>,
    bytes: usize,
    touched: u64,
    loaded: Instant,
}
#[derive(Default)]
struct Cache {
    entries: HashMap<String, CacheEntry>,
    flights: HashMap<String, Arc<KeyFlight>>,
    flight_bytes: usize,
    coalesced: u64,
    bytes: usize,
    tick: u64,
    hits: u64,
    misses: u64,
    loads: u64,
    evictions: u64,
}
// Flights retain only their own result, and never wait while holding the cache
// mutex. Their metadata shares the configured prepared-key retention budget.
#[derive(Default)]
struct KeyFlight {
    result: Mutex<Option<std::result::Result<Arc<PreparedKey>, String>>>,
    complete: Condvar,
}
impl KeyFlight {
    fn wait(&self) -> Result<Arc<PreparedKey>> {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while result.is_none() {
            result = self
                .complete
                .wait(result)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        result
            .as_ref()
            .expect("completed key flight")
            .clone()
            .map_err(anyhow::Error::msg)
    }
    fn publish(&self, result: &Result<Arc<PreparedKey>>) {
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(result.as_ref().map(Arc::clone).map_err(ToString::to_string));
        self.complete.notify_all();
    }
}
struct KeyFlightOwner<'a> {
    native: &'a NativeKeys,
    identity: &'a str,
    flight: Arc<KeyFlight>,
    weight: usize,
    finished: bool,
}
impl Drop for KeyFlightOwner<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Ok(mut cache) = self.native.cache.lock()
            && cache
                .flights
                .get(self.identity)
                .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            cache.flights.remove(self.identity);
            cache.flight_bytes -= self.weight;
        }
        self.flight
            .publish(&Err(anyhow!("Managed-key preparation aborted")));
    }
}
impl Cache {
    fn evict_oldest(&mut self) -> bool {
        let Some(oldest) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.touched)
            .map(|(key, _)| key.clone())
        else {
            return false;
        };
        let old = self.entries.remove(&oldest).expect("cache entry");
        self.bytes -= old.bytes;
        self.evictions += 1;
        true
    }
}
struct NativeKeys {
    provider: Option<Box<dyn WrappingProvider>>,
    previous: BTreeMap<String, Box<dyn WrappingProvider>>,
    budget: usize,
    lease: Option<Duration>,
    cache: Mutex<Cache>,
}
static NATIVE: OnceLock<std::result::Result<NativeKeys, String>> = OnceLock::new();
fn global() -> Result<&'static NativeKeys> {
    NATIVE
        .get_or_init(|| NativeKeys::from_env().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| anyhow!("{error}"))
}
fn env_usize(name: &str, default: usize) -> Result<usize> {
    std::env::var(name)
        .map(|value| value.parse().with_context(|| format!("Invalid {name}")))
        .unwrap_or(Ok(default))
}
impl NativeKeys {
    fn from_env() -> Result<Self> {
        let mut previous = BTreeMap::new();
        if let Ok(files) = std::env::var("FLOWER_KEYRING_PREVIOUS_FILES") {
            let paths: Vec<String> = serde_json::from_str(&files)
                .context("FLOWER_KEYRING_PREVIOUS_FILES must be a JSON array of paths")?;
            for path in paths {
                let provider = MountedKey::read(Path::new(&path))?;
                previous.insert(
                    provider.id.clone(),
                    Box::new(provider) as Box<dyn WrappingProvider>,
                );
            }
        }
        Ok(Self {
            previous,
            provider: std::env::var_os("FLOWER_KEYRING_FILE")
                .map(|path| {
                    MountedKey::read(Path::new(&path))
                        .map(|provider| Box::new(provider) as Box<dyn WrappingProvider>)
                })
                .transpose()?,
            budget: env_usize("FLOWER_KEY_CACHE_BYTES", 16 * 1024 * 1024)?,
            lease: match env_usize("FLOWER_KEY_CACHE_TTL_MS", 0)? {
                0 => None,
                n => Some(Duration::from_millis(n as u64)),
            },
            cache: Mutex::new(Cache::default()),
        })
    }
    fn provider(&self) -> Result<&dyn WrappingProvider> {
        self.provider
            .as_deref()
            .context("Managed keys are locked: configure FLOWER_KEYRING_FILE")
    }
    fn envelope(
        &self,
        domain: &str,
        id: &str,
        version: u64,
        algorithm: Algorithm,
        bytes: &[u8],
    ) -> Result<Envelope> {
        let provider = self.provider()?;
        let dek = Zeroizing::new(random::<32>()?);
        let nonce = random::<12>()?;
        let wrapping_nonce = random::<12>()?;
        Ok(Envelope {
            provider: "mounted".into(),
            wrapping_id: provider.id().into(),
            wrapped_dek: URL_SAFE_NO_PAD.encode(provider.wrap(
                wrapping_nonce,
                &aad(domain, id, version, algorithm, "dek")?,
                dek.as_ref(),
            )?),
            wrapping_nonce: URL_SAFE_NO_PAD.encode(wrapping_nonce),
            ciphertext: URL_SAFE_NO_PAD.encode(seal(
                &cipher(dek.as_ref())?,
                nonce,
                &aad(domain, id, version, algorithm, "material")?,
                bytes,
            )?),
            nonce: URL_SAFE_NO_PAD.encode(nonce),
        })
    }
    fn envelope_provider(&self, envelope: &Envelope) -> Result<&dyn WrappingProvider> {
        ensure!(
            envelope.provider == "mounted",
            "Unsupported key envelope provider"
        );
        self.provider
            .as_deref()
            .filter(|provider| provider.id() == envelope.wrapping_id)
            .or_else(|| self.previous.get(&envelope.wrapping_id).map(Box::as_ref))
            .context("Key envelope requires an unavailable wrapping key")
    }
    fn unwrap(&self, resolved: &Resolved) -> Result<Zeroizing<Vec<u8>>> {
        let envelope = &resolved.envelope;
        let provider = self.envelope_provider(envelope)?;
        let dek = provider.unwrap(
            nonce(&envelope.wrapping_nonce)?,
            &aad(
                &resolved.domain,
                &resolved.id,
                resolved.version,
                resolved.algorithm,
                "dek",
            )?,
            &decode(&envelope.wrapped_dek)?,
        )?;
        open(
            &cipher(&dek)?,
            nonce(&envelope.nonce)?,
            &aad(
                &resolved.domain,
                &resolved.id,
                resolved.version,
                resolved.algorithm,
                "material",
            )?,
            &decode(&envelope.ciphertext)?,
        )
    }
    fn get(&self, resolved: &Resolved) -> Result<Arc<PreparedKey>> {
        // Include the authenticated envelope in the identity: malformed replacement
        // ciphertext must fail even when a legitimate old context is still cached.
        let mut fingerprint = Sha256::new();
        fingerprint.update(resolved.version.to_le_bytes());
        fingerprint.update([resolved.algorithm as u8]);
        for part in [
            &resolved.domain,
            &resolved.id,
            &resolved.envelope.provider,
            &resolved.envelope.wrapping_id,
            &resolved.envelope.wrapped_dek,
            &resolved.envelope.wrapping_nonce,
            &resolved.envelope.ciphertext,
            &resolved.envelope.nonce,
        ] {
            fingerprint.update((part.len() as u64).to_le_bytes());
            fingerprint.update(part.as_bytes());
        }
        let identity = URL_SAFE_NO_PAD.encode(fingerprint.finalize());
        let flight_weight = identity.len().saturating_mul(2).saturating_add(512);
        let (flight, owner) = {
            let mut cache = self
                .cache
                .lock()
                .map_err(|_| anyhow!("Key cache poisoned"))?;
            cache.tick = cache.tick.wrapping_add(1);
            let tick = cache.tick;
            if let Some(entry) = cache.entries.get_mut(&identity)
                && self
                    .lease
                    .is_none_or(|lease| entry.loaded.elapsed() < lease)
            {
                entry.touched = tick;
                let value = entry.value.clone();
                cache.hits += 1;
                return Ok(value);
            }
            if let Some(old) = cache.entries.remove(&identity) {
                cache.bytes -= old.bytes;
                cache.evictions += 1;
            }
            cache.misses += 1;
            if let Some(flight) = cache.flights.get(&identity).cloned() {
                cache.coalesced += 1;
                (Some(flight), false)
            } else {
                if cache.flight_bytes.saturating_add(flight_weight) <= self.budget {
                    while cache
                        .bytes
                        .saturating_add(cache.flight_bytes)
                        .saturating_add(flight_weight)
                        > self.budget
                        && cache.evict_oldest()
                    {}
                    let flight = Arc::new(KeyFlight::default());
                    cache.flight_bytes += flight_weight;
                    cache.flights.insert(identity.clone(), flight.clone());
                    (Some(flight), true)
                } else {
                    // Zero/tiny/full flight budgets bypass coalescing without
                    // refusing a valid operation or waiting on unrelated keys.
                    (None, true)
                }
            }
        };
        if !owner {
            return flight.expect("waiter has a flight").wait();
        }
        let mut guard = flight.map(|flight| KeyFlightOwner {
            native: self,
            identity: &identity,
            flight,
            weight: flight_weight,
            finished: false,
        });
        // Provider unwrap and ASN.1/curve preparation are outside global locks.
        let result: Result<Arc<PreparedKey>> = (|| {
            let bytes = self.unwrap(resolved)?;
            Ok(Arc::new(PreparedKey::prepare(resolved.algorithm, &bytes)?))
        })();
        {
            let mut cache = self
                .cache
                .lock()
                .map_err(|_| anyhow!("Key cache poisoned"))?;
            if guard.is_some() {
                cache.flights.remove(&identity);
                cache.flight_bytes -= flight_weight;
            }
            if let Ok(value) = &result {
                cache.loads += 1;
                let weight = value.retained_bytes().saturating_add(identity.len());
                if cache.flight_bytes.saturating_add(weight) <= self.budget {
                    // A budget-bypassing cold call can finish after another
                    // call has populated this identity. Replacement must not
                    // charge the same entry twice.
                    if let Some(previous) = cache.entries.remove(&identity) {
                        cache.bytes -= previous.bytes;
                    }
                    while cache
                        .bytes
                        .saturating_add(cache.flight_bytes)
                        .saturating_add(weight)
                        > self.budget
                        && cache.evict_oldest()
                    {}
                    cache.tick = cache.tick.wrapping_add(1);
                    let touched = cache.tick;
                    cache.bytes += weight;
                    cache.entries.insert(
                        identity.clone(),
                        CacheEntry {
                            value: value.clone(),
                            bytes: weight,
                            touched,
                            loaded: value.created_at,
                        },
                    );
                }
            }
        }
        if let Some(guard) = &mut guard {
            guard.flight.publish(&result);
            guard.finished = true;
        }
        result
    }
}

pub fn validate_configuration() -> Result<()> {
    global().map(|_| ())
}
pub fn invocation_cache_settings() -> Result<(usize, Option<Duration>)> {
    let native = global()?;
    Ok((native.budget, native.lease))
}
pub fn metrics() -> Value {
    match global().and_then(|native|native.cache.lock().map_err(|_|anyhow!("Key cache poisoned")).map(|cache|json!({"entries":cache.entries.len(),"bytes":cache.bytes,"budgetBytes":native.budget,"hits":cache.hits,"misses":cache.misses,"loads":cache.loads,"evictions":cache.evictions,"flightEntries":cache.flights.len(),"flightBytes":cache.flight_bytes,"coalesced":cache.coalesced}))) {Ok(value)=>value,Err(_)=>json!({"available":false})}
}
fn catalog(value: Option<&Value>) -> Result<Catalog> {
    match value.filter(|value| !value.is_null()) {
        Some(value) => {
            Ok(serde_json::from_value(value.clone()).context("Invalid managed key catalog")?)
        }
        None => Ok(Catalog {
            domain: identity()?,
            revision: 0,
            keys: BTreeMap::new(),
            bindings: BTreeMap::new(),
        }),
    }
}
fn safe_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && !name.chars().any(char::is_control),
        "Key names must be nonempty and contain no control characters"
    );
    Ok(())
}
fn usages(value: &Value, algorithm: Algorithm) -> Result<BTreeSet<String>> {
    let usages: BTreeSet<String> =
        serde_json::from_value(value.clone()).context("Key usages must be an array")?;
    ensure!(
        !usages.is_empty()
            && usages
                .iter()
                .all(|usage| algorithm.usages().contains(&usage.as_str())),
        "Unsupported or empty key usages"
    );
    Ok(usages)
}

pub fn public_catalog(value: Option<&Value>) -> Result<Value> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(json!({"domain":null,"revision":0,"keys":{},"bindings":{}}));
    };
    let catalog = catalog(Some(value))?;
    let keys:BTreeMap<_,_>=catalog.keys.iter().map(|(name,key)|(name,json!({"id":key.id,"algorithm":key.algorithm,"activeVersion":key.active_version,"versions":key.versions.iter().map(|(version,data)|json!({"version":version,"revoked":data.revoked,"retired":data.retired,"destroyed":data.envelope.is_none(),"wrappingId":data.envelope.as_ref().map(|envelope|&envelope.wrapping_id),"kid":kid(&key.id,*version)})).collect::<Vec<_>>()}))).collect();
    Ok(
        json!({"domain":catalog.domain,"revision":catalog.revision,"keys":keys,"bindings":catalog.bindings}),
    )
}
fn kid(id: &str, version: u64) -> String {
    format!("flower.{id}.{version}")
}

/// Inputs may contain only ciphertext for imports. The returned catalog is safe
/// for Raft persistence; the second value is an operator response without secrets.
pub fn prepare(value: Option<&Value>, operation: &Value) -> Result<(Value, Value)> {
    prepare_with(global()?, value, operation)
}
fn prepare_with(
    native: &NativeKeys,
    value: Option<&Value>,
    operation: &Value,
) -> Result<(Value, Value)> {
    let mut catalog = catalog(value)?;
    let op = field(operation, "operation")?;
    let name = field(operation, "name")?;
    safe_name(name)?;
    let bits = operation
        .get("bits")
        .map(|value| value.as_u64().context("bits must be an unsigned integer"))
        .transpose()?;
    ensure!(
        bits.is_none() || matches!(op, "generate" | "rotate"),
        "bits is only valid for key generation or rotation"
    );
    match op {
        "generate" | "import" => {
            ensure!(!catalog.keys.contains_key(name), "Key name already exists");
            let algorithm: Algorithm = serde_json::from_value(operation["algorithm"].clone())
                .context("Invalid key algorithm")?;
            let bytes = if op == "generate" {
                generate(algorithm, bits)?
            } else {
                let (bytes, format) = open_import(native.provider()?, &operation["sealed"])?;
                canonical(algorithm, &bytes, &format)?
            };
            PreparedKey::prepare(algorithm, &bytes)?;
            let id = identity()?;
            let envelope = native.envelope(&catalog.domain, &id, 1, algorithm, &bytes)?;
            catalog.keys.insert(
                name.into(),
                KeyRecord {
                    id,
                    algorithm,
                    active_version: 1,
                    versions: BTreeMap::from([(
                        1,
                        Version {
                            envelope: Some(envelope),
                            revoked: false,
                            retired: false,
                        },
                    )]),
                },
            );
        }
        "bind" => {
            let key_name = field(operation, "key")?;
            let key = catalog.keys.get(key_name).context("Unknown key")?;
            let usages = usages(&operation["usages"], key.algorithm)?;
            catalog.bindings.insert(
                name.into(),
                Binding {
                    key: key_name.into(),
                    usages,
                },
            );
        }
        "unbind" => {
            ensure!(
                catalog.bindings.remove(name).is_some(),
                "Unknown key binding"
            );
        }
        "rotate" => {
            let key = catalog.keys.get_mut(name).context("Unknown key")?;
            let version = key
                .versions
                .last_key_value()
                .and_then(|(version, _)| version.checked_add(1))
                .context("Key version overflow")?;
            let bits = if key.algorithm == Algorithm::Rsa && bits.is_none() {
                let old = key
                    .versions
                    .get(&key.active_version)
                    .context("Unknown active RSA key")?;
                let prepared = native.get(&Resolved {
                    domain: catalog.domain.clone(),
                    id: key.id.clone(),
                    version: key.active_version,
                    algorithm: key.algorithm,
                    envelope: old
                        .envelope
                        .clone()
                        .context("Destroyed RSA key requires explicit bits for rotation")?,
                    usages: BTreeSet::new(),
                })?;
                let Material::Rsa { sign, .. } = &prepared.inner else {
                    unreachable!()
                };
                Some(sign.public_modulus_len() as u64 * 8)
            } else {
                bits
            };
            let bytes = generate(key.algorithm, bits)?;
            let envelope =
                native.envelope(&catalog.domain, &key.id, version, key.algorithm, &bytes)?;
            key.versions.insert(
                version,
                Version {
                    envelope: Some(envelope),
                    revoked: false,
                    retired: false,
                },
            );
            key.active_version = version;
        }
        "retire" | "revoke" | "destroy" | "rewrap" => {
            let key = catalog.keys.get_mut(name).context("Unknown key")?;
            let selected = operation
                .get("version")
                .map(|value| {
                    value
                        .as_u64()
                        .filter(|version| *version > 0)
                        .context("Invalid key version")
                })
                .transpose()?;
            if let Some(version) = selected {
                ensure!(key.versions.contains_key(&version), "Unknown key version");
            }
            for (number, version) in &mut key.versions {
                if selected.is_some_and(|selected| selected != *number) {
                    continue;
                }
                match op {
                    "retire" => version.retired = true,
                    "revoke" => version.revoked = true,
                    "destroy" => {
                        version.revoked = true;
                        version.retired = true;
                        version.envelope = None;
                    }
                    "rewrap" => {
                        let Some(envelope) = version.envelope.as_mut() else {
                            continue;
                        };
                        let provider = native.provider()?;
                        let old_provider = native.envelope_provider(envelope)?;
                        let aad = aad(&catalog.domain, &key.id, *number, key.algorithm, "dek")?;
                        let dek = old_provider.unwrap(
                            nonce(&envelope.wrapping_nonce)?,
                            &aad,
                            &decode(&envelope.wrapped_dek)?,
                        )?;
                        ensure!(dek.len() == 32, "Invalid wrapped DEK");
                        let nonce = random::<12>()?;
                        envelope.wrapped_dek =
                            URL_SAFE_NO_PAD.encode(provider.wrap(nonce, &aad, &dek)?);
                        envelope.wrapping_nonce = URL_SAFE_NO_PAD.encode(nonce);
                        envelope.wrapping_id = provider.id().into();
                    }
                    _ => unreachable!(),
                }
            }
        }
        _ => bail!("Unknown key management operation"),
    }
    catalog.revision = catalog
        .revision
        .checked_add(1)
        .context("Key catalog revision overflow")?;
    let value = serde_json::to_value(catalog)?;
    let result = public_catalog(Some(&value))?;
    Ok((value, result))
}

fn generate(algorithm: Algorithm, bits: Option<u64>) -> Result<Zeroizing<Vec<u8>>> {
    if algorithm != Algorithm::Rsa {
        ensure!(bits.is_none(), "bits is only valid for RSA");
    }
    Ok(Zeroizing::new(match algorithm {
        Algorithm::P256 => {
            let key = aws_lc_rs::signature::EcdsaKeyPair::generate(
                &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            )
            .map_err(|_| anyhow!("P256 key generation failed"))?;
            key.to_pkcs8v1()
                .map_err(|_| anyhow!("Encode P256 key"))?
                .as_ref()
                .to_vec()
        }
        Algorithm::Rsa => {
            use aws_lc_rs::rsa::KeySize;
            let size = match bits.unwrap_or(2048) {
                2048 => KeySize::Rsa2048,
                3072 => KeySize::Rsa3072,
                4096 => KeySize::Rsa4096,
                8192 => KeySize::Rsa8192,
                _ => bail!("RSA generation supports 2048, 3072, 4096 or 8192 bits"),
            };
            let key =
                RsaKeyPair::generate(size).map_err(|_| anyhow!("RSA key generation failed"))?;
            AsDer::<Pkcs8V1Der>::as_der(&key)
                .map_err(|_| anyhow!("Encode RSA key"))?
                .as_ref()
                .to_vec()
        }
        _ => random::<32>()?.to_vec(),
    }))
}
fn canonical(algorithm: Algorithm, bytes: &[u8], format: &str) -> Result<Zeroizing<Vec<u8>>> {
    ensure!(
        matches!(format, "raw" | "pem" | "der"),
        "Key format must be raw, pem or der"
    );
    if matches!(
        algorithm,
        Algorithm::HS256 | Algorithm::A256GCM | Algorithm::XSalsa20Poly1305 | Algorithm::X25519
    ) {
        ensure!(
            format == "raw",
            "Symmetric and X25519 keys require raw format"
        );
        if algorithm == Algorithm::HS256 {
            let start = bytes
                .iter()
                .position(|byte| !byte.is_ascii_whitespace())
                .unwrap_or(bytes.len());
            ensure!(
                !bytes[start..].starts_with(b"-----BEGIN "),
                "PEM key material cannot be used as an HS256 secret"
            );
        }
        return Ok(Zeroizing::new(bytes.to_vec()));
    }
    let der = if format == "pem" {
        super::der::validate_pem(bytes)?;
        let pem = pem::parse(bytes).context("Invalid private PEM")?;
        ensure!(
            pem.tag() == "PRIVATE KEY"
                || (algorithm == Algorithm::Rsa && pem.tag() == "RSA PRIVATE KEY"),
            "Import requires a private PKCS8 or RSA private key"
        );
        Zeroizing::new(pem.into_contents())
    } else {
        Zeroizing::new(bytes.to_vec())
    };
    if format != "raw" {
        super::der::validate_der(&der)?;
    }
    Ok(Zeroizing::new(match algorithm {
        Algorithm::Ed25519 => {
            if format == "raw" {
                if bytes.len() == 64 {
                    ed25519_dalek::SigningKey::from_keypair_bytes(
                        bytes.try_into().expect("length checked"),
                    )
                    .map_err(|_| anyhow!("Ed25519 secret key has inconsistent public bytes"))?
                    .to_bytes()
                    .to_vec()
                } else {
                    ensure!(
                        bytes.len() == 32,
                        "Ed25519 raw import requires seed32 or secret64"
                    );
                    bytes.to_vec()
                }
            } else {
                Ed25519KeyPair::from_pkcs8(&der)
                    .map_err(|_| anyhow!("Invalid Ed25519 private key"))?
                    .seed()
                    .map_err(|_| anyhow!("Read Ed25519 seed"))?
                    .as_be_bytes()
                    .map_err(|_| anyhow!("Encode Ed25519 seed"))?
                    .as_ref()
                    .to_vec()
            }
        }
        Algorithm::P256 => {
            let key = if format == "raw" {
                SigningKey::from_slice(bytes)
            } else {
                SigningKey::from_pkcs8_der(&der).map_err(|_| p256::ecdsa::Error::new())
            }
            .map_err(|_| anyhow!("Invalid P256 private key"))?;
            key.to_pkcs8_der()
                .map_err(|_| anyhow!("Encode P256 private key"))?
                .as_bytes()
                .to_vec()
        }
        Algorithm::Rsa => {
            ensure!(format != "raw", "RSA requires PEM or DER");
            let key = RsaKeyPair::from_pkcs8(&der)
                .or_else(|_| RsaKeyPair::from_der(&der))
                .map_err(|_| anyhow!("Invalid RSA private key"))?;
            AsDer::<Pkcs8V1Der>::as_der(&key)
                .map_err(|_| anyhow!("Encode RSA private key"))?
                .as_ref()
                .to_vec()
        }
        _ => unreachable!(),
    }))
}

const IMPORT_AAD: &[u8] = b"flower-sealed-key-import-v1";
pub fn seal_import_file(path: &Path, bytes: &[u8], format: &str) -> Result<Value> {
    seal_import(&MountedKey::read(path)?, bytes, format)
}
fn seal_import(provider: &dyn WrappingProvider, bytes: &[u8], format: &str) -> Result<Value> {
    ensure!(
        matches!(format, "raw" | "pem" | "der"),
        "Key format must be raw, pem or der"
    );
    let nonce = random::<12>()?;
    let mut plaintext = Zeroizing::new(vec![match format {
        "raw" => 0,
        "pem" => 1,
        _ => 2,
    }]);
    plaintext.extend_from_slice(bytes);
    Ok(
        json!({"version":1,"wrappingId":provider.id(),"nonce":URL_SAFE_NO_PAD.encode(nonce),"ciphertext":URL_SAFE_NO_PAD.encode(provider.wrap(nonce,IMPORT_AAD,&plaintext)?)}),
    )
}
fn open_import(
    provider: &dyn WrappingProvider,
    value: &Value,
) -> Result<(Zeroizing<Vec<u8>>, String)> {
    ensure!(
        value["version"] == 1 && value["wrappingId"].as_str() == Some(provider.id()),
        "Import envelope requires a different wrapping key or version"
    );
    let mut bytes = provider.unwrap(
        nonce(field(value, "nonce")?)?,
        IMPORT_AAD,
        &decode(field(value, "ciphertext")?)?,
    )?;
    let format = match bytes.first() {
        Some(0) => "raw",
        Some(1) => "pem",
        Some(2) => "der",
        _ => bail!("Invalid import envelope format"),
    };
    bytes.remove(0);
    Ok((bytes, format.into()))
}

pub fn validate_ready(value: &Value) -> Result<()> {
    let catalog = catalog(Some(value))?;
    for key in catalog.keys.values() {
        for (version, data) in &key.versions {
            if !data.revoked
                && let Some(envelope) = &data.envelope
            {
                global()?.get(&Resolved {
                    domain: catalog.domain.clone(),
                    id: key.id.clone(),
                    version: *version,
                    algorithm: key.algorithm,
                    envelope: envelope.clone(),
                    usages: BTreeSet::new(),
                })?;
            }
        }
    }
    Ok(())
}

fn operation_usage(operation: &str) -> Result<&'static str> {
    Ok(match operation {
        "jwt.sign" | "nacl.sign" | "nacl.sign.detached" => "sign",
        "jwt.verify" | "nacl.sign.open" | "nacl.sign.detached.verify" => "verify",
        "jwt.encrypt" | "nacl.secretbox" | "nacl.box" | "nacl.box.after" => "encrypt",
        "jwt.decrypt" | "nacl.secretbox.open" | "nacl.box.open" | "nacl.box.open.after" => {
            "decrypt"
        }
        "nacl.box.before" => "derive",
        "key.publicKey" | "nacl.scalarMult.base" => "publicKey",
        "key.version" => "metadata",
        "nacl.scalarMult" => {
            bail!("Managed shared secrets cannot be exported; use nacl.box.before")
        }
        _ => bail!("Unsupported managed crypto operation"),
    })
}
pub fn requested_kid(operation: &str, args: &[&[u8]]) -> Result<Option<String>> {
    if !matches!(operation, "jwt.verify" | "jwt.decrypt") {
        return Ok(None);
    }
    let token = std::str::from_utf8(args.first().context("Missing JWT token")?)
        .context("JWT token must be UTF-8")?;
    let header = jwt::managed_header(token, operation == "jwt.decrypt")?;
    Ok(header.get("kid").and_then(Value::as_str).map(str::to_owned))
}
#[cfg(test)]
pub fn resolve(
    value: &Value,
    declaration: &Value,
    operation: &str,
    requested_kid: Option<&str>,
) -> Result<Value> {
    let catalog = catalog(Some(value))?;
    resolve_catalog(&catalog, declaration, operation, requested_kid)
}

thread_local! {
    // Each evaluation thread retains only its current immutable catalog. Arc
    // identity cannot be recycled while this reference is alive. A new snapshot
    // always causes fresh policy parsing; there is no revision-only trust shortcut.
    static LAST_CATALOG:RefCell<Option<(Arc<Value>,Catalog)>>=const {RefCell::new(None)};
}
pub fn resolve_shared(
    value: &Arc<Value>,
    declaration: &Value,
    operation: &str,
    requested_kid: Option<&str>,
) -> Result<Value> {
    LAST_CATALOG.with(|cached| {
        let mut cached = cached.borrow_mut();
        if cached
            .as_ref()
            .is_none_or(|(previous, _)| !Arc::ptr_eq(previous, value))
        {
            *cached = Some((value.clone(), catalog(Some(value))?));
        }
        resolve_catalog(
            &cached.as_ref().expect("catalog was loaded").1,
            declaration,
            operation,
            requested_kid,
        )
    })
}
fn resolve_catalog(
    catalog: &Catalog,
    declaration: &Value,
    operation: &str,
    requested_kid: Option<&str>,
) -> Result<Value> {
    let binding = catalog
        .bindings
        .get(field(declaration, "name")?)
        .context("Managed key is not bound")?;
    let key = catalog
        .keys
        .get(&binding.key)
        .context("Managed key binding is invalid")?;
    let algorithm: Algorithm = serde_json::from_value(declaration["algorithm"].clone())
        .context("Invalid declared key algorithm")?;
    ensure!(
        key.algorithm == algorithm,
        "Managed key algorithm does not match declaration"
    );
    let requested_usages = usages(&declaration["usages"], algorithm)?;
    let usage = operation_usage(operation)?;
    let mut permitted: BTreeSet<String> = binding
        .usages
        .intersection(&requested_usages)
        .cloned()
        .collect();
    ensure!(
        if usage == "metadata" {
            !permitted.is_empty()
        } else {
            permitted.contains(usage)
        },
        "Managed key operation is not permitted"
    );
    if matches!(operation, "jwt.verify" | "jwt.decrypt") {
        ensure!(
            requested_kid.is_some(),
            "Managed JWT requires a versioned kid"
        );
    }
    let version = if let Some(selector) = requested_kid {
        let prefix = format!("flower.{}.", key.id);
        let version = selector
            .strip_prefix(&prefix)
            .context("Version selector is outside the bound key")?
            .parse::<u64>()
            .context("Invalid managed key version")?;
        ensure!(
            selector == kid(&key.id, version),
            "Invalid managed key version"
        );
        version
    } else {
        key.active_version
    };
    let data = key
        .versions
        .get(&version)
        .context("Unknown managed key version")?;
    let envelope = data
        .envelope
        .as_ref()
        .context("Managed key version is destroyed")?;
    ensure!(!data.revoked, "Managed key version is revoked");
    if data.retired || version != key.active_version {
        permitted.retain(|usage| !matches!(usage.as_str(), "sign" | "encrypt" | "derive"));
        ensure!(
            if usage == "metadata" {
                !permitted.is_empty()
            } else {
                permitted.contains(usage)
            },
            "Historical or retired key versions cannot sign, encrypt, or derive"
        );
    }
    Ok(serde_json::to_value(Resolved {
        domain: catalog.domain.clone(),
        id: key.id.clone(),
        version,
        algorithm,
        envelope: envelope.clone(),
        usages: permitted,
    })?)
}

pub enum ManagedOutput {
    Bytes(Vec<u8>),
    Text(String),
    Json(Value),
    Bool(bool),
    Null,
    Shared(Arc<PreparedKey>),
}

/// An invocation may pin this after recording its catalog dependency. It must
/// not share the authorization with another invocation or dependency collector.
pub struct AuthorizedKey {
    prepared: Arc<PreparedKey>,
    kid: String,
    usages: BTreeSet<String>,
}
impl AuthorizedKey {
    pub fn reusable(&self, ttl: Option<Duration>) -> bool {
        ttl.is_none_or(|ttl| self.prepared.created_at.elapsed() < ttl)
    }
    pub fn retained_bytes(&self) -> usize {
        self.prepared
            .retained_bytes()
            .saturating_add(std::mem::size_of::<Self>())
            .saturating_add(self.kid.capacity())
            .saturating_add(
                self.usages
                    .iter()
                    .map(|usage| usage.capacity().saturating_add(64))
                    .sum::<usize>(),
            )
    }
}

pub fn prepare_resolved(value: &Value) -> Result<AuthorizedKey> {
    let resolved: Resolved =
        serde_json::from_value(value.clone()).context("Invalid resolved key")?;
    Ok(AuthorizedKey {
        prepared: global()?.get(&resolved)?,
        kid: kid(&resolved.id, resolved.version),
        usages: resolved.usages,
    })
}

pub fn execute_prepared(
    key: &AuthorizedKey,
    operation: &str,
    args: &[&[u8]],
    options: &Value,
    now_ms: u64,
) -> Result<ManagedOutput> {
    ensure!(
        if operation == "key.version" {
            !key.usages.is_empty()
        } else {
            key.usages.contains(operation_usage(operation)?)
        },
        "Resolved key operation is not permitted"
    );
    execute_key(
        &key.prepared,
        operation,
        args,
        options,
        now_ms,
        Some(&key.kid),
    )
}

pub fn execute(
    value: &Value,
    operation: &str,
    args: &[&[u8]],
    options: &Value,
    now_ms: u64,
) -> Result<ManagedOutput> {
    execute_prepared(&prepare_resolved(value)?, operation, args, options, now_ms)
}
pub fn execute_shared(
    key: &Arc<PreparedKey>,
    operation: &str,
    args: &[&[u8]],
    options: &Value,
    now_ms: u64,
) -> Result<ManagedOutput> {
    ensure!(
        matches!(
            operation,
            "nacl.box.after" | "nacl.box.open.after" | "nacl.secretbox" | "nacl.secretbox.open"
        ),
        "Shared key supports only authenticated encryption/decryption"
    );
    execute_key(key, operation, args, options, now_ms, None)
}
fn execute_key(
    key: &PreparedKey,
    operation: &str,
    args: &[&[u8]],
    options: &Value,
    now_ms: u64,
    kid: Option<&str>,
) -> Result<ManagedOutput> {
    let arg = |index: usize| {
        args.get(index)
            .copied()
            .with_context(|| format!("Missing managed crypto argument {index}"))
    };
    let options = if options.is_null() {
        json!({})
    } else {
        options.clone()
    };
    ensure!(
        options.is_object(),
        "Managed crypto options must be an object"
    );
    Ok(match operation {
        "jwt.sign" => {
            let algorithm = key.algorithm.jwt()?;
            let mut options = options;
            ensure!(
                options.get("keyFormat").is_none(),
                "Managed keys do not accept keyFormat"
            );
            if let Some(value) = options.get("algorithm") {
                ensure!(
                    *value == serde_json::to_value(algorithm)?,
                    "JWT algorithm does not match managed key"
                );
            }
            if let Some(value) = options.get("kid") {
                ensure!(
                    value.as_str() == kid,
                    "Managed JWT kid is fixed by key version"
                );
            }
            options["algorithm"] = serde_json::to_value(algorithm)?;
            options["keyFormat"] = json!(if algorithm == jwt::Algorithm::HS256 {
                "raw"
            } else {
                "der"
            });
            options["kid"] = json!(kid);
            let options: jwt::SignOptions = serde_json::from_value(options)?;
            ManagedOutput::Text(jwt::sign_with(
                &jwt::parse_json(arg(0)?)?,
                &options,
                |message| key.sign(message),
            )?)
        }
        "jwt.verify" => {
            let algorithm = key.algorithm.jwt()?;
            let mut options = options;
            ensure!(
                options.get("keyFormat").is_none(),
                "Managed keys do not accept keyFormat"
            );
            if options.get("algorithms").is_none() {
                options["algorithms"] = json!([algorithm]);
            }
            options["keyFormat"] = json!(if algorithm == jwt::Algorithm::HS256 {
                "raw"
            } else {
                "der"
            });
            let options: jwt::VerifyOptions = serde_json::from_value(options)?;
            ensure!(
                options.algorithms == vec![algorithm],
                "Managed JWT algorithms must match its key"
            );
            let verified = jwt::verify_with(
                std::str::from_utf8(arg(0)?)?,
                &options,
                now_ms,
                |_, message, signature| Ok(key.verify(message, signature, false)),
            )?;
            ManagedOutput::Json(serde_json::to_value(verified)?)
        }
        "jwt.encrypt" => {
            let Material::Aes(cipher) = &key.inner else {
                bail!("JWE requires an A256GCM key")
            };
            let mut options = options;
            if let Some(value) = options.get("kid") {
                ensure!(
                    value.as_str() == kid,
                    "Managed JWT kid is fixed by key version"
                );
            }
            options["kid"] = json!(kid);
            ManagedOutput::Text(jwt::encrypt_with(
                &jwt::parse_json(arg(0)?)?,
                cipher,
                arg(1)?,
                &serde_json::from_value(options)?,
            )?)
        }
        "jwt.decrypt" => {
            let Material::Aes(cipher) = &key.inner else {
                bail!("JWE requires an A256GCM key")
            };
            ManagedOutput::Json(serde_json::to_value(jwt::decrypt_with(
                std::str::from_utf8(arg(0)?)?,
                cipher,
                &serde_json::from_value(options)?,
                now_ms,
            )?)?)
        }
        "key.publicKey" => ManagedOutput::Bytes(key.public()?),
        "key.version" => ManagedOutput::Text(kid.context("Missing key version")?.into()),
        "nacl.scalarMult.base" => {
            ensure!(
                key.algorithm == Algorithm::X25519,
                "NaCl scalar multiplication requires X25519"
            );
            ManagedOutput::Bytes(key.public()?)
        }
        "nacl.sign" | "nacl.sign.detached" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures require Ed25519"
            );
            let mut signature = key.sign(arg(0)?)?;
            if operation == "nacl.sign" {
                signature.extend_from_slice(arg(0)?)
            }
            ManagedOutput::Bytes(signature)
        }
        "nacl.sign.detached.verify" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures require Ed25519"
            );
            ensure!(arg(1)?.len() == 64, "Invalid NaCl signature length");
            ManagedOutput::Bool(key.verify(arg(0)?, arg(1)?, true))
        }
        "nacl.sign.open" => {
            ensure!(
                key.algorithm == Algorithm::Ed25519,
                "NaCl signatures require Ed25519"
            );
            let bytes = arg(0)?;
            if bytes.len() >= 64 && key.verify(&bytes[64..], &bytes[..64], true) {
                ManagedOutput::Bytes(bytes[64..].to_vec())
            } else {
                ManagedOutput::Null
            }
        }
        "nacl.secretbox" | "nacl.secretbox.open" | "nacl.box.after" | "nacl.box.open.after" => {
            ensure!(
                key.algorithm == Algorithm::XSalsa20Poly1305,
                "NaCl secretbox requires XSalsa20Poly1305"
            );
            let Material::Secret(secret) = &key.inner else {
                unreachable!()
            };
            let op = if matches!(operation, "nacl.secretbox" | "nacl.box.after") {
                1
            } else {
                2
            };
            from_nacl(nacl::execute(op, &[arg(0)?, arg(1)?, secret.as_ref()])?)
        }
        "nacl.box" | "nacl.box.open" | "nacl.box.before" => {
            ensure!(
                key.algorithm == Algorithm::X25519,
                "NaCl box requires X25519"
            );
            let Material::Secret(secret) = &key.inner else {
                unreachable!()
            };
            if operation == "nacl.box.before" {
                let nacl::Output::Bytes(bytes) = nacl::execute(5, &[arg(0)?, secret.as_ref()])?
                else {
                    unreachable!()
                };
                let bytes = Zeroizing::new(bytes);
                ManagedOutput::Shared(Arc::new(PreparedKey::prepare(
                    Algorithm::XSalsa20Poly1305,
                    &bytes,
                )?))
            } else {
                from_nacl(nacl::execute(
                    if operation == "nacl.box" { 6 } else { 7 },
                    &[arg(0)?, arg(1)?, arg(2)?, secret.as_ref()],
                )?)
            }
        }
        _ => bail!("Unsupported managed crypto operation"),
    })
}
fn from_nacl(value: nacl::Output) -> ManagedOutput {
    match value {
        nacl::Output::Bytes(bytes) => ManagedOutput::Bytes(bytes),
        nacl::Output::Bool(value) => ManagedOutput::Bool(value),
        nacl::Output::Null => ManagedOutput::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_fixture(budget: usize) -> NativeKeys {
        NativeKeys {
            provider: Some(Box::new(MountedKey {
                id: "test-wrapping-key".into(),
                cipher: cipher(&[9; 32]).unwrap(),
            })),
            previous: BTreeMap::new(),
            budget,
            lease: None,
            cache: Mutex::new(Cache::default()),
        }
    }
    fn make(native: &NativeKeys, algorithm: Algorithm) -> (Value, Value) {
        let (catalog, _) = prepare_with(
            native,
            None,
            &json!({"operation":"generate","name":"stored","algorithm":algorithm}),
        )
        .unwrap();
        let declared =
            json!({"kind":"key","name":"named","algorithm":algorithm,"usages":algorithm.usages()});
        let (catalog, _) = prepare_with(
            native,
            Some(&catalog),
            &json!({"operation":"bind","name":"named","key":"stored","usages":algorithm.usages()}),
        )
        .unwrap();
        (catalog, declared)
    }
    fn execute_test(
        native: &NativeKeys,
        catalog: &Value,
        declaration: &Value,
        operation: &str,
        args: &[&[u8]],
        options: &Value,
        now: u64,
    ) -> Result<ManagedOutput> {
        let kid = requested_kid(operation, args)?;
        let value = resolve(catalog, declaration, operation, kid.as_deref())?;
        let resolved: Resolved = serde_json::from_value(value)?;
        let key = native.get(&resolved)?;
        execute_key(
            &key,
            operation,
            args,
            options,
            now,
            Some(&self::kid(&resolved.id, resolved.version)),
        )
    }
    fn text(value: ManagedOutput) -> String {
        let ManagedOutput::Text(value) = value else {
            panic!("Expected text")
        };
        value
    }
    fn bytes(value: ManagedOutput) -> Vec<u8> {
        let ManagedOutput::Bytes(value) = value else {
            panic!("Expected bytes")
        };
        value
    }

    #[test]
    fn managed_jwt_all_algorithms_reuse_prepared_context_and_validate_every_time() {
        let native = native_fixture(1 << 20);
        for algorithm in [
            Algorithm::Ed25519,
            Algorithm::P256,
            Algorithm::Rsa,
            Algorithm::HS256,
        ] {
            let (catalog, declaration) = make(&native, algorithm);
            let token = text(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "jwt.sign",
                    &[br#"{"sub":"alice","exp":2000}"#],
                    &json!({}),
                    0,
                )
                .unwrap(),
            );
            let again = text(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "jwt.sign",
                    &[br#"{"sub":"alice","exp":2000}"#],
                    &json!({}),
                    0,
                )
                .unwrap(),
            );
            assert_eq!(token, again, "Managed signing remains deterministic");
            let result = execute_test(
                &native,
                &catalog,
                &declaration,
                "jwt.verify",
                &[token.as_bytes()],
                &json!({"subject":"alice"}),
                1_999_999,
            )
            .unwrap();
            let ManagedOutput::Json(result) = result else {
                panic!("expected JWT")
            };
            assert_eq!(result["claims"]["sub"], "alice");
            assert!(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "jwt.verify",
                    &[token.as_bytes()],
                    &json!({}),
                    2_000_000
                )
                .is_err()
            );
            assert!(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "jwt.verify",
                    &[token.as_bytes()],
                    &json!({"subject":"mallory"}),
                    0
                )
                .is_err()
            );
            assert!(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "jwt.sign",
                    &[br#"{}"#],
                    &json!({"kid":"other"}),
                    0
                )
                .is_err()
            );
        }
        let cache = native.cache.lock().unwrap();
        assert_eq!(cache.loads, 4);
        assert!(cache.hits >= 16);
    }

    #[test]
    fn managed_jwe_rotation_uses_pinned_kid_and_revocation_is_checked_before_cache() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::A256GCM);
        let token = text(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "jwt.encrypt",
                &[br#"{"exp":2000}"#, &[3; 12]],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        assert!(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "jwt.decrypt",
                &[token.as_bytes()],
                &json!({}),
                0
            )
            .is_ok()
        );
        let (rotated, _) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"rotate","name":"stored"}),
        )
        .unwrap();
        assert_eq!(rotated["keys"]["stored"]["activeVersion"], 2);
        assert!(
            execute_test(
                &native,
                &rotated,
                &declaration,
                "jwt.decrypt",
                &[token.as_bytes()],
                &json!({}),
                0
            )
            .is_ok()
        );
        let replacement = text(
            execute_test(
                &native,
                &rotated,
                &declaration,
                "jwt.encrypt",
                &[br#"{"exp":2000}"#, &[3; 12]],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        assert_ne!(token, replacement);
        assert_ne!(
            requested_kid("jwt.decrypt", &[token.as_bytes()]).unwrap(),
            requested_kid("jwt.decrypt", &[replacement.as_bytes()]).unwrap()
        );
        let (revoked, _) = prepare_with(
            &native,
            Some(&rotated),
            &json!({"operation":"revoke","name":"stored","version":1}),
        )
        .unwrap();
        let hits = native.cache.lock().unwrap().hits;
        assert!(
            execute_test(
                &native,
                &revoked,
                &declaration,
                "jwt.decrypt",
                &[token.as_bytes()],
                &json!({}),
                0
            )
            .unwrap_err_string()
            .contains("revoked")
        );
        assert_eq!(native.cache.lock().unwrap().hits, hits);
        assert!(
            execute_test(
                &native,
                &revoked,
                &declaration,
                "jwt.decrypt",
                &[replacement.as_bytes()],
                &json!({}),
                0
            )
            .is_ok()
        );
        let (other, other_declaration) = make(&native, Algorithm::A256GCM);
        assert!(
            execute_test(
                &native,
                &other,
                &other_declaration,
                "jwt.decrypt",
                &[token.as_bytes()],
                &json!({}),
                0
            )
            .is_err()
        );
    }

    trait ErrorString {
        fn unwrap_err_string(self) -> String;
    }
    impl ErrorString for Result<ManagedOutput> {
        fn unwrap_err_string(self) -> String {
            match self {
                Ok(_) => panic!("Expected failure"),
                Err(error) => error.to_string(),
            }
        }
    }

    #[test]
    fn capabilities_cannot_escalate_and_old_versions_are_verify_only() {
        let native = native_fixture(1 << 20);
        let (catalog, mut declaration) = make(&native, Algorithm::Ed25519);
        let (restricted, _) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"bind","name":"named","key":"stored","usages":["verify"]}),
        )
        .unwrap();
        assert!(resolve(&restricted, &declaration, "jwt.sign", None).is_err());
        declaration["usages"] = json!(["verify"]);
        assert!(resolve(&catalog, &declaration, "jwt.sign", None).is_err());
        declaration["algorithm"] = json!("P256");
        assert!(resolve(&catalog, &declaration, "jwt.verify", Some("flower.fake.1")).is_err());
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let first = resolve(&catalog, &declaration, "jwt.sign", None).unwrap();
        let (catalog, _) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"rotate","name":"stored"}),
        )
        .unwrap();
        assert_eq!(
            resolve(&catalog, &declaration, "jwt.sign", None).unwrap()["version"],
            2
        );
        let old_kid = kid(first["id"].as_str().unwrap(), 1);
        assert!(resolve(&catalog, &declaration, "jwt.sign", Some(&old_kid)).is_err());
        assert_eq!(
            resolve(&catalog, &declaration, "jwt.verify", Some(&old_kid)).unwrap()["version"],
            1
        );
        assert!(resolve(&catalog, &declaration, "jwt.verify", None).is_err());
        assert!(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "nacl.scalarMult.base",
                &[],
                &json!({}),
                0
            )
            .is_err()
        );
        assert!(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "nacl.sign.detached.verify",
                &[b"message", &[0; 63]],
                &json!({}),
                0
            )
            .is_err()
        );
        assert!(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "jwt.sign",
                &[br#"{}"#],
                &json!([]),
                0
            )
            .is_err()
        );
        for bits in [json!("2048"), json!(-1), json!(null), json!(1.5)] {
            assert!(
                prepare_with(
                    &native,
                    None,
                    &json!({"operation":"generate","name":"invalid","algorithm":"RSA","bits":bits})
                )
                .is_err()
            );
        }
        assert!(
            prepare_with(
                &native,
                None,
                &json!({"operation":"generate","name":"invalid","algorithm":"Ed25519","bits":2048})
            )
            .is_err()
        );
    }

    #[test]
    fn cached_catalog_identity_never_masks_new_policy() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let catalog = Arc::new(catalog);
        assert!(resolve_shared(&catalog, &declaration, "jwt.sign", None).is_ok());
        assert!(resolve_shared(&catalog, &declaration, "jwt.sign", None).is_ok());
        let (revoked, _) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"revoke","name":"stored"}),
        )
        .unwrap();
        assert!(resolve_shared(&Arc::new(revoked), &declaration, "jwt.sign", None).is_err());
        assert!(
            resolve_shared(&catalog, &declaration, "jwt.sign", None).is_ok(),
            "An already admitted snapshot remains pinned"
        );
    }

    #[test]
    fn envelope_binds_identity_algorithm_and_version_even_on_a_cache_hit() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let resolved = resolve(&catalog, &declaration, "jwt.sign", None).unwrap();
        let parsed: Resolved = serde_json::from_value(resolved.clone()).unwrap();
        native.get(&parsed).unwrap();
        for (field, value) in [
            ("domain", json!("other")),
            ("id", json!("other")),
            ("version", json!(2)),
            ("algorithm", json!("X25519")),
        ] {
            let mut tampered = resolved.clone();
            tampered[field] = value;
            assert!(
                native
                    .get(&serde_json::from_value(tampered).unwrap())
                    .is_err()
            );
        }
        let mut tampered = resolved;
        let ciphertext = tampered["envelope"]["ciphertext"].as_str().unwrap();
        let mut ciphertext = decode(ciphertext).unwrap();
        ciphertext[0] ^= 1;
        tampered["envelope"]["ciphertext"] = json!(URL_SAFE_NO_PAD.encode(ciphertext));
        assert!(
            native
                .get(&serde_json::from_value(tampered).unwrap())
                .is_err()
        );
        let wrong = NativeKeys {
            provider: Some(Box::new(MountedKey {
                id: "test-wrapping-key".into(),
                cipher: cipher(&[8; 32]).unwrap(),
            })),
            ..native_fixture(0)
        };
        assert!(wrong.get(&parsed).is_err());
    }

    #[test]
    fn encrypted_import_roundtrip_and_redacted_catalog() {
        let native = native_fixture(1 << 20);
        let material = [7; 32];
        let sealed = seal_import(native.provider().unwrap(), &material, "raw").unwrap();
        assert!(
            !sealed
                .to_string()
                .contains(&URL_SAFE_NO_PAD.encode(material))
        );
        let (catalog, redacted) = prepare_with(
            &native,
            None,
            &json!({"operation":"import","name":"stored","algorithm":"Ed25519","sealed":sealed}),
        )
        .unwrap();
        assert!(
            !catalog
                .to_string()
                .contains(&URL_SAFE_NO_PAD.encode(material))
        );
        assert!(!redacted.to_string().contains("ciphertext"));
        assert!(!redacted.to_string().contains("wrappedDek"));
        let (catalog,_)=prepare_with(&native,Some(&catalog),&json!({"operation":"bind","name":"named","key":"stored","usages":["sign","verify","publicKey"]})).unwrap();
        let declaration =
            json!({"name":"named","algorithm":"Ed25519","usages":["sign","verify","publicKey"]});
        let public = bytes(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "key.publicKey",
                &[],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            public,
            ed25519_dalek::SigningKey::from_bytes(&material)
                .verifying_key()
                .to_bytes()
        );
        let mut altered = sealed;
        altered["wrappingId"] = json!("another");
        assert!(open_import(native.provider().unwrap(), &altered).is_err());
        assert!(prepare_with(&native,None,&json!({"operation":"import","name":"x","algorithm":"Ed25519","key":material.to_vec()})).is_err());
    }

    #[test]
    fn nacl_managed_signatures_box_and_opaque_shared_secret_interoperate() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let signed = bytes(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "nacl.sign",
                &[b"hello"],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        let public = bytes(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "key.publicKey",
                &[],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            nacl::execute(9, &[&signed, &public]).unwrap(),
            nacl::Output::Bytes(b"hello".to_vec())
        );
        let (catalog, declaration) = make(&native, Algorithm::X25519);
        let peer_secret = [5; 32];
        let peer_public = x25519_dalek::x25519(peer_secret, x25519_dalek::X25519_BASEPOINT_BYTES);
        let nonce = [3; 24];
        let boxed = bytes(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "nacl.box",
                &[b"hello", &nonce, &peer_public],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        let public = bytes(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "key.publicKey",
                &[],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            nacl::execute(7, &[&boxed, &nonce, &public, &peer_secret]).unwrap(),
            nacl::Output::Bytes(b"hello".to_vec())
        );
        let ManagedOutput::Shared(shared) = execute_test(
            &native,
            &catalog,
            &declaration,
            "nacl.box.before",
            &[&peer_public],
            &json!({}),
            0,
        )
        .unwrap() else {
            panic!("Expected opaque shared key")
        };
        assert_eq!(
            bytes(
                execute_shared(
                    &shared,
                    "nacl.box.after",
                    &[b"hello", &nonce],
                    &json!({}),
                    0
                )
                .unwrap()
            ),
            boxed
        );
        assert!(execute_shared(&shared, "key.publicKey", &[], &json!({}), 0).is_err());
        assert!(resolve(&catalog, &declaration, "nacl.scalarMult", None).is_err());
        let (restricted, _) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"bind","name":"named","key":"stored","usages":["derive"]}),
        )
        .unwrap();
        assert!(resolve(&restricted, &declaration, "nacl.box.before", None).is_ok());
        assert!(resolve(&restricted, &declaration, "nacl.box.after", None).is_err());
    }

    #[test]
    fn explicit_nacl_versions_retain_reads_and_refuse_historical_writes() {
        let native = native_fixture(1 << 20);
        let message = b"durable encrypted flower";
        let nonce = [3; 24];
        let peer = x25519_dalek::x25519([5; 32], x25519_dalek::X25519_BASEPOINT_BYTES);
        for (algorithm, write, read) in [
            (
                Algorithm::XSalsa20Poly1305,
                "nacl.secretbox",
                "nacl.secretbox.open",
            ),
            (Algorithm::X25519, "nacl.box", "nacl.box.open"),
            (Algorithm::Ed25519, "nacl.sign", "nacl.sign.open"),
        ] {
            let (catalog, declaration) = make(&native, algorithm);
            let selector = text(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    "key.version",
                    &[],
                    &json!({}),
                    0,
                )
                .unwrap(),
            );
            let boxed = bytes(
                execute_test(
                    &native,
                    &catalog,
                    &declaration,
                    write,
                    &[message, &nonce, &peer],
                    &json!({}),
                    0,
                )
                .unwrap(),
            );
            let (rotated, _) = prepare_with(
                &native,
                Some(&catalog),
                &json!({"operation":"rotate","name":"stored"}),
            )
            .unwrap();
            let selected: Resolved = serde_json::from_value(
                resolve(&rotated, &declaration, read, Some(&selector)).unwrap(),
            )
            .unwrap();
            assert_eq!(
                bytes(
                    execute_key(
                        &native.get(&selected).unwrap(),
                        read,
                        &[&boxed, &nonce, &peer],
                        &json!({}),
                        0,
                        Some(&selector)
                    )
                    .unwrap()
                ),
                message
            );
            assert!(resolve(&rotated, &declaration, write, Some(&selector)).is_err());
            assert!(resolve(&rotated, &declaration, "nacl.box.before", Some(&selector)).is_err());
            assert!(resolve(&rotated, &declaration, read, Some("flower.someone-else.1")).is_err());
            let (revoked, _) = prepare_with(
                &native,
                Some(&rotated),
                &json!({"operation":"revoke","name":"stored","version":1}),
            )
            .unwrap();
            assert!(resolve(&revoked, &declaration, read, Some(&selector)).is_err());
        }
    }

    #[test]
    fn retire_destroy_and_rewrap_have_distinct_irreversible_current_catalog_semantics() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let selector = text(
            execute_test(
                &native,
                &catalog,
                &declaration,
                "key.version",
                &[],
                &json!({}),
                0,
            )
            .unwrap(),
        );
        let (retired, public) = prepare_with(
            &native,
            Some(&catalog),
            &json!({"operation":"retire","name":"stored","version":1}),
        )
        .unwrap();
        assert_eq!(public["keys"]["stored"]["versions"][0]["retired"], true);
        assert!(resolve(&retired, &declaration, "nacl.sign", None).is_err());
        assert!(resolve(&retired, &declaration, "nacl.sign.open", Some(&selector)).is_ok());
        let (destroyed, public) = prepare_with(
            &native,
            Some(&retired),
            &json!({"operation":"destroy","name":"stored","version":1}),
        )
        .unwrap();
        assert!(destroyed["keys"]["stored"]["versions"]["1"]["envelope"].is_null());
        assert_eq!(public["keys"]["stored"]["versions"][0]["destroyed"], true);
        assert!(public["keys"]["stored"]["versions"][0]["wrappingId"].is_null());
        assert!(resolve(&destroyed, &declaration, "nacl.sign.open", Some(&selector)).is_err());
        let (rotated, _) = prepare_with(
            &native,
            Some(&destroyed),
            &json!({"operation":"rotate","name":"stored"}),
        )
        .unwrap();
        assert_eq!(rotated["keys"]["stored"]["activeVersion"], 2);
        assert!(rotated["keys"]["stored"]["versions"]["1"]["envelope"].is_null());

        let mut rotated_provider = native_fixture(0);
        rotated_provider.previous.insert(
            "test-wrapping-key".into(),
            rotated_provider.provider.take().unwrap(),
        );
        rotated_provider.provider = Some(Box::new(MountedKey {
            id: "new-wrapping-key".into(),
            cipher: cipher(&[4; 32]).unwrap(),
        }));
        let (rewrapped, _) = prepare_with(
            &rotated_provider,
            Some(&catalog),
            &json!({"operation":"rewrap","name":"stored"}),
        )
        .unwrap();
        let old_envelope = &catalog["keys"]["stored"]["versions"]["1"]["envelope"];
        let new_envelope = &rewrapped["keys"]["stored"]["versions"]["1"]["envelope"];
        assert_eq!(old_envelope["ciphertext"], new_envelope["ciphertext"]);
        assert_eq!(old_envelope["nonce"], new_envelope["nonce"]);
        assert_ne!(old_envelope["wrappedDek"], new_envelope["wrappedDek"]);
        assert_eq!(new_envelope["wrappingId"], "new-wrapping-key");
        let selected: Resolved =
            serde_json::from_value(resolve(&rewrapped, &declaration, "nacl.sign", None).unwrap())
                .unwrap();
        let before = rotated_provider.get(&selected).unwrap().public().unwrap();
        rotated_provider.previous.clear();
        assert_eq!(
            rotated_provider.get(&selected).unwrap().public().unwrap(),
            before
        );
        assert!(native.get(&selected).is_err());
        let old: Resolved =
            serde_json::from_value(resolve(&catalog, &declaration, "nacl.sign", None).unwrap())
                .unwrap();
        assert!(
            rotated_provider.get(&old).is_err(),
            "Removing the old KEK makes old envelopes unreadable"
        );
        assert!(
            prepare_with(
                &native,
                Some(&catalog),
                &json!({"operation":"destroy","name":"stored","version":1.5})
            )
            .is_err()
        );
    }

    #[test]
    fn cache_coalesces_cold_loads_has_a_budget_and_can_be_disabled() {
        let native = Arc::new(native_fixture(10_000));
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let resolved: Resolved =
            serde_json::from_value(resolve(&catalog, &declaration, "jwt.sign", None).unwrap())
                .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let native = native.clone();
                let resolved = resolved.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    native.get(&resolved).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let contexts = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(contexts.iter().all(|key| Arc::ptr_eq(&contexts[0], key)));
        assert_eq!(native.cache.lock().unwrap().loads, 1);
        let (second, declaration) = make(&native, Algorithm::Ed25519);
        native
            .get(
                &serde_json::from_value(resolve(&second, &declaration, "jwt.sign", None).unwrap())
                    .unwrap(),
            )
            .unwrap();
        {
            let cache = native.cache.lock().unwrap();
            assert_eq!(cache.entries.len(), 1);
            assert_eq!(cache.evictions, 1);
            assert!(cache.bytes <= native.budget);
        }
        let disabled = native_fixture(0);
        disabled.get(&resolved).unwrap();
        disabled.get(&resolved).unwrap();
        let cache = disabled.cache.lock().unwrap();
        assert_eq!(cache.loads, 2);
        assert!(cache.entries.is_empty());
        assert!(cache.flights.is_empty());
        assert_eq!(cache.flight_bytes, 0);
    }

    #[test]
    fn cold_keys_prepare_independently_and_do_not_block_warm_hits() {
        struct GatedProvider {
            inner: Box<dyn WrappingProvider>,
            entered: std::sync::mpsc::Sender<()>,
            gate: Arc<(Mutex<bool>, Condvar)>,
        }
        impl WrappingProvider for GatedProvider {
            fn id(&self) -> &str {
                self.inner.id()
            }
            fn wrap(&self, nonce: [u8; 12], aad: &[u8], bytes: &[u8]) -> Result<Vec<u8>> {
                self.inner.wrap(nonce, aad, bytes)
            }
            fn unwrap(
                &self,
                nonce: [u8; 12],
                aad: &[u8],
                bytes: &[u8],
            ) -> Result<Zeroizing<Vec<u8>>> {
                self.entered.send(()).unwrap();
                let (lock, condition) = &*self.gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
                self.inner.unwrap(nonce, aad, bytes)
            }
        }
        let mut native = native_fixture(1 << 20);
        let resolved = (0..3)
            .map(|_| {
                let (catalog, declaration) = make(&native, Algorithm::Ed25519);
                serde_json::from_value::<Resolved>(
                    resolve(&catalog, &declaration, "jwt.sign", None).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let warm = native.get(&resolved[0]).unwrap();
        let (entered, receive) = std::sync::mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        native.provider = Some(Box::new(GatedProvider {
            inner: native.provider.take().unwrap(),
            entered,
            gate: gate.clone(),
        }));
        let native = Arc::new(native);
        let threads = resolved[1..]
            .iter()
            .cloned()
            .map(|key| {
                let native = native.clone();
                std::thread::spawn(move || native.get(&key))
            })
            .collect::<Vec<_>>();
        let first = receive.recv_timeout(Duration::from_secs(2));
        let second = receive.recv_timeout(Duration::from_secs(2));
        // Query the warm entry on its own thread so a regression cannot hang
        // the test while the gates are deliberately closed.
        let (send_hit, receive_hit) = std::sync::mpsc::channel();
        let warm_key = resolved[0].clone();
        let hit_native = native.clone();
        let hit = std::thread::spawn(move || send_hit.send(hit_native.get(&warm_key)).unwrap());
        let warm_result = receive_hit.recv_timeout(Duration::from_secs(2));
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        for thread in threads {
            thread.join().unwrap().unwrap();
        }
        hit.join().unwrap();
        assert!(
            first.is_ok() && second.is_ok(),
            "Unrelated cold keys must unwrap concurrently"
        );
        assert!(Arc::ptr_eq(
            &warm,
            &warm_result
                .expect("warm hit blocked on cold preparation")
                .unwrap()
        ));
        let cache = native.cache.lock().unwrap();
        assert_eq!(cache.loads, 3);
        assert!(cache.flights.is_empty());
        assert_eq!(cache.flight_bytes, 0);
    }

    #[test]
    fn failed_cold_load_releases_flight_and_allows_retry() {
        let native = native_fixture(1 << 20);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let good: Resolved =
            serde_json::from_value(resolve(&catalog, &declaration, "jwt.sign", None).unwrap())
                .unwrap();
        let mut bad = good.clone();
        bad.envelope.ciphertext = "invalid".into();
        assert!(native.get(&bad).is_err());
        assert!(native.get(&bad).is_err());
        native.get(&good).unwrap();
        let cache = native.cache.lock().unwrap();
        assert!(cache.flights.is_empty());
        assert_eq!(cache.flight_bytes, 0);
        assert_eq!(cache.loads, 1);
    }

    #[test]
    fn expiring_unlock_cache_reloads_and_owned_file_checks_fail_closed() {
        let mut native = native_fixture(1 << 20);
        native.lease = Some(Duration::ZERO);
        let (catalog, declaration) = make(&native, Algorithm::Ed25519);
        let resolved: Resolved =
            serde_json::from_value(resolve(&catalog, &declaration, "jwt.sign", None).unwrap())
                .unwrap();
        native.get(&resolved).unwrap();
        native.get(&resolved).unwrap();
        assert_eq!(native.cache.lock().unwrap().loads, 2);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), [5; 32]).unwrap();
        MountedKey::read(file.path()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(MountedKey::read(file.path()).is_err());
        }
        std::fs::write(file.path(), [5; 31]).unwrap();
        assert!(MountedKey::read(file.path()).is_err());
        let mut prepared = PreparedKey::prepare(Algorithm::Ed25519, &[3; 32]).unwrap();
        prepared.created_at = Instant::now() - Duration::from_secs(60);
        let authorized = AuthorizedKey {
            prepared: Arc::new(prepared),
            kid: "flower.old.1".into(),
            usages: BTreeSet::from(["sign".into()]),
        };
        assert!(authorized.reusable(None));
        assert!(
            !authorized.reusable(Some(Duration::from_secs(1))),
            "A new invocation cannot renew the lifetime of an old prepared context"
        );
        assert!(authorized.retained_bytes() >= authorized.prepared.retained_bytes());
    }
}
