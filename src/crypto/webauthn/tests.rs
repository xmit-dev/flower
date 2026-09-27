use super::*;
use aws_lc_rs::{
    rand::SystemRandom,
    rsa::KeySize,
    signature::{
        ECDSA_P256_SHA256_ASN1_SIGNING, ECDSA_P384_SHA384_ASN1_SIGNING,
        ECDSA_P521_SHA512_ASN1_SIGNING, EcdsaKeyPair, EcdsaSigningAlgorithm, KeyPair,
        RSA_PKCS1_SHA256, RSA_PSS_SHA256, RsaEncoding, RsaKeyPair,
    },
};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::json;

const RP_ID: &str = "example.com";
const ORIGIN: &str = "https://example.com";
const CHALLENGE: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const FLAGS: u8 = USER_PRESENT | USER_VERIFIED | BACKUP_ELIGIBLE | BACKUP_STATE;

/// CBOR fixtures: the decoder under test must not also encode its own inputs.
enum C<'a> {
    U(u64),
    I(i64),
    B(&'a [u8]),
    T(&'a str),
    M(Vec<(C<'a>, C<'a>)>),
    Bool(bool),
}

fn cbor(value: &C) -> Vec<u8> {
    fn head(major: u8, value: u64, out: &mut Vec<u8>) {
        match value {
            0..=23 => out.push((major << 5) | value as u8),
            24..=0xff => out.extend([(major << 5) | 24, value as u8]),
            0x100..=0xffff => {
                out.push((major << 5) | 25);
                out.extend((value as u16).to_be_bytes());
            }
            _ => {
                out.push((major << 5) | 26);
                out.extend((value as u32).to_be_bytes());
            }
        }
    }
    fn write(value: &C, out: &mut Vec<u8>) {
        match value {
            C::U(n) => head(0, *n, out),
            C::I(n) if *n >= 0 => head(0, *n as u64, out),
            C::I(n) => head(1, (-1 - *n) as u64, out),
            C::B(bytes) => {
                head(2, bytes.len() as u64, out);
                out.extend(*bytes);
            }
            C::T(text) => {
                head(3, text.len() as u64, out);
                out.extend(text.as_bytes());
            }
            C::M(entries) => {
                head(5, entries.len() as u64, out);
                for (key, value) in entries {
                    write(key, out);
                    write(value, out);
                }
            }
            C::Bool(value) => out.push(if *value { 0xf5 } else { 0xf4 }),
        }
    }
    let mut out = Vec::new();
    write(value, &mut out);
    out
}

enum TestKey {
    Ed25519(SigningKey),
    Ecdsa(EcdsaKeyPair, i64, i64, usize),
    Rsa(RsaKeyPair, i64, &'static dyn RsaEncoding),
}

impl TestKey {
    fn ecdsa(signing: &'static EcdsaSigningAlgorithm, alg: i64, crv: i64, size: usize) -> Self {
        Self::Ecdsa(EcdsaKeyPair::generate(signing).unwrap(), alg, crv, size)
    }

    fn cose(&self) -> Vec<u8> {
        match self {
            Self::Ed25519(key) => cbor(&C::M(vec![
                (C::U(1), C::U(1)),
                (C::U(3), C::I(-8)),
                (C::I(-1), C::U(6)),
                (C::I(-2), C::B(&key.verifying_key().to_bytes())),
            ])),
            Self::Ecdsa(key, alg, crv, size) => {
                let point = key.public_key().as_ref();
                cbor(&C::M(vec![
                    (C::U(1), C::U(2)),
                    (C::U(3), C::I(*alg)),
                    (C::I(-1), C::I(*crv)),
                    (C::I(-2), C::B(&point[1..1 + size])),
                    (C::I(-3), C::B(&point[1 + size..])),
                ]))
            }
            Self::Rsa(key, alg, _) => {
                let (n, e) = rsa_components(key.public_key().as_ref());
                cbor(&C::M(vec![
                    (C::U(1), C::U(3)),
                    (C::U(3), C::I(*alg)),
                    (C::I(-1), C::B(&n)),
                    (C::I(-2), C::B(&e)),
                ]))
            }
        }
    }

    fn sign(&self, message: &[u8]) -> Vec<u8> {
        match self {
            Self::Ed25519(key) => key.sign(message).to_bytes().to_vec(),
            Self::Ecdsa(key, ..) => key
                .sign(&SystemRandom::new(), message)
                .unwrap()
                .as_ref()
                .to_vec(),
            Self::Rsa(key, _, encoding) => {
                let mut signature = vec![0; key.public_modulus_len()];
                key.sign(*encoding, &SystemRandom::new(), message, &mut signature)
                    .unwrap();
                signature
            }
        }
    }
}

/// n and e from a DER RSAPublicKey, as unsigned big-endian COSE values.
fn rsa_components(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
    fn length(der: &[u8], position: &mut usize) -> usize {
        let first = der[*position];
        *position += 1;
        if first < 0x80 {
            return usize::from(first);
        }
        let mut value = 0;
        for _ in 0..first & 0x7f {
            value = (value << 8) | usize::from(der[*position]);
            *position += 1;
        }
        value
    }
    let mut position = 1;
    length(der, &mut position);
    let mut integers = Vec::new();
    for _ in 0..2 {
        assert_eq!(der[position], 0x02);
        position += 1;
        let size = length(der, &mut position);
        let bytes = &der[position..position + size];
        position += size;
        let start = bytes
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(bytes.len() - 1);
        integers.push(bytes[start..].to_vec());
    }
    (integers.remove(0), integers.remove(0))
}

fn keys() -> Vec<TestKey> {
    vec![
        TestKey::Ed25519(SigningKey::from_bytes(&[7; 32])),
        TestKey::ecdsa(&ECDSA_P256_SHA256_ASN1_SIGNING, -7, 1, 32),
        TestKey::ecdsa(&ECDSA_P384_SHA384_ASN1_SIGNING, -35, 2, 48),
        TestKey::ecdsa(&ECDSA_P521_SHA512_ASN1_SIGNING, -36, 3, 66),
        TestKey::Rsa(
            RsaKeyPair::generate(KeySize::Rsa2048).unwrap(),
            -257,
            &RSA_PKCS1_SHA256,
        ),
        TestKey::Rsa(
            RsaKeyPair::generate(KeySize::Rsa2048).unwrap(),
            -37,
            &RSA_PSS_SHA256,
        ),
    ]
}

fn b64(bytes: &[u8]) -> String {
    BASE64URL.encode(bytes)
}

fn authenticator_bytes(
    rp_id: &str,
    flags: u8,
    counter: u32,
    attested: Option<(&[u8], &[u8])>,
) -> Vec<u8> {
    let mut data = Sha256::digest(rp_id.as_bytes()).to_vec();
    data.push(flags);
    data.extend(counter.to_be_bytes());
    if let Some((id, key)) = attested {
        data.extend([0x11; 16]);
        data.extend((id.len() as u16).to_be_bytes());
        data.extend(id);
        data.extend(key);
    }
    data
}

fn client(kind: &str, challenge: &str, origin: &str) -> Vec<u8> {
    serde_json::to_vec(
        &json!({"type": kind, "challenge": challenge, "origin": origin, "crossOrigin": false}),
    )
    .unwrap()
}

fn registration_response(key: &TestKey, id: &[u8]) -> Value {
    let data = authenticator_bytes(RP_ID, FLAGS | ATTESTED, 0, Some((id, &key.cose())));
    registration_with(
        id,
        &client("webauthn.create", CHALLENGE, ORIGIN),
        &data,
        "none",
        C::M(vec![]),
    )
}

fn registration_with(
    id: &[u8],
    client_data_json: &[u8],
    data: &[u8],
    format: &str,
    statement: C,
) -> Value {
    let object = cbor(&C::M(vec![
        (C::T("fmt"), C::T(format)),
        (C::T("attStmt"), statement),
        (C::T("authData"), C::B(data)),
    ]));
    json!({
        "id": b64(id), "rawId": b64(id), "type": "public-key", "authenticatorAttachment": "platform",
        "response": {"clientDataJSON": b64(client_data_json), "attestationObject": b64(&object), "transports": ["internal", "hybrid"]},
        "clientExtensionResults": {},
    })
}

fn assertion(
    key: &TestKey,
    id: &[u8],
    data: &[u8],
    client_data_json: &[u8],
    handle: Option<&[u8]>,
) -> Value {
    let mut message = data.to_vec();
    message.extend(Sha256::digest(client_data_json));
    json!({
        "id": b64(id), "rawId": b64(id), "type": "public-key",
        "response": {
            "clientDataJSON": b64(client_data_json), "authenticatorData": b64(data),
            "signature": b64(&key.sign(&message)), "userHandle": handle.map(b64),
        },
        "clientExtensionResults": {},
    })
}

fn signed_in(key: &TestKey, id: &[u8], flags: u8, counter: u32) -> Value {
    let data = authenticator_bytes(RP_ID, flags, counter, None);
    assertion(
        key,
        id,
        &data,
        &client("webauthn.get", CHALLENGE, ORIGIN),
        Some(b"user-1"),
    )
}

fn registration_options() -> RegistrationOptions {
    RegistrationOptions {
        challenge: CHALLENGE.into(),
        origins: vec!["https://other.example".into(), ORIGIN.into()],
        rp_id: RP_ID.into(),
        user_verification: UserVerification::Required,
        algorithms: ALGORITHMS.to_vec(),
    }
}

fn authentication_options(credential: &Credential) -> AuthenticationOptions {
    AuthenticationOptions {
        challenge: CHALLENGE.into(),
        origins: vec![ORIGIN.into()],
        rp_id: RP_ID.into(),
        user_verification: UserVerification::Required,
        credential: StoredCredential {
            id: credential.id.clone(),
            public_key: credential.public_key.clone(),
            sign_count: credential.sign_count,
            user_handle: Some(b64(b"user-1")),
        },
    }
}

fn register(response: &Value, options: &RegistrationOptions) -> Result<Registration> {
    verify_registration(&serde_json::to_vec(response).unwrap(), options)
}

fn authenticate(response: &Value, options: &AuthenticationOptions) -> Result<Authentication> {
    verify_authentication(&serde_json::to_vec(response).unwrap(), options)
}

fn fails(result: Result<impl std::fmt::Debug>, reason: &str) {
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains(reason), "expected {reason:?}, got {error:?}");
}

#[test]
fn every_supported_algorithm_registers_and_signs_in() {
    for (index, key) in keys().iter().enumerate() {
        let id = [index as u8 + 1; 20];
        let registration =
            register(&registration_response(key, &id), &registration_options()).unwrap();
        let credential = &registration.credential;
        assert_eq!(credential.id, b64(&id));
        assert_eq!(credential.public_key, b64(&key.cose()));
        assert_eq!(credential.sign_count, 0);
        assert_eq!(credential.transports, ["internal", "hybrid"]);
        assert!(
            credential.backup_eligible && credential.backup_state && registration.user_verified
        );
        assert_eq!(credential.aaguid, "11111111-1111-1111-1111-111111111111");
        assert_eq!(registration.attestation.format, "none");

        let signed = authenticate(
            &signed_in(key, &id, FLAGS, 0),
            &authentication_options(credential),
        )
        .unwrap();
        assert_eq!(signed.credential_id, credential.id);
        assert_eq!(
            (signed.sign_count, signed.user_verified, signed.backup_state),
            (0, true, true)
        );
        assert_eq!(signed.user_handle, Some(b64(b"user-1")));
    }
    let key = TestKey::Ed25519(SigningKey::from_bytes(&[7; 32]));
    let json = serde_json::to_value(
        register(&registration_response(&key, &[1]), &registration_options()).unwrap(),
    )
    .unwrap();
    let mut fields: Vec<_> = json["credential"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    fields.sort();
    assert_eq!(
        fields,
        [
            "aaguid",
            "algorithm",
            "backupEligible",
            "backupState",
            "id",
            "publicKey",
            "signCount",
            "transports"
        ]
    );
    assert_eq!(json["userVerified"], true);
}

#[test]
fn registration_rejects_ceremonies_bound_elsewhere() {
    let key = TestKey::ecdsa(&ECDSA_P256_SHA256_ASN1_SIGNING, -7, 1, 32);
    let cose = key.cose();
    let id = [9u8; 16];
    let data = |rp_id: &str, flags: u8| {
        authenticator_bytes(rp_id, flags | ATTESTED, 0, Some((&id, &cose)))
    };
    let good_client = client("webauthn.create", CHALLENGE, ORIGIN);
    let good_data = data(RP_ID, FLAGS);
    let options = registration_options();
    let with = |client_data_json: &[u8], data: &[u8]| {
        registration_with(&id, client_data_json, data, "none", C::M(vec![]))
    };

    fails(
        register(
            &with(
                &client("webauthn.create", "AAAAAAAAAAAAAAAAAAAAAA", ORIGIN),
                &good_data,
            ),
            &options,
        ),
        "challenge does not match",
    );
    fails(
        register(
            &with(
                &client("webauthn.create", CHALLENGE, "https://evil.example"),
                &good_data,
            ),
            &options,
        ),
        "origin is not allowed",
    );
    fails(
        register(
            &with(&client("webauthn.get", CHALLENGE, ORIGIN), &good_data),
            &options,
        ),
        "not a webauthn.create ceremony",
    );
    let cross = serde_json::to_vec(&json!({"type": "webauthn.create", "challenge": CHALLENGE, "origin": ORIGIN, "crossOrigin": true})).unwrap();
    fails(
        register(&with(&cross, &good_data), &options),
        "cross-origin",
    );
    fails(
        register(&with(&good_client, &data("evil.example", FLAGS)), &options),
        "different RP ID",
    );
    fails(
        register(
            &with(&good_client, &data(RP_ID, FLAGS & !USER_PRESENT)),
            &options,
        ),
        "not present",
    );
    fails(
        register(
            &with(&good_client, &data(RP_ID, FLAGS & !USER_VERIFIED)),
            &options,
        ),
        "not verified",
    );
    let preferred = RegistrationOptions {
        user_verification: UserVerification::Preferred,
        ..registration_options()
    };
    assert!(
        !register(
            &with(&good_client, &data(RP_ID, FLAGS & !USER_VERIFIED)),
            &preferred
        )
        .unwrap()
        .user_verified
    );
    fails(
        register(
            &with(&good_client, &data(RP_ID, FLAGS & !BACKUP_ELIGIBLE)),
            &options,
        ),
        "without backup eligibility",
    );
    fails(
        register(
            &with(&good_client, &[good_data.clone(), vec![0]].concat()),
            &options,
        ),
        "trailing bytes",
    );
    fails(
        register(
            &with(&good_client, &authenticator_bytes(RP_ID, FLAGS, 0, None)),
            &options,
        ),
        "no attested credential",
    );
    fails(
        register(
            &with(
                &good_client,
                &authenticator_bytes(RP_ID, FLAGS | EXTENSIONS, 0, None),
            ),
            &options,
        ),
        "extensions are not valid CBOR",
    );
    fails(
        register(
            &registration_with(
                &id,
                &good_client,
                &good_data,
                "none",
                C::M(vec![(C::T("sig"), C::B(&[1]))]),
            ),
            &options,
        ),
        "none attestation carries a statement",
    );
    let packed = register(
        &registration_with(
            &id,
            &good_client,
            &good_data,
            "packed",
            C::M(vec![(C::T("alg"), C::I(-7)), (C::T("sig"), C::B(&[1]))]),
        ),
        &options,
    );
    assert_eq!(packed.unwrap().attestation.format, "packed");

    let mut other_id = with(&good_client, &good_data);
    other_id["id"] = json!(b64(&[8u8; 16]));
    fails(register(&other_id, &options), "id and rawId differ");
    other_id["rawId"] = json!(b64(&[8u8; 16]));
    fails(
        register(&other_id, &options),
        "differs from the response id",
    );
    let mut wrong_type = with(&good_client, &good_data);
    wrong_type["type"] = json!("password");
    fails(register(&wrong_type, &options), "type must be public-key");

    let only_eddsa = RegistrationOptions {
        algorithms: vec![-8],
        ..registration_options()
    };
    fails(
        register(&with(&good_client, &good_data), &only_eddsa),
        "algorithm is not allowed",
    );
    fails(
        register(
            &with(&good_client, &good_data),
            &RegistrationOptions {
                algorithms: vec![-7, -65535],
                ..registration_options()
            },
        ),
        "supported COSE identifiers",
    );
    fails(
        register(
            &with(&good_client, &good_data),
            &RegistrationOptions {
                challenge: "AAAA".into(),
                ..registration_options()
            },
        ),
        "at least 16 bytes",
    );
    fails(
        register(
            &with(&good_client, &good_data),
            &RegistrationOptions {
                origins: vec![],
                ..registration_options()
            },
        ),
        "allowed origin",
    );
    fails(
        verify_registration(b"{}", &options),
        "type must be public-key",
    );
    fails(verify_registration(b"[", &options), "not valid JSON");
}

#[test]
fn credential_keys_must_be_public_and_supported() {
    let key = TestKey::ecdsa(&ECDSA_P256_SHA256_ASN1_SIGNING, -7, 1, 32);
    let point = key.public_key_point();
    let id = [3u8; 8];
    let attempt = |cose: Vec<u8>| {
        let data = authenticator_bytes(RP_ID, FLAGS | ATTESTED, 0, Some((&id, &cose)));
        register(
            &registration_with(
                &id,
                &client("webauthn.create", CHALLENGE, ORIGIN),
                &data,
                "none",
                C::M(vec![]),
            ),
            &registration_options(),
        )
    };
    fn ec2<'a>(extra: Vec<(C<'a>, C<'a>)>) -> Vec<u8> {
        let mut entries = vec![(C::U(1), C::U(2)), (C::U(3), C::I(-7)), (C::I(-1), C::U(1))];
        entries.extend(extra);
        cbor(&C::M(entries))
    }
    let (x, y) = (&point[1..33], &point[33..]);
    fails(
        attempt(ec2(vec![(C::I(-2), C::B(x)), (C::I(-3), C::Bool(true))])),
        "uncompressed x and y",
    );
    fails(
        attempt(ec2(vec![
            (C::I(-2), C::B(x)),
            (C::I(-3), C::B(y)),
            (C::I(-4), C::B(&[1; 32])),
        ])),
        "private material",
    );
    fails(
        attempt(ec2(vec![(C::I(-2), C::B(x)), (C::I(-3), C::B(&y[1..]))])),
        "wrong length",
    );
    fails(
        attempt(ec2(vec![(C::I(-2), C::B(x)), (C::I(-3), C::B(x))])),
        "invalid EC2 public key",
    );
    fails(
        attempt(cbor(&C::M(vec![
            (C::U(1), C::U(2)),
            (C::U(3), C::I(-7)),
            (C::I(-1), C::U(2)),
            (C::I(-2), C::B(x)),
            (C::I(-3), C::B(y)),
        ]))),
        "curve does not match",
    );
    fails(
        attempt(cbor(&C::M(vec![
            (C::U(1), C::U(2)),
            (C::U(3), C::I(-65535)),
            (C::I(-1), C::U(1)),
        ]))),
        "unsupported COSE key type 2 with algorithm -65535",
    );
    fails(
        attempt(cbor(&C::M(vec![
            (C::U(1), C::U(1)),
            (C::U(3), C::I(-8)),
            (C::I(-1), C::U(6)),
            (C::I(-2), C::B(&[1; 31])),
        ]))),
        "32-byte x",
    );
    fails(
        attempt(cbor(&C::M(vec![
            (C::U(1), C::U(3)),
            (C::U(3), C::I(-257)),
            (C::I(-1), C::B(&[0xff; 128])),
            (C::I(-2), C::B(&[1, 0, 1])),
        ]))),
        "2048 to 8192 bits",
    );
    fails(attempt(cbor(&C::M(vec![(C::U(3), C::I(-7))]))), "no kty");
    fails(attempt(vec![0x81, 0x00]), "COSE key map");
}

impl TestKey {
    fn public_key_point(&self) -> Vec<u8> {
        match self {
            Self::Ecdsa(key, ..) => key.public_key().as_ref().to_vec(),
            _ => unreachable!(),
        }
    }
}

#[test]
fn sign_in_rejects_forgeries_other_credentials_and_regressed_counters() {
    let key = TestKey::ecdsa(&ECDSA_P256_SHA256_ASN1_SIGNING, -7, 1, 32);
    let id = [5u8; 32];
    let credential = register(&registration_response(&key, &id), &registration_options())
        .unwrap()
        .credential;
    let options = authentication_options(&credential);

    let impostor = TestKey::ecdsa(&ECDSA_P256_SHA256_ASN1_SIGNING, -7, 1, 32);
    fails(
        authenticate(&signed_in(&impostor, &id, FLAGS, 0), &options),
        "signature is invalid",
    );
    let mut tampered = signed_in(&key, &id, FLAGS, 0);
    let good_data = authenticator_bytes(RP_ID, FLAGS, 0, None);
    tampered["response"]["authenticatorData"] =
        json!(b64(&authenticator_bytes(RP_ID, FLAGS, 7, None)));
    fails(authenticate(&tampered, &options), "signature is invalid");
    fails(
        authenticate(&signed_in(&key, &[6u8; 32], FLAGS, 0), &options),
        "different credential",
    );
    fails(
        authenticate(
            &assertion(
                &key,
                &id,
                &good_data,
                &client("webauthn.create", CHALLENGE, ORIGIN),
                None,
            ),
            &options,
        ),
        "not a webauthn.get ceremony",
    );
    fails(
        authenticate(
            &assertion(
                &key,
                &id,
                &good_data,
                &client("webauthn.get", CHALLENGE, "https://other.example"),
                None,
            ),
            &options,
        ),
        "origin is not allowed",
    );
    let attested = authenticator_bytes(RP_ID, FLAGS | ATTESTED, 0, Some((&id, &key.cose())));
    fails(
        authenticate(
            &assertion(
                &key,
                &id,
                &attested,
                &client("webauthn.get", CHALLENGE, ORIGIN),
                None,
            ),
            &options,
        ),
        "no attested credential data",
    );
    fails(
        authenticate(
            &assertion(
                &key,
                &id,
                &good_data,
                &client("webauthn.get", CHALLENGE, ORIGIN),
                Some(b"user-2"),
            ),
            &options,
        ),
        "different user",
    );
    let anonymous = authenticate(
        &assertion(
            &key,
            &id,
            &good_data,
            &client("webauthn.get", CHALLENGE, ORIGIN),
            None,
        ),
        &options,
    )
    .unwrap();
    assert_eq!(anonymous.user_handle, None);

    // Counters must grow once either side has used one; forgeries never report as clones.
    let counted = |stored: u32| AuthenticationOptions {
        credential: StoredCredential {
            sign_count: stored,
            ..authentication_options(&credential).credential
        },
        ..authentication_options(&credential)
    };
    assert_eq!(
        authenticate(&signed_in(&key, &id, FLAGS, 5), &counted(0))
            .unwrap()
            .sign_count,
        5
    );
    assert_eq!(
        authenticate(&signed_in(&key, &id, FLAGS, 6), &counted(5))
            .unwrap()
            .sign_count,
        6
    );
    for (counter, stored) in [(5, 5), (4, 5), (0, 5)] {
        let error = format!(
            "{:#}",
            authenticate(&signed_in(&key, &id, FLAGS, counter), &counted(stored)).unwrap_err()
        );
        assert!(error.starts_with("WEBAUTHN_COUNTER: "), "{error}");
    }
    fails(
        authenticate(&signed_in(&impostor, &id, FLAGS, 1), &counted(5)),
        "signature is invalid",
    );
}
