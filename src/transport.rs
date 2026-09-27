//! Native transport policy, loaded once before accepting requests. All internal
//! clients use the same TLS roots and scheme; there is no downgrade or insecure
//! certificate/hostname-verification option.
use anyhow::{ensure, Context};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio_rustls::rustls::{
    self,
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
};
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct Tls {
    pub server: Arc<rustls::ServerConfig>,
    roots: Vec<reqwest::Certificate>,
    pub handshake_timeout: Duration,
}

impl Tls {
    pub fn from_files(
        certificate: &Path,
        key: &Path,
        ca: &Path,
        handshake_timeout: Duration,
    ) -> anyhow::Result<Self> {
        ensure!(
            !handshake_timeout.is_zero()
                && std::time::Instant::now()
                    .checked_add(handshake_timeout)
                    .is_some(),
            "TLS handshake timeout must be positive and fit the timer range"
        );
        let certificates = std::fs::read(certificate).context("read FLOWER_TLS_CERT_FILE")?;
        let certificates: Vec<_> = CertificateDer::pem_slice_iter(&certificates)
            .collect::<Result<_, _>>()
            .context("decode TLS certificate chain")?;
        ensure!(!certificates.is_empty(), "TLS certificate chain is empty");
        let key = Zeroizing::new(std::fs::read(key).context("read FLOWER_TLS_KEY_FILE")?);
        let key = PrivateKeyDer::from_pem_slice(&key).context("decode TLS private key")?;
        let roots = std::fs::read(ca).context("read FLOWER_TLS_CA_FILE")?;
        let roots =
            reqwest::Certificate::from_pem_bundle(&roots).context("decode TLS CA bundle")?;
        ensure!(!roots.is_empty(), "TLS CA bundle is empty");
        let mut server = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .context("TLS certificate and private key do not match or are invalid")?;
        server.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            server: Arc::new(server),
            roots,
            handshake_timeout,
        })
    }

    pub fn client_builder(&self) -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .tls_certs_only(self.roots.clone())
            .https_only(true)
    }
}

fn load(read: impl Fn(&str) -> Option<std::ffi::OsString>) -> anyhow::Result<Option<Tls>> {
    let paths = [
        "FLOWER_TLS_CERT_FILE",
        "FLOWER_TLS_KEY_FILE",
        "FLOWER_TLS_CA_FILE",
    ]
    .map(|name| read(name).map(PathBuf::from));
    let millis = match read("FLOWER_TLS_HANDSHAKE_TIMEOUT_MS") {
        Some(value) => value
            .to_str()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                anyhow::anyhow!("FLOWER_TLS_HANDSHAKE_TIMEOUT_MS must be a positive integer")
            })?,
        None => 10_000,
    };
    ensure!(
        std::time::Instant::now()
            .checked_add(Duration::from_millis(millis))
            .is_some(),
        "TLS handshake timeout exceeds timer range"
    );
    match paths {
        [None, None, None] => Ok(None),
        [Some(certificate), Some(key), Some(ca)] => {
            Tls::from_files(&certificate, &key, &ca, Duration::from_millis(millis)).map(Some)
        }
        _ => anyhow::bail!(
            "FLOWER_TLS_CERT_FILE, FLOWER_TLS_KEY_FILE and FLOWER_TLS_CA_FILE must be configured together"
        ),
    }
}

pub fn tls() -> anyhow::Result<Option<&'static Tls>> {
    static TLS: OnceLock<Result<Option<Tls>, String>> = OnceLock::new();
    TLS.get_or_init(|| load(|name| std::env::var_os(name)).map_err(|error| error.to_string()))
        .as_ref()
        .map(|tls| tls.as_ref())
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

pub fn validate_configuration() -> anyhow::Result<()> {
    // Build once here too: malformed custom roots fail before storage opens.
    client_builder()?.build()?;
    Ok(())
}

pub fn client_builder() -> anyhow::Result<reqwest::ClientBuilder> {
    Ok(tls()?
        .map_or_else(reqwest::Client::builder, Tls::client_builder)
        .http2_prior_knowledge())
}

pub(crate) fn peer_url(address: &str, path: &str) -> String {
    let scheme = if tls().expect("validated transport configuration").is_some() {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{address}{path}")
}

pub(crate) fn peer_token(operator: &str) -> anyhow::Result<String> {
    match std::env::var("FLOWER_PEER_TOKEN") {
        Ok(token) => {
            ensure!(
                !token.trim().is_empty(),
                "FLOWER_PEER_TOKEN must be nonempty"
            );
            Ok(token)
        }
        Err(std::env::VarError::NotPresent) => Ok(operator.into()),
        Err(error) => Err(error).context("FLOWER_PEER_TOKEN must be valid Unicode"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tls_requires_complete_explicit_configuration_and_valid_deadline() {
        assert!(load(|_| None).unwrap().is_none());
        for name in [
            "FLOWER_TLS_CERT_FILE",
            "FLOWER_TLS_KEY_FILE",
            "FLOWER_TLS_CA_FILE",
        ] {
            assert!(load(|option| (option == name).then(|| "missing".into())).is_err());
        }
        for value in ["0", "-1", "bad", "1.5"] {
            assert!(
                load(|name| (name == "FLOWER_TLS_HANDSHAKE_TIMEOUT_MS").then(|| value.into()))
                    .is_err()
            );
        }
    }
}
