//! Building app bundles: runs Node on the SDK's own `buildBundle` (`sdk/bundle.ts`), so esbuild's
//! flags, version and output — and so the hash and the `deploy:{partition}:{hash}` request IDs —
//! stay identical to the TypeScript deployer.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::FlowerError;

/// A JavaScript bundle: `hash` is the lowercase SHA-256 hex of `javascript`'s UTF-8 bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JavaScriptBundle {
    pub hash: String,
    pub javascript: String,
}

impl JavaScriptBundle {
    /// Wrap JavaScript, computing its hash.
    pub fn new(javascript: impl Into<String>) -> Self {
        let javascript = javascript.into();
        JavaScriptBundle {
            hash: sha256_hex(javascript.as_bytes()),
            javascript,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.javascript.as_bytes()
    }
}

/// What `/admin/deploy` accepts: `{hash, javascript}` or `{hash, wasm}` (base64 module bytes).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bundle {
    JavaScript(JavaScriptBundle),
    Wasm { hash: String, wasm: String },
}

impl From<JavaScriptBundle> for Bundle {
    fn from(bundle: JavaScriptBundle) -> Self {
        Bundle::JavaScript(bundle)
    }
}

impl Bundle {
    pub fn hash(&self) -> &str {
        match self {
            Bundle::JavaScript(bundle) => &bundle.hash,
            Bundle::Wasm { hash, .. } => hash,
        }
    }
}

/// Module initialization mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Initialization {
    /// Once at deploy; callbacks start from a copy-on-write snapshot (default; the bundle starts
    /// with `/* flower:static-init */`).
    #[default]
    Static,
    /// Module code reruns in each callback.
    PerInvocation,
}

/// Options of [`build_bundle`].
#[derive(Clone, Debug, Default)]
pub struct BundleOptions {
    pub initialization: Initialization,
    /// The Node binary; default `node` from `PATH`.
    pub node: Option<PathBuf>,
    /// The SDK directory holding `bundle.ts`; default `$FLOWER_SDK_DIR`, else this crate's
    /// `../../sdk` (the Flower checkout it lives in). `esbuild` resolves from there.
    pub sdk_dir: Option<PathBuf>,
}

/// The SDK directory [`build_bundle`] uses when none is given.
pub fn default_sdk_dir() -> PathBuf {
    match std::env::var_os("FLOWER_SDK_DIR").filter(|dir| !dir.is_empty()) {
        Some(dir) => PathBuf::from(dir),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk"),
    }
}

const SCRIPT: &str = r#"
const [module, entry, initialization] = process.argv.slice(1);
const { buildBundle } = await import(module);
const bundle = await buildBundle(entry, initialization ? { initialization } : {});
process.stdout.write(JSON.stringify(bundle));
"#;

/// Compile a default-exported `define(...)` module like `buildBundle(entry, options)`.
pub async fn build_bundle(
    entry: &Path,
    options: BundleOptions,
) -> Result<JavaScriptBundle, FlowerError> {
    let sdk = options.sdk_dir.unwrap_or_else(default_sdk_dir);
    let module = std::fs::canonicalize(sdk.join("bundle.ts")).map_err(|error| {
        FlowerError::invalid(format!(
            "Cannot find the SDK's bundle.ts in {}: {error}",
            sdk.display()
        ))
    })?;
    let entry = std::path::absolute(entry).map_err(|error| {
        FlowerError::invalid(format!("Invalid entry {}: {error}", entry.display()))
    })?;
    let initialization = match options.initialization {
        Initialization::Static => "",
        Initialization::PerInvocation => "per-invocation",
    };
    let output =
        tokio::process::Command::new(options.node.unwrap_or_else(|| PathBuf::from("node")))
            .arg("--no-warnings")
            .arg("--input-type=module")
            .arg("--eval")
            .arg(SCRIPT)
            .arg(file_url(&module))
            .arg(&entry)
            .arg(initialization)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|error| FlowerError::invalid(format!("Cannot run node: {error}")))?;
    if !output.status.success() {
        return Err(FlowerError::new(
            format!(
                "Building {} failed ({}): {}",
                entry.display(),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
            0,
            "BUNDLE_FAILED",
        ));
    }
    let bundle: JavaScriptBundle = serde_json::from_slice(&output.stdout).map_err(|error| {
        FlowerError::decode(format!("Unexpected buildBundle output: {error}"), 0)
    })?;
    if bundle.hash != sha256_hex(bundle.javascript.as_bytes()) {
        return Err(FlowerError::decode(
            "Bundle hash does not match its JavaScript",
            0,
        ));
    }
    Ok(bundle)
}

fn file_url(path: &Path) -> String {
    let mut url = String::from("file://");
    for byte in path.to_string_lossy().bytes() {
        if byte.is_ascii_alphanumeric() || b"/-_.~".contains(&byte) {
            url.push(byte as char);
        } else {
            url.push_str(&format!("%{byte:02X}"));
        }
    }
    url
}

/// Lowercase SHA-256 hex.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
