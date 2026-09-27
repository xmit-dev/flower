//! Process-wide operator budgets. Parse once before constructing an engine;
//! every request and every disposable guest uses the same validated settings.
use anyhow::{ensure, Context, Result};
use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct Settings {
    pub evaluation_timeout: Duration,
    pub bundle_max_bytes: usize,
    pub result_max_bytes: usize,
    pub guest_memory_bytes: usize,
    pub rust_memory_bytes: usize,
    pub index_memory_bytes: usize,
    pub wasm_pool_slots: u32,
    pub wasm_recycle_bytes: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            evaluation_timeout: Duration::from_secs(10),
            bundle_max_bytes: 2 * 1024 * 1024,
            result_max_bytes: 16 * 1024 * 1024,
            guest_memory_bytes: 128 * 1024 * 1024,
            rust_memory_bytes: 128 * 1024 * 1024,
            index_memory_bytes: 16 * 1024 * 1024,
            wasm_pool_slots: 256,
            wasm_recycle_bytes: 96 * 1024 * 1024,
        }
    }
}

const KEYS: [&str; 8] = [
    "FLOWER_EVALUATION_TIMEOUT_MS",
    "FLOWER_BUNDLE_MAX_BYTES",
    "FLOWER_RESULT_MAX_BYTES",
    "FLOWER_GUEST_MEMORY_BYTES",
    "FLOWER_RUST_MEMORY_BYTES",
    "FLOWER_INDEX_MEMORY_BYTES",
    "FLOWER_WASM_POOL_SLOTS",
    "FLOWER_WASM_RECYCLE_BYTES",
];

impl Settings {
    fn read(mut get: impl FnMut(&str) -> Result<Option<String>>) -> Result<Self> {
        let defaults = Self::default();
        let mut number = |key: &str, default: u64, allow_zero: bool| -> Result<u64> {
            let Some(value) = get(key)? else {
                return Ok(default);
            };
            ensure!(
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
                "{key} must be a {} decimal integer",
                if allow_zero {
                    "non-negative"
                } else {
                    "positive"
                }
            );
            let value: u64 = value
                .parse()
                .with_context(|| format!("{key} is too large"))?;
            ensure!(allow_zero || value > 0, "{key} must be positive");
            Ok(value)
        };
        let evaluation_timeout = Duration::from_millis(number(
            KEYS[0],
            defaults.evaluation_timeout.as_millis() as u64,
            false,
        )?);
        ensure!(
            Instant::now().checked_add(evaluation_timeout).is_some(),
            "FLOWER_EVALUATION_TIMEOUT_MS cannot be represented by the monotonic clock"
        );
        let mut bytes = |key: &str, default: usize| -> Result<usize> {
            let value = number(key, default as u64, false)?;
            ensure!(
                value <= isize::MAX as u64,
                "{key} exceeds host allocation representation"
            );
            Ok(value as usize)
        };
        let bundle_max_bytes = bytes(KEYS[1], defaults.bundle_max_bytes)?;
        let result_max_bytes = bytes(KEYS[2], defaults.result_max_bytes)?;
        let guest_memory_bytes = bytes(KEYS[3], defaults.guest_memory_bytes)?;
        let rust_memory_bytes = bytes(KEYS[4], defaults.rust_memory_bytes)?;
        let index_memory_bytes = bytes(KEYS[5], defaults.index_memory_bytes)?;
        let wasm_pool_slots =
            u32::try_from(number(KEYS[6], defaults.wasm_pool_slots.into(), false)?)
                .context("FLOWER_WASM_POOL_SLOTS exceeds Wasmtime's u32 slot representation")?;
        let wasm_recycle_bytes = number(KEYS[7], defaults.wasm_recycle_bytes as u64, true)?;
        ensure!(
            wasm_recycle_bytes <= isize::MAX as u64,
            "FLOWER_WASM_RECYCLE_BYTES exceeds host allocation representation"
        );
        let wasm_recycle_bytes = wasm_recycle_bytes as usize;
        // Buffers use a signed length in the guest ABI and need a trailing NUL.
        for (key, value) in [(KEYS[1], bundle_max_bytes), (KEYS[2], result_max_bytes)] {
            ensure!(
                value < i32::MAX as usize,
                "{key} exceeds the guest's signed 32-bit buffer representation"
            );
        }
        ensure!(
            guest_memory_bytes as u64 <= 1_u64 << 32,
            "FLOWER_GUEST_MEMORY_BYTES exceeds wasm32's 4 GiB address space"
        );
        ensure!(
            bundle_max_bytes <= guest_memory_bytes,
            "FLOWER_BUNDLE_MAX_BYTES must not exceed FLOWER_GUEST_MEMORY_BYTES"
        );
        ensure!(
            result_max_bytes <= guest_memory_bytes && result_max_bytes <= rust_memory_bytes,
            "FLOWER_RESULT_MAX_BYTES must not exceed guest or Rust memory budgets"
        );
        Ok(Self {
            evaluation_timeout,
            bundle_max_bytes,
            result_max_bytes,
            guest_memory_bytes,
            rust_memory_bytes,
            index_memory_bytes,
            wasm_pool_slots,
            wasm_recycle_bytes,
        })
    }

    /// Bytecode is an internal representation, bounded by guest memory and its
    /// ABI rather than an independent source-size multiplier or fixed ceiling.
    pub(crate) fn bytecode_max_bytes(&self) -> usize {
        self.guest_memory_bytes.min(i32::MAX as usize - 1)
    }

    pub(crate) fn pool_memory_bytes(&self) -> usize {
        // Wasm grows in pages. The resource limiter still enforces the exact
        // operator budget; reservation alone is rounded up to a complete page.
        self.guest_memory_bytes.div_ceil(65536) * 65536
    }
}

pub fn settings() -> Result<&'static Settings> {
    static SETTINGS: OnceLock<std::result::Result<Settings, String>> = OnceLock::new();
    SETTINGS
        .get_or_init(|| {
            Settings::read(|key| match std::env::var(key) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(error) => Err(error).with_context(|| format!("{key} is not valid Unicode")),
            })
            .map_err(|error| format!("{error:#}"))
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("invalid evaluator settings: {error}"))
}

#[cfg(test)]
mod tests;
