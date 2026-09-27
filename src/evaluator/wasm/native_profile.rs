//! Optional cold-path address maps for external profilers, including macOS sample.
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    io::Write,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use wasmtime::Module;

fn mapping(module: &Module, guest_hash: &str) -> Value {
    json!({
        "guest_sha256": guest_hash,
        "pid": std::process::id(),
        "text_base": module.text().as_ptr() as usize,
        "text_length": module.text().len(),
        "functions": module.functions().map(|function| json!({
            "index": function.index.as_u32(),
            "offset": function.offset,
            "length": function.len,
        })).collect::<Vec<_>>(),
    })
}

pub(super) fn record(module: &Module, guest_hash: &str) -> Result<()> {
    let Some(mut prefix) = std::env::var_os("FLOWER_PROFILE_WASM_MAP").filter(|v| !v.is_empty())
    else {
        return Ok(());
    };
    prefix.push(format!(".{}.jsonl", std::process::id()));
    // Compilation may race across bundles. Emit complete records; profiling
    // never changes the artifact, callback path, or lifetime of a compiled image.
    static WRITE: Mutex<()> = Mutex::new(());
    let _guard = WRITE
        .lock()
        .map_err(|_| anyhow::anyhow!("Wasm profile map lock poisoned"))?;
    let mut map = mapping(module, guest_hash);
    if std::env::var_os("FLOWER_PROFILE_WASM_CODE").is_some_and(|v| v == "1") {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let mut code_path = prefix.clone();
        code_path.push(format!(".{}.bin", SEQUENCE.fetch_add(1, Ordering::Relaxed)));
        std::fs::write(&code_path, module.text()).context("cannot write Wasm profile code")?;
        map["code_path"] = json!(std::path::PathBuf::from(code_path));
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&prefix)
        .context("cannot open FLOWER_PROFILE_WASM_MAP output")?;
    serde_json::to_writer(&mut file, &map)?;
    file.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_map_describes_real_executable_ranges_and_function_indices() {
        let prepared = super::super::prepare("", super::super::tests::limits()).unwrap();
        let module = prepared.pre.module();
        let map = mapping(module, "test-digest");
        assert_eq!(map["guest_sha256"], "test-digest");
        assert_eq!(map["text_base"], module.text().as_ptr() as usize);
        let functions = map["functions"].as_array().unwrap();
        assert_eq!(functions.len(), module.functions().len());
        assert!(functions.len() > 1000);
        for (actual, function) in functions.iter().zip(module.functions()) {
            assert_eq!(actual["index"], function.index.as_u32());
            assert_eq!(actual["offset"], function.offset);
            assert_eq!(actual["length"], function.len);
            assert!(function.len > 0);
            assert!(function.offset + function.len <= module.text().len());
        }
    }
}
