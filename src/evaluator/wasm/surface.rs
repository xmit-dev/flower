//! Reject capability or ABI drift before compiling a newly vendored guest.
use anyhow::{ensure, Context, Result};
use std::collections::BTreeMap;
use wasmparser::{ExternalKind, Parser, Payload, TypeRef, ValType};

pub(super) fn validate(wasm: &[u8]) -> Result<()> {
    let mut types = Vec::new();
    let mut functions = Vec::new();
    let mut imports = std::collections::BTreeSet::new();
    let mut memories = 0;
    let mut exports = BTreeMap::new();
    for payload in Parser::new(0).parse_all(wasm) {
        match payload? {
            Payload::TypeSection(section) => {
                for ty in section.into_iter_err_on_gc_types() {
                    types.push(ty?);
                }
            }
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    let import = import?;
                    ensure!(
                        import.module == "flower"
                            && matches!(import.name, "host_call" | "crypto_call")
                            && imports.insert(import.name),
                        "unexpected guest capability import"
                    );
                    let TypeRef::Func(index) = import.ty else {
                        anyhow::bail!("guest host callback must be a function");
                    };
                    let ty = types.get(index as usize).context("host callback type")?;
                    let valid = if import.name == "host_call" {
                        ty.params() == [ValType::I32; 3] && ty.results() == [ValType::I64]
                    } else {
                        ty.params() == [ValType::I32; 5] && ty.results() == [ValType::I32]
                    };
                    ensure!(valid, "guest host callback signature changed");
                    functions.push(index);
                }
            }
            Payload::FunctionSection(section) => {
                for ty in section {
                    functions.push(ty?);
                }
            }
            Payload::MemorySection(section) => {
                for memory in section {
                    let memory = memory?;
                    ensure!(
                        !memory.memory64
                            && !memory.shared
                            && memory.initial
                                <= super::super::config::settings()?.guest_memory_bytes as u64
                                    / 65536,
                        "unsupported guest memory layout"
                    );
                    memories += 1;
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export?;
                    ensure!(
                        exports
                            .insert(export.name, (export.kind, export.index))
                            .is_none(),
                        "duplicate guest export"
                    );
                }
            }
            Payload::StartSection { .. } => anyhow::bail!("guest must initialize explicitly"),
            _ => {}
        }
    }
    ensure!(
        imports.len() == 2 && memories == 1,
        "guest must have database and crypto callbacks and one private memory"
    );
    for (name, parameters, results) in [
        ("_initialize", 0, &[][..]),
        ("flower_init", 0, &[ValType::I32][..]),
        ("flower_alloc", 1, &[ValType::I32][..]),
        ("flower_free", 1, &[][..]),
        ("flower_eval", 2, &[ValType::I64][..]),
        ("flower_compile", 2, &[ValType::I64][..]),
        ("flower_load", 2, &[ValType::I64][..]),
        ("flower_invoke", 5, &[ValType::I64][..]),
        ("flower_manifest", 0, &[ValType::I64][..]),
        ("flower_snapshot_prepare", 0, &[ValType::I64][..]),
    ] {
        let (kind, index) = exports
            .remove(name)
            .with_context(|| format!("missing guest export {name}"))?;
        ensure!(
            kind == ExternalKind::Func,
            "guest export {name} must be a function"
        );
        let index = *functions
            .get(index as usize)
            .context("exported function index")?;
        let ty = types
            .get(index as usize)
            .context("exported function type")?;
        ensure!(
            ty.params().len() == parameters
                && ty.params().iter().all(|ty| *ty == ValType::I32)
                && ty.results() == results,
            "guest export {name} signature changed"
        );
    }
    ensure!(
        exports.remove("memory") == Some((ExternalKind::Memory, 0)),
        "guest must export its private memory"
    );
    ensure!(
        exports
            .remove("__stack_pointer")
            .is_some_and(|(kind, _)| kind == ExternalKind::Global),
        "guest must export its shadow stack pointer"
    );
    ensure!(exports.is_empty(), "unexpected guest export surface");
    // shadow_stack::instrument subsequently verifies the stack's mutable i32
    // global and exact 1 MiB top before inserting uncatchable interval guards.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_guest_has_only_the_declared_capability_and_abi() {
        let wasm = include_bytes!("../../../vendor/quickjs-ng/quickjs.wasm");
        validate(wasm).unwrap();
        let mut changed = wasm.to_vec();
        let position = changed
            .windows(9)
            .position(|part| part == b"host_call")
            .unwrap();
        changed[position..position + 9].copy_from_slice(b"host_cAll");
        assert!(validate(&changed).is_err());
        let mut changed = wasm.to_vec();
        let position = changed
            .windows(11)
            .position(|part| part == b"crypto_call")
            .unwrap();
        changed[position..position + 11].copy_from_slice(b"crypto_cAll");
        assert!(validate(&changed).is_err());
        let mut changed = wasm.to_vec();
        let position = changed
            .windows(13)
            .position(|part| part == b"flower_invoke")
            .unwrap();
        changed[position..position + 13].copy_from_slice(b"flower_invoKe");
        assert!(validate(&changed).is_err());
    }
}
