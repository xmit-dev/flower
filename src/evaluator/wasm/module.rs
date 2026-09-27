//! Deployed WebAssembly guests (GUEST_ABI.md). Check the declared surface
//! before compiling anything, then expose every mutable global so a pristine
//! image can restore them; reset_surface validates the final image.
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeMap;
use wasm_encoder::{
    reencode::{self, Reencode},
    ExportKind, ExportSection, Module,
};
use wasmparser::{ExternalKind, FuncType, Parser, Payload, TypeRef, ValType};

/// Exports the host adds for mutable globals the module keeps private.
const GLOBAL_EXPORT: &str = "flower:global:";

pub(super) fn validate(wasm: &[u8], memory_limit: usize) -> Result<()> {
    let mut types: Vec<FuncType> = Vec::new();
    let mut functions = Vec::new();
    let mut imports = BTreeMap::new();
    let mut memories = 0;
    let mut exports = BTreeMap::new();
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.context("invalid WebAssembly module")? {
            Payload::Version { encoding, .. } => {
                ensure!(
                    encoding == wasmparser::Encoding::Module,
                    "guest must be a core WebAssembly module"
                );
            }
            Payload::TypeSection(section) => {
                for ty in section.into_iter_err_on_gc_types() {
                    types.push(ty?);
                }
            }
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    let import = import?;
                    let TypeRef::Func(index) = import.ty else {
                        bail!("guest may import only functions");
                    };
                    ensure!(
                        import.module == "flower"
                            && matches!(import.name, "host_call" | "crypto_call")
                            && imports.insert(import.name, index).is_none(),
                        "guest may import only flower.host_call and flower.crypto_call"
                    );
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
                            && memory.initial <= memory_limit as u64 / 65536,
                        "guest memory must be private wasm32 memory within FLOWER_GUEST_MEMORY_BYTES"
                    );
                    memories += 1;
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export?;
                    ensure!(
                        !export.name.starts_with(GLOBAL_EXPORT)
                            && !export.name.starts_with("__wizer"),
                        "guest export {} uses a reserved name",
                        export.name
                    );
                    exports.insert(export.name, (export.kind, export.index));
                }
            }
            Payload::StartSection { .. } => bail!("guest must not have a start function"),
            _ => {}
        }
    }
    ensure!(memories == 1, "guest must define exactly one memory");
    let signature = |index: u32| types.get(index as usize).context("function type");
    for (name, index) in &imports {
        let ty = signature(*index)?;
        let expected: (&[ValType], &[ValType]) = if *name == "host_call" {
            (&[ValType::I32; 3], &[ValType::I64])
        } else {
            (&[ValType::I32; 5], &[ValType::I32])
        };
        ensure!(
            (ty.params(), ty.results()) == expected,
            "guest import flower.{name} has the wrong signature"
        );
    }
    ensure!(
        exports.get("memory") == Some(&(ExternalKind::Memory, 0)),
        "guest must export its memory as memory"
    );
    for (name, parameters, results, required) in [
        ("flower_alloc", 1, &[ValType::I32][..], true),
        ("flower_invoke", 5, &[ValType::I64][..], true),
        ("flower_manifest", 0, &[ValType::I64][..], true),
        ("flower_init", 0, &[ValType::I32][..], false),
    ] {
        let Some((kind, index)) = exports.get(name) else {
            ensure!(!required, "guest must export {name}");
            continue;
        };
        ensure!(
            *kind == ExternalKind::Func,
            "guest export {name} must be a function"
        );
        let ty = signature(
            *functions
                .get(*index as usize)
                .context("exported function")?,
        )?;
        ensure!(
            ty.params() == vec![ValType::I32; parameters] && ty.results() == results,
            "guest export {name} has the wrong signature"
        );
    }
    Ok(())
}

/// Export every mutable global that the module does not already export.
pub(super) fn export_globals(wasm: &[u8]) -> Result<Vec<u8>> {
    let mut mutable = Vec::new();
    let mut exported = std::collections::BTreeSet::new();
    for payload in Parser::new(0).parse_all(wasm) {
        match payload? {
            Payload::GlobalSection(section) => {
                for global in section {
                    mutable.push(global?.ty.mutable);
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export?;
                    if export.kind == ExternalKind::Global {
                        exported.insert(export.index);
                    }
                }
            }
            _ => {}
        }
    }
    let hidden: Vec<u32> = (0..mutable.len() as u32)
        .filter(|index| mutable[*index as usize] && !exported.contains(index))
        .collect();
    if hidden.is_empty() {
        return Ok(wasm.to_vec());
    }
    let mut module = Module::new();
    let mut exports_written = false;
    let add = |section: &mut ExportSection| {
        for index in &hidden {
            section.export(
                &format!("{GLOBAL_EXPORT}{index}"),
                ExportKind::Global,
                *index,
            );
        }
    };
    let mut reencoder = reencode::RoundtripReencoder;
    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload?;
        match &payload {
            Payload::ExportSection(section) => {
                let mut exports = ExportSection::new();
                for export in section.clone() {
                    reencoder.parse_export(&mut exports, export?)?;
                }
                add(&mut exports);
                module.section(&exports);
                exports_written = true;
                continue;
            }
            // Exports precede the start, element, data count, code and data
            // sections; a module without exports gets its section here.
            Payload::StartSection { .. }
            | Payload::ElementSection(_)
            | Payload::DataCountSection { .. }
            | Payload::CodeSectionStart { .. }
            | Payload::DataSection(_)
                if !exports_written =>
            {
                let mut exports = ExportSection::new();
                add(&mut exports);
                module.section(&exports);
                exports_written = true;
            }
            _ => {}
        }
        if let Some((id, range)) = payload.as_section() {
            module.section(&wasm_encoder::RawSection {
                id,
                data: &wasm[range.start as usize..range.end as usize],
            });
        }
    }
    if !exports_written {
        let mut exports = ExportSection::new();
        add(&mut exports);
        module.section(&exports);
    }
    Ok(module.finish())
}
