//! Prove that restoring linear memory and exported numeric globals restores all
//! guest-observable mutable state. Validate each final Wizer image, not a digest
//! or assumptions about the current C compiler. Growing or trapping instances
//! must still be discarded by the caller; Wasm memories cannot be shrunk.
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeMap;
use wasmparser::{Encoding, ExternalKind, Operator, Parser, Payload, RefType, TypeRef, ValType};

/// Return one export name per mutable global, in global-index order. The caller
/// captures their pristine values and restores every one before reusing a Store.
pub(super) fn validate(wasm: &[u8]) -> Result<Vec<String>> {
    let mut globals = Vec::new();
    let mut global_exports = BTreeMap::new();
    let mut memories = 0;
    let mut memory_exported = false;
    let mut tables = 0;
    for payload in Parser::new(0).parse_all(wasm) {
        match payload? {
            Payload::Version { encoding, .. } => {
                ensure!(encoding == Encoding::Module, "reset requires a core module");
            }
            Payload::TypeSection(section) => {
                for ty in section.into_iter_err_on_gc_types() {
                    let ty = ty?;
                    ensure!(
                        ty.params().iter().chain(ty.results()).all(numeric),
                        "reset forbids reference-valued function signatures"
                    );
                }
            }
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    ensure!(
                        matches!(import?.ty, TypeRef::Func(_)),
                        "reset forbids imported guest state"
                    );
                }
            }
            Payload::TableSection(section) => {
                for table in section {
                    let table = table?;
                    ensure!(
                        table.ty.element_type == RefType::FUNCREF
                            && !table.ty.table64
                            && !table.ty.shared,
                        "reset requires a private wasm32 function table"
                    );
                    tables += 1;
                }
            }
            Payload::MemorySection(section) => {
                for memory in section {
                    let memory = memory?;
                    ensure!(
                        !memory.memory64 && !memory.shared && memory.page_size_log2.is_none(),
                        "reset requires a private wasm32 memory with standard pages"
                    );
                    memories += 1;
                }
            }
            Payload::GlobalSection(section) => {
                for global in section {
                    let global = global?;
                    ensure!(
                        numeric(&global.ty.content_type) && !global.ty.shared,
                        "reset forbids reference-valued or shared globals"
                    );
                    globals.push(global.ty.mutable);
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export?;
                    if export.kind == ExternalKind::Global {
                        global_exports
                            .entry(export.index)
                            .or_insert_with(|| export.name.to_owned());
                    } else if export.kind == ExternalKind::Memory
                        && export.index == 0
                        && export.name == "memory"
                    {
                        memory_exported = true;
                    }
                }
            }
            Payload::StartSection { .. } => bail!("reset forbids a start function"),
            Payload::TagSection(_) => bail!("reset forbids exception state"),
            Payload::CodeSectionEntry(body) => {
                for local in body.get_locals_reader()? {
                    ensure!(numeric(&local?.1), "reset forbids reference-valued locals");
                }
                for operation in body.get_operators_reader()? {
                    let operation = operation?;
                    // Active segments have already been consumed when the
                    // pristine image is captured. Wizer also leaves empty
                    // passive segments behind. Neither can change thereafter
                    // because every segment-use/drop operation is forbidden.
                    ensure!(
                        !matches!(
                            operation,
                            Operator::TableSet { .. }
                                | Operator::TableGrow { .. }
                                | Operator::TableFill { .. }
                                | Operator::TableCopy { .. }
                                | Operator::TableInit { .. }
                                | Operator::MemoryInit { .. }
                                | Operator::DataDrop { .. }
                                | Operator::ElemDrop { .. }
                        ),
                        "reset forbids unresettable instruction {operation:?}"
                    );
                }
            }
            Payload::FunctionSection(_)
            | Payload::ElementSection(_)
            | Payload::DataCountSection { .. }
            | Payload::DataSection(_)
            | Payload::CodeSectionStart { .. }
            | Payload::CustomSection(_)
            | Payload::End(_) => {}
            _ => bail!("reset encountered an unsupported module section"),
        }
    }
    ensure!(
        memories == 1 && memory_exported && tables <= 1,
        "reset requires one exported memory and at most one function table"
    );
    let mut names = Vec::new();
    for (index, mutable) in globals.into_iter().enumerate() {
        if mutable {
            names.push(
                global_exports.remove(&(index as u32)).with_context(|| {
                    format!("reset cannot restore hidden mutable global {index}")
                })?,
            );
        }
    }
    // Opt in to proposals with no additional persistent state. In particular,
    // default-on future parser features must not silently admit GC, exceptions,
    // continuations, threads, memory control, or reference-valued state. Bulk
    // copying/filling memory and memory.grow remain legal; the caller restores
    // every byte and discards instances whose size changed.
    let features = wasmparser::WasmFeatures::WASM1
        | wasmparser::WasmFeatures::SIMD
        | wasmparser::WasmFeatures::SIGN_EXTENSION
        | wasmparser::WasmFeatures::SATURATING_FLOAT_TO_INT
        | wasmparser::WasmFeatures::MULTI_VALUE
        | wasmparser::WasmFeatures::BULK_MEMORY
        | wasmparser::WasmFeatures::CALL_INDIRECT_OVERLONG;
    wasmparser::Validator::new_with_features(features)
        .validate_all(wasm)
        .context("module uses unsupported reset features")?;
    Ok(names)
}

fn numeric(ty: &ValType) -> bool {
    matches!(
        ty,
        ValType::I32 | ValType::I64 | ValType::F32 | ValType::F64 | ValType::V128
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_encoder::{
        CodeSection, ConstExpr, DataCountSection, DataSection, ElementSection, Elements,
        ExportKind, ExportSection, Function, FunctionSection, GlobalSection, GlobalType,
        ImportSection, Instruction, MemorySection, MemoryType, Module, RefType as EncRefType,
        TableSection, TableType, TypeSection, ValType as EncValType,
    };

    fn module(globals: &[(EncValType, bool, Option<&str>)], ops: &[Instruction<'_>]) -> Vec<u8> {
        module_with_locals(globals, ops, &[])
    }

    fn module_with_locals(
        globals: &[(EncValType, bool, Option<&str>)],
        ops: &[Instruction<'_>],
        locals: &[(u32, EncValType)],
    ) -> Vec<u8> {
        let mut module = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        module.section(&types);
        let mut functions = FunctionSection::new();
        functions.function(0);
        module.section(&functions);
        let mut tables = TableSection::new();
        tables.table(TableType {
            element_type: EncRefType::FUNCREF,
            minimum: 1,
            maximum: Some(2),
            table64: false,
            shared: false,
        });
        module.section(&tables);
        let mut memories = MemorySection::new();
        memories.memory(MemoryType {
            minimum: 1,
            maximum: None,
            memory64: false,
            shared: false,
            page_size_log2: None,
        });
        module.section(&memories);
        let mut global_section = GlobalSection::new();
        for (ty, mutable, _) in globals {
            let init = match ty {
                EncValType::I32 => ConstExpr::i32_const(7),
                EncValType::I64 => ConstExpr::i64_const(9),
                EncValType::F32 => ConstExpr::f32_const(1.25_f32.into()),
                EncValType::F64 => ConstExpr::f64_const(2.5_f64.into()),
                EncValType::V128 => ConstExpr::v128_const(19),
                EncValType::Ref(ty) => ConstExpr::ref_null(ty.heap_type),
            };
            global_section.global(
                GlobalType {
                    val_type: *ty,
                    mutable: *mutable,
                    shared: false,
                },
                &init,
            );
        }
        module.section(&global_section);
        let mut exports = ExportSection::new();
        exports.export("memory", ExportKind::Memory, 0);
        for (index, (_, _, name)) in globals.iter().enumerate() {
            if let Some(name) = name {
                exports.export(name, ExportKind::Global, index as u32);
            }
        }
        module.section(&exports);
        let mut elements = ElementSection::new();
        elements.passive(Elements::Functions(std::borrow::Cow::Borrowed(&[0])));
        module.section(&elements);
        module.section(&DataCountSection { count: 2 });
        let mut code = CodeSection::new();
        let mut function = Function::new(locals.iter().copied());
        for op in ops {
            function.instruction(op);
        }
        function.instruction(&Instruction::End);
        code.function(&function);
        module.section(&code);
        let mut data = DataSection::new();
        data.passive([]);
        data.active(0, &ConstExpr::i32_const(4), [1, 2, 3]);
        module.section(&data);
        module.finish()
    }

    #[test]
    fn pinned_guest_has_no_unrestored_state() {
        assert_eq!(
            validate(include_bytes!("../../../vendor/quickjs-ng/quickjs.wasm")).unwrap(),
            ["__stack_pointer"]
        );
    }

    #[test]
    fn numeric_globals_and_inert_wizer_segments_are_resettable() {
        let wasm = module(
            &[
                (EncValType::I32, true, Some("stack")),
                (EncValType::I64, false, None),
                (EncValType::F32, true, Some("float")),
                (EncValType::F64, true, Some("double")),
                (EncValType::V128, true, Some("vector")),
            ],
            &[
                Instruction::I32Const(0),
                Instruction::MemoryGrow(0),
                Instruction::Drop,
                Instruction::I32Const(0),
                Instruction::I32Const(0),
                Instruction::I32Const(1),
                Instruction::MemoryCopy {
                    src_mem: 0,
                    dst_mem: 0,
                },
                Instruction::I32Const(0),
                Instruction::I32Const(0),
                Instruction::I32Const(1),
                Instruction::MemoryFill(0),
            ],
        );
        assert_eq!(
            validate(&wasm).unwrap(),
            ["stack", "float", "double", "vector"]
        );
    }

    #[test]
    fn hidden_mutable_and_reference_globals_are_rejected() {
        let hidden = module(&[(EncValType::I32, true, None)], &[]);
        assert!(validate(&hidden)
            .unwrap_err()
            .to_string()
            .contains("hidden mutable global 0"));
        for mutable in [false, true] {
            let refs = module(&[(EncValType::EXTERNREF, mutable, Some("reference"))], &[]);
            assert!(validate(&refs)
                .unwrap_err()
                .to_string()
                .contains("reference-valued"));
        }
    }

    #[test]
    fn table_and_segment_mutations_are_rejected_even_in_unused_functions() {
        let mut mutations = vec![
            Instruction::TableSet(0),
            Instruction::TableGrow(0),
            Instruction::TableFill(0),
            Instruction::TableCopy {
                src_table: 0,
                dst_table: 0,
            },
            Instruction::TableInit {
                elem_index: 0,
                table: 0,
            },
            Instruction::MemoryInit {
                data_index: 0,
                mem: 0,
            },
            Instruction::DataDrop(0),
            Instruction::ElemDrop(0),
        ];
        for mutation in mutations.drain(..) {
            // The function is unreachable and polymorphic, keeping every
            // candidate valid regardless of its operand/result signature.
            let wasm = module(
                &[],
                &[Instruction::Unreachable, mutation, Instruction::Unreachable],
            );
            wasmparser::Validator::new_with_features(wasmparser::WasmFeatures::all())
                .validate_all(&wasm)
                .unwrap();
            let error = validate(&wasm).unwrap_err().to_string();
            assert!(error.contains("unresettable instruction"), "{error}");
        }
    }

    #[test]
    fn imported_state_is_rejected() {
        for entity in [
            wasm_encoder::EntityType::Global(GlobalType {
                val_type: EncValType::I32,
                mutable: true,
                shared: false,
            }),
            wasm_encoder::EntityType::Memory(MemoryType {
                minimum: 1,
                maximum: None,
                memory64: false,
                shared: false,
                page_size_log2: None,
            }),
            wasm_encoder::EntityType::Table(TableType {
                element_type: EncRefType::FUNCREF,
                minimum: 1,
                maximum: None,
                table64: false,
                shared: false,
            }),
        ] {
            let mut wasm = Module::new();
            let mut imports = ImportSection::new();
            imports.import("outside", "state", entity);
            wasm.section(&imports);
            assert!(validate(&wasm.finish())
                .unwrap_err()
                .to_string()
                .contains("imported guest state"));
        }
    }

    #[test]
    fn reference_values_and_unapproved_instruction_features_are_rejected() {
        let locals = module_with_locals(&[], &[], &[(1, EncValType::EXTERNREF)]);
        assert!(validate(&locals)
            .unwrap_err()
            .to_string()
            .contains("reference-valued locals"));
        let mut wasm = Module::new();
        let mut types = TypeSection::new();
        types.ty().function([EncValType::EXTERNREF], []);
        wasm.section(&types);
        assert!(validate(&wasm.finish())
            .unwrap_err()
            .to_string()
            .contains("reference-valued function signatures"));
        let wasm = module(
            &[],
            &[
                Instruction::RefNull(wasm_encoder::HeapType::EXTERN),
                Instruction::Drop,
            ],
        );
        wasmparser::Validator::new_with_features(wasmparser::WasmFeatures::all())
            .validate_all(&wasm)
            .unwrap();
        assert!(validate(&wasm)
            .unwrap_err()
            .to_string()
            .contains("unsupported reset features"));
    }

    #[test]
    fn start_and_exception_sections_are_rejected() {
        // Insert valid start/tag sections into the otherwise eligible fixture.
        let original = module(&[], &[]);
        for exception in [false, true] {
            let mut changed = Module::new();
            for payload in Parser::new(0).parse_all(&original) {
                let payload = payload.unwrap();
                if matches!(payload, Payload::GlobalSection(_)) && exception {
                    let mut tags = wasm_encoder::TagSection::new();
                    tags.tag(wasm_encoder::TagType {
                        kind: wasm_encoder::TagKind::Exception,
                        func_type_idx: 0,
                    });
                    changed.section(&tags);
                }
                if matches!(payload, Payload::ElementSection(_)) && !exception {
                    changed.section(&wasm_encoder::StartSection { function_index: 0 });
                }
                if let Some((id, range)) = payload.as_section() {
                    changed.section(&wasm_encoder::RawSection {
                        id,
                        data: &original[range.start as usize..range.end as usize],
                    });
                }
            }
            let changed = changed.finish();
            wasmparser::Validator::new_with_features(wasmparser::WasmFeatures::all())
                .validate_all(&changed)
                .unwrap();
            assert!(validate(&changed).is_err());
        }
    }
}
