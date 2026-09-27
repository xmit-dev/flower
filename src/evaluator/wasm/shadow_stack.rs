//! Turn C shadow-stack under/overflow into uncatchable Wasm traps. Wasmtime's
//! native stack guard alone does not cover variable-size C alloca frames living
//! inside linear memory. Instrument the pinned guest before Wizer compilation.
use anyhow::{Context, Result, ensure};
use wasm_encoder::{
    BlockType, CodeSection, Instruction, Module,
    reencode::{self, Reencode},
};
use wasmparser::{ExternalKind, Operator, Parser, Payload};

pub(super) fn instrument(wasm: &[u8]) -> Result<Vec<u8>> {
    let mut globals = Vec::new();
    let mut stack_index = None;
    for payload in Parser::new(0).parse_all(wasm) {
        match payload? {
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    ensure!(
                        !matches!(import?.ty, wasmparser::TypeRef::Global(_)),
                        "pinned guest must not import globals"
                    );
                }
            }
            Payload::GlobalSection(section) => {
                for global in section {
                    let global = global?;
                    globals.push(
                        if global.ty.content_type == wasmparser::ValType::I32 && global.ty.mutable {
                            match global.init_expr.get_operators_reader().read()? {
                                Operator::I32Const { value } => Some(value as u32),
                                _ => None,
                            }
                        } else {
                            None
                        },
                    );
                }
            }
            Payload::ExportSection(section) => {
                for export in section {
                    let export = export?;
                    if export.name == "__stack_pointer" && export.kind == ExternalKind::Global {
                        stack_index = Some(export.index);
                    }
                }
            }
            _ => {}
        }
    }
    let index = stack_index.context("pinned guest must export its C shadow stack pointer")?;
    let top = globals
        .get(index as usize)
        .copied()
        .flatten()
        .context("C shadow stack must have constant i32 initial value")?;
    ensure!(top == 1024 * 1024, "pinned guest C stack layout changed");
    // The pinned upstream Makefile reserves exactly 1 MiB for the C stack.
    // Keep 64 KiB above its bottom as a guard; validate subtraction explicitly.
    let bottom = top
        .checked_sub(1024 * 1024)
        .context("invalid pinned C stack layout")?
        + 64 * 1024;
    let mut rewrite = CheckedStack {
        index,
        top,
        bottom,
        checks: 0,
    };
    let mut module = Module::new();
    rewrite
        .parse_core_module(&mut module, Parser::new(0), wasm)
        .map_err(|e| anyhow::anyhow!("cannot instrument C shadow stack: {e}"))?;
    ensure!(
        rewrite.checks > 0,
        "pinned guest does not manipulate exported C shadow stack"
    );
    Ok(module.finish())
}

struct CheckedStack {
    index: u32,
    top: u32,
    bottom: u32,
    checks: usize,
}
impl Reencode for CheckedStack {
    type Error = std::convert::Infallible;
    fn parse_function_body(
        &mut self,
        code: &mut CodeSection,
        body: wasmparser::FunctionBody<'_>,
    ) -> Result<(), reencode::Error<Self::Error>> {
        let mut function = self.new_function_with_parsed_locals(&body)?;
        let mut reader = body.get_operators_reader()?;
        while !reader.eof() {
            let operation = reader.read()?;
            let check = matches!(operation, Operator::GlobalSet { global_index } if global_index == self.index);
            function.instruction(&self.instruction(operation)?);
            if check {
                // A single unsigned interval test catches both underflow and
                // arithmetic wraparound above the original stack top.
                function.instruction(&Instruction::GlobalGet(self.index));
                function.instruction(&Instruction::I32Const(self.bottom as i32));
                function.instruction(&Instruction::I32Sub);
                function.instruction(&Instruction::I32Const((self.top - self.bottom) as i32));
                function.instruction(&Instruction::I32GtU);
                function.instruction(&Instruction::If(BlockType::Empty));
                function.instruction(&Instruction::Unreachable);
                function.instruction(&Instruction::End);
                self.checks += 1;
            }
        }
        code.function(&function);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_encoder::{
        ConstExpr, ExportKind, ExportSection, Function, FunctionSection, GlobalSection, GlobalType,
        TypeSection, ValType,
    };
    #[test]
    fn checked_shadow_stack_rejects_lower_upper_and_wrapping_boundaries() {
        for pointer in [
            64 * 1024,
            1024 * 1024,
            64 * 1024 - 1,
            1024 * 1024 + 1,
            u32::MAX,
            0,
        ] {
            let mut module = Module::new();
            let mut types = TypeSection::new();
            types.ty().function([], []);
            module.section(&types);
            let mut functions = FunctionSection::new();
            functions.function(0);
            module.section(&functions);
            let mut globals = GlobalSection::new();
            globals.global(
                GlobalType {
                    val_type: ValType::I32,
                    mutable: true,
                    shared: false,
                },
                &ConstExpr::i32_const(1024 * 1024),
            );
            module.section(&globals);
            let mut exports = ExportSection::new();
            exports.export("__stack_pointer", ExportKind::Global, 0);
            exports.export("run", ExportKind::Func, 0);
            module.section(&exports);
            let mut function = Function::new([]);
            function.instruction(&Instruction::I32Const(pointer as i32));
            function.instruction(&Instruction::GlobalSet(0));
            function.instruction(&Instruction::End);
            let mut code = CodeSection::new();
            code.function(&function);
            module.section(&code);
            let mut config = wasmtime::Config::new();
            config.macos_use_mach_ports(false);
            let engine = wasmtime::Engine::new(&config).unwrap();
            let compiled =
                wasmtime::Module::new(&engine, instrument(&module.finish()).unwrap()).unwrap();
            let mut store = wasmtime::Store::new(&engine, ());
            let instance = wasmtime::Instance::new(&mut store, &compiled, &[]).unwrap();
            let result = instance
                .get_typed_func::<(), ()>(&mut store, "run")
                .unwrap()
                .call(&mut store, ());
            assert_eq!(result.is_ok(), (64 * 1024..=1024 * 1024).contains(&pointer));
        }
    }
}
