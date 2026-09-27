use super::{host, store, Abi, Host, Limits, STATIC_INIT_MARKER};
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io::Write,
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant},
};
use wasmtime::{
    Config, Engine, Instance, InstanceAllocationStrategy, InstancePre, Module,
    PoolingAllocationConfig, Store, Val,
};
use wasmtime_wizer::{InstanceState, ModuleContext, SnapshotVal, ValType, Wizer};

mod flight;
mod profile;

const WASM: &[u8] = include_bytes!("../../../vendor/quickjs-ng/quickjs.wasm");
const WASM_HASH: &str = "4146efd6851a79d0f056cebd5e927682f51ef3a740ee006bceaa32eaeba0b822";
const MAX_CACHE_BYTES: usize = 96 * 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 8;
const TABLE_ELEMENTS: usize = 4096;

/// What a bundle deploys: JavaScript for the pinned QuickJS guest, or a guest
/// module of its own.
#[derive(Clone, Copy)]
pub(super) enum Source<'a> {
    JavaScript(&'a str),
    Module(&'a [u8]),
}

impl<'a> Source<'a> {
    /// Borrow a bundle's code, decoding a module into `decoded`.
    pub(super) fn of(bundle: &'a Value, decoded: &'a mut Vec<u8>) -> Result<Self> {
        match (bundle.get("javascript"), bundle.get("wasm")) {
            (Some(Value::String(javascript)), None) => Ok(Self::JavaScript(javascript)),
            (None, Some(Value::String(encoded))) => {
                use base64::Engine as _;
                *decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .context("bundle.wasm must be base64")?;
                Ok(Self::Module(decoded))
            }
            _ => anyhow::bail!("bundle must carry either a javascript or a wasm string"),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::JavaScript(javascript) => javascript.len(),
            Self::Module(wasm) => wasm.len(),
        }
    }

    /// Exact content identity. For JavaScript it includes the guest, sandbox
    /// and runner; no user-supplied hash or native code is ever trusted.
    fn key(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        let parts: &[&[u8]] = match self {
            Self::JavaScript(javascript) => &[
                WASM_HASH.as_bytes(),
                b"checked-shadow-stack-v1",
                super::super::SANDBOX.as_bytes(),
                super::super::CELL_RUNNER.as_bytes(),
                super::super::MANIFEST.as_bytes(),
                javascript.as_bytes(),
            ],
            Self::Module(wasm) => &[b"guest-module-v1", wasm],
        };
        for part in parts {
            hash.update(part.len().to_le_bytes());
            hash.update(part);
        }
        hash.finalize().into()
    }
}

pub(in crate::evaluator) struct Prepared {
    pub(super) pre: InstancePre<Host>,
    pub(super) exports: super::abi::Exports,
    pub(super) bytecode: Option<Vec<u8>>,
    pub(super) input_buffer: super::abi::InputBuffer,
    pub(super) image: Arc<super::recycle::Image>,
    weight: usize,
}
impl Prepared {
    fn new(
        pre: InstancePre<Host>,
        bytecode: Option<Vec<u8>>,
        input_buffer: super::abi::InputBuffer,
        reset_globals: &[String],
        weight: usize,
    ) -> Result<Self> {
        let exports = super::abi::Exports::new(pre.module())?;
        let image = Arc::new(super::recycle::Image::new(pre.module(), reset_globals)?);
        Ok(Self {
            pre,
            exports,
            bytecode,
            input_buffer,
            image,
            weight,
        })
    }
}
pub(super) struct Runtime {
    pub(super) engine: Engine,
    wizer: Wizer,
    context: ModuleContext<'static>,
    instrumented: InstancePre<Host>,
    base: InstancePre<Host>,
    base_input: super::abi::InputBuffer,
    base_reset_globals: Vec<String>,
    base_memory_bytes: usize,
    cache: Mutex<Cache>,
    preparing: flight::Flights<Prepared>,
}

#[derive(Default)]
struct Cache {
    images: VecDeque<([u8; 32], Arc<Prepared>)>,
    identities: VecDeque<(Weak<Value>, [u8; 32])>,
}
impl Cache {
    fn touch(&mut self, key: &[u8; 32]) -> Option<Arc<Prepared>> {
        let position = self
            .images
            .iter()
            .position(|(candidate, _)| candidate == key)?;
        let entry = self.images.remove(position).unwrap();
        let prepared = entry.1.clone();
        self.images.push_back(entry);
        Some(prepared)
    }
    fn identity(&mut self, bundle: &Arc<Value>) -> Option<Arc<Prepared>> {
        self.identities
            .retain(|(value, _)| value.strong_count() != 0);
        let position = self.identities.iter().position(|(value, _)| {
            // A weak reference keeps the allocation identity unique; upgrading
            // and comparing Arc identity also rejects a dropped/replaced value.
            value.as_ptr() == Arc::as_ptr(bundle)
                && value
                    .upgrade()
                    .is_some_and(|value| Arc::ptr_eq(&value, bundle))
        })?;
        let entry = self.identities.remove(position).unwrap();
        let prepared = self.touch(&entry.1);
        if prepared.is_some() {
            self.identities.push_back(entry);
        }
        prepared
    }
    fn remember(&mut self, bundle: &Arc<Value>, prepared: &Arc<Prepared>) {
        let Some((key, _)) = self
            .images
            .iter()
            .find(|(_, image)| Arc::ptr_eq(image, prepared))
        else {
            return;
        };
        let key = *key;
        self.identities.retain(|(value, _)| {
            value.strong_count() != 0 && value.as_ptr() != Arc::as_ptr(bundle)
        });
        while self.identities.len() >= 32 {
            self.identities.pop_front();
        }
        // Neither the JSON value nor another snapshot is kept alive by this
        // shortcut. Image retention remains exclusively the bounded main LRU.
        self.identities.push_back((Arc::downgrade(bundle), key));
    }
}

pub(super) fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Runtime::new().map_err(|error| format!("{error:#}")))
        .as_ref()
        .map_err(|error| anyhow::anyhow!("cannot initialize QuickJS Wasm: {error}"))
}

impl Runtime {
    fn new() -> Result<Self> {
        ensure!(
            crate::evaluator::hash(WASM) == WASM_HASH,
            "vendored QuickJS Wasm checksum mismatch"
        );
        super::surface::validate(WASM)?;
        let settings = crate::evaluator::config::settings()?;
        let mut pool = PoolingAllocationConfig::default();
        pool.total_core_instances(settings.wasm_pool_slots)
            .total_memories(settings.wasm_pool_slots)
            .total_tables(settings.wasm_pool_slots)
            .max_memory_size(settings.pool_memory_bytes())
            .table_elements(TABLE_ELEMENTS)
            // Reset the bounded funcref table with memset instead of discarding
            // its pages. Wasmtime 49 uses this path on macOS too, avoiding a
            // MAP_FIXED remap per callback; every new instance still starts
            // with a clean table. Linear-memory CoW reset remains unchanged.
            .table_keep_resident(TABLE_ELEMENTS * std::mem::size_of::<usize>());
        let mut config = Config::new();
        // Custom per-Store dirty-page handling uses Wasmtime's supported Unix
        // signal path on macOS too. Trap mode must agree for every process Engine.
        config.macos_use_mach_ports(false);
        config
            .memory_init_cow(true)
            // Reserve the whole wasm32 address space so Cranelift can use guard
            // pages instead of explicit bounds checks for ordinary loads/stores.
            // This is virtual address space, not committed guest memory. Keep
            // both the pool's growth maximum above and Store's aggregate limiter
            // at the operator budget; unused reservation remains inaccessible.
            .memory_reservation(if usize::BITS >= 64 {
                1_u64 << 32
            } else {
                settings.pool_memory_bytes() as u64
            })
            .max_wasm_stack(256 * 1024)
            .epoch_interruption(true)
            .allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        config.enable_incremental_compilation(Arc::new(super::code_cache::CodeCache::default()))?;
        let engine = Engine::new(&config)?;
        // One engine-wide metronome interrupts every guest, including pure loops
        // that never call QuickJS's own interrupt hook or any database callback.
        let clock = engine.clone();
        std::thread::Builder::new()
            .name("flower-wasm-epoch".into())
            .spawn(move || loop {
                std::thread::sleep(Duration::from_millis(5));
                clock.increment_epoch();
            })?;
        let mut wizer = Wizer::new();
        wizer.init_func("_initialize");
        // One bounded trusted image lives for the process lifetime, like Engine.
        let checked_wasm = Box::leak(super::shadow_stack::instrument(WASM)?.into_boxed_slice());
        let (context, instrumented_wasm) = wizer.instrument(checked_wasm)?;
        let module = Module::new(&engine, instrumented_wasm)?;
        super::native_profile::record(&module, WASM_HASH)?;
        let instrumented = host::linker(&engine)?.instantiate_pre(&module)?;
        let shared = Limits::new(
            Instant::now() + settings.evaluation_timeout.max(Duration::from_secs(30)),
            settings.guest_memory_bytes,
        );
        let (mut store, instance, abi) = initialize(&engine, &instrumented, shared)?;
        abi.prepare_snapshot(&mut store, instance, "base")?;
        let base_input = abi.reserve_input(&mut store)?;
        let bytes = futures_executor::block_on(wizer.snapshot(
            &context,
            &mut Snapshot {
                store: &mut store,
                instance,
            },
        ))?;
        drop(store);
        let (base, _, base_reset_globals) = compile_image(&engine, &bytes)?;
        let base_memory_bytes =
            super::recycle::Image::new(base.module(), &base_reset_globals)?.memory_bytes;
        Ok(Self {
            engine,
            wizer,
            context,
            instrumented,
            base,
            base_input,
            base_reset_globals,
            base_memory_bytes,
            cache: Mutex::new(Cache::default()),
            preparing: flight::Flights::default(),
        })
    }

    pub(super) fn prepare_shared_bundle(
        &self,
        bundle: &Arc<Value>,
        shared: Arc<Limits>,
    ) -> Result<Arc<Prepared>> {
        shared.check()?;
        if let Some(prepared) = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("Wasm cache lock poisoned"))?
            .identity(bundle)
        {
            return Ok(prepared);
        }
        let mut decoded = Vec::new();
        let prepared = self.prepare_source(Source::of(bundle, &mut decoded)?, shared)?;
        self.cache
            .lock()
            .map_err(|_| anyhow::anyhow!("Wasm cache lock poisoned"))?
            .remember(bundle, &prepared);
        Ok(prepared)
    }

    pub(super) fn prepare(&self, bundle: &str, shared: Arc<Limits>) -> Result<Arc<Prepared>> {
        self.prepare_source(Source::JavaScript(bundle), shared)
    }

    pub(super) fn prepare_source(
        &self,
        source: Source<'_>,
        shared: Arc<Limits>,
    ) -> Result<Arc<Prepared>> {
        ensure!(
            source.len() <= crate::evaluator::config::settings()?.bundle_max_bytes,
            "bundle exceeds FLOWER_BUNDLE_MAX_BYTES"
        );
        shared.check()?;
        let key = source.key();
        loop {
            if let Some(prepared) = self.cached(&key)? {
                return Ok(prepared);
            }
            // A replica can receive many requests for its first bundle at once.
            // Compile one image per exact content key while keeping unrelated
            // bundles and ordinary cache hits independent of this cold work.
            match self.preparing.join(key)? {
                flight::Attempt::Wait(pending) => {
                    let prepared = profile::observe("coalesced_wait", || {
                        pending.wait(shared.deadline, || shared.check())
                    })?;
                    if let Some(prepared) = prepared {
                        return Ok(prepared);
                    }
                    // The preparing caller may have exhausted its own budget.
                    // Retry with this caller's limits instead of caching errors.
                }
                flight::Attempt::Lead(leader) => {
                    // Another preparation may have completed between the cache
                    // lookup and claiming the now-vacant in-flight entry.
                    if let Some(prepared) = self.cached(&key)? {
                        shared.check()?;
                        leader.publish(prepared.clone());
                        return Ok(prepared);
                    }
                    let prepared = profile::observe("total", || match source {
                        Source::JavaScript(bundle) => self.prepare_uncached(bundle, shared.clone()),
                        Source::Module(wasm) => self.prepare_module(wasm, shared.clone()),
                    })?;
                    let prepared = Arc::new(prepared);
                    let mut cache = self
                        .cache
                        .lock()
                        .map_err(|_| anyhow::anyhow!("Wasm cache lock poisoned"))?;
                    while !cache.images.is_empty()
                        && (cache.images.len() >= MAX_CACHE_ENTRIES
                            || cache
                                .images
                                .iter()
                                .map(|(_, entry)| entry.weight)
                                .sum::<usize>()
                                + prepared.weight
                                > MAX_CACHE_BYTES)
                    {
                        cache.images.pop_front();
                    }
                    if prepared.weight <= MAX_CACHE_BYTES {
                        cache.images.push_back((key, prepared.clone()));
                    }
                    drop(cache);
                    leader.publish(prepared.clone());
                    return Ok(prepared);
                }
            }
        }
    }

    fn cached(&self, key: &[u8; 32]) -> Result<Option<Arc<Prepared>>> {
        Ok(self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("Wasm cache lock poisoned"))?
            .touch(key))
    }

    fn prepare_uncached(&self, bundle: &str, shared: Arc<Limits>) -> Result<Prepared> {
        shared.check()?;
        let (mut store, instance, abi) = profile::observe("initialize", || {
            initialize(&self.engine, &self.instrumented, shared.clone())
        })?;
        let bytecode = profile::observe("bytecode_compile", || abi.compile(&mut store, bundle))?;
        let prepared = if bundle.starts_with(STATIC_INIT_MARKER) {
            // Explicit opt-in promises initialization independent of invocation.
            // Database calls fail here: no host callback is bound yet.
            profile::observe("static_initialize", || abi.bytecode(&mut store, &bytecode))?;
            let memory_bytes = abi.memory.data_size(&store);
            tracing::debug!(target: "flower::evaluator_profile", guest_memory_bytes = memory_bytes,
                initialized_snapshot = memory_bytes <= 8 * 1024 * 1024,
                "static QuickJS bundle image preparation");
            // Large initialized heaps run from the shared base image instead:
            // never construct/cache snapshots near the guest budget per bundle.
            if memory_bytes > 8 * 1024 * 1024 {
                let weight = bytecode.len() + self.base_memory_bytes;
                Prepared::new(
                    self.base.clone(),
                    Some(bytecode),
                    self.base_input,
                    &self.base_reset_globals,
                    weight,
                )?
            } else {
                profile::observe("snapshot_prepare", || {
                    abi.prepare_snapshot(&mut store, instance, "bundle")
                })?;
                let input_buffer = abi.reserve_input(&mut store)?;
                let memory_bytes = abi.memory.data_size(&store);
                let bytes = profile::observe("snapshot", || {
                    futures_executor::block_on(self.wizer.snapshot(
                        &self.context,
                        &mut Snapshot {
                            store: &mut store,
                            instance,
                        },
                    ))
                })?;
                drop(store);
                shared.check()?;
                let (pre, compiled_bytes, reset_globals) =
                    profile::observe("native_compile", || compile_image(&self.engine, &bytes))?;
                let weight = bytes.len() + memory_bytes + compiled_bytes;
                if weight > MAX_CACHE_BYTES {
                    let weight = bytecode.len() + self.base_memory_bytes;
                    Prepared::new(
                        self.base.clone(),
                        Some(bytecode),
                        self.base_input,
                        &self.base_reset_globals,
                        weight,
                    )?
                } else {
                    Prepared::new(pre, None, input_buffer, &reset_globals, weight)?
                }
            }
        } else {
            let weight = bytecode.len() + self.base_memory_bytes;
            Prepared::new(
                self.base.clone(),
                Some(bytecode),
                self.base_input,
                &self.base_reset_globals,
                weight,
            )?
        };
        shared.check()?;
        Ok(prepared)
    }
}

impl Runtime {
    /// Snapshot a deployed guest after its optional flower_init, as a pristine
    /// image of its own. Guest modules run without the QuickJS shadow-stack
    /// instrumentation: their memory is reset after every call regardless.
    fn prepare_module(&self, wasm: &[u8], shared: Arc<Limits>) -> Result<Prepared> {
        let settings = crate::evaluator::config::settings()?;
        super::module::validate(wasm, settings.guest_memory_bytes)?;
        let exported = super::module::export_globals(wasm)?;
        let mut wizer = Wizer::new();
        wizer.init_func("flower_init");
        let (context, instrumented) = wizer.instrument(&exported)?;
        let module = Module::new(&self.engine, instrumented)?;
        let pre = host::linker(&self.engine)?.instantiate_pre(&module)?;
        let mut store = store(&self.engine, shared.clone(), None);
        let instance = pre.instantiate(&mut store).map_err(|error| error.context("Wasm instance allocation failed; check FLOWER_WASM_POOL_SLOTS and FLOWER_GUEST_MEMORY_BYTES"))?;
        let abi = Abi::load(&mut store, instance)?;
        store.data_mut().abi = Some(abi.clone());
        if let Ok(init) = instance.get_typed_func::<(), i32>(&mut store, "flower_init") {
            ensure!(init.call(&mut store, ())? == 0, "guest flower_init failed");
        }
        let input_buffer = abi.reserve_input(&mut store)?;
        let memory_bytes = abi.memory.data_size(&store);
        let bytes = futures_executor::block_on(wizer.snapshot(
            &context,
            &mut Snapshot {
                store: &mut store,
                instance,
            },
        ))?;
        drop(store);
        shared.check()?;
        let (pre, compiled_bytes, reset_globals) = compile_image(&self.engine, &bytes)?;
        Prepared::new(
            pre,
            None,
            input_buffer,
            &reset_globals,
            bytes.len() + memory_bytes + compiled_bytes,
        )
    }
}

fn initialize(
    engine: &Engine,
    pre: &InstancePre<Host>,
    shared: Arc<Limits>,
) -> Result<(Store<Host>, Instance, Abi)> {
    let mut store = store(engine, shared, None);
    let instance = pre.instantiate(&mut store).map_err(|error| error.context("Wasm instance allocation failed; check FLOWER_WASM_POOL_SLOTS and FLOWER_GUEST_MEMORY_BYTES"))?;
    instance
        .get_typed_func::<(), ()>(&mut store, "_initialize")?
        .call(&mut store, ())?;
    ensure!(
        instance
            .get_typed_func::<(), i32>(&mut store, "flower_init")?
            .call(&mut store, ())?
            == 0,
        "QuickJS initialization failed"
    );
    let abi = Abi::load(&mut store, instance)?;
    store.data_mut().abi = Some(abi.clone());
    abi.discard(&mut store, super::super::SANDBOX)?;
    // Construct trusted helpers before application code exists. The guest roots
    // the returned function privately and removes both temporary setup globals.
    abi.discard(
        &mut store,
        &format!(
            "__flowerSetRunner(...{},{});",
            super::super::CELL_RUNNER,
            super::super::MANIFEST
        ),
    )?;
    Ok((store, instance, abi))
}

fn compile_image(engine: &Engine, wasm: &[u8]) -> Result<(InstancePre<Host>, usize, Vec<String>)> {
    // Validate the final Wizer image as well as the original guest: memory
    // bytes and these numeric globals must contain every mutable guest state.
    let reset_globals = super::reset_surface::validate(wasm)?;
    let compiled = engine.precompile_module(wasm)?;
    // Private, newly created file. On macOS file-backed loading is required for
    // Wasmtime COW memory images; unlinking afterwards leaves mappings alive.
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("image.cwasm");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&compiled)?;
    drop(file);
    // SAFETY: this file contains only this Engine's just-compiled module; it is
    // never modified or loaded from an external cache. The private directory is
    // removed immediately, so no mutable path remains for the mapped lifetime.
    let module = unsafe { Module::deserialize_file(engine, &path)? };
    super::native_profile::record(&module, WASM_HASH)?;
    let pre = host::linker(engine)?.instantiate_pre(&module)?;
    Ok((pre, compiled.len(), reset_globals))
}

struct Snapshot<'a> {
    store: &'a mut Store<Host>,
    instance: Instance,
}
impl InstanceState for Snapshot<'_> {
    fn global_get(
        &mut self,
        name: &str,
        _ty: ValType,
    ) -> impl std::future::Future<Output = SnapshotVal> + Send {
        std::future::ready(
            match self
                .instance
                .get_global(&mut *self.store, name)
                .unwrap()
                .get(&mut *self.store)
            {
                Val::I32(v) => SnapshotVal::I32(v),
                Val::I64(v) => SnapshotVal::I64(v),
                Val::F32(v) => SnapshotVal::F32(v),
                Val::F64(v) => SnapshotVal::F64(v),
                Val::V128(v) => SnapshotVal::V128(v.as_u128()),
                _ => unreachable!("Wizer rejects unsupported globals"),
            },
        )
    }
    fn memory_contents(
        &mut self,
        name: &str,
        contents: impl FnOnce(&[u8]) + Send,
    ) -> impl std::future::Future<Output = ()> + Send {
        let memory = self.instance.get_memory(&mut *self.store, name).unwrap();
        contents(memory.data(&*self.store));
        std::future::ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resident_table_slots_reset_every_function_reference_between_stores() {
        use wasm_encoder::{ExportKind, ExportSection, RefType, TableSection, TableType};

        let mut pool = PoolingAllocationConfig::default();
        pool.total_core_instances(1)
            .total_tables(1)
            .table_elements(TABLE_ELEMENTS)
            .table_keep_resident(TABLE_ELEMENTS * std::mem::size_of::<usize>());
        let mut config = Config::new();
        config.macos_use_mach_ports(false);
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));
        let engine = Engine::new(&config).unwrap();
        let mut wasm = wasm_encoder::Module::new();
        let mut tables = TableSection::new();
        tables.table(TableType {
            element_type: RefType::FUNCREF,
            table64: false,
            minimum: TABLE_ELEMENTS as u64,
            maximum: Some(TABLE_ELEMENTS as u64),
            shared: false,
        });
        wasm.section(&tables);
        let mut exports = ExportSection::new();
        exports.export("table", ExportKind::Table, 0);
        wasm.section(&exports);
        let module = Module::new(&engine, wasm.finish()).unwrap();
        // A one-slot pool must reuse the same storage, including the last page.
        // A prior Store's function pointers must never survive the reset.
        for _ in 0..3 {
            let mut store = Store::new(&engine, ());
            let instance = Instance::new(&mut store, &module, &[]).unwrap();
            let table = instance.get_table(&mut store, "table").unwrap();
            let function = wasmtime::Func::wrap(&mut store, || {});
            for index in 0..TABLE_ELEMENTS as u64 {
                assert!(matches!(
                    table.get(&mut store, index),
                    Some(wasmtime::Ref::Func(None))
                ));
                table
                    .set(&mut store, index, wasmtime::Ref::Func(Some(function)))
                    .unwrap();
            }
        }
    }

    #[test]
    fn snapshot_preparation_collects_cycles_and_restores_quickjs_headroom() {
        let runtime = runtime().unwrap();
        let mut store = store(&runtime.engine, super::super::tests::limits(), None);
        let instance = runtime.base.instantiate(&mut store).unwrap();
        let abi = Abi::load(&mut store, instance).unwrap();
        abi.discard(&mut store, r#"
            globalThis.kept=(()=>{let value=40;return()=>++value;})();
            globalThis.garbage=Array.from({length:1000},()=>{const value={};value.self=value;return value;});
            garbage=null;
        "#).unwrap();
        let stats = abi.prepare_snapshot(&mut store, instance, "test").unwrap();
        assert!(stats.objects_before >= stats.objects_after + 1000);
        assert!(stats.bytes_after < stats.bytes_before);
        assert_eq!(
            stats.threshold_after,
            stats.bytes_after + stats.bytes_after / 2
        );
        assert!(stats.threshold_after > stats.bytes_after);
        assert_eq!(abi.eval_string(&mut store, "kept()").unwrap(), "41");
    }

    #[test]
    fn virtual_reservation_does_not_relax_guest_memory_budget() {
        let runtime = runtime().unwrap();
        let expected = if usize::BITS >= 64 {
            1_u64 << 32
        } else {
            crate::evaluator::config::settings()
                .unwrap()
                .pool_memory_bytes() as u64
        };
        assert_eq!(runtime.engine.get_memory_reservation(), expected);
        let initial_pages = runtime
            .base
            .module()
            .get_export("memory")
            .unwrap()
            .memory()
            .unwrap()
            .minimum();
        let shared = Limits::new(
            Instant::now() + Duration::from_secs(30),
            usize::try_from(initial_pages * 65536).unwrap(),
        );
        let mut store = store(&runtime.engine, shared.clone(), None);
        let instance = runtime.base.instantiate(&mut store).unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        assert_eq!(memory.size(&store), initial_pages);
        assert!(
            memory.grow(&mut store, 1).is_err(),
            "reserved address space must not count as an allocation allowance"
        );
        assert_eq!(memory.size(&store), initial_pages);
        assert!(shared.check().is_err(), "memory failure remains sticky");
    }

    #[test]
    fn weak_bundle_shortcuts_reject_mutation_and_eviction_and_stay_bounded() {
        let prepared = runtime()
            .unwrap()
            .prepare(
                "var __flowerBundle={default:{definitions:{},http:{}}};",
                Limits::new(
                    Instant::now() + Duration::from_secs(30),
                    crate::evaluator::config::settings()
                        .unwrap()
                        .guest_memory_bytes,
                ),
            )
            .unwrap();
        let mut cache = Cache::default();
        cache.images.push_back(([7; 32], prepared.clone()));
        let mut bundle = Arc::new(json!({"javascript":"first"}));
        cache.remember(&bundle, &prepared);
        assert_eq!(
            Arc::strong_count(&bundle),
            1,
            "shortcut must not retain the JSON value"
        );
        assert!(Arc::ptr_eq(&cache.identity(&bundle).unwrap(), &prepared));
        assert!(
            Arc::get_mut(&mut bundle).is_none(),
            "weak identity prevents in-place mutation"
        );
        Arc::make_mut(&mut bundle)["javascript"] = json!("second");
        assert!(
            cache.identity(&bundle).is_none(),
            "copy-on-write value replacement invalidates old identity"
        );
        cache.remember(&bundle, &prepared);
        cache.images.clear();
        assert!(
            cache.identity(&bundle).is_none(),
            "external live images cannot bypass image-cache eviction"
        );
        cache.images.push_back(([7; 32], prepared.clone()));
        let values: Vec<_> = (0..100)
            .map(|index| Arc::new(json!({"javascript":index})))
            .collect();
        for value in &values {
            cache.remember(value, &prepared);
        }
        assert_eq!(cache.identities.len(), 32);
        drop(values);
        assert!(cache.identity(&bundle).is_none());
        assert!(
            cache.identities.is_empty(),
            "dead identity headers are reclaimed"
        );
    }
}
