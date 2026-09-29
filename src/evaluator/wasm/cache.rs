use super::{Abi, Host, Limits, STATIC_INIT_MARKER, host, store};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io::Write,
    sync::{Arc, Mutex, OnceLock},
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
const WASM_HASH: &str = "ab4a8c3637a27d30e1f66f45db5fa0b6e4171b09498db7f3f30eed0e525800fb";
const MAX_CACHE_BYTES: usize = 96 * 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 8;
/// Stored bundle records whose content keys are remembered, by version.
const MAX_STORED_VERSIONS: usize = 64;
/// The largest initialized heap a static-init bundle is snapshotted with; a
/// larger one runs from the shared base image instead, reloading its bytecode
/// and rerunning its module code in every callback. The fallback saves no guest
/// memory (each callback rebuilds the same heap), only image-cache bytes, which
/// MAX_CACHE_BYTES bounds as well.
const STATIC_SNAPSHOT_MAX_BYTES: usize = 32 * 1024 * 1024;
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
        let key = self.content_key();
        #[cfg(test)]
        HASHES.with(|hashes| *hashes.borrow_mut().entry(key).or_default() += 1);
        key
    }

    fn content_key(&self) -> [u8; 32] {
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

#[cfg(test)]
thread_local! {
    static HASHES: std::cell::RefCell<std::collections::HashMap<[u8; 32], usize>> =
        Default::default();
}

// Set by a test on its own thread: after a static bundle's guest code has run,
// its preparation waits until then, as a busy host would make it.
#[cfg(test)]
thread_local! {
    static HOLD_AFTER_GUEST: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn hold_after_guest() {
    if let Some(until) = HOLD_AFTER_GUEST.with(std::cell::Cell::get) {
        std::thread::sleep(until.saturating_duration_since(Instant::now()));
    }
}

/// How many times this thread hashed `javascript` to find its image.
#[cfg(test)]
pub(in crate::evaluator) fn hashes(javascript: &str) -> usize {
    let key = Source::JavaScript(javascript).content_key();
    HASHES.with(|hashes| hashes.borrow().get(&key).copied().unwrap_or(0))
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
    /// Stored bundle records' content keys, by the version of the write that
    /// stored them, least recently used first. Versions identify writes, so a
    /// record found again at its version needs neither reading, parsing nor
    /// hashing again; its image stays in `images` alone, evicted as ever.
    stored: VecDeque<(u64, [u8; 32])>,
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
    /// The content key of the bundle record stored by write `version`, if
    /// known, now the most recently used.
    fn stored(&mut self, version: u64) -> Option<[u8; 32]> {
        let position = self
            .stored
            .iter()
            .position(|(candidate, _)| *candidate == version)?;
        let entry = self.stored.remove(position).unwrap();
        self.stored.push_back(entry);
        Some(entry.1)
    }
    fn remember(&mut self, version: u64, key: [u8; 32]) {
        self.stored.retain(|(candidate, _)| *candidate != version);
        while self.stored.len() >= MAX_STORED_VERSIONS {
            self.stored.pop_front();
        }
        self.stored.push_back((version, key));
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
            .spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_millis(5));
                    clock.increment_epoch();
                }
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

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Cache>> {
        self.cache
            .lock()
            .map_err(|_| anyhow::anyhow!("Wasm cache lock poisoned"))
    }

    /// The image of the bundle record that write `version` stored. `bundle`
    /// reads the record, only for a version not seen before or whose image
    /// left the cache, and gives None for a record without code: then so does
    /// this. Every evaluation finds the deployed bundle this way, and hashing
    /// its code, or parsing a record grown past the value cache's slices, took
    /// most of a busy replica's time.
    pub(super) fn prepare_stored_bundle(
        &self,
        version: u64,
        bundle: impl FnOnce() -> Option<Arc<Value>>,
        shared: Arc<Limits>,
    ) -> Result<Option<Arc<Prepared>>> {
        shared.check()?;
        let known = {
            let mut cache = self.lock()?;
            let known = cache.stored(version);
            if let Some(prepared) = known.and_then(|key| cache.touch(&key)) {
                return Ok(Some(prepared));
            }
            known
        };
        let Some(bundle) = bundle() else {
            return Ok(None);
        };
        let mut decoded = Vec::new();
        let source = Source::of(&bundle, &mut decoded)?;
        admit(&source, &shared)?;
        let key = known.unwrap_or_else(|| source.key());
        let prepared = self.prepare_keyed(source, key, shared)?;
        self.lock()?.remember(version, key);
        Ok(Some(prepared))
    }

    /// Whether `source`'s image is in the cache.
    pub(super) fn is_prepared(&self, source: Source<'_>) -> Result<bool> {
        Ok(self.cached(&source.key())?.is_some())
    }

    pub(super) fn prepare(&self, bundle: &str, shared: Arc<Limits>) -> Result<Arc<Prepared>> {
        self.prepare_source(Source::JavaScript(bundle), shared)
    }

    pub(super) fn prepare_source(
        &self,
        source: Source<'_>,
        shared: Arc<Limits>,
    ) -> Result<Arc<Prepared>> {
        admit(&source, &shared)?;
        self.prepare_keyed(source, source.key(), shared)
    }

    /// The image of `source`, whose content key is `key`.
    fn prepare_keyed(
        &self,
        source: Source<'_>,
        key: [u8; 32],
        shared: Arc<Limits>,
    ) -> Result<Arc<Prepared>> {
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
        // Compile in an instance of its own. The parser's garbage grows linear
        // memory, which never shrinks: initializing in the same instance would
        // measure and snapshot that garbage along with the application's heap.
        let bytecode = {
            let (mut store, _, abi) = profile::observe("initialize", || {
                initialize(&self.engine, &self.instrumented, shared.clone())
            })?;
            profile::observe("bytecode_compile", || abi.compile(&mut store, bundle))?
        };
        if !bundle.starts_with(STATIC_INIT_MARKER) {
            return self.on_base_image(bytecode, &shared);
        }
        // Explicit opt-in promises initialization independent of invocation.
        // Database calls fail here: no host callback is bound yet.
        let (mut store, instance, abi) = profile::observe("initialize", || {
            initialize(&self.engine, &self.instrumented, shared.clone())
        })?;
        profile::observe("static_initialize", || abi.bytecode(&mut store, &bytecode))?;
        let memory_bytes = abi.memory.data_size(&store);
        tracing::debug!(target: "flower::evaluator_profile", guest_memory_bytes = memory_bytes,
            initialized_snapshot = memory_bytes <= STATIC_SNAPSHOT_MAX_BYTES,
            "static QuickJS bundle image preparation");
        if memory_bytes > STATIC_SNAPSHOT_MAX_BYTES {
            fallback_warning(
                bundle,
                memory_bytes,
                "its initialized heap exceeds",
                STATIC_SNAPSHOT_MAX_BYTES,
            );
            return self.on_base_image(bytecode, &shared);
        }
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
        #[cfg(test)]
        hold_after_guest();
        // The guest code has run. Compiling its snapshot takes seconds on a busy
        // host but no guest time: finish it, and keep the image, even if this
        // caller runs out of time meanwhile (its evaluation still fails at its
        // next check), so that a retry finds the image instead of starting over.
        shared.check_sound()?;
        let (pre, compiled_bytes, reset_globals) =
            profile::observe("native_compile", || compile_image(&self.engine, &bytes))?;
        let weight = bytes.len() + memory_bytes + compiled_bytes;
        if weight > MAX_CACHE_BYTES {
            fallback_warning(
                bundle,
                weight,
                "its image would exceed the image cache's",
                MAX_CACHE_BYTES,
            );
            return self.on_base_image(bytecode, &shared);
        }
        let prepared = Prepared::new(pre, None, input_buffer, &reset_globals, weight)?;
        shared.check_sound()?;
        Ok(prepared)
    }

    /// Run a bundle from the shared base image: every callback loads its
    /// bytecode and runs its module code again.
    fn on_base_image(&self, bytecode: Vec<u8>, shared: &Limits) -> Result<Prepared> {
        let weight = bytecode.len() + self.base_memory_bytes;
        let prepared = Prepared::new(
            self.base.clone(),
            Some(bytecode),
            self.base_input,
            &self.base_reset_globals,
            weight,
        )?;
        shared.check_sound()?;
        Ok(prepared)
    }
}

/// Refuse a bundle past FLOWER_BUNDLE_MAX_BYTES, or a caller out of budget,
/// before hashing or compiling anything.
fn admit(source: &Source<'_>, shared: &Limits) -> Result<()> {
    ensure!(
        source.len() <= crate::evaluator::config::settings()?.bundle_max_bytes,
        "bundle exceeds FLOWER_BUNDLE_MAX_BYTES"
    );
    shared.check()
}

/// A static-init bundle that cannot keep its initialized snapshot still works,
/// but at a price the operator should hear about: every callback reloads its
/// bytecode and reruns its module code, for as long as it stays deployed.
fn fallback_warning(bundle: &str, bytes: usize, reason: &str, limit: usize) {
    tracing::warn!(
        bundle = %crate::evaluator::hash(bundle.as_bytes()),
        bytes,
        limit,
        "static-init bundle runs without its initialized snapshot: {reason} {} MiB, so every callback reloads its bytecode and reruns its module code",
        limit / (1024 * 1024),
    );
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
        shared.check_sound()?;
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

    fn prepare_static(code: String) -> Arc<Prepared> {
        runtime()
            .unwrap()
            .prepare(
                &format!("{STATIC_INIT_MARKER}{code}"),
                super::super::tests::limits(),
            )
            .unwrap()
    }

    fn call(prepared: &Prepared) -> Value {
        super::super::execute_prepared(
            prepared,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(Value::Null),
            super::super::tests::limits(),
        )
        .unwrap()
    }

    #[test]
    fn an_image_whose_caller_ran_out_of_time_after_its_guest_code_is_kept() {
        // On a busy host, compiling a snapshot can take longer than the caller
        // that asked for it may run: a deployment or the first call after a
        // restart then failed, and so did each retry, compiling it all again.
        let bundle = format!(
            "{STATIC_INIT_MARKER}{}",
            super::super::tests::bundle("()=>'kept past its caller'", false)
        );
        let runtime = runtime().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let late = Limits::new(deadline, super::super::MAX_MEMORY_BYTES);
        HOLD_AFTER_GUEST.with(|hold| hold.set(Some(deadline + Duration::from_millis(20))));
        let prepared = runtime.prepare(&bundle, late.clone());
        HOLD_AFTER_GUEST.with(|hold| hold.set(None));
        let prepared = prepared.expect("the compiled image is returned to its late caller");
        assert!(late.check().is_err(), "the caller itself is out of time");
        assert!(
            prepared.bytecode.is_none(),
            "its initialized snapshot is kept"
        );
        let again = runtime
            .prepare(&bundle, super::super::tests::limits())
            .unwrap();
        assert!(Arc::ptr_eq(&prepared, &again), "a retry finds the image");
        assert_eq!(
            call(&again),
            json!({"ok":true,"value":"kept past its caller"})
        );
        // Guest code that runs past its deadline still fails, and is not kept.
        let looping = format!(
            "{STATIC_INIT_MARKER}for(;;){{}}{}",
            super::super::tests::bundle("()=>1", false)
        );
        let short = Limits::new(
            Instant::now() + Duration::from_millis(200),
            super::super::MAX_MEMORY_BYTES,
        );
        assert!(runtime.prepare(&looping, short).is_err());
        assert!(
            runtime
                .cached(&Source::JavaScript(&looping).key())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn static_bundles_keep_their_snapshot_past_eight_mib() {
        // Trinity's initialized heap outgrew 8 MiB, the old limit, and from then on
        // every callback reloaded its bytecode and reran its module code.
        let prepared = prepare_static(format!(
            "const heap=new Uint8Array(12*1024*1024).fill(7);let runs=0;{}",
            super::super::tests::bundle("()=>[heap.length,heap[12345],++runs]", false),
        ));
        assert!(
            prepared.bytecode.is_none(),
            "a 12 MiB initialized heap keeps its snapshot"
        );
        assert!(prepared.image.memory_bytes > 12 * 1024 * 1024);
        for _ in 0..2 {
            assert_eq!(
                call(&prepared),
                json!({"ok":true,"value":[12*1024*1024,7,1]})
            );
        }
    }

    #[test]
    fn compiling_leaves_no_garbage_in_the_initialized_snapshot() {
        let code = super::super::tests::bundle("()=>1", false);
        let plain = prepare_static(code.clone());
        // The parser holds this source, then drops it: none of it is the heap.
        let padded = prepare_static(format!("/*{}*/{code}", "x".repeat(1536 * 1024)));
        assert!(plain.bytecode.is_none() && padded.bytecode.is_none());
        assert!(
            padded.image.memory_bytes <= plain.image.memory_bytes + 256 * 1024,
            "compiling 1.5 MiB of source grew the snapshot from {} to {} bytes",
            plain.image.memory_bytes,
            padded.image.memory_bytes,
        );
        assert_eq!(call(&padded), json!({"ok":true,"value":1}));
    }

    #[test]
    fn heaps_past_the_snapshot_limit_still_run_from_the_base_image() {
        let length = STATIC_SNAPSHOT_MAX_BYTES + 1024 * 1024;
        let prepared = prepare_static(format!(
            "const heap=new Uint8Array({length}).fill(7);{}",
            super::super::tests::bundle("()=>heap.length", false),
        ));
        assert!(
            prepared.bytecode.is_some(),
            "a heap past the limit reruns its module code in every callback"
        );
        assert_eq!(call(&prepared), json!({"ok":true,"value":length}));
    }

    #[test]
    fn stored_bundle_versions_find_cached_images_only_and_stay_bounded() {
        let prepared = runtime()
            .unwrap()
            .prepare(
                "var __flowerBundle={default:{definitions:{},http:{}}};",
                super::super::tests::limits(),
            )
            .unwrap();
        let mut cache = Cache::default();
        cache.images.push_back(([7; 32], prepared.clone()));
        cache.remember(5, [7; 32]);
        assert_eq!(cache.stored(5), Some([7; 32]));
        assert!(Arc::ptr_eq(&cache.touch(&[7; 32]).unwrap(), &prepared));
        cache.images.clear();
        assert_eq!(
            cache.stored(5),
            Some([7; 32]),
            "the key outlives its image, sparing a hash"
        );
        assert!(
            cache.touch(&[7; 32]).is_none(),
            "versions cannot keep an evicted image alive"
        );
        for version in 100..200 {
            cache.remember(version, [8; 32]);
            // The version in use stays while others come and go.
            cache.stored(5);
        }
        assert_eq!(cache.stored.len(), MAX_STORED_VERSIONS);
        assert_eq!(cache.stored(5), Some([7; 32]));
        assert_eq!(cache.stored(100), None, "the least recently used left");
    }

    #[test]
    fn stored_bundles_are_read_and_hashed_once_per_version() {
        let runtime = runtime().unwrap();
        let javascript = format!(
            "/*{}*/var __flowerBundle={{default:{{definitions:{{}},http:{{}}}}}};",
            "read once".repeat(64)
        );
        let bundle = Arc::new(json!({ "javascript": javascript }));
        let version = crate::consensus::next_version();
        let mut reads = 0;
        let mut images = Vec::new();
        for _ in 0..4 {
            let read = || {
                reads += 1;
                Some(bundle.clone())
            };
            let prepared = runtime
                .prepare_stored_bundle(version, read, super::super::tests::limits())
                .unwrap()
                .expect("a bundle");
            images.push(prepared);
        }
        assert_eq!(reads, 1);
        assert_eq!(hashes(&javascript), 1);
        assert!(
            images
                .windows(2)
                .all(|pair| Arc::ptr_eq(&pair[0], &pair[1]))
        );
        assert_eq!(
            call(&images[0])["ok"],
            false,
            "the empty bundle has no test method"
        );
        let without_code = runtime
            .prepare_stored_bundle(
                crate::consensus::next_version(),
                || None,
                super::super::tests::limits(),
            )
            .unwrap();
        assert!(without_code.is_none());
    }
}
