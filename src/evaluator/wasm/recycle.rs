//! Recycle resident storage, never logical interpreter state. Idle instances
//! form one process-wide pool per image. A background thread restores each
//! returned instance's written pages from the pristine snapshot, so callers
//! rarely pay for a reset, and any thread can check the instance out again.
//! No callback, transaction budget or key cache survives into the pool. Final
//! Wasm images are checked by reset_surface first.
use super::{Abi, Callback, Host, Limits, MemoryLimit, Prepared};
use anyhow::{Context, Result, ensure};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};
use wasmtime::{Engine, Global, Module, ModuleExport, Store, Val};

/// Idle instances unused for this long are released by the background thread.
const IDLE_LIFETIME: Duration = Duration::from_secs(30);

pub(super) struct Image {
    globals: Vec<ModuleExport>,
    pub(super) memory_bytes: usize,
    initial: OnceLock<Snapshot>,
    learning: Arc<super::dirty::Learning>,
    #[cfg(test)]
    stats: test_stats::Counters,
}

struct Snapshot {
    memory: Box<[u8]>,
    globals: Vec<Number>,
}

// Store-independent bit representations only. Wasmtime Val also supports rooted
// references; those must never become shared snapshot state.
#[derive(Clone, Copy)]
enum Number {
    I32(i32),
    I64(i64),
    F32(u32),
    F64(u64),
    V128(u128),
}

impl Number {
    fn capture(value: Val) -> Result<Self> {
        Ok(match value {
            Val::I32(value) => Self::I32(value),
            Val::I64(value) => Self::I64(value),
            Val::F32(value) => Self::F32(value),
            Val::F64(value) => Self::F64(value),
            Val::V128(value) => Self::V128(value.as_u128()),
            _ => anyhow::bail!("reset snapshot contains a reference-valued global"),
        })
    }

    fn value(self) -> Val {
        match self {
            Self::I32(value) => Val::I32(value),
            Self::I64(value) => Val::I64(value),
            Self::F32(value) => Val::F32(value),
            Self::F64(value) => Val::F64(value),
            Self::V128(value) => Val::V128(value.into()),
        }
    }
}

impl Image {
    pub(super) fn new(module: &Module, globals: &[String]) -> Result<Self> {
        let memory = module
            .get_export("memory")
            .and_then(|ty| ty.memory().cloned())
            .context("reset image memory")?;
        let memory_bytes = usize::try_from(memory.minimum())?
            .checked_mul(65536)
            .context("reset image memory size overflow")?;
        let globals = globals
            .iter()
            .map(|name| module.get_export_index(name).context("reset global export"))
            .collect::<Result<_>>()?;
        Ok(Self {
            globals,
            memory_bytes,
            initial: OnceLock::new(),
            learning: Arc::default(),
            #[cfg(test)]
            stats: Default::default(),
        })
    }

    fn key(self: &Arc<Self>) -> usize {
        Arc::as_ptr(self) as usize
    }
}

pub(super) struct Cell {
    pub(super) store: Store<Host>,
    pub(super) abi: Abi,
    image: Arc<Image>,
    globals: Vec<Global>,
    recyclable: bool,
    dirty: Option<Arc<super::dirty::Tracker>>,
    #[cfg(test)]
    reported_faults: usize,
}

impl Drop for Cell {
    fn drop(&mut self) {
        // This runs before Store's fields/destructor, including traps, native
        // panics and partially completed setup. Never return a protected mapping
        // to Wasmtime's allocator if OS cleanup fails.
        if let Some(dirty) = &self.dirty
            && dirty.unprotect().is_err()
        {
            std::process::abort();
        }
    }
}

struct Idle {
    cell: Cell,
    since: Instant,
    _reservation: Reservation,
}

impl Idle {
    /// Leave the pool: return its budget now, since destroying a Store (which
    /// returns Wasmtime slots and unprotects dirty tracking) can take a while.
    fn into_cell(self) -> Cell {
        self.cell
    }
}

// SAFETY: a Store is Send when its data is. Host is not Send only because an
// active invocation's scoped Callback token points into its caller's stack.
// finish() replaces the Host with an inert one, without a callback, before an
// instance enters the pool, and checkout() rebinds the dirty tracker's errno
// slot to the executing thread before any Wasm runs there.
unsafe impl Send for Idle {}

struct State {
    // Pristine instances, ready for any thread.
    ready: Vec<Idle>,
    // Returned instances that still need their written pages restored.
    pending: VecDeque<Idle>,
    // Images of instances the background thread is resetting right now.
    resetting: Vec<usize>,
    // The background thread is waiting for work, and callers are waiting for
    // one of its resets: only then does anyone pay for a wakeup.
    sleeping: bool,
    waiters: usize,
}

struct Pool {
    state: Mutex<State>,
    work: Condvar,
    changed: Condvar,
}

static POOL: Pool = Pool {
    state: Mutex::new(State {
        ready: Vec::new(),
        pending: VecDeque::new(),
        resetting: Vec::new(),
        sleeping: false,
        waiters: 0,
    }),
    work: Condvar::new(),
    changed: Condvar::new(),
};

impl Pool {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        !std::env::var("FLOWER_WASM_RECYCLE").is_ok_and(|value| value == "0" || value == "false")
    })
}

fn start_resetter() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        // Without the thread, checkout() still resets queued instances inline.
        let _ = std::thread::Builder::new()
            .name("flower-wasm-reset".into())
            .spawn(resetter);
    });
}

fn resetter() {
    let mut state = POOL.lock();
    loop {
        let Some(mut idle) = state.pending.pop_front() else {
            state.sleeping = true;
            state = POOL
                .work
                .wait_timeout(state, IDLE_LIFETIME / 4)
                .unwrap_or_else(|error| error.into_inner())
                .0;
            state.sleeping = false;
            let expired = take_expired(&mut state);
            if !expired.is_empty() {
                // Store destruction returns slots to Wasmtime; keep it unlocked.
                drop(state);
                drop(expired);
                state = POOL.lock();
            }
            continue;
        };
        let key = idle.cell.image.key();
        state.resetting.push(key);
        drop(state);
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reset(&mut idle.cell)));
        state = POOL.lock();
        if let Some(position) = state.resetting.iter().position(|item| *item == key) {
            state.resetting.swap_remove(position);
        }
        if state.waiters > 0 {
            POOL.changed.notify_all();
        }
        if matches!(outcome, Ok(Ok(_))) {
            idle.since = Instant::now();
            state.ready.push(idle);
        } else {
            drop(state);
            discard(idle.into_cell());
            state = POOL.lock();
        }
    }
}

fn take_expired(state: &mut State) -> Vec<Cell> {
    let mut expired = Vec::new();
    let mut index = 0;
    while index < state.ready.len() {
        if state.ready[index].since.elapsed() >= IDLE_LIFETIME {
            expired.push(state.ready.swap_remove(index).into_cell());
        } else {
            index += 1;
        }
    }
    expired
}

/// Make room for a returning instance: release the least recently used idle
/// instance of another image. Returns whether anything was released.
fn evict_least_recent(except: usize) -> bool {
    fn oldest<'a>(idle: impl Iterator<Item = &'a Idle>, except: usize) -> Option<(Instant, usize)> {
        idle.enumerate()
            .filter(|(_, idle)| idle.cell.image.key() != except)
            .min_by_key(|(_, idle)| idle.since)
            .map(|(position, idle)| (idle.since, position))
    }
    let mut state = POOL.lock();
    // Instances still queued for reset hold budget too; under churn they can
    // be the only idle instances left.
    let evicted = match (
        oldest(state.ready.iter(), except),
        oldest(state.pending.iter(), except),
    ) {
        (Some((ready, _)), Some((pending, position))) if pending < ready => {
            state.pending.remove(position)
        }
        (Some((_, position)), _) => Some(state.ready.swap_remove(position)),
        (None, Some((_, position))) => state.pending.remove(position),
        (None, None) => None,
    };
    drop(state);
    let evicted = evicted.map(Idle::into_cell);
    evicted.is_some()
}

/// Release every idle instance, for example when Wasm pool slots run out.
fn evict_ready() {
    let evicted = std::mem::take(&mut POOL.lock().ready);
    let evicted: Vec<Cell> = evicted.into_iter().map(Idle::into_cell).collect();
    drop(evicted);
}

/// Restore every byte this instance may have written, including the C stack,
/// dead allocations, invocation inputs, crypto buffers and retained results.
/// Guest code never runs until checkout attaches fresh invocation capabilities.
fn reset(cell: &mut Cell) -> Result<usize> {
    let initial = cell
        .image
        .initial
        .get()
        .context("missing pristine reset image")?;
    let memory = cell.abi.memory.data_mut(&mut cell.store);
    let (copied, reusable) = if let Some(dirty) = &cell.dirty {
        let restored = dirty.restore(memory, &initial.memory)?;
        (restored.copied, restored.reusable)
    } else {
        memory.copy_from_slice(&initial.memory);
        (memory.len(), true)
    };
    for (global, initial) in cell.globals.iter().zip(&initial.globals) {
        global.set(&mut cell.store, initial.value())?;
    }
    #[cfg(test)]
    {
        let faults = cell.dirty.as_ref().map_or(0, |dirty| dirty.faults());
        cell.image.stats.reset(
            faults - cell.reported_faults,
            copied,
            cell.image.memory_bytes,
        );
        cell.reported_faults = faults;
    }
    // Memory is pristine either way, but tracking could not be re-armed.
    ensure!(reusable, "dirty-page protection failed after reset");
    Ok(copied)
}

/// A ready instance of this image, or a queued one (`true`: not yet reset).
/// While the background thread resets one of its instances, waiting for it is
/// cheaper than instantiating another.
fn take(image: &Arc<Image>) -> Option<(Cell, bool)> {
    let key = image.key();
    let mut state = POOL.lock();
    loop {
        if let Some(position) = state
            .ready
            .iter()
            .position(|idle| idle.cell.image.key() == key)
        {
            return Some((state.ready.swap_remove(position).into_cell(), false));
        }
        if let Some(position) = state
            .pending
            .iter()
            .position(|idle| idle.cell.image.key() == key)
        {
            let idle = state.pending.remove(position).expect("queued instance");
            return Some((idle.into_cell(), true));
        }
        if !state.resetting.contains(&key) {
            return None;
        }
        state = wait(state);
    }
}

/// Wait for the background thread to finish a reset.
fn wait(mut state: MutexGuard<'_, State>) -> MutexGuard<'_, State> {
    state.waiters += 1;
    let mut state = POOL
        .changed
        .wait(state)
        .unwrap_or_else(|error| error.into_inner());
    state.waiters -= 1;
    state
}

#[derive(Default)]
struct Occupancy {
    slots: usize,
    bytes: usize,
}
static OCCUPANCY: Mutex<Occupancy> = Mutex::new(Occupancy { slots: 0, bytes: 0 });

struct Reservation {
    bytes: usize,
}
impl Reservation {
    fn acquire(bytes: usize) -> Option<Self> {
        let settings = super::super::config::settings().ok()?;
        let slots = settings.wasm_pool_slots as usize / 2;
        let mut occupied = OCCUPANCY.lock().unwrap_or_else(|error| error.into_inner());
        if occupied.slots >= slots
            || bytes > settings.wasm_recycle_bytes.saturating_sub(occupied.bytes)
        {
            return None;
        }
        occupied.slots += 1;
        occupied.bytes += bytes;
        Some(Self { bytes })
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut occupied = OCCUPANCY.lock().unwrap_or_else(|error| error.into_inner());
        occupied.slots -= 1;
        occupied.bytes -= self.bytes;
    }
}

fn attach(cell: &mut Cell, shared: Arc<Limits>, callback: Callback) -> Result<()> {
    let memory = MemoryLimit::existing(shared.clone(), cell.image.memory_bytes)?;
    let mut host = Host::new(shared, Some(callback));
    host.memory = memory;
    host.abi = Some(cell.abi.clone());
    host.dirty = cell.dirty.clone();
    host.memory.track(cell.dirty.clone());
    *cell.store.data_mut() = host;
    cell.store.set_epoch_deadline(1);
    if let Some(dirty) = &cell.dirty {
        dirty.bind_thread();
    }
    Ok(())
}

pub(super) fn checkout(
    prepared: &Prepared,
    engine: &Engine,
    shared: Arc<Limits>,
    callback: Callback,
    profile: &Option<Arc<super::super::profile::Invocation>>,
) -> Result<Cell> {
    let settings = super::super::config::settings()?;
    let recyclable = enabled()
        && settings.wasm_pool_slots > 1
        && settings.wasm_recycle_bytes > 0
        && prepared.image.memory_bytes <= settings.wasm_recycle_bytes;
    if recyclable && let Some((mut cell, queued)) = take(&prepared.image) {
        // The background thread fell behind: reset this one here.
        let copied = if queued {
            match reset(&mut cell) {
                Ok(copied) => copied,
                Err(_) => {
                    discard(cell);
                    return fresh(prepared, engine, shared, callback, profile, recyclable);
                }
            }
        } else {
            0
        };
        attach(&mut cell, shared, callback)?;
        #[cfg(test)]
        cell.image.stats.reused();
        super::super::profile::cell_storage(profile, true, true, cell.image.memory_bytes);
        super::super::profile::cell_reset(profile, false, copied);
        return Ok(cell);
    }
    fresh(prepared, engine, shared, callback, profile, recyclable)
}

fn fresh(
    prepared: &Prepared,
    engine: &Engine,
    shared: Arc<Limits>,
    callback: Callback,
    profile: &Option<Arc<super::super::profile::Invocation>>,
    recyclable: bool,
) -> Result<Cell> {
    let mut store = super::store(engine, shared.clone(), Some(callback));
    let instance = match prepared.pre.instantiate(&mut store) {
        Ok(instance) => instance,
        Err(_) => {
            // Idle instances must never keep an active callback, including a
            // recursively entered one, from obtaining a pool slot.
            evict_ready();
            store = super::store(engine, shared, Some(callback));
            prepared.pre.instantiate(&mut store).map_err(|error| error.context(
                "Wasm instance allocation failed; check FLOWER_WASM_POOL_SLOTS and FLOWER_GUEST_MEMORY_BYTES"
            ))?
        }
    };
    let abi = Abi::load_cached(&mut store, instance, &prepared.exports)?;
    store.data_mut().abi = Some(abi.clone());
    let mut globals = Vec::new();
    if recyclable {
        for export in &prepared.image.globals {
            globals.push(
                instance
                    .get_module_export(&mut store, export)
                    .and_then(|export| export.into_global())
                    .context("reset global handle")?,
            );
        }
        ensure!(
            abi.memory.data_size(&store) == prepared.image.memory_bytes,
            "reset image initial memory size changed"
        );
        if prepared.image.initial.get().is_none() {
            let values = globals
                .iter()
                .map(|global| Number::capture(global.get(&mut store)))
                .collect::<Result<_>>()?;
            let snapshot = Snapshot {
                memory: abi.memory.data(&store).to_vec().into_boxed_slice(),
                globals: values,
            };
            // Cold requests can race; both observed an unexecuted instance of
            // the same immutable module. Retain only one complete pristine copy.
            let _ = prepared.image.initial.set(snapshot);
        }
    }
    #[cfg(test)]
    prepared.image.stats.created();
    super::super::profile::cell_storage(profile, false, recyclable, abi.memory.data_size(&store));
    let mut cell = Cell {
        store,
        abi,
        image: prepared.image.clone(),
        globals,
        recyclable,
        dirty: None,
        #[cfg(test)]
        reported_faults: 0,
    };
    if recyclable
        && let Some(dirty) = super::dirty::install(
            &mut cell.store,
            cell.abi.memory,
            cell.image.learning.clone(),
        )?
    {
        // Own cleanup before the first protection call; protection may fail
        // after changing only part of a mapping.
        cell.dirty = Some(dirty.clone());
        if dirty.protect().is_ok() {
            cell.store.data_mut().dirty = Some(dirty.clone());
            cell.store.data_mut().memory.track(Some(dirty));
        } else {
            dirty.unprotect()?;
            cell.dirty = None;
        }
    }
    Ok(cell)
}

pub(super) fn finish(
    mut cell: Cell,
    shared: &Arc<Limits>,
    profile: &Option<Arc<super::super::profile::Invocation>>,
) -> Result<()> {
    let grew = cell.abi.memory.data_size(&cell.store) != cell.image.memory_bytes;
    if !cell.recyclable || grew {
        super::super::profile::cell_reset(profile, grew, 0);
        discard(cell);
        return Ok(());
    }
    shared.check()?;
    ensure!(
        cell.image.initial.get().is_some(),
        "missing pristine reset image"
    );
    // Replacing the complete Host drops every authorization/key cache and the
    // callback token, and releases this transaction's memory charge now. Idle
    // budgets deny all work; checkout attaches and charges a fresh allowance.
    *cell.store.data_mut() = Host::new(Limits::new(Instant::now(), 0), None);
    let bytes = cell.image.memory_bytes;
    let reservation = loop {
        if let Some(reservation) = Reservation::acquire(bytes) {
            break Some(reservation);
        }
        if !evict_least_recent(cell.image.key()) {
            break None;
        }
    };
    let Some(reservation) = reservation else {
        discard(cell);
        return Ok(());
    };
    let idle = Idle {
        cell,
        since: Instant::now(),
        _reservation: reservation,
    };
    let mut state = POOL.lock();
    state.pending.push_back(idle);
    let wake = state.sleeping;
    drop(state);
    if wake {
        POOL.work.notify_one();
    }
    start_resetter();
    Ok(())
}

pub(super) fn discard(cell: Cell) {
    #[cfg(test)]
    cell.image.stats.discarded();
    drop(cell);
}

#[cfg(test)]
mod test_stats {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    pub(super) struct Counters {
        created: AtomicUsize,
        reused: AtomicUsize,
        discarded: AtomicUsize,
        signal_faults: AtomicUsize,
        reset_bytes: AtomicUsize,
        reset_total_bytes: AtomicUsize,
    }

    impl Counters {
        pub(super) fn created(&self) {
            self.created.fetch_add(1, Ordering::Relaxed);
        }
        pub(super) fn reused(&self) {
            self.reused.fetch_add(1, Ordering::Relaxed);
        }
        pub(super) fn discarded(&self) {
            self.discarded.fetch_add(1, Ordering::Relaxed);
        }
        pub(super) fn reset(&self, faults: usize, bytes: usize, total: usize) {
            self.signal_faults.fetch_add(faults, Ordering::Relaxed);
            self.reset_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.reset_total_bytes.fetch_add(total, Ordering::Relaxed);
        }
        pub(super) fn snapshot(&self, idle: usize) -> super::Stats {
            super::Stats {
                created: self.created.load(Ordering::Relaxed),
                reused: self.reused.load(Ordering::Relaxed),
                discarded: self.discarded.load(Ordering::Relaxed),
                idle,
                signal_faults: self.signal_faults.load(Ordering::Relaxed),
                reset_bytes: self.reset_bytes.load(Ordering::Relaxed),
                reset_total_bytes: self.reset_total_bytes.load(Ordering::Relaxed),
            }
        }
    }
}

#[cfg(test)]
#[derive(Default, Clone, Copy, Debug)]
pub(super) struct Stats {
    pub(super) created: usize,
    pub(super) reused: usize,
    pub(super) discarded: usize,
    pub(super) idle: usize,
    pub(super) signal_faults: usize,
    pub(super) reset_bytes: usize,
    pub(super) reset_total_bytes: usize,
}

/// Counters for one image. Waits for background resets first, so `idle` and
/// the reset totals are complete.
#[cfg(test)]
pub(super) fn stats(prepared: &Prepared) -> Stats {
    let key = prepared.image.key();
    let mut state = POOL.lock();
    while state.resetting.contains(&key)
        || state
            .pending
            .iter()
            .any(|idle| idle.cell.image.key() == key)
    {
        state = wait(state);
    }
    let idle = state
        .ready
        .iter()
        .filter(|idle| idle.cell.image.key() == key)
        .count();
    drop(state);
    prepared.image.stats.snapshot(idle)
}

/// Release this image's idle instances, as the background thread does once
/// they outlive IDLE_LIFETIME.
#[cfg(test)]
pub(super) fn release(prepared: &Prepared) {
    stats(prepared);
    let key = prepared.image.key();
    let mut state = POOL.lock();
    let mut released = Vec::new();
    let mut index = 0;
    while index < state.ready.len() {
        if state.ready[index].cell.image.key() == key {
            released.push(state.ready.swap_remove(index).into_cell());
        } else {
            index += 1;
        }
    }
    drop(state);
    drop(released);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_overwrites_every_guest_byte_and_mutable_global_and_detaches_host() {
        let shared = super::super::tests::limits();
        let prepared = super::super::prepare(
            &super::super::tests::bundle("()=>'reset every byte'", true),
            shared.clone(),
        )
        .unwrap();
        let runtime = super::super::cache::runtime().unwrap();
        let mut host = |_: &str, _: serde_json::Value| Ok(serde_json::Value::Null);
        let mut bridge: &mut dyn FnMut(&str, serde_json::Value) -> Result<serde_json::Value> =
            &mut host;
        // The callback lives until both checked-out Stores below are detached.
        let callback = unsafe { Callback::scoped(&mut bridge) };
        let mut cell =
            checkout(&prepared, &runtime.engine, shared.clone(), callback, &None).unwrap();
        let pristine = cell.abi.memory.data(&cell.store).to_vec();
        for global in &cell.globals {
            global.set(&mut cell.store, Val::I32(-17)).unwrap();
        }
        let length = cell.abi.memory.data_size(&cell.store);
        super::super::dirty::prepare_write(&mut cell.store, 0, length).unwrap();
        cell.abi.memory.data_mut(&mut cell.store).fill(0xa5);
        cell.store.data_mut().entropy_allowed = true;
        // Directly exercise reset without executing the deliberately corrupted
        // stack/heap. The production caller also detaches this pointer first.
        cell.store.data_mut().callback = None;
        finish(cell, &shared, &None).unwrap();
        assert_eq!(stats(&prepared).idle, 1);
        {
            let state = POOL.lock();
            let idle = state
                .ready
                .iter()
                .find(|idle| idle.cell.image.key() == prepared.image.key())
                .unwrap();
            let host = idle.cell.store.data();
            assert!(host.callback.is_none());
            assert!(host.abi.is_none());
            assert!(!host.entropy_allowed);
            assert!(host.managed_shared.is_empty());
            assert!(host.managed_authorizations.is_empty());
            assert!(host.shared.check().is_err(), "idle Stores cannot execute");
        }
        // Check the reset instance out on another thread: nothing about the
        // pristine image depends on where it was reset or last executed.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut host = |_: &str, _: serde_json::Value| Ok(serde_json::Value::Null);
                    let mut bridge: &mut dyn FnMut(
                        &str,
                        serde_json::Value,
                    ) -> Result<serde_json::Value> = &mut host;
                    // Detached by discard() before this thread's bridge ends.
                    let callback = unsafe { Callback::scoped(&mut bridge) };
                    let mut cell = checkout(
                        &prepared,
                        &runtime.engine,
                        super::super::tests::limits(),
                        callback,
                        &None,
                    )
                    .unwrap();
                    assert_eq!(cell.abi.memory.data(&cell.store), pristine);
                    for global in &cell.globals {
                        assert!(
                            matches!(global.get(&mut cell.store), Val::I32(value) if value == 1024 * 1024)
                        );
                    }
                    assert!(!cell.store.data().entropy_allowed);
                    discard(cell);
                })
                .join()
                .unwrap();
        });
        assert_eq!(stats(&prepared).reused, 1);
    }
}
