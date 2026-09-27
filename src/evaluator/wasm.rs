//! QuickJS cells in logically fresh heaps backed by reusable COW images.
//!
//! Rust owns coordination and database state. The shared engine, trusted compiled
//! code, linked imports and pristine guest snapshots survive calls; mutable guest
//! heaps and invocation host callbacks never do. Synchronous writer batches may
//! reuse resident storage after restoring every byte and mutable global.
mod abi;
mod cache;
mod code_cache;
mod crypto;
#[cfg(test)]
mod crypto_tests;
mod dirty;
mod host;
mod limits;
mod module;
mod native_profile;
mod recycle;
mod reset_surface;
mod shadow_stack;
mod surface;
#[cfg(test)]
mod tests;

use super::wire;
use abi::Abi;
use anyhow::{Result, ensure};
pub(super) use cache::Prepared;
use host::Callback;
pub use limits::Limits;
use limits::MemoryLimit;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use std::sync::Arc;
use wasmtime::{Engine, Store, UpdateDeadline};

#[cfg(test)]
const MAX_MEMORY_BYTES: usize = 128 * 1024 * 1024;
fn max_json_bytes() -> Result<usize> {
    Ok(super::config::settings()?.result_max_bytes)
}
pub const STATIC_INIT_MARKER: &str = "/* flower:static-init */";

struct Host {
    abi: Option<Abi>,
    callback: Option<Callback>,
    entropy_allowed: bool,
    managed_shared: std::collections::HashMap<String, crypto::SharedKey>,
    managed_shared_bytes: usize,
    managed_authorizations: std::collections::HashMap<String, crypto::CachedAuthorization>,
    managed_authorization_bytes: usize,
    shared: Arc<Limits>,
    memory: MemoryLimit,
    dirty: Option<Arc<dirty::Tracker>>,
}

impl Host {
    fn new(shared: Arc<Limits>, callback: Option<Callback>) -> Self {
        Self {
            abi: None,
            callback,
            entropy_allowed: false,
            managed_shared: std::collections::HashMap::new(),
            managed_shared_bytes: 0,
            managed_authorizations: std::collections::HashMap::new(),
            managed_authorization_bytes: 0,
            memory: MemoryLimit::new(shared.clone()),
            shared,
            dirty: None,
        }
    }
}

fn store(engine: &Engine, shared: Arc<Limits>, callback: Option<Callback>) -> Store<Host> {
    let mut store = Store::new(engine, Host::new(shared, callback));
    store.limiter(|host| &mut host.memory);
    store.set_epoch_deadline(1);
    store.epoch_deadline_callback(|context| {
        context
            .data()
            .shared
            .check()
            .map_err(wasmtime::Error::from_anyhow)?;
        Ok(UpdateDeadline::Continue(1))
    });
    store
}

/// Compile and initialize the shared sandbox before accepting timed requests.
pub fn warmup() -> Result<()> {
    cache::runtime().map(|_| ())
}

pub(super) fn prepare(bundle: &str, shared: Arc<Limits>) -> Result<Arc<Prepared>> {
    cache::runtime()?.prepare(bundle, shared)
}

/// Prepare a bundle being deployed: JavaScript or a guest module.
pub(super) fn prepare_bundle(bundle: &Value, shared: Arc<Limits>) -> Result<Arc<Prepared>> {
    let mut decoded = Vec::new();
    cache::runtime()?.prepare_source(cache::Source::of(bundle, &mut decoded)?, shared)
}

pub(super) fn prepare_shared_bundle(
    bundle: &Arc<Value>,
    shared: Arc<Limits>,
) -> Result<Arc<Prepared>> {
    cache::runtime()?.prepare_shared_bundle(bundle, shared)
}

#[cfg(test)]
fn execute(
    bundle: &str,
    name: &str,
    args: &Value,
    kind: &str,
    host: &mut dyn FnMut(&str, Value) -> Result<Value>,
    shared: Arc<Limits>,
) -> Result<Value> {
    let prepared = prepare(bundle, shared.clone())?;
    execute_prepared(&prepared, name, args, kind, host, shared)
}

/// Run one isolated callback from an image acquired once per transaction.
/// Tests see outcomes as {ok:true,value} or {ok:false,error:{code,message}}
/// envelopes; resource failures remain Err and poison the shared allowance.
#[cfg(test)]
pub(super) fn execute_prepared(
    prepared: &Prepared,
    name: &str,
    args: &Value,
    kind: &str,
    host: &mut dyn FnMut(&str, Value) -> Result<Value>,
    shared: Arc<Limits>,
) -> Result<Value> {
    Ok(
        match execute_prepared_profiled(prepared, name, args, kind, host, shared, &None)? {
            wire::Outcome::Success(value) => json!({"ok": true, "value": value}),
            wire::Outcome::Failure {
                code,
                message,
                details,
            } => {
                let mut error = json!({"code": code, "message": message});
                if let Some(details) = details {
                    error["details"] = details;
                }
                json!({"ok": false, "error": error})
            }
        },
    )
}

fn kind_number(kind: &str) -> Result<i32> {
    Ok(match kind {
        "query" => 0,
        "mutation" => 1,
        "transaction" => 2,
        "derived" => 3,
        _ => anyhow::bail!("unknown callback kind {kind}"),
    })
}

pub(super) fn execute_prepared_profiled(
    prepared: &Prepared,
    name: &str,
    args: &Value,
    kind: &str,
    host: &mut dyn FnMut(&str, Value) -> Result<Value>,
    shared: Arc<Limits>,
    profile: &Option<Arc<super::profile::Invocation>>,
) -> Result<wire::Outcome> {
    let _depth = shared.enter()?;
    let kind_number = kind_number(kind)?;
    let args = wire::encode(args)?;
    ensure!(
        args.len() <= max_json_bytes()?,
        "INPUT_INVALID: arguments exceed FLOWER_RESULT_MAX_BYTES"
    );
    let runtime = cache::runtime()?;
    shared.check()?;
    // SAFETY: the bridge is borrowed only while this function is active; every
    // Store using its pointer is dropped or detached below before the bridge or
    // caller's callback can go away. Host callbacks are synchronous
    // and confined to this thread, including recursively entered child Stores.
    let mut bridge = host;
    let callback = unsafe { Callback::scoped(&mut bridge) };
    let setup_timer = super::profile::timer(profile, super::profile::Stage::CellRuntime);
    let mut cell = recycle::checkout(prepared, &runtime.engine, shared.clone(), callback, profile)?;
    cell.store.data_mut().entropy_allowed = kind == "mutation";
    let result = (|| {
        let store = &mut cell.store;
        let abi = &cell.abi;
        drop(setup_timer);
        let load_timer = super::profile::timer(profile, super::profile::Stage::CellLoad);
        if let Some(bytecode) = prepared.bytecode.as_deref() {
            abi.bytecode(store, bytecode)?;
        }
        let invocation = abi.invocation(store, kind_number, name, &args, prepared.input_buffer)?;
        drop(load_timer);
        let run_timer = super::profile::timer(profile, super::profile::Stage::CellExecute);
        let encoded = abi.run_once(store, invocation)?;
        drop(run_timer);
        abi.outcome(store, encoded)
    })();
    // Never let the borrowed callback escape, including a failed evaluation.
    cell.store.data_mut().callback = None;
    if result.as_ref().err().is_some_and(is_guest_trap) {
        shared.fail();
    }
    if let Err(error) = shared.check() {
        recycle::discard(cell);
        return Err(error);
    }
    if result.is_ok() {
        let _reset_timer = super::profile::timer(profile, super::profile::Stage::CellReset);
        recycle::finish(cell, &shared, profile)?;
    } else {
        recycle::discard(cell);
    }
    result
}

fn is_guest_trap(error: &anyhow::Error) -> bool {
    error.downcast_ref::<wasmtime::Trap>().is_some()
}

#[cfg(test)]
pub(super) fn manifest(bundle: &str, shared: Arc<Limits>) -> Result<Value> {
    let prepared = prepare(bundle, shared.clone())?;
    manifest_prepared(&prepared, shared)
}

/// The guest's raw manifest, from a fresh isolated instance without host
/// capabilities. A failure is a deployment error carrying the guest's message.
pub(super) fn manifest_prepared(prepared: &Prepared, shared: Arc<Limits>) -> Result<Value> {
    let _depth = shared.enter()?;
    let runtime = cache::runtime()?;
    let mut store = store(&runtime.engine, shared.clone(), None);
    let result = (|| {
        let instance = prepared.pre.instantiate(&mut store).map_err(|error| error.context("Wasm instance allocation failed; check FLOWER_WASM_POOL_SLOTS and FLOWER_GUEST_MEMORY_BYTES"))?;
        let abi = Abi::load_cached(&mut store, instance, &prepared.exports)?;
        store.data_mut().abi = Some(abi.clone());
        if let Some(bytecode) = &prepared.bytecode {
            abi.bytecode(&mut store, bytecode)?;
        }
        match abi.manifest(&mut store)? {
            wire::Outcome::Success(manifest) => Ok(manifest),
            wire::Outcome::Failure { message, .. } => anyhow::bail!("{message}"),
        }
    })();
    drop(store);
    if result.as_ref().err().is_some_and(is_guest_trap) {
        shared.fail();
    }
    shared.check()?;
    result
}

pub(super) fn check_json(value: &Value) -> Result<()> {
    fn walk(value: &Value, depth: usize) -> Result<()> {
        ensure!(
            depth <= 128,
            "INVALID_VALUE: JSON nesting exceeds 128 levels"
        );
        match value {
            Value::Array(values) => {
                for value in values {
                    walk(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    walk(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(value, 1)
}

/// A host callback's error as the guest sees it: code, message and details.
pub(super) fn failure_parts(error: anyhow::Error) -> (String, String, Option<Value>) {
    let error = match error.downcast::<super::rust_engine::EngineError>() {
        Ok(error) => return (error.code, error.message, error.details),
        Err(error) => error,
    };
    let message = error.to_string();
    // Rust coordinator errors have a stable CODE: message display. Arbitrary
    // callback errors get COMPUTE_ERROR; never reinterpret punctuation as a code.
    match message.split_once(": ").filter(|(code, _)| {
        !code.is_empty() && code.bytes().all(|b| b.is_ascii_uppercase() || b == b'_')
    }) {
        Some((code, rest)) => (code.to_owned(), rest.to_owned(), None),
        None => ("COMPUTE_ERROR".to_owned(), message, None),
    }
}

/// Test-only access to the same bounded guest, used by independent JavaScript
/// reference algorithms without linking a second native interpreter.
#[cfg(test)]
pub(super) fn reference_script(code: &str, shared: Arc<Limits>) -> Result<String> {
    let prepared = prepare("", shared.clone())?;
    let _depth = shared.enter()?;
    let runtime = cache::runtime()?;
    let mut store = store(&runtime.engine, shared.clone(), None);
    let result = (|| {
        let instance = prepared.pre.instantiate(&mut store)?;
        let abi = Abi::load_cached(&mut store, instance, &prepared.exports)?;
        store.data_mut().abi = Some(abi.clone());
        abi.eval_string(&mut store, code)
    })();
    drop(store);
    shared.check()?;
    result
}
