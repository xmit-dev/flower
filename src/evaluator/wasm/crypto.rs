//! Borrow guest byte spans while computing; allocate the guest result only after
//! releasing every memory borrow. Neither NaCl inputs nor outputs traverse JSON.
use super::{Host, check_json};
use crate::crypto::{jwt, managed, nacl, webauthn};
use anyhow::{Context, Result, ensure};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{ops::Range, sync::Arc};
use wasmtime::Caller;
use zeroize::Zeroizing;

struct Output {
    kind: u32,
    bytes: Zeroizing<Vec<u8>>,
    integer: u32,
}

pub(super) struct SharedKey {
    prepared: Arc<managed::PreparedKey>,
    declaration: Value,
}

pub(super) struct CachedAuthorization {
    key: Arc<managed::AuthorizedKey>,
    bytes: usize,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedRequest {
    operation: String,
    key: Value,
    #[serde(default)]
    version: Option<String>,
    #[serde(default = "empty_options")]
    options: Value,
}
fn empty_options() -> Value {
    json!({})
}
impl Output {
    fn bytes(kind: u32, bytes: Vec<u8>) -> Self {
        Self {
            kind,
            bytes: Zeroizing::new(bytes),
            integer: 0,
        }
    }
    fn scalar(kind: u32) -> Self {
        Self::bytes(kind, Vec::new())
    }
}

fn arity(op: u32) -> Option<usize> {
    Some(match op {
        0 => 0,
        1 | 2 | 11 | 100 | 101 | 103 => 3,
        3 | 5 | 8 | 9 | 10 | 16 | 110 | 111 | 201 | 202 => 2,
        4 | 12..=15 | 17 => 1,
        6 | 7 | 102 | 200 => 4,
        _ => return None,
    })
}

fn options<T: DeserializeOwned>(input: &[u8]) -> Result<T> {
    Ok(serde_json::from_value(jwt::parse_json(input)?)?)
}

fn dispatch(op: u32, args: &[&[u8]], now: u64) -> Result<Output> {
    if op < 100 {
        return Ok(match nacl::execute(op, args)? {
            nacl::Output::Bytes(bytes) => Output::bytes(0, bytes),
            nacl::Output::Bool(value) => Output::scalar(if value { 2 } else { 1 }),
            nacl::Output::Null => Output::scalar(3),
        });
    }
    let result = match op {
        100 | 102 => {
            let claims = jwt::parse_json(args[0])?;
            check_json(&claims)?;
            if op == 100 {
                jwt::sign(&claims, args[1], &options(args[2])?)?
            } else {
                jwt::encrypt(&claims, args[1], args[2], &options(args[3])?)?
            }
        }
        101 => serde_json::to_string(&jwt::verify(
            std::str::from_utf8(args[0])?,
            args[1],
            &options(args[2])?,
            now,
        )?)?,
        110 => serde_json::to_string(&webauthn::verify_registration(args[0], &options(args[1])?)?)?,
        111 => serde_json::to_string(&webauthn::verify_authentication(
            args[0],
            &options(args[1])?,
        )?)?,
        _ => anyhow::bail!("unsupported crypto operation"),
    };
    Ok(Output::bytes(6, result.into_bytes()))
}

fn clock_tolerance(options: &Value) -> f64 {
    options
        .get("clockToleranceSeconds")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

/// A decrypted token's watches wake when it expires. Only the key opens the
/// claims, so a failure can't say when decryption would succeed: that reads
/// the clock as ctx.now() does.
fn declare_decryption_change(
    caller: &Caller<'_, Host>,
    claims: Option<&Value>,
    tolerance: f64,
    now: u64,
) -> Result<()> {
    let Some(callback) = caller.data().callback else {
        return Ok(());
    };
    let (operation, arguments) = match claims {
        None => ("now", json!([])),
        Some(claims) => match jwt::claims_change(claims, tolerance, now) {
            Some(time) => ("changesAt", json!([time])),
            None => return Ok(()),
        },
    };
    // SAFETY: same synchronous scoped lifetime as host_call.
    unsafe { callback.invoke(operation, arguments)? };
    Ok(())
}

fn decrypt(caller: &Caller<'_, Host>, args: &[&[u8]], now: u64) -> Result<Output> {
    let options: jwt::DecryptOptions = options(args[2])?;
    let decrypted = jwt::decrypt(std::str::from_utf8(args[0])?, args[1], &options, now);
    let claims = decrypted.as_ref().ok().map(|verified| &verified.claims);
    declare_decryption_change(caller, claims, options.clock_tolerance_seconds, now)?;
    Ok(Output::bytes(6, serde_json::to_string(&decrypted?)?.into_bytes()))
}

/// Watches of a verified token wake when it expires or activates, not on a timer.
fn declare_verification_change(caller: &Caller<'_, Host>, token: &[u8], tolerance: f64, now: u64) -> Result<()> {
    let token = std::str::from_utf8(token).unwrap_or_default();
    if let (Some(time), Some(callback)) = (jwt::changes_at(token, tolerance, now), caller.data().callback) {
        // SAFETY: same synchronous scoped lifetime as host_call.
        unsafe { callback.invoke("changesAt", json!([time]))? };
    }
    Ok(())
}

fn bounded_range(pointer: u32, length: usize, size: usize) -> Result<std::ops::Range<usize>> {
    let start = pointer as usize;
    let end = start.checked_add(length).context("crypto span overflow")?;
    ensure!(end <= size, "crypto span outside guest memory");
    Ok(start..end)
}

pub(super) fn call(
    mut caller: Caller<'_, Host>,
    op: i32,
    parameter: i32,
    spans_pointer: i32,
    span_count: i32,
    result_pointer: i32,
) -> wasmtime::Result<i32> {
    (|| -> Result<i32> {
        let shared = caller.data().shared.clone();
        shared.check()?;
        let settings = super::super::config::settings()?;
        let abi = caller.data().abi.clone().context("guest ABI not bound")?;
        let op = op as u32;
        let count = span_count as u32 as usize;
        let size = abi.memory.data_size(&caller);
        let result_range = bounded_range(result_pointer as u32, 12, size)?;
        // Reject unknown operations/counts before allocating a descriptor array.
        // Protocol arities are fixed; they are not operator concurrency limits.
        let valid = arity(op) == Some(count) && (matches!(op, 0 | 201 | 202) || parameter == 0);
        let outcome = (|| -> Result<Output> {
            ensure!(
                valid,
                "CRYPTO_INVALID: invalid operation, parameter or argument count"
            );
            let descriptors = bounded_range(spans_pointer as u32, count * 8, size)?;
            let now = if matches!(op, 101 | 103) {
                let callback = caller
                    .data()
                    .callback
                    .context("JWT validation requires an invocation clock")?;
                // SAFETY: same synchronous scoped lifetime as host_call. This
                // also records a time dependency, preventing stale query caching.
                // Validation then reports when its outcome changes.
                unsafe { callback.invoke("clock", Value::Array(Vec::new()))? }
                    .as_u64()
                    .context("invalid invocation clock")?
            } else {
                0
            };
            if op == 0 {
                ensure!(
                    caller.data().entropy_allowed && caller.data().callback.is_some(),
                    "CRYPTO_RANDOM_FORBIDDEN: system randomness is available only in mutations"
                );
                let length = parameter as u32 as usize;
                budget(length, settings.result_max_bytes, &shared)?;
                budget(length, settings.rust_memory_bytes, &shared)?;
                let mut bytes = Zeroizing::new(vec![0; length]);
                getrandom::fill(&mut bytes).context("system randomness unavailable")?;
                return Ok(Output::bytes(0, std::mem::take(&mut *bytes)));
            }
            let memory = abi.memory.data(&caller);
            let mut ranges: [Range<usize>; 4] = std::array::from_fn(|_| 0..0);
            let mut total = 0usize;
            for (range, descriptor) in ranges
                .iter_mut()
                .zip(memory[descriptors].as_chunks::<8>().0)
            {
                let pointer = u32::from_le_bytes(descriptor[..4].try_into()?);
                let length = u32::from_le_bytes(descriptor[4..].try_into()?) as usize;
                *range = bounded_range(pointer, length, size)?;
                total = total
                    .checked_add(length)
                    .context("crypto input size overflow")?;
            }
            budget(total, settings.result_max_bytes, &shared)?;
            if matches!(op, 200..=202) {
                budget(
                    total.saturating_mul(64),
                    settings.rust_memory_bytes,
                    &shared,
                )?;
                return if op == 200 {
                    managed_dispatch(&mut caller, &ranges)
                } else {
                    shared_dispatch(&mut caller, op, parameter as u32, &ranges)
                };
            }
            let inputs = ranges.map(|range| &memory[range]);
            let args = &inputs[..count];
            // Reserve a conservative workspace allowance before native code:
            // JWT JSON/ASN.1/base64 uses more space than borrowed NaCl buffers.
            let workspace = if op >= 100 {
                total.saturating_mul(64)
            } else {
                let output = nacl::output_len(op, args)?;
                budget(output, settings.result_max_bytes, &shared)?;
                output.saturating_mul(2)
            };
            budget(workspace, settings.rust_memory_bytes, &shared)?;
            if op == 101 {
                let tolerance =
                    jwt::parse_json(args[2]).map_or(0.0, |options| clock_tolerance(&options));
                declare_verification_change(&caller, args[0], tolerance, now)?;
            }
            if op == 103 {
                return decrypt(&caller, args, now);
            }
            dispatch(op, args, now)
        })();
        // Native calls cannot be interrupted midway by Wasmtime epochs. A late
        // result poisons the transaction, even if JS would catch its exception.
        shared.check()?;
        let output = match outcome {
            Ok(output) => output,
            Err(error) => Output::bytes(5, format!("CRYPTO_ERROR: {error}").into_bytes()),
        };
        budget(output.bytes.len(), settings.result_max_bytes, &shared)?;
        let pointer = if matches!(output.kind, 0 | 5 | 6) {
            // abi.bytes may grow memory. No guest input is still borrowed here.
            abi.bytes(&mut caller, &output.bytes)? as u32
        } else {
            0
        };
        let mut descriptor = [0u8; 12];
        descriptor[..4].copy_from_slice(&output.kind.to_le_bytes());
        descriptor[4..8].copy_from_slice(&pointer.to_le_bytes());
        let length = if output.kind == 7 {
            output.integer
        } else {
            output.bytes.len() as u32
        };
        descriptor[8..].copy_from_slice(&length.to_le_bytes());
        super::dirty::prepare_write(&mut caller, result_range.start, descriptor.len())?;
        abi.memory
            .write(&mut caller, result_range.start, &descriptor)?;
        shared.check()?;
        Ok(0)
    })()
    .map_err(wasmtime::Error::from_anyhow)
}

fn managed_dispatch(caller: &mut Caller<'_, Host>, ranges: &[Range<usize>; 4]) -> Result<Output> {
    let abi = caller.data().abi.clone().context("guest ABI not bound")?;
    let callback = caller
        .data()
        .callback
        .context("Managed keys require an invocation")?;
    let request: ManagedRequest = options(&abi.memory.data(&*caller)[ranges[0].clone()])?;
    ensure!(
        request.options.is_object(),
        "Managed crypto options must be an object"
    );
    check_json(&request.key)?;
    check_json(&request.options)?;
    ensure!(
        request.key["kind"] == "key",
        "Managed operations require a key declaration; shared keys require the native handle bridge"
    );
    // Parse only public routing metadata before entering a context callback. No
    // guest memory borrow may survive a recursive host invocation.
    let kid = {
        let memory = abi.memory.data(&*caller);
        let args = std::array::from_fn::<_, 3, _>(|i| &memory[ranges[i + 1].clone()]);
        managed::requested_kid(&request.operation, &args)?
    };
    if let Some(version) = &request.version {
        ensure!(
            !matches!(request.operation.as_str(), "jwt.verify" | "jwt.decrypt") || kid.is_some(),
            "Managed JWT requires a versioned kid"
        );
        ensure!(
            kid.as_ref().is_none_or(|kid| kid == version),
            "Explicit version differs from authenticated JWT selector"
        );
    }
    let kid = request.version.or(kid);
    let declaration = &request.key;
    let operation = request.operation.as_str();
    let (cache_budget, ttl) = managed::invocation_cache_settings()?;
    let memory_budget = super::super::config::settings()?.rust_memory_bytes;
    let cache_budget = cache_budget.min(memory_budget);
    // Each Host belongs to exactly one callback/dependency collector and one
    // immutable policy snapshot. Reuse only a successful authorization for
    // the same declaration, operation and version selector within that scope.
    let identity = if cache_budget > 0 {
        Some(serde_json::to_string(&json!([
            declaration,
            operation,
            kid
        ]))?)
    } else {
        None
    };
    let mut authorized = identity.as_ref().and_then(|id| {
        caller
            .data()
            .managed_authorizations
            .get(id)
            .filter(|entry| entry.key.reusable(ttl))
            .map(|entry| entry.key.clone())
    });
    let mut resolved = None;
    if authorized.is_none() {
        if let Some(id) = &identity
            && let Some(expired) = caller.data_mut().managed_authorizations.remove(id)
        {
            caller.data_mut().managed_authorization_bytes -= expired.bytes;
        }
        // SAFETY: same synchronous scoped lifetime as host_call. This initial
        // resolution records dependencies even when the operation later fails.
        let value = unsafe {
            callback.invoke(
                "managedKey",
                json!([{"key":declaration,"operation":operation,"kid":kid}]),
            )?
        };
        if let Some(id) = identity {
            let key = Arc::new(managed::prepare_resolved(&value)?);
            let weight = key
                .retained_bytes()
                .saturating_add(id.len())
                .saturating_add(128);
            let next_bytes = caller
                .data()
                .managed_authorization_bytes
                .saturating_add(weight);
            if next_bytes <= cache_budget
                && next_bytes.saturating_add(caller.data().managed_shared_bytes) <= memory_budget
            {
                caller.data_mut().managed_authorizations.insert(
                    id,
                    CachedAuthorization {
                        key: key.clone(),
                        bytes: weight,
                    },
                );
                caller.data_mut().managed_authorization_bytes = next_bytes;
            }
            authorized = Some(key);
        } else {
            resolved = Some(value);
        }
    }
    let now = if matches!(operation, "jwt.verify" | "jwt.decrypt") {
        // As for raw keys: this records the time dependency that prevents
        // caching validity past exp. Validation then reports when its
        // outcome changes.
        unsafe { callback.invoke("clock", json!([]))? }
            .as_u64()
            .context("Invalid invocation clock")?
    } else {
        0
    };
    if operation == "jwt.verify" {
        // Copied: no guest memory borrow may survive a host invocation.
        let token = abi.memory.data(&*caller)[ranges[1].clone()].to_vec();
        declare_verification_change(caller, &token, clock_tolerance(&request.options), now)?;
    }
    let outcome = {
        let memory = abi.memory.data(&*caller);
        let args = std::array::from_fn::<_, 3, _>(|i| &memory[ranges[i + 1].clone()]);
        if let Some(key) = &authorized {
            managed::execute_prepared(key, operation, &args, &request.options, now)
        } else {
            managed::execute(
                resolved.as_ref().expect("resolved uncached key"),
                operation,
                &args,
                &request.options,
                now,
            )
        }
    };
    if operation == "jwt.decrypt" {
        let claims = match &outcome {
            Ok(managed::ManagedOutput::Json(verified)) => verified.get("claims"),
            _ => None,
        };
        declare_decryption_change(caller, claims, clock_tolerance(&request.options), now)?;
    }
    Ok(match outcome? {
        managed::ManagedOutput::Bytes(bytes) => Output::bytes(0, bytes),
        managed::ManagedOutput::Text(text) => Output::bytes(6, text.into_bytes()),
        managed::ManagedOutput::Json(value) => {
            check_json(&value)?;
            Output::bytes(6, serde_json::to_vec(&value)?)
        }
        managed::ManagedOutput::Bool(value) => Output::scalar(if value { 2 } else { 1 }),
        managed::ManagedOutput::Null => Output::scalar(3),
        managed::ManagedOutput::Shared(prepared) => {
            // Charge native retained objects, including declarations, against
            // the invocation budget even when JS drops its handle immediately.
            let weight = prepared
                .retained_bytes()
                .saturating_add(serde_json::to_vec(declaration)?.len())
                .saturating_add(256);
            let bytes = caller.data().managed_shared_bytes.saturating_add(weight);
            if bytes.saturating_add(caller.data().managed_authorization_bytes) > memory_budget {
                // Opportunistic reuse must not crowd out functional shared
                // handles. Their native storage remains charged until return.
                caller.data_mut().managed_authorizations.clear();
                caller.data_mut().managed_authorization_bytes = 0;
            }
            budget(
                bytes,
                super::super::config::settings()?.rust_memory_bytes,
                &caller.data().shared,
            )?;
            let slot = u32::try_from(caller.data().managed_shared.len())?
                .checked_add(1)
                .context("Shared key address space exhausted")?;
            caller.data_mut().managed_shared.insert(
                slot.to_string(),
                SharedKey {
                    prepared,
                    declaration: declaration.clone(),
                },
            );
            caller.data_mut().managed_shared_bytes = bytes;
            Output {
                kind: 7,
                bytes: Zeroizing::new(Vec::new()),
                integer: slot,
            }
        }
    })
}

/// The guest C bridge extracts this slot only from a non-forgeable native class.
/// JS cannot pass a numeric slot, proxy, copied properties, or JSON descriptor.
fn shared_dispatch(
    caller: &mut Caller<'_, Host>,
    op: u32,
    slot: u32,
    ranges: &[Range<usize>; 4],
) -> Result<Output> {
    let abi = caller.data().abi.clone().context("guest ABI not bound")?;
    let callback = caller
        .data()
        .callback
        .context("Shared keys require an invocation")?;
    let key = caller
        .data()
        .managed_shared
        .get(&slot.to_string())
        .context("Shared key belongs to a different invocation or is invalid")?;
    let prepared = key.prepared.clone();
    let declaration = key.declaration.clone();
    let operation = if op == 201 {
        "nacl.box.after"
    } else {
        "nacl.box.open.after"
    };
    // A derive grant does not grant encryption/decryption. Authorization is
    // pinned to the same invocation snapshot as the underlying private key.
    unsafe {
        callback.invoke(
            "managedKey",
            json!([{"key":declaration,"operation":operation,"kid":null}]),
        )?;
    }
    let memory = abi.memory.data(&*caller);
    let args = [&memory[ranges[0].clone()], &memory[ranges[1].clone()]];
    Ok(
        match managed::execute_shared(&prepared, operation, &args, &json!({}), 0)? {
            managed::ManagedOutput::Bytes(bytes) => Output::bytes(0, bytes),
            managed::ManagedOutput::Null => Output::scalar(3),
            _ => anyhow::bail!("Unexpected shared key result"),
        },
    )
}

fn budget(bytes: usize, maximum: usize, shared: &super::Limits) -> Result<()> {
    if bytes > maximum {
        shared.fail();
    }
    shared.check()
}
