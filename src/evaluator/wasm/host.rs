use super::{failure_parts, max_json_bytes, Host};
use crate::evaluator::wire;
use anyhow::{bail, ensure, Result};
use serde_json::Value;
use wasmtime::{Caller, Engine, Linker};

type HostCall<'a> = dyn FnMut(&str, Value) -> Result<Value> + 'a;

#[derive(Clone, Copy)]
pub(super) struct Callback {
    data: *mut (),
    call: unsafe fn(*mut (), &str, Value) -> Result<Value>,
}
impl Callback {
    /// Caller must keep the borrowed bridge alive for every synchronous guest
    /// Store that contains this token. Tokens are detached or their Store is
    /// dropped before execute() returns; a recycled Store never retains one.
    pub(super) unsafe fn scoped(bridge: &mut &mut HostCall<'_>) -> Self {
        unsafe fn dispatch(data: *mut (), method: &str, args: Value) -> Result<Value> {
            // SAFETY: execute owns the pointed-to bridge on its stack, and
            // synchronous Wasmtime callbacks cannot outlive that scope.
            let callback = unsafe { &mut *(data as *mut &mut HostCall<'static>) };
            callback(method, args)
        }
        Self {
            data: (bridge as *mut &mut HostCall<'_>).cast(),
            call: dispatch,
        }
    }
    pub(super) unsafe fn invoke(self, method: &str, args: Value) -> Result<Value> {
        // SAFETY: same scoped execution invariant as scoped().
        unsafe { (self.call)(self.data, method, args) }
    }
}

/// Operation numbers from GUEST_ABI.md. Native-only operations, such as
/// resolving managed keys for the crypto bridge, have no number.
fn operation(op: i32) -> Result<&'static str> {
    Ok(match op {
        1 => "now",
        2 => "principal",
        3 => "history",
        4 => "get",
        5 => "scan",
        6 => "range",
        7 => "query",
        8 => "set",
        9 => "delete",
        10 => "materialize",
        11 => "unmaterialize",
        12 => "clock",
        13 => "changesAt",
        _ => bail!("unknown host operation {op}"),
    })
}

fn host_call(
    mut caller: Caller<'_, Host>,
    op: i32,
    payload_pointer: i32,
    payload_length: i32,
) -> wasmtime::Result<i64> {
    (|| -> Result<i64> {
        caller.data().shared.check()?;
        let abi = caller
            .data()
            .abi
            .clone()
            .ok_or_else(|| anyhow::anyhow!("guest ABI not bound"))?;
        let method = operation(op)?;
        let response = match abi.arguments(&caller, payload_pointer, payload_length)? {
            Err(invalid) => wire::failure("INVALID_VALUE", invalid.0, None),
            Ok(arguments) => {
                let callback = caller.data().callback.ok_or_else(|| {
                    anyhow::anyhow!("database access is unavailable during bundle initialization")
                })?;
                // SAFETY: callbacks can only be bound by execute, which holds the
                // borrowed bridge until this Store has been dropped or completely
                // reset. Take a copy of the token, releasing the Host borrow
                // before nested calls can execute.
                let result = unsafe { callback.invoke(method, Value::Array(arguments)) };
                caller.data().shared.check()?;
                match result {
                    Ok(value) => wire::success(&value)?,
                    Err(error) => {
                        let (code, message, details) = failure_parts(error);
                        wire::failure(&code, &message, details.as_ref())
                    }
                }
            }
        };
        ensure!(
            response.len() <= max_json_bytes()?,
            "host result exceeds FLOWER_RESULT_MAX_BYTES"
        );
        abi.response(&mut caller, &response)
    })()
    .map_err(wasmtime::Error::from_anyhow)
}

pub(super) fn linker(engine: &Engine) -> Result<Linker<Host>> {
    let mut linker = Linker::new(engine);
    linker.func_wrap("flower", "host_call", host_call)?;
    linker.func_wrap("flower", "crypto_call", super::crypto::call)?;
    Ok(linker)
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_numbered_operations_reach_the_database() {
        assert_eq!(super::operation(4).unwrap(), "get");
        assert_eq!(super::operation(11).unwrap(), "unmaterialize");
        assert_eq!(super::operation(12).unwrap(), "clock");
        assert_eq!(super::operation(13).unwrap(), "changesAt");
        for op in [0, 14, -1, i32::MAX] {
            assert!(super::operation(op).is_err(), "{op}");
        }
    }
}
