//! Aggregate, sticky transaction limits shared by nested, disposable guests.
use anyhow::{ensure, Result};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use wasmtime::ResourceLimiter;

pub struct Limits {
    pub(super) deadline: Instant,
    maximum: usize,
    used: AtomicUsize,
    depth: AtomicUsize,
    failed: AtomicBool,
    native_stack_root: usize,
}

#[inline(never)]
fn stack_position() -> usize {
    let marker = 0u8;
    std::ptr::from_ref(&marker) as usize
}

impl Limits {
    pub fn new(deadline: Instant, memory_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            maximum: memory_bytes,
            used: AtomicUsize::new(0),
            depth: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            native_stack_root: stack_position(),
        })
    }
    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            self.failed.store(true, Ordering::SeqCst);
        }
        // A child Store receives its own 256 KiB Wasm stack allowance. Reserve
        // that plus host/unwind frames before allowing another nested entry;
        // otherwise independently safe guests could overflow a 2 MiB worker.
        if self.native_stack_root.abs_diff(stack_position()) > 1152 * 1024 {
            self.fail();
        }
        ensure!(
            !self.failed.load(Ordering::SeqCst),
            "EVALUATION_BUDGET: transaction exhausted its memory, stack, or execution deadline"
        );
        Ok(())
    }
    pub(in crate::evaluator) fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }
    pub(in crate::evaluator) fn enter(self: &Arc<Self>) -> Result<Depth> {
        self.check()?;
        if self.depth.load(Ordering::SeqCst) >= 32 {
            self.fail();
        }
        self.check()?;
        self.depth.fetch_add(1, Ordering::SeqCst);
        Ok(Depth(self.clone()))
    }
}

pub(in crate::evaluator) struct Depth(Arc<Limits>);
impl Drop for Depth {
    fn drop(&mut self) {
        self.0.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

pub(super) struct MemoryLimit {
    shared: Arc<Limits>,
    charged: usize,
    dirty: Option<Arc<super::dirty::Tracker>>,
}
impl MemoryLimit {
    pub(super) fn new(shared: Arc<Limits>) -> Self {
        Self {
            shared,
            charged: 0,
            dirty: None,
        }
    }

    /// A resident instance bypasses Wasmtime's initial memory_growing callback.
    /// Charge exactly the same initial bytes before allowing it to execute.
    pub(super) fn existing(shared: Arc<Limits>, bytes: usize) -> Result<Self> {
        let mut memory = Self::new(shared);
        memory.memory_growing(0, bytes, None)?;
        Ok(memory)
    }

    pub(super) fn track(&mut self, dirty: Option<Arc<super::dirty::Tracker>>) {
        self.dirty = dirty;
    }
}
impl Drop for MemoryLimit {
    fn drop(&mut self) {
        self.shared.used.fetch_sub(self.charged, Ordering::SeqCst);
    }
}
impl ResourceLimiter for MemoryLimit {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > current && current != 0 {
            // Growth can change mappings. Stop protecting this instance before
            // Wasmtime touches them; grown or failed-growth cells are discarded.
            if let Some(dirty) = &self.dirty {
                if let Err(error) = dirty.unprotect() {
                    self.shared.fail();
                    return Err(wasmtime::Error::from_anyhow(error));
                }
            }
        }
        self.shared.check().map_err(wasmtime::Error::from_anyhow)?;
        let additional = desired.saturating_sub(current);
        if maximum.is_some_and(|max| desired > max)
            || self
                .shared
                .used
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                    used.checked_add(additional)
                        .filter(|total| *total <= self.shared.maximum)
                })
                .is_err()
        {
            self.shared.fail();
            wasmtime::bail!(
                "EVALUATION_BUDGET: aggregate guest linear memory exceeded transaction limit"
            );
        }
        self.charged += additional;
        Ok(true)
    }
    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.shared.fail();
        Err(error)
    }
    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > 4096 {
            self.shared.fail();
            wasmtime::bail!("EVALUATION_BUDGET: guest table exceeded limit");
        }
        Ok(true)
    }
    fn instances(&self) -> usize {
        1
    }
    fn memories(&self) -> usize {
        1
    }
    fn tables(&self) -> usize {
        1
    }
}
