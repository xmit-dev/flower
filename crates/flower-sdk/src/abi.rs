//! Guest exports (GUEST_ABI.md). `flower_alloc` lives here; `export!` defines
//! `flower_invoke` and `flower_manifest` for an application.
use crate::{App, Kind, app::Runtime, wire};
use alloc::vec::Vec;

/// Abort the invocation. The host discards the instance and its effects.
pub fn trap() -> ! {
    #[cfg(target_arch = "wasm32")]
    core::arch::wasm32::unreachable();
    #[cfg(not(target_arch = "wasm32"))]
    panic!("guest trap");
}

fn packed(outcome: Vec<u8>) -> u64 {
    // The outcome must stay valid until the next reset, which frees everything.
    let outcome = outcome.leak();
    outcome.as_ptr() as usize as u64 | (outcome.len() as u64) << 32
}

/// `flower_invoke` for `app`.
///
/// # Safety
/// The host passes buffers it wrote with flower_alloc.
pub unsafe fn invoke(
    app: &'static App,
    kind: i32,
    name: *const u8,
    name_length: u32,
    args: *const u8,
    args_length: u32,
) -> u64 {
    let name = unsafe { core::slice::from_raw_parts(name, name_length as usize) };
    let args = unsafe { core::slice::from_raw_parts(args, args_length as usize) };
    let (Ok(name), Ok(args)) = (core::str::from_utf8(name), wire::decode(args)) else {
        trap()
    };
    packed(match Runtime::new(app).invoke(kind, name, args) {
        Ok(value) => wire::success(&value),
        Err(failure) => wire::failure(&failure, kind != Kind::Derived as i32),
    })
}

/// `flower_manifest` for `app`.
pub fn manifest(app: &'static App) -> u64 {
    packed(wire::success(&Runtime::new(app).manifest()))
}

/// Export an application's `flower_invoke` and `flower_manifest`.
#[macro_export]
macro_rules! export {
    ($app:path) => {
        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn flower_invoke(
            kind: i32,
            name: *const u8,
            name_length: u32,
            args: *const u8,
            args_length: u32,
        ) -> u64 {
            unsafe { $crate::abi::invoke(&$app, kind, name, name_length, args, args_length) }
        }

        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn flower_manifest() -> u64 {
            $crate::abi::manifest(&$app)
        }
    };
}

#[cfg(target_arch = "wasm32")]
mod heap {
    //! Every callback starts from a pristine image and nothing survives it, so
    //! memory is a bump region that only ever gives back its latest block. The
    //! region lives in zeroed static memory: it costs no module bytes and keeps
    //! ordinary callbacks from growing memory, which makes the host discard the
    //! instance instead of reusing it.
    use core::alloc::{GlobalAlloc, Layout};
    use core::arch::wasm32;
    use core::cell::UnsafeCell;

    const REGION: usize = 4 << 20;
    const PAGE: usize = 65536;

    #[repr(C, align(16))]
    struct Region(UnsafeCell<[u8; REGION]>);
    // SAFETY: guests are single-threaded.
    unsafe impl Sync for Region {}
    static BASE: Region = Region(UnsafeCell::new([0; REGION]));

    struct State {
        next: usize,
        limit: usize,
        last: usize,
    }
    struct Bump(UnsafeCell<State>);
    unsafe impl Sync for Bump {}

    #[global_allocator]
    static HEAP: Bump = Bump(UnsafeCell::new(State {
        next: 0,
        limit: 0,
        last: 0,
    }));

    impl Bump {
        #[allow(clippy::mut_from_ref)]
        fn state(&self) -> &mut State {
            // SAFETY: single-threaded, and no reference outlives a call.
            let state = unsafe { &mut *self.0.get() };
            if state.limit == 0 {
                state.next = BASE.0.get() as usize;
                state.limit = state.next + REGION;
            }
            state
        }
    }

    unsafe impl GlobalAlloc for Bump {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let state = self.state();
            let mut start = (state.next + layout.align() - 1) & !(layout.align() - 1);
            if start
                .checked_add(layout.size())
                .is_none_or(|end| end > state.limit)
            {
                // Continue past the end of memory, growing it.
                let end = wasm32::memory_size(0) * PAGE;
                start = (end + layout.align() - 1) & !(layout.align() - 1);
                let Some(needed) = start.checked_add(layout.size()) else {
                    return core::ptr::null_mut();
                };
                if wasm32::memory_grow(0, (needed - end).div_ceil(PAGE)) == usize::MAX {
                    return core::ptr::null_mut();
                }
                state.limit = wasm32::memory_size(0) * PAGE;
            }
            state.last = start;
            state.next = start + layout.size();
            start as *mut u8
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            let state = self.state();
            if pointer as usize == state.last && state.last + layout.size() == state.next {
                state.next = state.last;
            }
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let state = self.state();
            if pointer as usize == state.last
                && state.last + layout.size() == state.next
                && state.last + size <= state.limit
            {
                state.next = state.last + size;
                return pointer;
            }
            let Ok(layout_new) = Layout::from_size_align(size, layout.align()) else {
                return core::ptr::null_mut();
            };
            let moved = unsafe { self.alloc(layout_new) };
            if !moved.is_null() {
                unsafe { core::ptr::copy_nonoverlapping(pointer, moved, layout.size().min(size)) };
            }
            moved
        }
    }

    /// Memory the host writes invocation inputs and host-call responses into.
    #[unsafe(no_mangle)]
    pub extern "C" fn flower_alloc(size: u32) -> *mut u8 {
        let Ok(layout) = Layout::from_size_align(size as usize, 8) else {
            super::trap()
        };
        let pointer = unsafe { HEAP.alloc(layout) };
        if pointer.is_null() {
            super::trap();
        }
        pointer
    }

    #[cfg(not(test))]
    #[panic_handler]
    fn panic(_: &core::panic::PanicInfo) -> ! {
        wasm32::unreachable()
    }
}
