//! Track the pages a recyclable Store writes so reset copies only those pages
//! from its pristine snapshot. Hot pages stay writable and every reset copies
//! them; every other page stays write-protected until the guest or host writes
//! it. There are no hashes, sampled comparisons or guessed dirty ranges.
//!
//! Signal tracking (macOS, or Linux without userfaultfd) catches a first write
//! with SIGSEGV/SIGBUS; that page stays hot for the Store's life unless the hot
//! set is trimmed. Linux userfaultfd tracking lets the kernel resolve writes to
//! protected pages without a signal. Each reset reads and re-protects exactly
//! the pages written since the previous one; only frequently written pages
//! stay hot, so an unusual callback does not inflate every later reset.
//!
//! A new Store starts with its image's learned seed already hot: the pages two
//! recent exact single-callback footprints both wrote. Seeded pages never take
//! a first-write fault; they are simply copied on every reset.
use super::Host;
use anyhow::{Result, ensure};
use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use wasmtime::{AsContextMut, Memory, Store};

#[cfg(target_os = "linux")]
mod uffd;

/// Resets between complete re-protections. The next reset records an exact
/// footprint for the image seed, and pages no longer written stop being hot.
const RELEARN_RESETS: u32 = 1024;
/// Signal tracking re-protects hot pages beyond the image seed once they exceed
/// twice the expected hot set plus this allowance.
const GROWTH_SLACK_BYTES: usize = 256 * 1024;
/// Userfaultfd tracking keeps a page hot once two resets within this window
/// both found it written. Rarer writes cost one fault instead of a copy per reset.
#[cfg(target_os = "linux")]
const PROMOTE_WITHIN: u32 = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Auto,
    Signal,
}

fn mode() -> Mode {
    static MODE: OnceLock<Mode> = OnceLock::new();
    *MODE.get_or_init(
        || match std::env::var("FLOWER_WASM_DIRTY_PAGES").as_deref() {
            Ok("0" | "false") => Mode::Off,
            Ok("signal") => Mode::Signal,
            _ => Mode::Auto,
        },
    )
}

/// Recent exact single-callback footprints of one image, shared by its Stores.
#[derive(Default)]
pub(super) struct Learning(Mutex<Footprints>);

#[derive(Default)]
struct Footprints {
    recent: VecDeque<Box<[bool]>>,
    seed: Option<Arc<[bool]>>,
}

impl Learning {
    fn seed(&self, pages: usize) -> Option<Arc<[bool]>> {
        let footprints = self.0.lock().unwrap_or_else(|error| error.into_inner());
        footprints.seed.clone().filter(|seed| seed.len() == pages)
    }

    /// Seed only pages both recent footprints wrote: a missing page costs one
    /// fault per Store, while an extra page would be copied on every reset.
    fn observe(&self, footprint: Box<[bool]>) {
        let mut footprints = self.0.lock().unwrap_or_else(|error| error.into_inner());
        footprints
            .recent
            .retain(|recent| recent.len() == footprint.len());
        footprints.recent.push_back(footprint);
        if footprints.recent.len() > 2 {
            footprints.recent.pop_front();
        }
        if let (Some(first), Some(second)) = (footprints.recent.front(), footprints.recent.back())
            && footprints.recent.len() == 2
        {
            let seed = first
                .iter()
                .zip(second.iter())
                .map(|(a, b)| *a && *b)
                .collect();
            footprints.seed = Some(seed);
        }
    }
}

pub(super) struct Tracker {
    base: usize,
    length: usize,
    page_size: usize,
    /// Writable pages that every reset copies.
    hot: Box<[AtomicBool]>,
    active: AtomicBool,
    faults: AtomicUsize,
    // The executing thread's native errno slot; see bind_thread().
    errno: AtomicUsize,
    backend: Backend,
    learning: Arc<Learning>,
    adaptive: Mutex<Adaptive>,
}

enum Backend {
    Signal,
    #[cfg(target_os = "linux")]
    Kernel(uffd::Uffd),
}

struct Adaptive {
    /// Resets since the last complete protection.
    resets: u32,
    /// Nothing was hot before this callback: its reset records a footprint.
    observe: bool,
    /// Hot pages expected per reset: the seed or the Store's first footprint.
    baseline: usize,
    /// Signal tracking trimmed the previous reset: this one measures whether
    /// the growth persists, and raises the baseline if it does.
    rebase: bool,
    /// Kernel tracking: reset counter and each page's previous written reset.
    #[cfg(target_os = "linux")]
    clock: u32,
    #[cfg(target_os = "linux")]
    written: Box<[u32]>,
}

/// Bytes copied by a reset, and whether the Store's tracking stayed intact.
pub(super) struct Restored {
    pub(super) copied: usize,
    pub(super) reusable: bool,
}

pub(super) fn install(
    store: &mut Store<Host>,
    memory: Memory,
    learning: Arc<Learning>,
) -> Result<Option<Arc<Tracker>>> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if mode() != Mode::Off {
        return native::install(store, memory, learning);
    }
    let _ = (store, memory, learning);
    Ok(None)
}

/// Host writes may happen between Wasm activations (or under a different parent
/// Store's activation). Mark/unprotect them explicitly before touching bytes.
pub(super) fn prepare_write<S: AsContextMut<Data = Host>>(
    store: &mut S,
    offset: usize,
    length: usize,
) -> Result<()> {
    let context = store.as_context_mut();
    if let Some(tracker) = &context.data().dirty {
        tracker.prepare_write(offset, length)?;
    }
    Ok(())
}

/// Maximal runs of page indexes satisfying `select`.
fn runs(pages: usize, select: impl Fn(usize) -> bool) -> impl Iterator<Item = Range<usize>> {
    let mut page = 0;
    std::iter::from_fn(move || {
        while page < pages && !select(page) {
            page += 1;
        }
        let first = page;
        while page < pages && select(page) {
            page += 1;
        }
        (first < page).then_some(first..page)
    })
}

impl Tracker {
    /// The caller must already own this tracker in a cleanup guard alongside
    /// the still-live Store, even if protection fails partway through its range.
    pub(super) fn protect(&self) -> Result<()> {
        self.active.store(true, Ordering::Relaxed);
        native::arm(self)
    }

    /// Pooled Stores move between threads while idle. Resolve the errno slot
    /// here, before Wasm runs on this thread, never through TLS or dynamic
    /// binding inside the signal handler.
    pub(super) fn bind_thread(&self) {
        self.errno.store(native::errno(), Ordering::Release);
    }

    fn prepare_write(&self, offset: usize, length: usize) -> Result<()> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("guest write overflow"))?
            .min(self.length);
        // The kernel tracker also records host and kernel writes itself.
        if offset >= end
            || !self.active.load(Ordering::Relaxed)
            || !matches!(self.backend, Backend::Signal)
        {
            return Ok(());
        }
        let first = offset / self.page_size;
        let last = (end - 1) / self.page_size + 1;
        for run in runs(last, |page| {
            page >= first && !self.hot[page].load(Ordering::Relaxed)
        }) {
            native::writable(self.byte(run.start), run.len() * self.page_size)?;
            for page in run {
                self.hot[page].store(true, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// End protection before memory growth or Store destruction. This must
    /// succeed before the pooled mapping can be handed back to Wasmtime.
    pub(super) fn unprotect(&self) -> Result<()> {
        if self.active.load(Ordering::Relaxed) {
            native::disarm(self)?;
            self.active.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn restore(&self, memory: &mut [u8], pristine: &[u8]) -> Result<Restored> {
        ensure!(
            memory.as_ptr() as usize == self.base
                && memory.len() == self.length
                && pristine.len() == self.length,
            "tracked guest mapping changed"
        );
        if !self.active.load(Ordering::Relaxed) {
            memory.copy_from_slice(pristine);
            return Ok(Restored {
                copied: memory.len(),
                reusable: true,
            });
        }
        let mut adaptive = self
            .adaptive
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        adaptive.resets += 1;
        match &self.backend {
            Backend::Signal => Ok(self.restore_signal(&mut adaptive, memory, pristine)),
            #[cfg(target_os = "linux")]
            Backend::Kernel(uffd) => Ok(self.restore_kernel(uffd, &mut adaptive, memory, pristine)),
        }
    }

    fn restore_signal(
        &self,
        adaptive: &mut Adaptive,
        memory: &mut [u8],
        pristine: &[u8],
    ) -> Restored {
        let pages = self.hot.len();
        let mut copied = 0;
        for run in runs(pages, |page| self.hot[page].load(Ordering::Relaxed)) {
            copied += self.copy(run, memory, pristine);
        }
        let hot = copied / self.page_size;
        if std::mem::take(&mut adaptive.observe) {
            self.learning.observe(self.snapshot());
            adaptive.baseline = hot;
        } else if std::mem::take(&mut adaptive.rebase) {
            // Growth that returns right after a trim is the workload, not an
            // outlier; trimming it every reset would only add faults.
            adaptive.baseline = adaptive.baseline.max(hot);
        }
        let relearn = adaptive.resets >= RELEARN_RESETS;
        if !relearn && hot <= 2 * adaptive.baseline + GROWTH_SLACK_BYTES / self.page_size {
            return Restored {
                copied,
                reusable: true,
            };
        }
        // Growth drops pages beyond the image seed; relearning drops all of
        // them, so the next reset records an exact footprint. Bytes are already
        // pristine, and a page is protected before it stops being marked.
        let keep = if relearn {
            None
        } else {
            self.learning.seed(pages)
        };
        let kept = |page: usize| keep.as_ref().is_some_and(|keep| keep[page]);
        for run in runs(pages, |page| {
            self.hot[page].load(Ordering::Relaxed) && !kept(page)
        }) {
            // A partly protected page must never stay marked, since the handler
            // ignores faults on marked pages. Discarding unprotects everything.
            if native::readonly(self.byte(run.start), run.len() * self.page_size).is_err() {
                return Restored {
                    copied,
                    reusable: false,
                };
            }
            for page in run {
                self.hot[page].store(false, Ordering::Relaxed);
            }
        }
        if relearn {
            adaptive.resets = 0;
            adaptive.observe = true;
        } else {
            adaptive.rebase = true;
        }
        Restored {
            copied,
            reusable: true,
        }
    }

    #[cfg(target_os = "linux")]
    fn restore_kernel(
        &self,
        uffd: &uffd::Uffd,
        adaptive: &mut Adaptive,
        memory: &mut [u8],
        pristine: &[u8],
    ) -> Restored {
        let pages = self.hot.len();
        adaptive.clock = adaptive.clock.wrapping_add(1);
        let clock = adaptive.clock;
        let mut footprint =
            std::mem::take(&mut adaptive.observe).then(|| vec![false; pages].into_boxed_slice());
        let mut reprotect: Vec<Range<usize>> = Vec::new();
        let mut copied = 0;
        let mut written = 0;
        // Hot pages are never protected, so the scan reports them as written
        // too: exactly the pages this reset must copy. Unreported pages are
        // protected and unwritten, hence still pristine.
        let scanned = uffd.written(self.base, self.length, |range| {
            let run = range.start / self.page_size..range.end.div_ceil(self.page_size);
            copied += self.copy(run.clone(), memory, pristine);
            for page in run {
                if self.hot[page].load(Ordering::Relaxed) {
                    continue;
                }
                written += 1;
                if let Some(footprint) = &mut footprint {
                    footprint[page] = true;
                }
                let previous = std::mem::replace(&mut adaptive.written[page], clock);
                if clock.wrapping_sub(previous) <= PROMOTE_WITHIN {
                    self.hot[page].store(true, Ordering::Relaxed);
                } else if let Some(last) = reprotect.last_mut()
                    && last.end == page
                {
                    last.end += 1;
                } else {
                    reprotect.push(page..page + 1);
                }
            }
        });
        if scanned.is_err() {
            memory.copy_from_slice(pristine);
            return Restored {
                copied: memory.len(),
                reusable: false,
            };
        }
        self.faults.fetch_add(written, Ordering::Relaxed);
        for run in reprotect {
            if uffd
                .protect(self.byte(run.start), run.len() * self.page_size)
                .is_err()
            {
                return Restored {
                    copied,
                    reusable: false,
                };
            }
        }
        if let Some(footprint) = footprint {
            self.learning.observe(footprint);
        }
        if adaptive.resets >= RELEARN_RESETS {
            for run in runs(pages, |page| self.hot[page].load(Ordering::Relaxed)) {
                if uffd
                    .protect(self.byte(run.start), run.len() * self.page_size)
                    .is_err()
                {
                    return Restored {
                        copied,
                        reusable: false,
                    };
                }
                for page in run {
                    self.hot[page].store(false, Ordering::Relaxed);
                }
            }
            adaptive.resets = 0;
            adaptive.observe = true;
        }
        Restored {
            copied,
            reusable: true,
        }
    }

    fn byte(&self, page: usize) -> usize {
        self.base + page * self.page_size
    }

    fn copy(&self, run: Range<usize>, memory: &mut [u8], pristine: &[u8]) -> usize {
        let bytes = run.start * self.page_size..(run.end * self.page_size).min(self.length);
        memory[bytes.clone()].copy_from_slice(&pristine[bytes.clone()]);
        bytes.len()
    }

    fn snapshot(&self) -> Box<[bool]> {
        self.hot
            .iter()
            .map(|page| page.load(Ordering::Relaxed))
            .collect()
    }

    #[cfg(test)]
    pub(super) fn faults(&self) -> usize {
        self.faults.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn kernel_tracked(&self) -> bool {
        !matches!(self.backend, Backend::Signal)
    }

    #[cfg(test)]
    pub(super) fn hot_pages(&self) -> usize {
        self.snapshot().iter().filter(|hot| **hot).count()
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod native {
    use super::*;
    use wasmtime::unix::StoreExt;

    pub(super) fn install(
        store: &mut Store<Host>,
        memory: Memory,
        learning: Arc<Learning>,
    ) -> Result<Option<Arc<Tracker>>> {
        // Query page geometry outside signal context. Wasm pages are 64 KiB,
        // divisible by Linux and macOS native pages; otherwise use full copies.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Ok(None);
        }
        let page_size = page_size as usize;
        let base = memory.data_ptr(&*store) as usize;
        let length = memory.data_size(&*store);
        if !page_size.is_power_of_two()
            || !base.is_multiple_of(page_size)
            || !length.is_multiple_of(page_size)
            || base.checked_add(length).is_none()
            || length == 0
        {
            return Ok(None);
        }
        let pages = length / page_size;
        #[cfg(target_os = "linux")]
        let backend = match mode() {
            Mode::Auto => uffd::Uffd::open().map_or(Backend::Signal, Backend::Kernel),
            _ => Backend::Signal,
        };
        #[cfg(not(target_os = "linux"))]
        let backend = Backend::Signal;
        let signal = matches!(backend, Backend::Signal);
        // Resolve the mprotect dynamic symbol before any signal can need it.
        // Failure leaves the normal unprotected copy path available.
        if signal && writable(base, length).is_err() {
            return Ok(None);
        }
        let seed = learning.seed(pages);
        let seeded = |page: usize| seed.as_ref().is_some_and(|seed| seed[page]);
        let tracker = Arc::new(Tracker {
            base,
            length,
            page_size,
            hot: (0..pages)
                .map(|page| AtomicBool::new(seeded(page)))
                .collect(),
            active: AtomicBool::new(false),
            faults: AtomicUsize::new(0),
            errno: AtomicUsize::new(errno()),
            backend,
            learning,
            adaptive: Mutex::new(Adaptive {
                resets: 0,
                observe: seed.is_none(),
                baseline: (0..pages).filter(|page| seeded(*page)).count(),
                rebase: false,
                #[cfg(target_os = "linux")]
                clock: PROMOTE_WITHIN + 1,
                #[cfg(target_os = "linux")]
                written: if signal {
                    Box::new([])
                } else {
                    vec![0; pages].into()
                },
            }),
        });
        if signal {
            let handler = tracker.clone();
            // SAFETY: this closure uses only immutable geometry, preallocated
            // lock-free atomics, and mprotect. It neither accesses Host/TLS nor
            // allocates, locks, logs, formats, unwinds, or enters Wasm. It
            // handles only first-write faults in this Store's initial memory.
            unsafe {
                store.set_signal_handler(move |signal, info, _| handler.handle(signal, info));
            }
        }
        // The caller installs its RAII cleanup owner before protect(). Until
        // then this tracker is inert and memory is still normally writable.
        Ok(Some(tracker))
    }

    /// Protect every page that is not already hot.
    pub(super) fn arm(tracker: &Tracker) -> Result<()> {
        let cold = runs(tracker.hot.len(), |page| {
            !tracker.hot[page].load(Ordering::Relaxed)
        });
        match &tracker.backend {
            Backend::Signal => {
                for run in cold {
                    readonly(tracker.byte(run.start), run.len() * tracker.page_size)?;
                }
            }
            #[cfg(target_os = "linux")]
            Backend::Kernel(uffd) => {
                uffd.register(tracker.base, tracker.length)?;
                for run in cold {
                    uffd.protect(tracker.byte(run.start), run.len() * tracker.page_size)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn disarm(tracker: &Tracker) -> Result<()> {
        match &tracker.backend {
            Backend::Signal => writable(tracker.base, tracker.length),
            #[cfg(target_os = "linux")]
            Backend::Kernel(uffd) => Ok(uffd.release(tracker.base, tracker.length)?),
        }
    }

    /// This thread's native errno slot.
    pub(super) fn errno() -> usize {
        #[cfg(target_os = "macos")]
        let errno = unsafe { libc::__error() };
        #[cfg(target_os = "linux")]
        let errno = unsafe { libc::__errno_location() };
        errno as usize
    }

    pub(super) fn readonly(base: usize, length: usize) -> Result<()> {
        // SAFETY: this private, idle Wasm mapping has page-aligned geometry; no
        // other thread executes or accesses this Store. Cleanup is already owned.
        if unsafe { libc::mprotect(base as *mut libc::c_void, length, libc::PROT_READ) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    pub(super) fn writable(base: usize, length: usize) -> Result<()> {
        // SAFETY: all callers use checked page-aligned portions of the live
        // Store's private initial mapping; no Wasm executes concurrently.
        if unsafe {
            libc::mprotect(
                base as *mut libc::c_void,
                length,
                libc::PROT_READ | libc::PROT_WRITE,
            )
        } != 0
        {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    impl Tracker {
        fn handle(&self, signal: libc::c_int, info: *const libc::siginfo_t) -> bool {
            if (signal != libc::SIGSEGV && signal != libc::SIGBUS)
                || info.is_null()
                || !self.active.load(Ordering::Relaxed)
            {
                return false;
            }
            // SAFETY: Wasmtime provides the OS siginfo for this synchronous
            // memory fault. Bounds below exclude guard pages and other Stores.
            let address = unsafe { (*info).si_addr() as usize };
            let offset = address.wrapping_sub(self.base);
            if offset >= self.length {
                return false;
            }
            let page = offset / self.page_size;
            let Some(hot) = self.hot.get(page) else {
                return false;
            };
            // A fault on an already-writable page is not ours. In particular do
            // not loop on execute faults or hide unrelated memory corruption.
            if hot.load(Ordering::Relaxed) {
                return false;
            }
            // The mprotect system-call wrapper can change errno; guest/native
            // code observes exactly the prior value when the instruction retries.
            let errno = self.errno.load(Ordering::Acquire) as *mut libc::c_int;
            // SAFETY: errno is the executing thread's native integer slot,
            // bound at checkout. The atomic page index is in bounds.
            let success = unsafe {
                let saved = *errno;
                let result = libc::mprotect(
                    (self.base + page * self.page_size) as *mut libc::c_void,
                    self.page_size,
                    libc::PROT_READ | libc::PROT_WRITE,
                );
                *errno = saved;
                result == 0
            };
            if success {
                hot.store(true, Ordering::Relaxed);
                self.faults.fetch_add(1, Ordering::Relaxed);
            }
            success
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod native {
    // Trackers are never installed here; these keep shared code compiling.
    use super::*;
    pub(super) fn arm(_: &Tracker) -> Result<()> {
        anyhow::bail!("dirty-page tracking is unsupported")
    }
    pub(super) fn disarm(_: &Tracker) -> Result<()> {
        Ok(())
    }
    pub(super) fn errno() -> usize {
        0
    }
    pub(super) fn readonly(_: usize, _: usize) -> Result<()> {
        anyhow::bail!("dirty-page tracking is unsupported")
    }
    pub(super) fn writable(_: usize, _: usize) -> Result<()> {
        anyhow::bail!("dirty-page tracking is unsupported")
    }
}
