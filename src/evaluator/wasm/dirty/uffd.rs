//! Linux 6.7+ asynchronous userfaultfd write protection. The kernel resolves a
//! write to a protected page itself, including writes made by host code or
//! system calls, and PAGEMAP_SCAN reports pages written since last protected.
//! Unprivileged processes need UFFD_USER_MODE_ONLY; seccomp or other policy
//! failures fall back to signal tracking for the rest of the process.
use std::{
    io::{Error, ErrorKind, Result},
    ops::Range,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

#[repr(C)]
struct Api {
    api: u64,
    features: u64,
    ioctls: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Span {
    start: u64,
    len: u64,
}

#[repr(C)]
struct Register {
    range: Span,
    mode: u64,
    ioctls: u64,
}

#[repr(C)]
struct WriteProtect {
    range: Span,
    mode: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct PageRegion {
    start: u64,
    end: u64,
    categories: u64,
}

#[repr(C)]
struct ScanArgument {
    size: u64,
    flags: u64,
    start: u64,
    end: u64,
    walk_end: u64,
    vec: u64,
    vec_len: u64,
    max_pages: u64,
    category_inverted: u64,
    category_mask: u64,
    category_anyof_mask: u64,
    return_mask: u64,
}

// Generic Linux ioctl encoding, used by x86_64, aarch64 and riscv64.
const fn request(direction: u64, kind: u8, number: u8, size: usize) -> u64 {
    (direction << 30) | ((size as u64) << 16) | ((kind as u64) << 8) | number as u64
}
const READ: u64 = 2;
const READ_WRITE: u64 = 3;
const UFFD: u8 = 0xAA;
const UFFD_API: u64 = 0xAA;
const UFFDIO_API: u64 = request(READ_WRITE, UFFD, 0x3F, size_of::<Api>());
const UFFDIO_REGISTER: u64 = request(READ_WRITE, UFFD, 0x00, size_of::<Register>());
const UFFDIO_UNREGISTER: u64 = request(READ, UFFD, 0x01, size_of::<Span>());
const UFFDIO_WRITEPROTECT: u64 = request(READ_WRITE, UFFD, 0x06, size_of::<WriteProtect>());
const PAGEMAP_SCAN: u64 = request(READ_WRITE, b'f', 16, size_of::<ScanArgument>());
const UFFD_USER_MODE_ONLY: libc::c_int = 1;
const FEATURE_WP_UNPOPULATED: u64 = 1 << 13;
const FEATURE_WP_ASYNC: u64 = 1 << 15;
const REGISTER_MODE_WP: u64 = 1 << 1;
const WRITEPROTECT_MODE_WP: u64 = 1 << 0;
const PAGE_IS_WRITTEN: u64 = 1 << 1;
const PM_SCAN_CHECK_WPASYNC: u64 = 1 << 1;

/// Set after a failure that will recur, such as EPERM from seccomp.
static UNAVAILABLE: AtomicBool = AtomicBool::new(false);

fn pagemap() -> Option<libc::c_int> {
    static PAGEMAP: OnceLock<Option<OwnedFd>> = OnceLock::new();
    PAGEMAP
        .get_or_init(|| {
            let path = c"/proc/self/pagemap";
            // SAFETY: a static C path; the descriptor is owned immediately.
            let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
            (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) })
        })
        .as_ref()
        .map(AsRawFd::as_raw_fd)
}

fn ioctl<T>(fd: libc::c_int, request: u64, argument: &mut T) -> Result<libc::c_int> {
    // SAFETY: every request constant above matches its argument's kernel ABI
    // layout, and the argument outlives this synchronous call.
    let result = unsafe { libc::ioctl(fd, request as _, argument as *mut T) };
    if result < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(result)
    }
}

pub(super) struct Uffd {
    fd: OwnedFd,
    registered: AtomicBool,
}

impl Uffd {
    pub(super) fn open() -> Option<Self> {
        if !cfg!(any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64"
        )) || UNAVAILABLE.load(Ordering::Relaxed)
        {
            return None;
        }
        if pagemap().is_none() {
            UNAVAILABLE.store(true, Ordering::Relaxed);
            return None;
        }
        let flags = libc::O_CLOEXEC | libc::O_NONBLOCK | UFFD_USER_MODE_ONLY;
        // SAFETY: userfaultfd takes only flags and returns a new descriptor.
        let fd = unsafe { libc::syscall(libc::SYS_userfaultfd, flags) };
        if fd < 0 {
            // Descriptor or memory exhaustion can clear; policy denials cannot.
            let error = Error::last_os_error().raw_os_error();
            if !matches!(error, Some(libc::EMFILE | libc::ENFILE | libc::ENOMEM)) {
                UNAVAILABLE.store(true, Ordering::Relaxed);
            }
            return None;
        }
        // SAFETY: the syscall returned a new descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) };
        let wanted = FEATURE_WP_UNPOPULATED | FEATURE_WP_ASYNC;
        let mut api = Api {
            api: UFFD_API,
            features: wanted,
            ioctls: 0,
        };
        if ioctl(fd.as_raw_fd(), UFFDIO_API, &mut api).is_err() || api.features & wanted != wanted {
            UNAVAILABLE.store(true, Ordering::Relaxed);
            return None;
        }
        Some(Self {
            fd,
            registered: AtomicBool::new(false),
        })
    }

    pub(super) fn register(&self, base: usize, length: usize) -> Result<()> {
        let mut register = Register {
            range: span(base, length),
            mode: REGISTER_MODE_WP,
            ioctls: 0,
        };
        ioctl(self.fd.as_raw_fd(), UFFDIO_REGISTER, &mut register)?;
        self.registered.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Protect pages, including unpopulated ones, until their next write.
    pub(super) fn protect(&self, base: usize, length: usize) -> Result<()> {
        self.write_protect(base, length, WRITEPROTECT_MODE_WP)
    }

    fn write_protect(&self, base: usize, length: usize, mode: u64) -> Result<()> {
        let mut protect = WriteProtect {
            range: span(base, length),
            mode,
        };
        ioctl(self.fd.as_raw_fd(), UFFDIO_WRITEPROTECT, &mut protect).map(drop)
    }

    /// Clear protection and registration before Wasmtime may reuse the range.
    pub(super) fn release(&self, base: usize, length: usize) -> Result<()> {
        if self.registered.load(Ordering::Relaxed) {
            self.write_protect(base, length, 0)?;
            let mut range = span(base, length);
            ioctl(self.fd.as_raw_fd(), UFFDIO_UNREGISTER, &mut range)?;
            self.registered.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Report each maximal run of unprotected (written) pages as offsets from
    /// `base`, without changing protection.
    pub(super) fn written(
        &self,
        base: usize,
        length: usize,
        mut each: impl FnMut(Range<usize>),
    ) -> Result<()> {
        let pagemap = pagemap().ok_or_else(|| Error::from(ErrorKind::Unsupported))?;
        let mut regions = [PageRegion::default(); 64];
        let end = (base + length) as u64;
        let mut start = base as u64;
        while start < end {
            let mut scan = ScanArgument {
                size: size_of::<ScanArgument>() as u64,
                // Fail rather than silently miss pages outside write tracking.
                flags: PM_SCAN_CHECK_WPASYNC,
                start,
                end,
                walk_end: 0,
                vec: regions.as_mut_ptr() as u64,
                vec_len: regions.len() as u64,
                max_pages: 0,
                category_inverted: 0,
                category_mask: PAGE_IS_WRITTEN,
                category_anyof_mask: 0,
                return_mask: PAGE_IS_WRITTEN,
            };
            let count = ioctl(pagemap, PAGEMAP_SCAN, &mut scan)? as usize;
            for region in regions.iter().take(count) {
                if region.start < start || region.end > end || region.start >= region.end {
                    return Err(Error::from(ErrorKind::InvalidData));
                }
                each(region.start as usize - base..region.end as usize - base);
            }
            if scan.walk_end <= start || scan.walk_end > end {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            start = scan.walk_end;
        }
        Ok(())
    }
}

fn span(base: usize, length: usize) -> Span {
    Span {
        start: base as u64,
        len: length as u64,
    }
}
