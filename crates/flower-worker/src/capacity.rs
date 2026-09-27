//! How many jobs a worker process takes on at once: the twin of `sdk/capacity.ts`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::client::js_round;
use crate::clock::Clock;

/// How loaded the process is: 1 where it falls behind, with what loads it most.
#[derive(Clone, Debug, PartialEq)]
pub struct Load {
    pub load: f64,
    pub reason: String,
}

impl Load {
    pub fn new(load: f64, reason: impl Into<String>) -> Self {
        Load {
            load,
            reason: reason.into(),
        }
    }

    /// `{ load: 0, reason: "idle" }`.
    pub fn idle() -> Self {
        Load::new(0.0, "idle")
    }
}

/// Read at every adjustment: the load since the previous read.
pub trait Health: Send + Sync {
    fn load(&self) -> Load;
}

impl<F: Fn() -> Load + Send + Sync> Health for F {
    fn load(&self) -> Load {
        self()
    }
}

/// A health that always reads idle, as fixed pools use.
pub fn idle_health() -> Arc<dyn Health> {
    Arc::new(Load::idle)
}

/// Bounds for a limit that adapts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Adaptive {
    /// Default 1.
    pub min: Option<u64>,
    /// Default 16 (or `min`, when larger).
    pub max: Option<u64>,
    /// Where the limit starts, within min and max. Default min.
    pub initial: Option<u64>,
}

/// A fixed number of jobs at once, or a limit that adapts within bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Concurrency {
    Fixed(u64),
    Adaptive(Adaptive),
}

impl Concurrency {
    /// `{ min, initial, max }`.
    pub fn adaptive(min: u64, initial: u64, max: u64) -> Self {
        Concurrency::Adaptive(Adaptive {
            min: Some(min),
            max: Some(max),
            initial: Some(initial),
        })
    }

    /// `{ initial, max }`: min 1.
    pub fn up_to(initial: u64, max: u64) -> Self {
        Concurrency::Adaptive(Adaptive {
            min: None,
            max: Some(max),
            initial: Some(initial),
        })
    }
}

impl From<u64> for Concurrency {
    fn from(value: u64) -> Self {
        Concurrency::Fixed(value)
    }
}

impl From<Adaptive> for Concurrency {
    fn from(value: Adaptive) -> Self {
        Concurrency::Adaptive(value)
    }
}

/// Bounds of the process's health.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HealthLimits {
    /// The share of the time the runtime's workers spend busy at which the process falls behind.
    /// Default 0.9.
    pub busy: f64,
    /// The share of the memory the process may use at which it is full. Default 0.85.
    pub memory: f64,
}

impl Default for HealthLimits {
    fn default() -> Self {
        HealthLimits {
            busy: 0.9,
            memory: 0.85,
        }
    }
}

/// `processHealth()`: the load of this process, the higher of how busy the tokio runtime's worker
/// threads were since the last read (Node: the event loop's utilization) and how full its memory
/// is (resident memory against what it may still use, within a cgroup's limit or the machine's
/// free memory). There is no heap limit to read, unlike V8's.
pub struct ProcessHealth {
    limits: HealthLimits,
    runtime: Option<tokio::runtime::Handle>,
    last: Mutex<Option<(Instant, Duration)>>,
}

/// `processHealth(limits)`.
pub fn process_health(limits: HealthLimits) -> Result<ProcessHealth, String> {
    // NaN is not positive either.
    if limits.busy.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater)
        || limits.memory.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater)
    {
        return Err("Health limits must be positive".into());
    }
    let runtime = tokio::runtime::Handle::try_current().ok();
    let last = runtime
        .as_ref()
        .map(|handle| (Instant::now(), busy_total(handle)));
    Ok(ProcessHealth {
        limits,
        runtime,
        last: Mutex::new(last),
    })
}

/// `processHealth()` with the default limits.
pub fn default_process_health() -> Arc<dyn Health> {
    Arc::new(process_health(HealthLimits::default()).expect("default limits are positive"))
}

fn busy_total(handle: &tokio::runtime::Handle) -> Duration {
    let metrics = handle.metrics();
    (0..metrics.num_workers())
        .map(|worker| metrics.worker_total_busy_duration(worker))
        .sum()
}

impl Health for ProcessHealth {
    fn load(&self) -> Load {
        let mut loads = vec![Load::idle()];
        if let Some(handle) = &self.runtime {
            let mut last = self.last.lock();
            if let Some((at, busy)) = *last {
                let now = Instant::now();
                let total = busy_total(handle);
                let wall = now.duration_since(at).as_secs_f64()
                    * handle.metrics().num_workers().max(1) as f64;
                let share = if wall > 0.0 {
                    (total.saturating_sub(busy).as_secs_f64() / wall).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                *last = Some((now, total));
                loads.push(Load::new(
                    share / self.limits.busy,
                    format!("event loop {}% busy", js_round(share * 100.0)),
                ));
            }
        }
        if let (Some(rss), Some(available)) = (memory::resident(), memory::available())
            && available > 0
        {
            let used = rss as f64 / (rss as f64 + available as f64);
            loads.push(Load::new(
                used / self.limits.memory,
                format!("memory {}% used", js_round(used * 100.0)),
            ));
        }
        loads
            .into_iter()
            .reduce(|most, next| if next.load > most.load { next } else { most })
            .unwrap_or_else(Load::idle)
    }
}

mod memory {
    //! Resident memory and what the process may still use, as Node's `process.memoryUsage().rss`
    //! and `process.availableMemory()` (libuv) read them.

    #[cfg(target_os = "linux")]
    pub fn resident() -> Option<u64> {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(pages * page_size())
    }

    #[cfg(target_os = "linux")]
    fn page_size() -> u64 {
        // SAFETY: sysconf has no preconditions.
        let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if size > 0 { size as u64 } else { 4096 }
    }

    /// uv_get_available_memory: a cgroup v2 limit minus its usage when one applies, else the
    /// machine's MemAvailable.
    #[cfg(target_os = "linux")]
    pub fn available() -> Option<u64> {
        let free = meminfo_available();
        if let Some((limit, current)) = cgroup() {
            let total = meminfo("MemTotal:");
            if total.is_none_or(|total| limit <= total) {
                return Some(limit.saturating_sub(current));
            }
        }
        free
    }

    #[cfg(target_os = "linux")]
    fn meminfo(field: &str) -> Option<u64> {
        let info = std::fs::read_to_string("/proc/meminfo").ok()?;
        let line = info.lines().find(|line| line.starts_with(field))?;
        let kib: u64 = line[field.len()..]
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .ok()?;
        Some(kib * 1024)
    }

    #[cfg(target_os = "linux")]
    fn meminfo_available() -> Option<u64> {
        meminfo("MemAvailable:")
    }

    #[cfg(target_os = "linux")]
    fn cgroup() -> Option<(u64, u64)> {
        let groups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = groups.lines().find_map(|line| line.strip_prefix("0::"))?;
        let dir = format!("/sys/fs/cgroup{}", path.trim_end_matches('/'));
        let max = std::fs::read_to_string(format!("{dir}/memory.max")).ok()?;
        let limit: u64 = max.trim().parse().ok()?;
        let current: u64 = std::fs::read_to_string(format!("{dir}/memory.current"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        Some((limit, current))
    }

    #[cfg(target_os = "macos")]
    pub fn resident() -> Option<u64> {
        // SAFETY: proc_pid_rusage fills the zeroed struct it is given for this process.
        unsafe {
            let mut info: libc::rusage_info_v4 = std::mem::zeroed();
            let status = libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                (&mut info as *mut libc::rusage_info_v4).cast(),
            );
            (status == 0).then_some(info.ri_resident_size)
        }
    }

    /// uv_get_free_memory on macOS: free pages times the page size.
    #[cfg(target_os = "macos")]
    pub fn available() -> Option<u64> {
        let mut pages: u32 = 0;
        let mut size = std::mem::size_of::<u32>();
        // SAFETY: the name is NUL-terminated and the output buffer matches the reported size.
        let status = unsafe {
            libc::sysctlbyname(
                c"vm.page_free_count".as_ptr(),
                (&mut pages as *mut u32).cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        // SAFETY: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        (status == 0 && page > 0).then(|| pages as u64 * page as u64)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub fn resident() -> Option<u64> {
        None
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub fn available() -> Option<u64> {
        None
    }
}

/// Below this load, a limit the queue's work presses on doubles; up to 1, it grows by a sixteenth.
const HEADROOM: f64 = 0.5;
/// How long after a cut the limit grows by a sixteenth at most, so it settles under what the
/// process can take, or for longer under what its provider does: a rate limit refills by the minute.
const SETTLE_MS: f64 = 5_000.0;
const SETTLE_THROTTLED_MS: f64 = 30_000.0;
/// Cuts at least this far apart, so a burst of failures or a busy second cuts once rather than to
/// the floor.
const CUT_EVERY_MS: f64 = 1_000.0;

/// A change of the limit, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitChange {
    pub limit: u64,
    pub reason: String,
}

/// The largest integer a JS number holds exactly.
pub(crate) const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// How many jobs a process runs at once, like a TCP congestion window. While the queue holds more
/// work than the process takes, the limit doubles at each adjustment if the process is under half
/// its load, and grows by a sixteenth (at least one) if it is busier or was recently cut. Once the
/// process falls behind the limit shrinks by a quarter; when a provider pushes back it halves, and
/// claims pause. A number fixes it.
pub struct Limiter {
    pub min: u64,
    pub max: u64,
    current: f64,
    wanted: bool,
    paused_until: f64,
    last_cut: f64,
    settle_until: f64,
    health: Arc<dyn Health>,
    now: Arc<dyn Fn() -> f64 + Send + Sync>,
}

impl Limiter {
    /// A limiter on the wall clock.
    pub fn new(concurrency: Concurrency, health: Arc<dyn Health>) -> Result<Self, String> {
        Self::with_clock(concurrency, health, &Clock::system())
    }

    pub fn with_clock(
        concurrency: Concurrency,
        health: Arc<dyn Health>,
        clock: &Clock,
    ) -> Result<Self, String> {
        let clock = clock.clone();
        Self::with_now(concurrency, health, move || clock.now_ms() as f64)
    }

    /// `new Limiter(concurrency, health, now)`.
    pub fn with_now(
        concurrency: Concurrency,
        health: Arc<dyn Health>,
        now: impl Fn() -> f64 + Send + Sync + 'static,
    ) -> Result<Self, String> {
        let bounds = match concurrency {
            Concurrency::Fixed(n) => Adaptive {
                min: Some(n),
                max: Some(n),
                initial: Some(n),
            },
            Concurrency::Adaptive(bounds) => bounds,
        };
        let min = bounds.min.unwrap_or(1);
        let max = bounds.max.unwrap_or(16.max(min));
        let initial = bounds.initial.unwrap_or(min);
        if [min, max, initial].iter().any(|&n| n > MAX_SAFE_INTEGER)
            || min < 1
            || max < min
            || initial < min
            || initial > max
        {
            return Err("concurrency needs whole numbers with 1 <= min <= initial <= max".into());
        }
        Ok(Limiter {
            min,
            max,
            current: initial as f64,
            wanted: false,
            paused_until: 0.0,
            last_cut: f64::NEG_INFINITY,
            settle_until: f64::NEG_INFINITY,
            health,
            now: Arc::new(now),
        })
    }

    pub fn fixed(&self) -> bool {
        self.min == self.max
    }

    /// Jobs the process may hold now.
    pub fn limit(&self) -> u64 {
        self.current.floor() as u64
    }

    /// Milliseconds until claims may resume after a provider pushed back; 0 when they may now.
    pub fn pause(&self) -> u64 {
        (self.paused_until - (self.now)()).max(0.0) as u64
    }

    /// The queue had more work than the process had room for.
    pub fn want(&mut self) {
        self.wanted = true;
    }

    /// A provider refused work for being asked too much: claim nothing for `ms`, and hold fewer jobs.
    pub fn throttle(&mut self, ms: u64, reason: &str) -> Option<LimitChange> {
        self.paused_until = self.paused_until.max((self.now)() + ms as f64);
        self.cut(0.5, reason, SETTLE_THROTTLED_MS)
    }

    /// Once per adjustment interval: follow the process's load and the demand seen since the last call.
    pub fn adjust(&mut self) -> Option<LimitChange> {
        let wanted = self.wanted;
        self.wanted = false;
        if self.fixed() {
            return None;
        }
        let Load { load, reason } = self.health.load();
        if load >= 1.0 {
            return self.cut(0.75, &reason, SETTLE_MS);
        }
        if !wanted || self.pause() > 0 || self.current >= self.max as f64 {
            return None;
        }
        let before = self.limit();
        let gentle = load >= HEADROOM || (self.now)() < self.settle_until;
        let grown = if gentle {
            self.current + (self.current / 16.0).max(1.0)
        } else {
            self.current * 2.0
        };
        self.current = grown.min(self.max as f64);
        (self.limit() != before).then(|| LimitChange {
            limit: self.limit(),
            reason: "more work is waiting".into(),
        })
    }

    fn cut(&mut self, factor: f64, reason: &str, settle_ms: f64) -> Option<LimitChange> {
        let now = (self.now)();
        if self.fixed() || now - self.last_cut < CUT_EVERY_MS {
            return None;
        }
        self.last_cut = now;
        self.settle_until = self.settle_until.max(now + settle_ms);
        let before = self.limit();
        self.current = (self.current * factor).max(self.min as f64);
        (self.limit() != before).then(|| LimitChange {
            limit: self.limit(),
            reason: reason.to_owned(),
        })
    }
}
