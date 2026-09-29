//! What this server process uses, as its operating system counts it, for `GET /admin/resources`:
//! the CPU time it has spent since it started and the memory it holds now, beside the cores it
//! may run on and the memory it may take. CPU time only grows, so a watcher samples it twice and
//! divides the difference by the difference in `uptimeMs` for a rate (1,000 µs of CPU per
//! millisecond is one busy core). Fields the platform does not report are null.

use std::{sync::OnceLock, time::Instant};

use serde_json::{Value, json};

static STARTED: OnceLock<Instant> = OnceLock::new();

/// Start the uptime clock; the router calls it as the server starts.
pub(super) fn started() {
    STARTED.get_or_init(Instant::now);
}

/// `{uptimeMs, cpuMicros, userCpuMicros, systemCpuMicros, residentBytes, peakResidentBytes,
/// threads, cpus, memoryLimitBytes}`.
pub(super) fn metrics() -> Value {
    let uptime = STARTED.get_or_init(Instant::now).elapsed();
    let cpu = cpu_times();
    let memory = memory();
    json!({
        "uptimeMs": u64::try_from(uptime.as_millis()).unwrap_or(u64::MAX),
        "cpuMicros": cpu.map(|(user, system)| user + system),
        "userCpuMicros": cpu.map(|(user, _)| user),
        "systemCpuMicros": cpu.map(|(_, system)| system),
        "residentBytes": memory.resident,
        "peakResidentBytes": memory.peak,
        "threads": memory.threads,
        "cpus": std::thread::available_parallelism().ok().map(usize::from),
        "memoryLimitBytes": memory_limit(),
    })
}

/// User and system CPU time of every thread of this process since it started, in microseconds.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cpu_times() -> Option<(u64, u64)> {
    // SAFETY: getrusage fills the zeroed struct it is given and reads nothing else.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return None;
        }
        usage
    };
    let micros = |time: libc::timeval| {
        Some(u64::try_from(time.tv_sec).ok()? * 1_000_000 + u64::try_from(time.tv_usec).ok()?)
    };
    Some((micros(usage.ru_utime)?, micros(usage.ru_stime)?))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn cpu_times() -> Option<(u64, u64)> {
    None
}

#[derive(Default)]
struct Memory {
    resident: Option<u64>,
    peak: Option<u64>,
    threads: Option<u64>,
}

/// Resident memory now and at its highest, and threads, from `/proc/self/status`.
#[cfg(target_os = "linux")]
fn memory() -> Memory {
    std::fs::read_to_string("/proc/self/status")
        .map(|status| memory_from_status(&status))
        .unwrap_or_default()
}

/// Resident memory now, and the physical footprint at its highest (as Activity Monitor counts it).
#[cfg(target_os = "macos")]
fn memory() -> Memory {
    // SAFETY: proc_pid_rusage fills the zeroed struct of the flavor it is asked for.
    unsafe {
        let mut info: libc::rusage_info_v4 = std::mem::zeroed();
        let status = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            (&mut info as *mut libc::rusage_info_v4).cast(),
        );
        if status != 0 {
            return Memory::default();
        }
        Memory {
            resident: Some(info.ri_resident_size),
            peak: Some(info.ri_lifetime_max_phys_footprint),
            threads: None,
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn memory() -> Memory {
    Memory::default()
}

/// `VmRSS`, `VmHWM` (both in kB) and `Threads` of a `/proc/<pid>/status`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn memory_from_status(status: &str) -> Memory {
    let field = |name: &str| {
        status.lines().find_map(|line| {
            let value = line.strip_prefix(name)?.strip_prefix(':')?.trim();
            value.split_whitespace().next()?.parse::<u64>().ok()
        })
    };
    Memory {
        resident: field("VmRSS").map(|kb| kb * 1024),
        peak: field("VmHWM").map(|kb| kb * 1024),
        threads: field("Threads"),
    }
}

/// The most memory this process may hold: the machine's, or less when its cgroup (v2) or one
/// above it sets `memory.max`.
fn memory_limit() -> Option<u64> {
    let physical = physical_memory();
    #[cfg(target_os = "linux")]
    let limit = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|cgroups| {
            cgroup_memory_limit(&cgroups, |path| std::fs::read_to_string(path).ok())
        });
    #[cfg(not(target_os = "linux"))]
    let limit: Option<u64> = None;
    match (physical, limit) {
        (Some(physical), Some(limit)) => Some(physical.min(limit)),
        (physical, limit) => physical.or(limit),
    }
}

/// The lowest `memory.max` from this process's cgroup (the `0::` line of `/proc/self/cgroup`)
/// up to the root of `/sys/fs/cgroup`; None when none sets one.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_memory_limit(cgroups: &str, read: impl Fn(&str) -> Option<String>) -> Option<u64> {
    let path = cgroups.lines().find_map(|line| line.strip_prefix("0::"))?;
    let mut lowest: Option<u64> = None;
    let mut path = path.trim().trim_end_matches('/').to_owned();
    loop {
        if let Some(limit) = read(&format!("/sys/fs/cgroup{path}/memory.max"))
            .and_then(|max| max.trim().parse::<u64>().ok())
        {
            lowest = Some(lowest.map_or(limit, |lowest| lowest.min(limit)));
        }
        match path.rfind('/') {
            Some(slash) => path.truncate(slash),
            None => break,
        }
    }
    lowest
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn physical_memory() -> Option<u64> {
    // SAFETY: sysconf only reads the system's configuration.
    let (pages, page) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    (pages > 0 && page > 0).then(|| pages as u64 * page as u64)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn physical_memory() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_gives_resident_peak_and_threads() {
        let status = "Name:\tflower\nVmPeak:\t 9000000 kB\nVmHWM:\t    2048 kB\nVmRSS:\t    1024 kB\nThreads:\t42\n";
        let memory = memory_from_status(status);
        assert_eq!(
            (memory.resident, memory.peak, memory.threads),
            (Some(1024 * 1024), Some(2048 * 1024), Some(42))
        );
        let none = memory_from_status("Name:\tflower\n");
        assert_eq!((none.resident, none.peak, none.threads), (None, None, None));
    }

    #[test]
    fn the_lowest_memory_max_up_the_cgroup_tree_is_the_limit() {
        let files = |files: &'static [(&'static str, &'static str)]| {
            move |path: &str| {
                files
                    .iter()
                    .find(|(name, _)| *name == path)
                    .map(|(_, value)| (*value).to_owned())
            }
        };
        let cgroups = "0::/system.slice/flower.service\n";
        let parent = files(&[
            (
                "/sys/fs/cgroup/system.slice/flower.service/memory.max",
                "max\n",
            ),
            ("/sys/fs/cgroup/system.slice/memory.max", "8589934592\n"),
        ]);
        assert_eq!(cgroup_memory_limit(cgroups, parent), Some(8_589_934_592));
        let own = files(&[
            (
                "/sys/fs/cgroup/system.slice/flower.service/memory.max",
                "1073741824\n",
            ),
            ("/sys/fs/cgroup/system.slice/memory.max", "8589934592\n"),
        ]);
        assert_eq!(cgroup_memory_limit(cgroups, own), Some(1_073_741_824));
        let unlimited = files(&[(
            "/sys/fs/cgroup/system.slice/flower.service/memory.max",
            "max\n",
        )]);
        assert_eq!(cgroup_memory_limit(cgroups, unlimited), None);
        let root = files(&[("/sys/fs/cgroup/memory.max", "4096\n")]);
        assert_eq!(cgroup_memory_limit("0::/\n", root), Some(4096));
        // cgroup v1 only: no `0::` line.
        assert_eq!(
            cgroup_memory_limit("4:memory:/user.slice\n", files(&[])),
            None
        );
    }

    #[test]
    fn this_process_reports_what_it_uses() {
        started();
        // Spend some CPU so that the counter has moved.
        let mut sum = 0u64;
        for each in 0..5_000_000u64 {
            sum = sum.wrapping_add(std::hint::black_box(each).wrapping_mul(each));
        }
        std::hint::black_box(sum);
        let metrics = metrics();
        assert!(
            metrics["cpus"].as_u64().is_some_and(|cpus| cpus >= 1),
            "{metrics}"
        );
        assert!(metrics["uptimeMs"].is_u64(), "{metrics}");
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            let cpu = metrics["cpuMicros"].as_u64().expect("CPU time");
            let (user, system) = (
                metrics["userCpuMicros"].as_u64().unwrap(),
                metrics["systemCpuMicros"].as_u64().unwrap(),
            );
            assert!(cpu > 0 && cpu == user + system, "{metrics}");
            let resident = metrics["residentBytes"].as_u64().expect("resident memory");
            assert!(resident > 1024 * 1024, "{metrics}");
            let limit = metrics["memoryLimitBytes"].as_u64().expect("memory limit");
            assert!(limit >= resident, "{metrics}");
        }
        if cfg!(target_os = "linux") {
            assert!(
                metrics["threads"]
                    .as_u64()
                    .is_some_and(|threads| threads >= 1),
                "{metrics}"
            );
            let peak = metrics["peakResidentBytes"].as_u64().expect("peak");
            assert!(
                peak >= metrics["residentBytes"].as_u64().unwrap() / 2,
                "{metrics}"
            );
        }
    }
}
