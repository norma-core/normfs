use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// The same three clocks station stamps its records with, so the two can be
/// lined up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// Keeps counting through suspend on Linux, unlike `Instant`.
    pub monotonic_ns: u64,
    pub local_ns: u64,
    /// Unix seconds when the process first took a stamp; tells lives apart
    /// even when the wall clock jumps.
    pub app_start_id: u64,
}

impl Stamp {
    pub fn now() -> Self {
        Self {
            monotonic_ns: monotonic_ns(),
            local_ns: local_ns(),
            app_start_id: app_start_id(),
        }
    }
}

#[cfg(target_os = "linux")]
const MONOTONIC: libc::clockid_t = libc::CLOCK_BOOTTIME;
#[cfg(not(target_os = "linux"))]
const MONOTONIC: libc::clockid_t = libc::CLOCK_MONOTONIC_RAW;

pub fn monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a valid, exclusively borrowed timespec for the call.
    if unsafe { libc::clock_gettime(MONOTONIC, &mut ts) } != 0 {
        return 0;
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

pub fn local_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos().min(u64::MAX as u128) as u64)
}

pub fn app_start_id() -> u64 {
    static START: OnceLock<u64> = OnceLock::new();
    *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    })
}
