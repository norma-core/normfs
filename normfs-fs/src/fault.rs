//! A seam for making a flush fail, so paths that only run on failing hardware
//! can be tested on hardware that works.
//!
//! The two techniques already here cannot reach them: a directory where the
//! next file goes injects an *open* failure, and `/dev/full` is Linux only and
//! cannot be a WAL path, which is derived from the queue and the file id.
//!
//! Off, this is a function returning `false`. The feature exists because this
//! crate's `cfg(test)` is invisible to `normfs`.

#[cfg(any(test, feature = "fault-injection"))]
mod on {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};

    fn scheduled() -> &'static Mutex<HashMap<PathBuf, u32>> {
        static SCHEDULED: OnceLock<Mutex<HashMap<PathBuf, u32>>> = OnceLock::new();
        SCHEDULED.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Keyed by path because a rotation builds a new writer per file: a test
    /// that wants one file's close to fail has no writer to hold.
    pub fn fail_flushes(path: impl AsRef<Path>, times: u32) {
        scheduled()
            .lock()
            .unwrap()
            .insert(path.as_ref().to_path_buf(), times);
    }

    fn truncates() -> &'static Mutex<HashMap<PathBuf, u32>> {
        static FAILURES: OnceLock<Mutex<HashMap<PathBuf, u32>>> = OnceLock::new();
        FAILURES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn fail_truncates(path: impl AsRef<Path>, times: u32) {
        truncates()
            .lock()
            .unwrap()
            .insert(path.as_ref().to_path_buf(), times);
    }

    pub(crate) fn take_truncate_failure(path: &Path) -> bool {
        let mut failures = truncates().lock().unwrap();
        match failures.get_mut(path) {
            Some(left) if *left > 0 => {
                *left -= 1;
                true
            }
            _ => false,
        }
    }

    pub fn heal(path: impl AsRef<Path>) {
        scheduled().lock().unwrap().remove(path.as_ref());
        truncates().lock().unwrap().remove(path.as_ref());
    }

    pub(crate) fn take_failure(path: &Path) -> bool {
        let mut scheduled = scheduled().lock().unwrap();
        let Some(left) = scheduled.get_mut(path) else {
            return false;
        };
        if *left == 0 {
            scheduled.remove(path);
            return false;
        }
        *left -= 1;
        true
    }
}

#[cfg(any(test, feature = "fault-injection"))]
pub use on::{fail_flushes, fail_truncates, heal};

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) use on::{take_failure, take_truncate_failure};

#[cfg(not(any(test, feature = "fault-injection")))]
#[inline(always)]
pub(crate) fn take_failure(_path: &std::path::Path) -> bool {
    false
}

#[cfg(not(any(test, feature = "fault-injection")))]
pub(crate) fn take_truncate_failure(_path: &std::path::Path) -> bool {
    false
}
