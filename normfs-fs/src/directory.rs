use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::plan::PlanError;

#[repr(C)]
struct Status {
    code: c_int,
    os_error: c_int,
}

unsafe extern "C" {
    fn normfs_fs_sync_created_dir(path: *const c_char, len: usize, stage: *mut c_int) -> Status;
}

type Pending = HashMap<PathBuf, Arc<Mutex<c_int>>>;

// Entries precede mkdir so an observer of the new name must await its barriers.
static PENDING: OnceLock<Mutex<Pending>> = OnceLock::new();

pub(crate) fn mkdir_all(path: &Path) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => {
            let entry = {
                if PENDING
                    .get_or_init(Mutex::default)
                    .lock()
                    .unwrap()
                    .is_empty()
                {
                    return Ok(());
                }
                let canonical = std::fs::canonicalize(path)?;
                let pending = PENDING.get().unwrap().lock().unwrap();
                pending
                    .get(&canonical)
                    .cloned()
                    .map(|entry| (canonical, entry))
            };
            return match entry {
                Some((path, entry)) => finish(&path, entry),
                None => Ok(()),
            };
        }
        Ok(_) => return Err(io::Error::from_raw_os_error(libc::ENOTDIR)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    mkdir_all(parent)?;
    let canonical = std::fs::canonicalize(parent)?.join(
        path.file_name()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?,
    );
    let entry = {
        let mut pending = PENDING.get_or_init(Mutex::default).lock().unwrap();
        pending
            .entry(canonical.clone())
            .or_insert_with(|| Arc::new(Mutex::new(0)))
            .clone()
    };
    finish(&canonical, entry)
}

fn finish(path: &Path, entry: Arc<Mutex<c_int>>) -> io::Result<()> {
    let mut stage = entry.lock().unwrap();
    // A removal does not reset a retained stage, so resume only if the directory is still there.
    if *stage != 0 {
        match std::fs::symlink_metadata(path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => *stage = 0,
            Err(e) => return Err(e),
        }
    }
    if *stage == 0 {
        match std::fs::create_dir(path) {
            Ok(()) => *stage = 1,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => *stage = 3,
            Err(e) => {
                let mut pending = PENDING.get().unwrap().lock().unwrap();
                // No name exists yet; the last waiter may discard this reservation.
                if Arc::strong_count(&entry) == 2 {
                    pending.remove(path);
                }
                return Err(e);
            }
        }
    }
    if *stage != 3 {
        sync_created(path, &mut stage)?;
    }
    let mut pending = PENDING.get().unwrap().lock().unwrap();
    if pending
        .get(path)
        .is_some_and(|current| Arc::ptr_eq(current, &entry))
    {
        pending.remove(path);
    }
    Ok(())
}

fn sync_created(path: &Path, stage: &mut c_int) -> io::Result<()> {
    let cpath = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    if cpath.as_bytes().len() >= 4096 {
        return Err(io::Error::from_raw_os_error(libc::ENAMETOOLONG));
    }
    // SAFETY: the owned NUL-terminated path is bounded and disjoint from the retained stage.
    let status =
        unsafe { normfs_fs_sync_created_dir(cpath.as_ptr(), cpath.as_bytes().len(), stage) };
    match status.code {
        0 => Ok(()),
        1 => Err(io::Error::from_raw_os_error(status.os_error)),
        other => Err(io::Error::other(PlanError::UnknownStatus(other))),
    }
}
