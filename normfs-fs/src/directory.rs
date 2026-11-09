use std::collections::HashMap;
use std::io;
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::beneath::{self, At};
use crate::plan::PlanError;

#[repr(C)]
struct Status {
    code: c_int,
    os_error: c_int,
}

unsafe extern "C" {
    fn normfs_fs_sync_created_dir(
        path: *const c_char,
        len: usize,
        root: usize,
        stage: *mut c_int,
    ) -> Status;
}

type Pending = HashMap<PathBuf, Arc<Mutex<c_int>>>;

// Entries precede mkdir so an observer of the new name must await its barriers.
static PENDING: OnceLock<Mutex<Pending>> = OnceLock::new();

/// With a data directory the key is the path as given: canonicalizing would
/// follow the links the walk refuses.
fn walked(root: Option<&Path>) -> bool {
    root.is_some() && cfg!(unix)
}

pub(crate) fn mkdir_all(path: &Path, root: Option<&Path>) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    match beneath::stat_as_given(&At::new(root, path)?) {
        Ok(Some(meta)) if meta.is_dir() => {
            let entry = {
                if PENDING
                    .get_or_init(Mutex::default)
                    .lock()
                    .unwrap()
                    .is_empty()
                {
                    return Ok(());
                }
                let canonical = if walked(root) {
                    path.to_path_buf()
                } else {
                    std::fs::canonicalize(path)?
                };
                let pending = PENDING.get().unwrap().lock().unwrap();
                pending
                    .get(&canonical)
                    .cloned()
                    .map(|entry| (canonical, entry))
            };
            return match entry {
                Some((path, entry)) => finish(&path, root, entry),
                None => Ok(()),
            };
        }
        Ok(Some(_)) => return Err(io::Error::from_raw_os_error(crate::os::ENOTDIR)),
        Ok(None) => {}
        Err(e) => return Err(e),
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    mkdir_all(parent, root)?;
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::from_raw_os_error(crate::os::EINVAL))?;
    let canonical = if walked(root) {
        path.to_path_buf()
    } else {
        std::fs::canonicalize(parent)?.join(name)
    };
    let entry = {
        let mut pending = PENDING.get_or_init(Mutex::default).lock().unwrap();
        pending
            .entry(canonical.clone())
            .or_insert_with(|| Arc::new(Mutex::new(0)))
            .clone()
    };
    finish(&canonical, root, entry)
}

fn finish(path: &Path, root: Option<&Path>, entry: Arc<Mutex<c_int>>) -> io::Result<()> {
    let at = At::new(root, path)?;
    let mut stage = entry.lock().unwrap();
    // A removal does not reset a retained stage, so resume only if the directory is still there.
    if *stage != 0 && beneath::stat_as_given(&at)?.is_none() {
        *stage = 0;
    }
    if *stage == 0 {
        match beneath::mkdir(&at) {
            Ok(()) => *stage = 1,
            Err(e)
                if e.kind() == io::ErrorKind::AlreadyExists
                    && beneath::stat_as_given(&at)
                        .ok()
                        .flatten()
                        .is_some_and(|m| m.is_dir()) =>
            {
                *stage = 3
            }
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
        sync_created(&at, &mut stage)?;
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

fn sync_created(at: &At, stage: &mut c_int) -> io::Result<()> {
    #[cfg(unix)]
    let (path, len, root) = at.raw();
    #[cfg(windows)]
    let owned = {
        let bytes = crate::os::path_bytes(at.path()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path is not valid UTF-8")
        })?;
        std::ffi::CString::new(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
    };
    #[cfg(windows)]
    let (path, len, root) = (owned.as_ptr(), owned.as_bytes().len(), 0);
    if len >= 4096 {
        return Err(io::Error::from_raw_os_error(crate::os::ENAMETOOLONG));
    }
    // SAFETY: the owned NUL-terminated path is bounded and disjoint from the retained stage.
    let status = unsafe { normfs_fs_sync_created_dir(path, len, root, stage) };
    match status.code {
        0 => Ok(()),
        1 => Err(io::Error::from_raw_os_error(status.os_error)),
        other => Err(io::Error::other(PlanError::UnknownStatus(other))),
    }
}
