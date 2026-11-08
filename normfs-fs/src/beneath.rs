//! Path operations that follow no link below the data directory. On Windows
//! they still resolve the path as given.

use std::ffi::{CString, OsString};
use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::raw::{c_char, c_int, c_void};

#[cfg(unix)]
unsafe extern "C" {
    fn normfs_fs_sys_stat(
        path: *const c_char,
        path_len: usize,
        root: usize,
        len: *mut u64,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_open_file(
        path: *const c_char,
        path_len: usize,
        root: usize,
        write: c_int,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_mkdir(
        path: *const c_char,
        path_len: usize,
        root: usize,
        remove: c_int,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_unlink(
        path: *const c_char,
        path_len: usize,
        root: usize,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_list_open(
        path: *const c_char,
        path_len: usize,
        root: usize,
        os_error: *mut c_int,
    ) -> *mut c_void;
    fn normfs_fs_sys_list_next(
        list: *mut c_void,
        name: *mut c_char,
        cap: usize,
        name_len: *mut usize,
        kind: *mut c_int,
        len: *mut u64,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_list_close(list: *mut c_void);
}

/// How many leading bytes of `path` are the data directory, for the shims.
/// Zero for a path at or above it, which is resolved as given.
pub(crate) fn root_len(root: Option<&Path>, path: &Path) -> io::Result<usize> {
    let Some(root) = root else { return Ok(0) };
    if cfg!(windows) || root.starts_with(path) {
        return Ok(0);
    }
    let bytes = crate::os::path_bytes(path);
    let mut prefix = crate::os::path_bytes(root);
    while let Some([rest @ .., b'/']) = prefix {
        prefix = Some(rest);
    }
    match (bytes, prefix) {
        (Some(p), Some(r))
            if !r.is_empty() && p.starts_with(r) && p.get(r.len()) == Some(&b'/') =>
        {
            Ok(r.len())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is outside the data directory", path.display()),
        )),
    }
}

/// A path the way the shims take it.
pub(crate) struct At {
    path: CString,
    root: usize,
}

impl At {
    pub(crate) fn new(root: Option<&Path>, path: &Path) -> io::Result<At> {
        let root = root_len(root, path)?;
        let mut bytes = crate::os::path_bytes(path).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "path is not valid UTF-8")
        })?;
        // "wal/" names what "wal" does; the walk would see an empty last name.
        while let [rest @ .., b'/'] = bytes
            && rest.len() > root
        {
            bytes = rest;
        }
        let path =
            CString::new(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if path.as_bytes().len() >= 4096 {
            return Err(io::Error::from_raw_os_error(crate::os::ENAMETOOLONG));
        }
        Ok(At { path, root })
    }

    #[cfg(windows)]
    pub(crate) fn path(&self) -> &Path {
        crate::os::path_of(&self.path)
    }

    pub(crate) fn raw(&self) -> (*const std::os::raw::c_char, usize, usize) {
        (self.path.as_ptr(), self.path.as_bytes().len(), self.root)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stat {
    kind: i32,
    len: u64,
}

impl Stat {
    pub fn is_file(&self) -> bool {
        self.kind == 1
    }

    pub fn is_dir(&self) -> bool {
        self.kind == 2
    }

    /// The length of a regular file, 0 for anything else.
    pub fn size(&self) -> u64 {
        self.len
    }
}

pub(crate) struct Entry {
    pub name: OsString,
    pub stat: Stat,
}

#[cfg(unix)]
fn os_err(e: c_int) -> io::Error {
    io::Error::from_raw_os_error(e)
}

/// `None` when nothing is at the path; a link there is not followed.
#[cfg(unix)]
pub(crate) fn stat(at: &At) -> io::Result<Option<Stat>> {
    let (p, n, root) = at.raw();
    let (mut len, mut e) = (0u64, 0);
    // SAFETY: p is NUL-terminated and n long; len and e are valid for writes.
    let kind = unsafe { normfs_fs_sys_stat(p, n, root, &mut len, &mut e) };
    match kind {
        0 => Ok(None),
        1..=3 => Ok(Some(Stat { kind, len })),
        _ => Err(os_err(e)),
    }
}

#[cfg(unix)]
pub(crate) fn open(at: &At, write: bool) -> io::Result<File> {
    use std::os::fd::FromRawFd;

    let (p, n, root) = at.raw();
    let mut e = 0;
    // SAFETY: p is NUL-terminated and n long; e is valid for writes.
    let fd = unsafe { normfs_fs_sys_open_file(p, n, root, write as c_int, &mut e) };
    if fd < 0 {
        return Err(os_err(e));
    }
    // SAFETY: fd is a fresh descriptor the shim returned to us.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn mkdir_or_rmdir(at: &At, remove: bool) -> io::Result<()> {
    let (p, n, root) = at.raw();
    let mut e = 0;
    // SAFETY: p is NUL-terminated and n long; e is valid for writes.
    match unsafe { normfs_fs_sys_mkdir(p, n, root, remove as c_int, &mut e) } {
        0 => Ok(()),
        _ => Err(os_err(e)),
    }
}

#[cfg(unix)]
pub(crate) fn mkdir(at: &At) -> io::Result<()> {
    mkdir_or_rmdir(at, false)
}

#[cfg(unix)]
pub(crate) fn rmdir(at: &At) -> io::Result<()> {
    mkdir_or_rmdir(at, true)
}

/// Fails with `NotFound` when nothing is at the path, as `remove_file` does.
#[cfg(unix)]
pub(crate) fn unlink(at: &At) -> io::Result<()> {
    let (p, n, root) = at.raw();
    let mut e = 0;
    // SAFETY: p is NUL-terminated and n long; e is valid for writes.
    match unsafe { normfs_fs_sys_unlink(p, n, root, &mut e) } {
        0 => Ok(()),
        1 => Err(io::ErrorKind::NotFound.into()),
        _ => Err(os_err(e)),
    }
}

#[cfg(unix)]
pub(crate) fn list(at: &At) -> io::Result<Vec<Entry>> {
    use std::os::unix::ffi::OsStringExt;

    let (p, n, root) = at.raw();
    let mut e = 0;
    // SAFETY: p is NUL-terminated and n long; e is valid for writes.
    let list = unsafe { normfs_fs_sys_list_open(p, n, root, &mut e) };
    if list.is_null() {
        return Err(os_err(e));
    }
    let mut entries = Vec::new();
    let mut name = [0u8; 1024];
    let result = loop {
        let (mut name_len, mut kind, mut len) = (0usize, 0, 0u64);
        // SAFETY: list is open until list_close below; name holds cap bytes;
        // the outputs are valid for writes.
        let rc = unsafe {
            normfs_fs_sys_list_next(
                list,
                name.as_mut_ptr().cast(),
                name.len(),
                &mut name_len,
                &mut kind,
                &mut len,
                &mut e,
            )
        };
        match rc {
            1 => entries.push(Entry {
                name: OsString::from_vec(name[..name_len].to_vec()),
                stat: Stat { kind, len },
            }),
            0 => break Ok(entries),
            _ => break Err(os_err(e)),
        }
    };
    // SAFETY: list came from list_open and is closed once.
    unsafe { normfs_fs_sys_list_close(list) };
    result
}

#[cfg(windows)]
fn stat_of(m: &std::fs::Metadata) -> Stat {
    let kind = if m.is_file() {
        1
    } else if m.is_dir() {
        2
    } else {
        3
    };
    let len = if kind == 1 { m.len() } else { 0 };
    Stat { kind, len }
}

#[cfg(windows)]
pub(crate) fn stat(at: &At) -> io::Result<Option<Stat>> {
    match std::fs::symlink_metadata(at.path()) {
        Ok(m) => Ok(Some(stat_of(&m))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(windows)]
pub(crate) fn open(at: &At, write: bool) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .read(!write)
        .write(write)
        .open(at.path())
}

#[cfg(windows)]
pub(crate) fn mkdir(at: &At) -> io::Result<()> {
    std::fs::create_dir(at.path())
}

#[cfg(windows)]
pub(crate) fn rmdir(at: &At) -> io::Result<()> {
    std::fs::remove_dir(at.path())
}

#[cfg(windows)]
pub(crate) fn unlink(at: &At) -> io::Result<()> {
    std::fs::remove_file(at.path())
}

#[cfg(windows)]
pub(crate) fn list(at: &At) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(at.path())? {
        let entry = entry?;
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        entries.push(Entry {
            name: entry.file_name(),
            stat: stat_of(&meta),
        });
    }
    Ok(entries)
}
