//! What differs between Unix and Windows below the plans: descriptors, path
//! bytes, positioned reads, file identity, and the error codes Rust reports
//! itself, in the numbering `io::Error::from_raw_os_error` reads on each.

use std::ffi::CStr;
use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(unix)]
pub(crate) use libc::{EINVAL, EIO, ENAMETOOLONG, ENOTDIR};

#[cfg(windows)]
mod codes {
    use windows_sys::Win32::Foundation;
    pub(crate) const EIO: i32 = Foundation::ERROR_IO_DEVICE as i32;
    pub(crate) const EINVAL: i32 = Foundation::ERROR_INVALID_PARAMETER as i32;
    pub(crate) const ENAMETOOLONG: i32 = Foundation::ERROR_FILENAME_EXCED_RANGE as i32;
    pub(crate) const ENOTDIR: i32 = Foundation::ERROR_DIRECTORY as i32;
}
#[cfg(windows)]
pub(crate) use codes::{EINVAL, EIO, ENAMETOOLONG, ENOTDIR};

/// What `normfs_fs_fd` is on the C side.
#[cfg(unix)]
pub(crate) type SysFd = std::os::raw::c_int;
#[cfg(windows)]
pub(crate) type SysFd = isize;

/// No descriptor; on Windows -1 would be the current process's handle.
#[cfg(unix)]
pub(crate) const NO_FD: SysFd = -1;
#[cfg(windows)]
pub(crate) const NO_FD: SysFd = 0;

#[cfg(unix)]
pub(crate) type OwnedSysFd = std::os::fd::OwnedFd;
#[cfg(windows)]
pub(crate) type OwnedSysFd = std::os::windows::io::OwnedHandle;

#[cfg(unix)]
pub(crate) fn raw(f: &impl std::os::fd::AsRawFd) -> SysFd {
    f.as_raw_fd()
}
#[cfg(windows)]
pub(crate) fn raw(f: &impl std::os::windows::io::AsRawHandle) -> SysFd {
    f.as_raw_handle() as SysFd
}

/// # Safety
/// `fd` is open and owned by nobody else.
#[cfg(unix)]
pub(crate) unsafe fn own(fd: SysFd) -> OwnedSysFd {
    // SAFETY: the caller hands over sole ownership of an open descriptor.
    unsafe { std::os::fd::FromRawFd::from_raw_fd(fd) }
}
/// # Safety
/// `fd` is an open handle owned by nobody else.
#[cfg(windows)]
pub(crate) unsafe fn own(fd: SysFd) -> OwnedSysFd {
    // SAFETY: the caller hands over sole ownership of an open handle.
    unsafe { std::os::windows::io::FromRawHandle::from_raw_handle(fd as _) }
}

/// The bytes the C layer takes for a path; on Windows it takes UTF-8 and
/// widens it, so a path that is not valid UTF-8 has none.
#[cfg(unix)]
pub(crate) fn path_bytes(path: &Path) -> Option<&[u8]> {
    Some(std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str()))
}
#[cfg(windows)]
pub(crate) fn path_bytes(path: &Path) -> Option<&[u8]> {
    path.to_str().map(str::as_bytes)
}

/// The inverse of `path_bytes` for a path the C layer holds.
pub(crate) fn path_of(c: &CStr) -> &Path {
    #[cfg(unix)]
    let path =
        Path::new(<std::ffi::OsStr as std::os::unix::ffi::OsStrExt>::from_bytes(c.to_bytes()));
    #[cfg(windows)]
    let path = Path::new(c.to_str().unwrap_or_default());
    path
}

#[cfg(unix)]
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}
/// seek_read also moves the cursor, which no reader of these files uses.
#[cfg(windows)]
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

#[cfg(unix)]
pub(crate) fn inode(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(file.metadata()?.ino())
}
/// The NTFS file index, which is what the C side reports as the inode too.
#[cfg(windows)]
pub(crate) fn inode(file: &File) -> io::Result<u64> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    // SAFETY: an all-zero BY_HANDLE_FILE_INFORMATION is a valid value of it.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is open for as long as file lives; info is writable.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    match (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow) {
        0 => Err(io::Error::new(io::ErrorKind::InvalidData, "file index 0")),
        ino => Ok(ino),
    }
}
