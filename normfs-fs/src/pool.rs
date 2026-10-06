//! A bounded pool limits concurrent flushes, which can contend on the device's journal.

use std::ffi::CStr;
use std::fs::File;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::raw::{c_char, c_int};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::executor::{Executor, Finished, Job, PlanJob, Resources, Task};
use crate::plan::{Kind, Op, Plan, PlanError};
use crate::{FsError, PublishReport};

#[repr(C)]
struct Iov {
    base: *const u8,
    len: usize,
}

unsafe extern "C" {
    fn normfs_fs_sys_open_create(
        path: *const c_char,
        path_len: usize,
        mode: c_int,
        ino: *mut u64,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_pwritev_all(
        fd: c_int,
        iov: *const Iov,
        cnt: usize,
        off: u64,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_fsync(fd: c_int, os_error: *mut c_int) -> c_int;
    fn normfs_fs_sys_close(fd: c_int, os_error: *mut c_int) -> c_int;
    fn normfs_fs_sys_file_len(
        path: *const c_char,
        path_len: usize,
        len: *mut u64,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_rename(
        src: *const c_char,
        src_len: usize,
        dst: *const c_char,
        dst_len: usize,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_fsync_parent(
        path: *const c_char,
        path_len: usize,
        os_error: *mut c_int,
    ) -> c_int;
    fn normfs_fs_sys_ftruncate(fd: c_int, len: u64, os_error: *mut c_int) -> c_int;
    fn normfs_fs_sys_unlink(path: *const c_char, path_len: usize, os_error: *mut c_int) -> c_int;
}

// Must match the shim's fixed-size iovec array.
const IOV_MAX: usize = 1024;

pub(crate) struct Pool {
    tx: Mutex<Option<Sender<Job>>>,
}

impl Pool {
    pub(crate) fn start(threads: usize) -> std::io::Result<Arc<Pool>> {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads.max(1) {
            let rx = rx.clone();
            std::thread::Builder::new()
                .name(format!("normfs-fs-{i}"))
                .spawn(move || worker(rx))?;
        }
        Ok(Arc::new(Pool {
            tx: Mutex::new(Some(tx)),
        }))
    }
}

impl Executor for Pool {
    fn submit(&self, job: Job) -> Result<(), FsError> {
        let guard = self.tx.lock().unwrap();
        match guard.as_ref() {
            Some(tx) => tx.send(job).map_err(|_| FsError::ExecutorGone),
            None => Err(FsError::ExecutorGone),
        }
    }

    fn name(&self) -> &'static str {
        "pool"
    }
}

fn worker(rx: Arc<Mutex<Receiver<Job>>>) {
    loop {
        let job = rx.lock().unwrap().recv();
        match job {
            Ok(Job { task, permit }) => {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match task {
                    Task::Plan(job) => run_plan(job),
                    Task::Blocking(f) => f(),
                }));
                drop(permit);
            }
            Err(_) => return,
        }
    }
}

struct Errno(c_int);

impl Errno {
    fn new() -> Errno {
        Errno(0)
    }
}

fn path_of(c: &CStr) -> &Path {
    use std::os::unix::ffi::OsStrExt;
    Path::new(std::ffi::OsStr::from_bytes(c.to_bytes()))
}

fn run_plan(job: PlanJob) {
    let PlanJob {
        mut plan,
        res,
        then,
        reply,
    } = job;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Driven {
            file,
            absent,
            opened,
            renamed,
        } = drive(&mut plan, &res)?;
        if opened && plan.kind() == Kind::Publish && plan.next().ok() == Some(Op::Failed) {
            // An unsuccessful exclusive open never owns the existing name.
            let mut e = Errno::new();
            // SAFETY: tmp is a NUL-terminated path owned by the plan.
            unsafe {
                normfs_fs_sys_unlink(plan.tmp().as_ptr(), plan.tmp().to_bytes().len(), &mut e.0)
            };
        }
        // The rename replaced the file; a directory sync failing after it does not undo that.
        if let Some(then) = then
            && renamed
        {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                then(&PublishReport {
                    old_len: plan.old_len(),
                    new_len: plan.total(),
                });
            }))
            .is_err()
            {
                log::error!("publish committed but its accounting callback panicked");
            }
        }
        Ok(Finished { plan, file, absent })
    }))
    .unwrap_or(Err(FsError::JobPanicked));
    let _ = reply.send(result);
}

struct Driven {
    file: Option<File>,
    absent: bool,
    opened: bool,
    renamed: bool,
}

/// The step loop. Errors here are executor faults (a report the planner
/// refuses); a failing syscall is a report, not an error.
fn drive(plan: &mut Plan, res: &Resources) -> Result<Driven, FsError> {
    if plan.kind() == Kind::Publish {
        if path_of(plan.tmp()) == path_of(plan.dst()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "publish paths alias",
            )
            .into());
        }
    }
    let sync = res.sync;
    let mut owned: Option<OwnedFd> = None;
    let mut absent = false;
    let mut opened = false;
    let mut renamed = false;
    let fd_of = |owned: &Option<OwnedFd>| -> RawFd {
        match (owned, &res.file) {
            (Some(fd), _) => fd.as_raw_fd(),
            (None, Some(f)) => f.as_raw_fd(),
            (None, None) => -1,
        }
    };

    loop {
        let op = plan.next()?;
        let mut e = Errno::new();
        match op {
            Op::Open => {
                let target = if plan.kind() == Kind::Publish {
                    plan.tmp()
                } else {
                    plan.dst()
                };
                let mode = match plan.tmp_mode() {
                    crate::TmpMode::Excl => crate::plan::NORMFS_FS_TMP_EXCL,
                    crate::TmpMode::Trunc => crate::plan::NORMFS_FS_TMP_TRUNC,
                };
                let mut ino = 0u64;
                // SAFETY: target is a NUL-terminated path owned by the plan;
                // ino and e are valid for writes.
                let fd = unsafe {
                    normfs_fs_sys_open_create(
                        target.as_ptr(),
                        target.to_bytes().len(),
                        mode,
                        &mut ino,
                        &mut e.0,
                    )
                };
                if fd >= 0 {
                    opened = true;
                    // SAFETY: fd is a fresh descriptor the shim returned to us.
                    owned = Some(unsafe { OwnedFd::from_raw_fd(fd) });
                    plan.ok(ino)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::Write => {
                let fd = fd_of(&owned);
                let (iov, n) = tail_iovs(&res.runs, plan.written());
                if iov.is_empty() {
                    plan.err(libc::EIO)?;
                    continue;
                }
                let off = plan.at() + plan.written();
                // SAFETY: iov points into res.runs, which outlives the call.
                let rc = unsafe {
                    normfs_fs_sys_pwritev_all(fd, iov.as_ptr(), iov.len(), off, &mut e.0)
                };
                if rc == 0 {
                    plan.ok(n)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::FsyncFile => {
                // Fail after writing to exercise rollback; a fault armed before
                // file creation must affect the append, not the creation sync.
                if plan.kind() == Kind::Append && crate::fault::take_failure(path_of(plan.dst())) {
                    plan.err(libc::EIO)?;
                    continue;
                }
                if !sync {
                    plan.ok(0)?;
                    continue;
                }
                // SAFETY: fd is open; e is valid for writes.
                let rc = unsafe { normfs_fs_sys_fsync(fd_of(&owned), &mut e.0) };
                if rc == 0 {
                    plan.ok(0)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::CloseFile => {
                let rc = match owned.take() {
                    // SAFETY: we own the descriptor and forget it after.
                    Some(fd) => unsafe {
                        let raw = fd.as_raw_fd();
                        std::mem::forget(fd);
                        normfs_fs_sys_close(raw, &mut e.0)
                    },
                    None => 0,
                };
                if rc == 0 {
                    plan.ok(0)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::StatDst => {
                let mut len = 0u64;
                // SAFETY: dst is a NUL-terminated path owned by the plan.
                let rc = unsafe {
                    normfs_fs_sys_file_len(
                        plan.dst().as_ptr(),
                        plan.dst().to_bytes().len(),
                        &mut len,
                        &mut e.0,
                    )
                };
                match rc {
                    1 => plan.ok(len)?,
                    0 => plan.absent()?,
                    _ => plan.err(e.0)?,
                }
            }
            Op::Rename => {
                // SAFETY: both are NUL-terminated paths owned by the plan.
                let rc = unsafe {
                    normfs_fs_sys_rename(
                        plan.tmp().as_ptr(),
                        plan.tmp().to_bytes().len(),
                        plan.dst().as_ptr(),
                        plan.dst().to_bytes().len(),
                        &mut e.0,
                    )
                };
                if rc == 0 {
                    renamed = true;
                    plan.ok(0)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::FsyncDir => {
                if !sync {
                    plan.ok(0)?;
                    continue;
                }
                let dst = plan.dst();
                let err = crate::dir_sync::sync_parent(dst, || {
                    // SAFETY: dst is a NUL-terminated path owned by the plan.
                    let rc = unsafe {
                        normfs_fs_sys_fsync_parent(dst.as_ptr(), dst.to_bytes().len(), &mut e.0)
                    };
                    match rc {
                        0 => 0,
                        _ if e.0 > 0 => e.0,
                        _ => libc::EIO,
                    }
                });
                if err == 0 {
                    plan.ok(0)?;
                } else {
                    plan.err(err)?;
                }
            }
            Op::TruncateBack => {
                if crate::fault::take_truncate_failure(path_of(plan.dst())) {
                    plan.err(libc::EIO)?;
                    continue;
                }
                // SAFETY: fd is open; e is valid for writes.
                let rc = unsafe { normfs_fs_sys_ftruncate(fd_of(&owned), plan.at(), &mut e.0) };
                if rc == 0 {
                    plan.ok(0)?;
                } else {
                    plan.err(e.0)?;
                }
            }
            Op::Unlink => {
                // SAFETY: dst is a NUL-terminated path owned by the plan.
                let rc = unsafe {
                    normfs_fs_sys_unlink(plan.dst().as_ptr(), plan.dst().to_bytes().len(), &mut e.0)
                };
                match rc {
                    0 => plan.ok(0)?,
                    1 => {
                        absent = true;
                        plan.absent()?;
                    }
                    _ => plan.err(e.0)?,
                }
            }
            Op::Done => {
                let file = if plan.kind() == Kind::Create {
                    owned.take().map(File::from)
                } else {
                    None
                };
                return Ok(Driven {
                    file,
                    absent,
                    opened,
                    renamed,
                });
            }
            Op::Failed => {
                return Ok(Driven {
                    file: None,
                    absent,
                    opened,
                    renamed,
                });
            }
        }
    }
}

fn tail_iovs(runs: &[bytes::Bytes], written: u64) -> (Vec<Iov>, u64) {
    let mut skip = written;
    let mut out = Vec::new();
    let mut n = 0u64;
    for run in runs {
        if out.len() == IOV_MAX {
            break;
        }
        let len = run.len() as u64;
        if skip >= len {
            skip -= len;
            continue;
        }
        let start = skip as usize;
        skip = 0;
        out.push(Iov {
            base: run[start..].as_ptr(),
            len: run.len() - start,
        });
        n += (run.len() - start) as u64;
    }
    (out, n)
}

impl From<PlanError> for FsError {
    fn from(e: PlanError) -> Self {
        FsError::Plan(e)
    }
}
