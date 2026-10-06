//! The proved protocol planner, as Rust sees it.
//!
//! `Plan` owns the path bytes the C plan points into, so the plan can be
//! moved between threads and the pointers stay valid; the C side never
//! allocates and never frees.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::Path;

// Mirrors c/include/normfs/fs_plan.h. Field order and widths must match:
// six 4-byte ints, then pointer-sized and 8-byte fields.
#[repr(C)]
struct RawPlan {
    kind: c_int,
    op: c_int,
    tmp_mode: c_int,
    restored: c_int,
    old_present: c_int,
    os_error: c_int,
    tmp: *const c_char,
    tmp_len: usize,
    dst: *const c_char,
    dst_len: usize,
    at: u64,
    total: u64,
    written: u64,
    ino: u64,
    old_len: u64,
}

const NORMFS_FS_OK: c_int = 0;
const NORMFS_FS_ERR_STATE: c_int = 1;

pub(crate) const NORMFS_FS_TMP_EXCL: c_int = 0;
pub(crate) const NORMFS_FS_TMP_TRUNC: c_int = 1;

unsafe extern "C" {
    fn normfs_fs_publish_init(
        plan: *mut RawPlan,
        tmp: *const c_char,
        tmp_len: usize,
        dst: *const c_char,
        dst_len: usize,
        tmp_mode: c_int,
        total: u64,
    );
    fn normfs_fs_append_init(
        plan: *mut RawPlan,
        dst: *const c_char,
        dst_len: usize,
        ino: u64,
        at: u64,
        total: u64,
    );
    fn normfs_fs_create_init(
        plan: *mut RawPlan,
        dst: *const c_char,
        dst_len: usize,
        tmp_mode: c_int,
        total: u64,
    );
    fn normfs_fs_remove_init(plan: *mut RawPlan, dst: *const c_char, dst_len: usize);
    fn normfs_fs_restore_init(
        plan: *mut RawPlan,
        dst: *const c_char,
        dst_len: usize,
        ino: u64,
        at: u64,
    );
    fn normfs_fs_plan_next(plan: *const RawPlan) -> c_int;
    fn normfs_fs_publish_ok(plan: *mut RawPlan, n: u64) -> c_int;
    fn normfs_fs_publish_absent(plan: *mut RawPlan) -> c_int;
    fn normfs_fs_publish_err(plan: *mut RawPlan, os_error: c_int) -> c_int;
    fn normfs_fs_append_ok(plan: *mut RawPlan, n: u64) -> c_int;
    fn normfs_fs_append_err(plan: *mut RawPlan, os_error: c_int) -> c_int;
    fn normfs_fs_create_ok(plan: *mut RawPlan, n: u64) -> c_int;
    fn normfs_fs_create_err(plan: *mut RawPlan, os_error: c_int) -> c_int;
    fn normfs_fs_remove_ok(plan: *mut RawPlan) -> c_int;
    fn normfs_fs_remove_err(plan: *mut RawPlan, os_error: c_int) -> c_int;
    fn normfs_fs_restore_report(plan: *mut RawPlan, os_error: c_int) -> c_int;
}

/// The operation an executor performs next. Values mirror `enum normfs_fs_op`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Open = 1,
    Write = 2,
    FsyncFile = 3,
    CloseFile = 4,
    StatDst = 5,
    Rename = 6,
    FsyncDir = 7,
    TruncateBack = 8,
    Unlink = 9,
    Done = 10,
    Failed = 11,
}

impl Op {
    fn from_raw(v: c_int) -> Result<Op, PlanError> {
        Ok(match v {
            1 => Op::Open,
            2 => Op::Write,
            3 => Op::FsyncFile,
            4 => Op::CloseFile,
            5 => Op::StatDst,
            6 => Op::Rename,
            7 => Op::FsyncDir,
            8 => Op::TruncateBack,
            9 => Op::Unlink,
            10 => Op::Done,
            11 => Op::Failed,
            other => return Err(PlanError::UnknownOp(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Publish,
    Append,
    Create,
    Remove,
    Restore,
}

/// Whether a temporary or created file replaces what is at its path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TmpMode {
    /// Fail if the path exists: for names chosen to be unique.
    Excl,
    /// Truncate what is there: for a fixed temporary name, and for a file
    /// whose previous incarnation is meant to be discarded.
    Trunc,
}

impl TmpMode {
    fn raw(self) -> c_int {
        match self {
            TmpMode::Excl => NORMFS_FS_TMP_EXCL,
            TmpMode::Trunc => NORMFS_FS_TMP_TRUNC,
        }
    }
}

#[derive(Debug)]
pub enum PlanError {
    /// A report that does not fit the plan's state: an executor bug.
    State,
    /// A path with an interior NUL.
    Path,
    PathTooLong,
    Size,
    UnknownOp(c_int),
    UnknownStatus(c_int),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::State => write!(f, "report does not fit the plan's state"),
            PlanError::PathTooLong => write!(f, "path exceeds the C planner limit"),
            PlanError::Size => write!(f, "file offset exceeds the supported range"),
            PlanError::Path => write!(f, "path contains an interior NUL"),
            PlanError::UnknownOp(v) => write!(f, "unknown op {v} from the C fs planner"),
            PlanError::UnknownStatus(v) => write!(f, "unknown status {v} from the C fs planner"),
        }
    }
}

impl std::error::Error for PlanError {}

pub struct Plan {
    raw: RawPlan,
    kind: Kind,
    _tmp: CString,
    _dst: CString,
}

// SAFETY: the raw pointers point into the CStrings this struct owns, which
// move with it; nothing else references them.
unsafe impl Send for Plan {}

fn cstring(path: &Path) -> Result<CString, PlanError> {
    use std::os::unix::ffi::OsStrExt;
    if path.as_os_str().as_bytes().len() >= 4096 {
        return Err(PlanError::PathTooLong);
    }
    CString::new(path.as_os_str().as_bytes()).map_err(|_| PlanError::Path)
}

fn status(rc: c_int) -> Result<(), PlanError> {
    match rc {
        NORMFS_FS_OK => Ok(()),
        NORMFS_FS_ERR_STATE => Err(PlanError::State),
        other => Err(PlanError::UnknownStatus(other)),
    }
}

impl Plan {
    fn blank(kind: Kind, tmp: CString, dst: CString) -> Plan {
        Plan {
            raw: RawPlan {
                kind: 0,
                op: 0,
                tmp_mode: 0,
                restored: 0,
                old_present: 0,
                os_error: 0,
                tmp: std::ptr::null(),
                tmp_len: 0,
                dst: std::ptr::null(),
                dst_len: 0,
                at: 0,
                total: 0,
                written: 0,
                ino: 0,
                old_len: 0,
            },
            kind,
            _tmp: tmp,
            _dst: dst,
        }
    }

    pub fn publish(tmp: &Path, dst: &Path, mode: TmpMode, total: u64) -> Result<Plan, PlanError> {
        let (tmp, dst) = (cstring(tmp)?, cstring(dst)?);
        let mut plan = Plan::blank(Kind::Publish, tmp, dst);
        // SAFETY: the CStrings outlive the plan; the C side stores the
        // pointers and lengths and reads nothing else.
        unsafe {
            normfs_fs_publish_init(
                &mut plan.raw,
                plan._tmp.as_ptr(),
                plan._tmp.as_bytes().len(),
                plan._dst.as_ptr(),
                plan._dst.as_bytes().len(),
                mode.raw(),
                total,
            );
        }
        Ok(plan)
    }

    pub fn append(path: &Path, ino: u64, at: u64, total: u64) -> Result<Plan, PlanError> {
        if at
            .checked_add(total)
            .is_none_or(|end| end > i64::MAX as u64)
        {
            return Err(PlanError::Size);
        }
        let dst = cstring(path)?;
        let mut plan = Plan::blank(Kind::Append, CString::default(), dst);
        // SAFETY: as in `publish`.
        unsafe {
            normfs_fs_append_init(
                &mut plan.raw,
                plan._dst.as_ptr(),
                plan._dst.as_bytes().len(),
                ino,
                at,
                total,
            );
        }
        Ok(plan)
    }

    pub fn create(dst: &Path, mode: TmpMode, total: u64) -> Result<Plan, PlanError> {
        let dst = cstring(dst)?;
        let mut plan = Plan::blank(Kind::Create, CString::default(), dst);
        // SAFETY: as in `publish`.
        unsafe {
            normfs_fs_create_init(
                &mut plan.raw,
                plan._dst.as_ptr(),
                plan._dst.as_bytes().len(),
                mode.raw(),
                total,
            );
        }
        Ok(plan)
    }

    pub fn remove(dst: &Path) -> Result<Plan, PlanError> {
        let dst = cstring(dst)?;
        let mut plan = Plan::blank(Kind::Remove, CString::default(), dst);
        // SAFETY: as in `publish`.
        unsafe {
            normfs_fs_remove_init(
                &mut plan.raw,
                plan._dst.as_ptr(),
                plan._dst.as_bytes().len(),
            );
        }
        Ok(plan)
    }

    pub fn restore(path: &Path, ino: u64, at: u64) -> Result<Plan, PlanError> {
        let dst = cstring(path)?;
        let mut plan = Plan::blank(Kind::Restore, CString::default(), dst);
        // SAFETY: as in `publish`.
        unsafe {
            normfs_fs_restore_init(
                &mut plan.raw,
                plan._dst.as_ptr(),
                plan._dst.as_bytes().len(),
                ino,
                at,
            );
        }
        Ok(plan)
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn next(&self) -> Result<Op, PlanError> {
        // SAFETY: raw is initialised by one of the constructors.
        Op::from_raw(unsafe { normfs_fs_plan_next(&self.raw) })
    }

    /// The operation named by `next` completed; `n` is the inode for `Open`,
    /// the bytes written for `Write`, the length for `StatDst`.
    pub fn ok(&mut self, n: u64) -> Result<(), PlanError> {
        // SAFETY: raw is initialised and exclusively borrowed.
        let rc = unsafe {
            match self.kind {
                Kind::Publish => normfs_fs_publish_ok(&mut self.raw, n),
                Kind::Append => normfs_fs_append_ok(&mut self.raw, n),
                Kind::Create => normfs_fs_create_ok(&mut self.raw, n),
                Kind::Remove => normfs_fs_remove_ok(&mut self.raw),
                Kind::Restore => normfs_fs_restore_report(&mut self.raw, 0),
            }
        };
        status(rc)
    }

    /// The operation named by `next` failed with `os_error` (> 0).
    pub fn err(&mut self, os_error: i32) -> Result<(), PlanError> {
        let os_error = if os_error > 0 { os_error } else { libc::EIO };
        // SAFETY: as in `ok`.
        let rc = unsafe {
            match self.kind {
                Kind::Publish => normfs_fs_publish_err(&mut self.raw, os_error),
                Kind::Append => normfs_fs_append_err(&mut self.raw, os_error),
                Kind::Create => normfs_fs_create_err(&mut self.raw, os_error),
                Kind::Remove => normfs_fs_remove_err(&mut self.raw, os_error),
                Kind::Restore => normfs_fs_restore_report(&mut self.raw, os_error),
            }
        };
        status(rc)
    }

    /// `StatDst` or `Unlink` found no file at the path.
    pub fn absent(&mut self) -> Result<(), PlanError> {
        // SAFETY: as in `ok`.
        let rc = unsafe {
            match self.kind {
                Kind::Publish => normfs_fs_publish_absent(&mut self.raw),
                Kind::Remove => normfs_fs_remove_ok(&mut self.raw),
                _ => NORMFS_FS_ERR_STATE,
            }
        };
        status(rc)
    }

    pub fn tmp(&self) -> &CStr {
        &self._tmp
    }

    pub fn dst(&self) -> &CStr {
        &self._dst
    }

    pub fn tmp_mode(&self) -> TmpMode {
        if self.raw.tmp_mode == NORMFS_FS_TMP_TRUNC {
            TmpMode::Trunc
        } else {
            TmpMode::Excl
        }
    }

    pub fn inode(&self) -> u64 {
        self.raw.ino
    }

    pub fn at(&self) -> u64 {
        self.raw.at
    }

    pub fn total(&self) -> u64 {
        self.raw.total
    }

    pub fn written(&self) -> u64 {
        self.raw.written
    }

    pub fn old_len(&self) -> Option<u64> {
        (self.raw.old_present != 0).then_some(self.raw.old_len)
    }

    pub fn restored(&self) -> bool {
        self.raw.restored != 0
    }

    pub fn os_error(&self) -> i32 {
        self.raw.os_error
    }
}

/// Byte-for-byte agreement with the C struct, checked where the FFI is
/// defined rather than trusted.
const _: () = assert!(std::mem::size_of::<RawPlan>() == 6 * 4 + 4 * 8 + 5 * 8);
