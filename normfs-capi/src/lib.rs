//! The functions declared in `include/normfs.h`. Each one blocks on the
//! handle's own runtime and turns a panic into `NORMFS_ERR_PANIC`.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs::{File, TryLockError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, QueueId, ReadPosition, UintN};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch};

pub const NORMFS_OK: c_int = 0;
pub const NORMFS_ERR_INVALID: c_int = 1;
pub const NORMFS_ERR_NOT_FOUND: c_int = 2;
pub const NORMFS_ERR_EMPTY: c_int = 3;
pub const NORMFS_ERR_TOO_LARGE: c_int = 4;
pub const NORMFS_ERR_CLOSED: c_int = 5;
pub const NORMFS_ERR_ID_RANGE: c_int = 6;
pub const NORMFS_ERR_IO: c_int = 7;
pub const NORMFS_ERR_PANIC: c_int = 8;

pub type RecordCb =
    Option<unsafe extern "C" fn(ctx: *mut c_void, id: u64, data: *const u8, len: usize) -> c_int>;

/// The `normfs_t` behind the C handle.
pub struct Handle {
    fs: Arc<NormFS>,
    rt: Runtime,
    stop_follows: watch::Sender<bool>,
    _lock: File,
}

const FLUSH_TIMEOUT: Duration = Duration::from_secs(10);

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn fail(code: c_int, message: impl std::fmt::Display) -> c_int {
    let text = CString::new(message.to_string().replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = text);
    code
}

fn fail_with(e: Error) -> c_int {
    let code = match &e {
        Error::QueueNotFound | Error::NotFound => NORMFS_ERR_NOT_FOUND,
        Error::QueueEmpty => NORMFS_ERR_EMPTY,
        Error::RecordTooLarge(_) => NORMFS_ERR_TOO_LARGE,
        Error::QueueClosed | Error::Closing => NORMFS_ERR_CLOSED,
        Error::BadQueueName(_) | Error::ReservedQueue => NORMFS_ERR_INVALID,
        _ => NORMFS_ERR_IO,
    };
    fail(code, e)
}

fn guard(f: impl FnOnce() -> c_int) -> c_int {
    catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| fail(NORMFS_ERR_PANIC, "NormFS panicked; see its log"))
}

/// # Safety
/// `s` is NULL or a NUL-terminated string that outlives the returned slice.
unsafe fn text<'a>(s: *const c_char) -> Result<&'a str, c_int> {
    if s.is_null() {
        return Err(fail(NORMFS_ERR_INVALID, "a string argument is NULL"));
    }
    // SAFETY: non-NULL and NUL-terminated by the caller's contract.
    unsafe { CStr::from_ptr(s) }
        .to_str()
        .map_err(|_| fail(NORMFS_ERR_INVALID, "a string argument is not UTF-8"))
}

/// # Safety
/// `fs` is NULL or a handle from `normfs_open` not yet closed.
unsafe fn handle<'a>(fs: *mut Handle) -> Result<&'a Handle, c_int> {
    // SAFETY: by the caller's contract a non-NULL `fs` is a live handle.
    unsafe { fs.as_ref() }.ok_or_else(|| fail(NORMFS_ERR_INVALID, "the handle is NULL"))
}

fn queue_id(h: &Handle, name: &str) -> Result<QueueId, c_int> {
    // "" is the instance root and a leading '/' leaves the instance.
    // A '\\' would split the name into directories on Windows only.
    if name.is_empty() || name.starts_with('/') || name.contains('\\') {
        return Err(fail(NORMFS_ERR_INVALID, format!("bad queue name {name:?}")));
    }
    Ok(h.fs.resolve(name))
}

fn id64(id: &UintN) -> Result<u64, c_int> {
    id.to_u64()
        .map_err(|_| fail(NORMFS_ERR_ID_RANGE, format!("id {id} does not fit 64 bits")))
}

/// # Safety
/// `data_dir` is a NUL-terminated string; `out` points to writable storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_open(data_dir: *const c_char, out: *mut *mut Handle) -> c_int {
    guard(|| {
        if out.is_null() {
            return fail(NORMFS_ERR_INVALID, "out is NULL");
        }
        // SAFETY: checked non-NULL; the caller gives writable storage.
        unsafe { *out = std::ptr::null_mut() };
        // SAFETY: forwarded from this function's contract.
        let dir = match unsafe { text(data_dir) } {
            Ok(dir) => PathBuf::from(dir),
            Err(code) => return code,
        };
        // A second handle on one directory would hand out the same ids twice.
        // Windows locks only a handle opened for reading or writing.
        let lock = std::fs::create_dir_all(&dir).and_then(|()| {
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(dir.join(".lock"))
        });
        let lock = match lock {
            Ok(lock) => lock,
            Err(e) => return fail(NORMFS_ERR_IO, e),
        };
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return fail(NORMFS_ERR_IO, "data_dir is already open");
            }
            Err(TryLockError::Error(e)) => return fail(NORMFS_ERR_IO, e),
        }
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => return fail(NORMFS_ERR_IO, e),
        };
        match rt.block_on(NormFS::new(dir, NormFsSettings::all_active())) {
            Ok(fs) => {
                let fs = Arc::new(fs);
                // SAFETY: checked non-NULL above.
                let handle = Handle {
                    fs,
                    rt,
                    stop_follows: watch::Sender::new(false),
                    _lock: lock,
                };
                unsafe { *out = Box::into_raw(Box::new(handle)) };
                NORMFS_OK
            }
            Err(e) => fail_with(e),
        }
    })
}

/// # Safety
/// `fs` is NULL or a handle from `normfs_open`; it is invalid afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_close(fs: *mut Handle) -> c_int {
    guard(|| {
        if fs.is_null() {
            return NORMFS_OK;
        }
        // SAFETY: a live handle from normfs_open, taken back exactly once.
        let Handle {
            fs,
            rt,
            _lock: lock,
            ..
        } = *unsafe { Box::from_raw(fs) };
        let closed = rt.block_on(fs.close());
        // Dropped inside the runtime, before it, so its tasks can still see it.
        let entered = rt.enter();
        drop(fs);
        drop(entered);
        drop(rt);
        drop(lock);
        match closed {
            Ok(()) => NORMFS_OK,
            Err(e) => fail_with(e),
        }
    })
}

/// # Safety
/// `fs` is a live handle, `queue` a NUL-terminated string, `data` points to
/// `len` readable bytes (or `len` is 0), and `id` is NULL or writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_append(
    fs: *mut Handle,
    queue: *const c_char,
    data: *const u8,
    len: usize,
    id: *mut u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let (h, queue) = match unsafe { (handle(fs), text(queue)) } {
            (Ok(h), Ok(q)) => match queue_id(h, q) {
                Ok(q) => (h, q),
                Err(code) => return code,
            },
            (Err(code), _) | (_, Err(code)) => return code,
        };
        if data.is_null() && len > 0 {
            return fail(NORMFS_ERR_INVALID, "data is NULL");
        }
        let record = if len == 0 {
            Bytes::new()
        } else {
            // SAFETY: `data` holds `len` readable bytes; copied before return.
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) })
        };
        let appended = h.rt.block_on(async {
            h.fs.ensure_queue_exists_for_write(&queue).await?;
            h.fs.enqueue(&queue, record).await
        });
        let new_id = match appended {
            Ok(new_id) => new_id,
            Err(e) => return fail_with(e),
        };
        if !id.is_null() {
            match id64(&new_id) {
                // SAFETY: checked non-NULL; writable by contract.
                Ok(v) => unsafe { *id = v },
                Err(code) => return code,
            }
        }
        NORMFS_OK
    })
}

/// # Safety
/// `fs` is a live handle and `queue` a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_flush(fs: *mut Handle, queue: *const c_char) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let (h, queue) = match unsafe { (handle(fs), text(queue)) } {
            (Ok(h), Ok(q)) => match queue_id(h, q) {
                Ok(q) => (h, q),
                Err(code) => return code,
            },
            (Err(code), _) | (_, Err(code)) => return code,
        };
        let flushed = h.rt.block_on(async {
            h.fs.ensure_queue_exists_for_read(&queue).await?;
            h.fs.flush_queue(&queue).await
        });
        if let Err(e) = flushed {
            return fail_with(e);
        }
        let last = match h.fs.get_last_id(&queue) {
            Ok(last) => match id64(&last) {
                Ok(last) => last,
                Err(code) => return code,
            },
            Err(Error::QueueEmpty) => return NORMFS_OK,
            Err(e) => return fail_with(e),
        };
        // A WAL queue has nothing to seal: its writer syncs within a few ms.
        let deadline = Instant::now() + FLUSH_TIMEOUT;
        while !h.fs.is_durable_through(&queue, last) {
            if Instant::now() >= deadline {
                return fail(NORMFS_ERR_IO, "records did not reach the disk in 10 s");
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        NORMFS_OK
    })
}

/// # Safety
/// `fs` is a live handle, `queue` a NUL-terminated string, `id` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_last_id(
    fs: *mut Handle,
    queue: *const c_char,
    id: *mut u64,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let (h, queue) = match unsafe { (handle(fs), text(queue)) } {
            (Ok(h), Ok(q)) => match queue_id(h, q) {
                Ok(q) => (h, q),
                Err(code) => return code,
            },
            (Err(code), _) | (_, Err(code)) => return code,
        };
        if id.is_null() {
            return fail(NORMFS_ERR_INVALID, "id is NULL");
        }
        if let Err(e) = h.rt.block_on(h.fs.ensure_queue_exists_for_read(&queue)) {
            return fail_with(e);
        }
        match h
            .fs
            .get_last_id(&queue)
            .map_err(fail_with)
            .and_then(|v| id64(&v))
        {
            Ok(v) => {
                // SAFETY: checked non-NULL; writable by contract.
                unsafe { *id = v };
                NORMFS_OK
            }
            Err(code) => code,
        }
    })
}

/// A `limit` of 0 follows: the read waits for new records.
fn read_records(
    h: &Handle,
    queue: QueueId,
    from: u64,
    limit: u64,
    cb: RecordCb,
    ctx: *mut c_void,
) -> c_int {
    // Before anything that can wait, so a stop sent from now on is seen.
    let mut stop = h.stop_follows.subscribe();
    let Some(cb) = cb else {
        return fail(NORMFS_ERR_INVALID, "the callback is NULL");
    };
    if let Err(e) = h.rt.block_on(h.fs.ensure_queue_exists_for_read(&queue)) {
        return fail_with(e);
    }
    let follow = limit == 0;
    if follow && *stop.borrow() {
        return NORMFS_OK;
    }
    let mut next = from;
    loop {
        // Keeps `next + limit - 1` inside 64 bits.
        let limit = if follow {
            0
        } else {
            limit.min(u64::MAX - next).max(1)
        };
        let (tx, mut rx) = mpsc::channel(64);
        let fs = h.fs.clone();
        let q = queue.clone();
        let reader = h.rt.spawn(async move {
            fs.read(&q, ReadPosition::Absolute(UintN::from(next)), limit, 1, tx)
                .await
        });
        let mut delivered = 0u64;
        let mut stopped = false;
        let mut code = NORMFS_OK;
        loop {
            let entry = h.rt.block_on(async {
                tokio::select! {
                    entry = rx.recv() => Some(entry),
                    _ = stop.wait_for(|stopped| *stopped), if follow => None,
                }
            });
            let entry = match entry {
                Some(Some(entry)) => entry,
                Some(None) => break,
                None => {
                    stopped = true;
                    break;
                }
            };
            let id = match id64(&entry.id) {
                Ok(id) => id,
                Err(e) => {
                    code = e;
                    stopped = true;
                    break;
                }
            };
            // The reader can hand back ids below `from` once it goes live.
            if id < next {
                continue;
            }
            next = id.saturating_add(1);
            delivered += 1;
            // SAFETY: the caller's callback; `entry.data` lives across the call.
            if unsafe { cb(ctx, id, entry.data.as_ptr(), entry.data.len()) } != 0 {
                stopped = true;
                break;
            }
        }
        // Dropping the receiver ends a follow the callback stopped.
        drop(rx);
        let read = h.rt.block_on(reader);
        if code != NORMFS_OK {
            return code;
        }
        match read {
            Ok(Ok(_)) => {}
            Ok(Err(_)) if stopped => return NORMFS_OK,
            Ok(Err(e)) => return fail_with(e),
            Err(e) => return fail(NORMFS_ERR_PANIC, e),
        }
        // A follower that falls 64 records behind is dropped by NormFS, so
        // the follow picks up again from the next id; an empty pass is a close.
        if stopped || !follow || delivered == 0 {
            return NORMFS_OK;
        }
    }
}

/// # Safety
/// `fs` is a live handle, `queue` a NUL-terminated string, and `cb` safe to
/// call with `ctx` from this thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_read(
    fs: *mut Handle,
    queue: *const c_char,
    from: u64,
    limit: u64,
    cb: RecordCb,
    ctx: *mut c_void,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let (h, queue) = match unsafe { (handle(fs), text(queue)) } {
            (Ok(h), Ok(q)) => match queue_id(h, q) {
                Ok(q) => (h, q),
                Err(code) => return code,
            },
            (Err(code), _) | (_, Err(code)) => return code,
        };
        if limit == 0 {
            return NORMFS_OK;
        }
        read_records(h, queue, from, limit, cb, ctx)
    })
}

/// # Safety
/// As for `normfs_read`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_follow(
    fs: *mut Handle,
    queue: *const c_char,
    from: u64,
    cb: RecordCb,
    ctx: *mut c_void,
) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        let (h, queue) = match unsafe { (handle(fs), text(queue)) } {
            (Ok(h), Ok(q)) => match queue_id(h, q) {
                Ok(q) => (h, q),
                Err(code) => return code,
            },
            (Err(code), _) | (_, Err(code)) => return code,
        };
        read_records(h, queue, from, 0, cb, ctx)
    })
}

/// # Safety
/// `fs` is a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn normfs_stop_follows(fs: *mut Handle) -> c_int {
    guard(|| {
        // SAFETY: forwarded from this function's contract.
        match unsafe { handle(fs) } {
            Ok(h) => {
                h.stop_follows.send_replace(true);
                NORMFS_OK
            }
            Err(code) => code,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn normfs_last_error() -> *const c_char {
    LAST_ERROR
        .try_with(|e| e.borrow().as_ptr())
        .unwrap_or(c"the thread is exiting".as_ptr())
}
