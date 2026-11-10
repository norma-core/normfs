use std::ffi::{CStr, CString, c_int, c_void};
use std::path::Path;
use std::ptr;
use std::sync::mpsc;
use std::time::Duration;

use normfs_capi::*;

struct Fs(*mut Handle);

type Records = Vec<(u64, Vec<u8>)>;

unsafe impl Send for Fs {}
unsafe impl Sync for Fs {}

fn open(dir: &Path) -> Fs {
    let dir = CString::new(dir.to_str().unwrap()).unwrap();
    let mut fs = ptr::null_mut();
    let rc = unsafe { normfs_open(dir.as_ptr(), &mut fs) };
    assert_eq!(rc, NORMFS_OK, "{}", last_error());
    assert!(!fs.is_null());
    Fs(fs)
}

fn append(fs: &Fs, queue: &str, data: &[u8]) -> u64 {
    let queue = CString::new(queue).unwrap();
    let mut id = u64::MAX;
    let rc = unsafe { normfs_append(fs.0, queue.as_ptr(), data.as_ptr(), data.len(), &mut id) };
    assert_eq!(rc, NORMFS_OK, "{}", last_error());
    id
}

fn last_error() -> String {
    unsafe { CStr::from_ptr(normfs_last_error()) }
        .to_string_lossy()
        .into_owned()
}

/// Collects records into the Vec behind `ctx`; stops once it holds `stop_at`.
struct Sink {
    got: Records,
    stop_at: usize,
}

unsafe extern "C" fn collect(ctx: *mut c_void, id: u64, data: *const u8, len: usize) -> c_int {
    let sink = unsafe { &mut *(ctx as *mut Sink) };
    let bytes = unsafe { std::slice::from_raw_parts(data, len) }.to_vec();
    sink.got.push((id, bytes));
    (sink.got.len() >= sink.stop_at) as c_int
}

fn read(fs: &Fs, queue: &str, from: u64, limit: u64) -> Records {
    let queue = CString::new(queue).unwrap();
    let mut sink = Sink {
        got: Vec::new(),
        stop_at: usize::MAX,
    };
    let rc = unsafe {
        normfs_read(
            fs.0,
            queue.as_ptr(),
            from,
            limit,
            Some(collect),
            &mut sink as *mut Sink as *mut c_void,
        )
    };
    assert_eq!(rc, NORMFS_OK, "{}", last_error());
    sink.got
}

#[test]
fn records_are_read_back_by_id_and_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    let ids: Vec<u64> = [&b"one"[..], b"two", b""]
        .iter()
        .map(|r| append(&fs, "mail/alice", r))
        .collect();
    assert_eq!(ids, [0, 1, 2]);

    let queue = CString::new("mail/alice").unwrap();
    let mut last = 0;
    assert_eq!(
        unsafe { normfs_last_id(fs.0, queue.as_ptr(), &mut last) },
        NORMFS_OK
    );
    assert_eq!(last, 2);
    assert_eq!(unsafe { normfs_flush(fs.0, queue.as_ptr()) }, NORMFS_OK);
    assert_eq!(
        read(&fs, "mail/alice", 1, 10),
        [(1, b"two".to_vec()), (2, Vec::new())]
    );
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);

    let fs = open(dir.path());
    assert_eq!(
        read(&fs, "mail/alice", 0, 2),
        [(0, b"one".to_vec()), (1, b"two".to_vec())]
    );
    assert_eq!(append(&fs, "mail/alice", b"four"), 3);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_follow_waits_for_new_records_until_the_callback_stops_it() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"old");

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|s| {
        let fs = &fs;
        s.spawn(move || {
            let queue = CString::new("inbox").unwrap();
            let mut sink = Sink {
                got: Vec::new(),
                stop_at: 3,
            };
            let rc = unsafe {
                normfs_follow(
                    fs.0,
                    queue.as_ptr(),
                    0,
                    Some(collect),
                    &mut sink as *mut Sink as *mut c_void,
                )
            };
            done_tx.send((rc, sink.got)).unwrap();
        });
        std::thread::sleep(Duration::from_millis(100));
        append(fs, "inbox", b"new 1");
        append(fs, "inbox", b"new 2");
        let (rc, got) = done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(rc, NORMFS_OK);
        let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [0, 1, 2]);
        assert_eq!(got[2].1, b"new 2");
    });
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

unsafe extern "C" fn collect_slowly(
    ctx: *mut c_void,
    id: u64,
    data: *const u8,
    len: usize,
) -> c_int {
    std::thread::sleep(Duration::from_millis(2));
    unsafe { collect(ctx, id, data, len) }
}

fn follow_in_thread(
    fs: &Fs,
    queue: &str,
    from: u64,
    stop_at: usize,
    cb: normfs_capi::RecordCb,
) -> mpsc::Receiver<(c_int, Records)> {
    let (done_tx, done_rx) = mpsc::channel();
    let fs = Fs(fs.0);
    let queue = CString::new(queue).unwrap();
    std::thread::spawn(move || {
        let fs = fs;
        let mut sink = Sink {
            got: Vec::new(),
            stop_at,
        };
        let rc = unsafe {
            normfs_follow(
                fs.0,
                queue.as_ptr(),
                from,
                cb,
                &mut sink as *mut Sink as *mut c_void,
            )
        };
        done_tx.send((rc, sink.got)).unwrap();
    });
    done_rx
}

#[test]
fn a_follow_slower_than_the_writer_still_gets_every_record() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"0");
    let done = follow_in_thread(&fs, "inbox", 0, 300, Some(collect_slowly));
    std::thread::sleep(Duration::from_millis(100));
    for i in 1..300u64 {
        append(&fs, "inbox", i.to_string().as_bytes());
    }
    let (rc, got) = done.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(rc, NORMFS_OK, "{}", last_error());
    let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, (0..300).collect::<Vec<_>>());
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_follow_past_the_end_starts_at_its_from_id() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"0");
    let done = follow_in_thread(&fs, "inbox", 5, 1, Some(collect));
    std::thread::sleep(Duration::from_millis(100));
    for i in 1..=5u64 {
        append(&fs, "inbox", i.to_string().as_bytes());
    }
    let (rc, got) = done.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(rc, NORMFS_OK);
    assert_eq!(got, [(5, b"5".to_vec())]);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

// The drop needs the follower to fall 64 behind, which a busy machine
// decides; ten rounds make missing it on every one unlikely.
#[test]
fn a_follow_far_past_the_end_outlives_a_burst_below_its_from() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    for round in 0..10 {
        let queue = format!("inbox{round}");
        append(&fs, &queue, b"0");
        let done = follow_in_thread(&fs, &queue, 8000, 1, Some(collect));
        std::thread::sleep(Duration::from_millis(50));
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for _ in 0..1000 {
                        append(&fs, &queue, b"x");
                    }
                });
            }
        });
        let (rc, got) = done.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(rc, NORMFS_OK);
        let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [8000], "round {round}");
    }
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn stop_follows_ends_an_idle_follow() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"0");
    let done = follow_in_thread(&fs, "inbox", 0, usize::MAX, Some(collect));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(unsafe { normfs_stop_follows(fs.0) }, NORMFS_OK);
    let (rc, got) = done.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(rc, NORMFS_OK);
    assert_eq!(got, [(0, b"0".to_vec())]);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_read_with_the_largest_limit_reads_to_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    for r in [b"a", b"b", b"c"] {
        append(&fs, "q", r);
    }
    assert_eq!(read(&fs, "q", 2, u64::MAX), [(2, b"c".to_vec())]);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

fn file_holding(dir: &Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir).unwrap().flatten().any(|e| {
        let path = e.path();
        if path.is_dir() {
            return file_holding(&path, needle);
        }
        std::fs::read(&path).is_ok_and(|b| b.windows(needle.len()).any(|w| w == needle))
    })
}

#[test]
fn a_flushed_record_is_already_in_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "q", b"flushed-record-7f3a");
    let queue = CString::new("q").unwrap();
    assert_eq!(unsafe { normfs_flush(fs.0, queue.as_ptr()) }, NORMFS_OK);
    assert!(file_holding(dir.path(), b"flushed-record-7f3a"));
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_flush_after_a_reopen_returns_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"old");
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
    let fs = open(dir.path());
    let queue = CString::new("inbox").unwrap();
    let mut last = 0;
    assert_eq!(
        unsafe { normfs_last_id(fs.0, queue.as_ptr(), &mut last) },
        NORMFS_OK
    );
    let started = std::time::Instant::now();
    let rc = unsafe { normfs_flush(fs.0, queue.as_ptr()) };
    assert_eq!(rc, NORMFS_OK, "{}", last_error());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_record_of_200_kib_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    let big = vec![7u8; 200 * 1024];
    assert_eq!(append(&fs, "mail/big", &big), 0);
    assert_eq!(read(&fs, "mail/big", 0, 1), [(0, big)]);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

unsafe extern "C" fn block_until_released(
    ctx: *mut c_void,
    _id: u64,
    _data: *const u8,
    _len: usize,
) -> c_int {
    let release = unsafe { &*(ctx as *const mpsc::Receiver<()>) };
    let _ = release.recv_timeout(Duration::from_secs(30));
    1
}

#[test]
fn a_flush_does_not_wait_for_a_reader_that_is_behind() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    let record = [1u8; 1024];
    for _ in 0..500 {
        append(&fs, "inbox", &record);
    }
    let (release_tx, release_rx) = mpsc::channel::<()>();
    std::thread::scope(|s| {
        let fs = &fs;
        s.spawn(move || {
            let queue = CString::new("inbox").unwrap();
            let rx = release_rx;
            unsafe {
                normfs_follow(
                    fs.0,
                    queue.as_ptr(),
                    0,
                    Some(block_until_released),
                    &rx as *const _ as *mut c_void,
                )
            };
        });
        std::thread::sleep(Duration::from_millis(100));
        append(fs, "inbox", &record);
        let queue = CString::new("inbox").unwrap();
        let started = std::time::Instant::now();
        assert_eq!(
            unsafe { normfs_flush(fs.0, queue.as_ptr()) },
            NORMFS_OK,
            "{}",
            last_error()
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        release_tx.send(()).unwrap();
    });
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_follow_started_after_stop_follows_returns_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    append(&fs, "inbox", b"0");
    assert_eq!(unsafe { normfs_stop_follows(fs.0) }, NORMFS_OK);
    let done = follow_in_thread(&fs, "inbox", 1, usize::MAX, Some(collect));
    let (rc, got) = done.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(rc, NORMFS_OK);
    assert!(got.is_empty());
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn a_data_dir_takes_one_handle_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    let path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let mut second = ptr::null_mut();
    assert_eq!(
        unsafe { normfs_open(path.as_ptr(), &mut second) },
        NORMFS_ERR_IO
    );
    assert!(second.is_null());
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
    let fs = open(dir.path());
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn errors_come_back_as_codes_with_a_message() {
    let dir = tempfile::tempdir().unwrap();
    let fs = open(dir.path());
    let mut id = 0;
    let bad = CString::new("../escape").unwrap();
    let rc = unsafe { normfs_append(fs.0, bad.as_ptr(), b"x".as_ptr(), 1, &mut id) };
    assert_eq!(rc, NORMFS_ERR_INVALID);
    assert!(!last_error().is_empty());
    assert_eq!(
        unsafe { normfs_flush(fs.0, bad.as_ptr()) },
        NORMFS_ERR_INVALID
    );
    for name in ["", "/abs", "a\\b"] {
        let name = CString::new(name).unwrap();
        let rc = unsafe { normfs_append(fs.0, name.as_ptr(), b"x".as_ptr(), 1, &mut id) };
        assert_eq!(rc, NORMFS_ERR_INVALID);
    }

    assert_eq!(
        unsafe { normfs_append(fs.0, ptr::null(), b"x".as_ptr(), 1, &mut id) },
        NORMFS_ERR_INVALID
    );
    let queue = CString::new("q").unwrap();
    assert_eq!(
        unsafe { normfs_append(fs.0, queue.as_ptr(), ptr::null(), 1, &mut id) },
        NORMFS_ERR_INVALID
    );
    assert_eq!(
        unsafe { normfs_read(fs.0, queue.as_ptr(), 0, 1, None, ptr::null_mut()) },
        NORMFS_ERR_INVALID
    );
    let mut last = 0;
    let empty = CString::new("never-written").unwrap();
    let rc = unsafe { normfs_last_id(fs.0, empty.as_ptr(), &mut last) };
    assert_eq!(rc, NORMFS_ERR_EMPTY);

    let mut out = ptr::null_mut();
    assert_eq!(
        unsafe { normfs_open(ptr::null(), &mut out) },
        NORMFS_ERR_INVALID
    );
    assert!(out.is_null());
    assert_eq!(unsafe { normfs_close(ptr::null_mut()) }, NORMFS_OK);
    assert_eq!(unsafe { normfs_close(fs.0) }, NORMFS_OK);
}

#[test]
fn the_header_compiles_as_c_and_as_cpp() {
    let header = Path::new(env!("CARGO_MANIFEST_DIR")).join("include/normfs.h");
    for (compiler, lang) in [("cc", "c"), ("c++", "c++")] {
        let Ok(out) = std::process::Command::new(compiler)
            .args([
                "-x",
                lang,
                "-fsyntax-only",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-pedantic",
            ])
            .arg(&header)
            .output()
        else {
            eprintln!("no {compiler}; skipped");
            continue;
        };
        assert!(
            out.status.success(),
            "{compiler}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
