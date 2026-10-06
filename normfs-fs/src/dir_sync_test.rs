use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use crate::dir_sync::{sync_parent, waiting_on};

fn wait_for_waiters(dst: &CString, n: usize) {
    for _ in 0..2000 {
        if waiting_on(dst) == n {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("waiters never arrived");
}

#[test]
fn arrivals_during_a_sync_share_the_next_one() {
    let a = CString::new("shared/a").unwrap();
    let b = CString::new("shared/b").unwrap();
    let c = CString::new("shared/c").unwrap();
    let (started, on_started) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let leader = thread::spawn(move || {
        sync_parent(&a, || {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            0
        })
    });
    on_started.recv().unwrap();
    let syncs = Arc::new(AtomicUsize::new(0));
    let followers: Vec<_> = [b.clone(), c]
        .into_iter()
        .map(|dst| {
            let syncs = syncs.clone();
            thread::spawn(move || {
                sync_parent(&dst, || {
                    syncs.fetch_add(1, Ordering::SeqCst);
                    5
                })
            })
        })
        .collect();
    wait_for_waiters(&b, 2);
    assert_eq!(syncs.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    assert_eq!(leader.join().unwrap(), 0);
    for follower in followers {
        assert_eq!(follower.join().unwrap(), 5);
    }
    assert_eq!(syncs.load(Ordering::SeqCst), 1);
    assert_eq!(waiting_on(&b), 0);
}

#[test]
fn other_directories_do_not_wait() {
    let a = CString::new("one/a").unwrap();
    let elsewhere = CString::new("two/b").unwrap();
    let (started, on_started) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let leader = thread::spawn(move || {
        sync_parent(&a, || {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            0
        })
    });
    on_started.recv().unwrap();
    assert_eq!(sync_parent(&elsewhere, || 0), 0);
    release.send(()).unwrap();
    leader.join().unwrap();
}

#[test]
fn a_bare_name_is_the_working_directory() {
    let bare = CString::new("bare").unwrap();
    let dotted = CString::new("./x").unwrap();
    assert_eq!(sync_parent(&bare, || 0), 0);
    assert_eq!(sync_parent(&dotted, || 0), 0);
}
