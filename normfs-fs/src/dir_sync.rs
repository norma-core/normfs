//! Plans can share a directory sync only if it starts after their renames.
//! Arrivals during a sync wait for the next epoch so an earlier barrier cannot
//! certify a later rename.

use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_int;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

#[derive(Default)]
struct State {
    running: bool,
    /// The number the next sync to start will have.
    epoch: u64,
    /// Plans waiting for each epoch; a result is kept only while someone waits.
    waiting: HashMap<u64, usize>,
    done: HashMap<u64, c_int>,
}

#[derive(Default)]
struct Dir {
    state: Mutex<State>,
    finished: Condvar,
}

type Registry = Mutex<HashMap<Vec<u8>, Arc<Dir>>>;

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

// Keep parent identity consistent with the syscall shim.
fn parent_key(path: &CStr) -> Vec<u8> {
    let bytes = path.to_bytes();
    match bytes.iter().rposition(|&b| b == b'/') {
        Some(cut) => bytes[..=cut].to_vec(),
        None => b".".to_vec(),
    }
}

/// Runs `sync` for `dst`'s parent, or waits on a sync that covers it.
/// `sync` returns 0 or an `errno`, and so does this.
pub(crate) fn sync_parent(dst: &CStr, sync: impl FnOnce() -> c_int) -> c_int {
    let key = parent_key(dst);
    let dir = registry()
        .lock()
        .unwrap()
        .entry(key.clone())
        .or_default()
        .clone();
    let rc = barrier(&dir, sync);
    let mut registry = registry().lock().unwrap();
    // Cloning out of the registry needs its lock, so this count is exact.
    let idle = Arc::strong_count(&dir) == 2 && {
        let state = dir.state.lock().unwrap();
        !state.running && state.waiting.is_empty()
    };
    if idle {
        registry.remove(&key);
    }
    rc
}

fn barrier(dir: &Dir, sync: impl FnOnce() -> c_int) -> c_int {
    let mut state = dir.state.lock().unwrap();
    if !state.running {
        return lead(dir, state, sync);
    }
    let mine = state.epoch + 1;
    *state.waiting.entry(mine).or_insert(0) += 1;
    loop {
        if let Some(&rc) = state.done.get(&mine) {
            leave(&mut state, mine);
            return rc;
        }
        if !state.running {
            debug_assert_eq!(state.epoch, mine);
            leave(&mut state, mine);
            return lead(dir, state, sync);
        }
        state = dir.finished.wait(state).unwrap();
    }
}

fn leave(state: &mut State, epoch: u64) {
    let left = state.waiting.get_mut(&epoch).expect("a registered waiter");
    *left -= 1;
    if *left == 0 {
        state.waiting.remove(&epoch);
        state.done.remove(&epoch);
    }
}

fn lead(
    dir: &Dir,
    mut state: std::sync::MutexGuard<'_, State>,
    sync: impl FnOnce() -> c_int,
) -> c_int {
    let mine = state.epoch;
    state.running = true;
    drop(state);
    let rc = sync();
    let mut state = dir.state.lock().unwrap();
    state.running = false;
    state.epoch = mine + 1;
    if state.waiting.contains_key(&mine) {
        state.done.insert(mine, rc);
    }
    dir.finished.notify_all();
    rc
}

#[cfg(test)]
pub(crate) fn waiting_on(dst: &CStr) -> usize {
    let key = parent_key(dst);
    let Some(dir) = registry().lock().unwrap().get(&key).cloned() else {
        return 0;
    };
    let state = dir.state.lock().unwrap();
    state.waiting.values().sum()
}
