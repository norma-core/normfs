use std::os::raw::c_int;

use normfs_types::QueueId;

unsafe extern "C" {
    fn normfs_queue_name_ok(name: *const u8, len: usize, windows: c_int) -> c_int;
}

/// Whether the queue's directory stays inside the data directory.
pub(crate) fn queue_name_ok(queue: &QueueId) -> bool {
    let name = queue.as_str().as_bytes();
    // SAFETY: the C reads exactly len bytes of name and keeps no pointer to it.
    unsafe { normfs_queue_name_ok(name.as_ptr(), name.len(), cfg!(windows) as c_int) == 1 }
}
