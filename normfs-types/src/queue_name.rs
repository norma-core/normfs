use std::os::raw::c_int;

unsafe extern "C" {
    fn normfs_queue_name_canonical(name: *const u8, len: usize, out: *mut u8) -> usize;
    fn normfs_queue_name_ok(name: *const u8, len: usize, windows: c_int) -> c_int;
}

pub(crate) fn canonical(path: &str) -> String {
    let name = path.as_bytes();
    let mut out = vec![0u8; name.len()];
    // SAFETY: out holds name.len() bytes, the C writes at most that many and
    // keeps no pointer to either buffer.
    let len = unsafe { normfs_queue_name_canonical(name.as_ptr(), name.len(), out.as_mut_ptr()) };
    out.truncate(len);
    // Only '/' bytes are dropped, and '/' is never part of a multibyte character.
    String::from_utf8(out).expect("dropping '/' keeps UTF-8")
}

pub(crate) fn ok(path: &str) -> bool {
    let name = path.as_bytes();
    // SAFETY: the C reads exactly len bytes of name and keeps no pointer to it.
    unsafe { normfs_queue_name_ok(name.as_ptr(), name.len(), cfg!(windows) as c_int) == 1 }
}
