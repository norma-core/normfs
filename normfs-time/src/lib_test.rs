use super::*;

#[test]
fn monotonic_time_does_not_go_back() {
    let first = monotonic_stamp_ns();
    let second = monotonic_stamp_ns();
    assert!(first > 0);
    assert!(second >= first);
}

#[test]
fn a_process_keeps_one_start_id() {
    let first = Stamp::now();
    let second = Stamp::now();
    assert!(first.app_start_id > 0);
    assert_eq!(first.app_start_id, second.app_start_id);
}
