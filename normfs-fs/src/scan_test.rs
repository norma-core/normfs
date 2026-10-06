use std::fs;

use uintn::UintN;

use crate::scan::{Scan, ScanResult, scan_ids};

#[test]
fn scans_the_chunked_layout() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    fs::create_dir_all(root.join("000")).unwrap();
    fs::create_dir_all(root.join("001")).unwrap();
    fs::write(root.join("000").join("00a.store"), b"").unwrap();
    fs::write(root.join("000").join("0ff.store"), b"").unwrap();
    fs::write(root.join("001").join("000.store"), b"").unwrap();
    fs::write(root.join("001").join("000.wal"), b"").unwrap();

    assert_eq!(
        scan_ids(&root, "store", Scan::Min).unwrap(),
        ScanResult::One(UintN::from(0x00000au64))
    );
    assert_eq!(
        scan_ids(&root, "store", Scan::Max).unwrap(),
        ScanResult::One(UintN::from(0x001000u64))
    );
    match scan_ids(&root, "store", Scan::All).unwrap() {
        ScanResult::All(ids) => assert_eq!(ids.len(), 3),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        scan_ids(&root, "wal", Scan::Max).unwrap(),
        ScanResult::One(UintN::from(0x001000u64))
    );
}

#[test]
fn nothing_is_none() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        scan_ids(dir.path(), "wal", Scan::Min).unwrap(),
        ScanResult::None
    );
    assert_eq!(
        scan_ids(dir.path(), "wal", Scan::All).unwrap(),
        ScanResult::None
    );
    assert_eq!(
        scan_ids(&dir.path().join("absent"), "wal", Scan::Max).unwrap(),
        ScanResult::None
    );
}
