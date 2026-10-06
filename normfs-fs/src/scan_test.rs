use std::fs;

use uintn::UintN;

use crate::scan::{PathError, Scan, ScanResult, find_max_id, find_min_id, get_files_ids, scan_ids};
use tempfile::tempdir;

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

#[test]
fn test_get_files_ids() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::create_dir_all(base_path.join("001/002")).unwrap();
    fs::write(base_path.join("000.store"), "").unwrap();
    fs::write(base_path.join("001/001.store"), "").unwrap();
    fs::write(base_path.join("001/002/003.store"), "").unwrap();

    let result = get_files_ids(base_path, "store").unwrap();
    assert_eq!(
        result,
        vec![
            UintN::from(0x00u64),
            UintN::from(0x001001u64),
            UintN::from(0x001002003u64)
        ]
    );
}

#[test]
fn test_find_max_id() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::create_dir_all(base_path.join("001/002")).unwrap();
    fs::write(base_path.join("000.store"), "").unwrap();
    fs::write(base_path.join("001/001.store"), "").unwrap();
    fs::write(base_path.join("001/002/003.store"), "").unwrap();

    let result = find_max_id(base_path, "store").unwrap();
    assert_eq!(result, UintN::from(0x001002003u64));
}

#[test]
fn test_find_min_id() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::create_dir_all(base_path.join("001/002")).unwrap();
    fs::write(base_path.join("000.store"), "").unwrap();
    fs::write(base_path.join("001/001.store"), "").unwrap();
    fs::write(base_path.join("001/002/003.store"), "").unwrap();

    let result = find_min_id(base_path, "store").unwrap();
    assert_eq!(result, UintN::from(0x00u64));
}

#[test]
fn test_find_min_id_large_number() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    // 0xffffffffffffffe = 18446744073709551614 (15 hex chars, padded to 15)
    // 0xffffffffffffff = 18446744073709551615 (16 hex chars, padded to 18)
    fs::create_dir_all(base_path.join("00f/fff/fff/fff/fff")).unwrap();
    fs::write(base_path.join("00f/fff/fff/fff/fff/ffe.store"), "").unwrap();
    fs::write(base_path.join("00f/fff/fff/fff/fff/fff.store"), "").unwrap();

    let result = find_min_id(base_path, "store").unwrap();
    let expected = "18446744073709551614";
    assert_eq!(result.to_string(), expected);
}

#[test]
fn test_find_max_id_empty() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    let result = find_max_id(base_path, "store");
    assert!(matches!(result, Err(PathError::NoFilesFound)));
}

#[test]
fn test_find_max_id_no_extension_match() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::write(base_path.join("000.other"), "").unwrap();

    let result = find_max_id(base_path, "store");
    assert!(matches!(result, Err(PathError::NoFilesFound)));
}

#[test]
fn test_single_file_at_root() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::write(base_path.join("001.store"), "").unwrap();

    let result = find_min_id(base_path, "store").unwrap();
    assert_eq!(
        result,
        UintN::from(0x01u64),
        "Single file '001.store' should be ID 0x01"
    );

    let result = find_max_id(base_path, "store").unwrap();
    assert_eq!(
        result,
        UintN::from(0x01u64),
        "Single file '001.store' should be ID 0x01"
    );

    let ids = get_files_ids(base_path, "store").unwrap();
    assert_eq!(
        ids,
        vec![UintN::from(0x01u64)],
        "Single file '001.store' should be ID 0x01"
    );
}

#[test]
fn test_mixed_hierarchy() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::write(base_path.join("001.store"), "").unwrap(); // Should be 0x01
    fs::create_dir_all(base_path.join("001")).unwrap();
    fs::write(base_path.join("001/000.store"), "").unwrap(); // Should be 0x001000

    let ids = get_files_ids(base_path, "store").unwrap();
    assert_eq!(
        ids,
        vec![UintN::from(0x01u64), UintN::from(0x001000u64)],
        "Should correctly parse both '001.store' (0x01) and '001/000.store' (0x001000)"
    );

    let min_id = find_min_id(base_path, "store").unwrap();
    assert_eq!(
        min_id,
        UintN::from(0x01u64),
        "Min should be 0x01 from '001.store'"
    );

    let max_id = find_max_id(base_path, "store").unwrap();
    assert_eq!(
        max_id,
        UintN::from(0x001000u64),
        "Max should be 0x001000 from '001/000.store'"
    );
}

#[test]
fn test_issue_reproduction() {
    // This test reproduces the exact issue from the logs where
    // file IDs are reported as U16(256) to U16(1014) when they should be U16(1) to U16(14)
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::write(base_path.join("001.store"), "").unwrap();
    fs::write(base_path.join("002.store"), "").unwrap();
    fs::write(base_path.join("00e.store"), "").unwrap();

    fs::create_dir_all(base_path.join("001")).unwrap();
    fs::write(base_path.join("001/000.store"), "").unwrap();

    let min_id = find_min_id(base_path, "store").unwrap();
    let max_id = find_max_id(base_path, "store").unwrap();

    assert_eq!(
        min_id,
        UintN::from(0x01u64),
        "Min should be 0x01 from '001.store'"
    );

    assert_eq!(
        max_id,
        UintN::from(0x001000u64),
        "Max should be 0x001000 from '001/000.store'"
    );
}

#[test]
fn test_find_max_id_lexicographic_vs_numeric() {
    // Test that max_id search uses numeric comparison, not lexicographic
    // Case: "fff/fff.store" (16777215) vs "001/000/000.store" (16777216)
    // Lexicographically: "fff" > "001"
    // Numerically: 16777215 < 16777216
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    fs::create_dir_all(base_path.join("fff")).unwrap();
    fs::write(base_path.join("fff/fff.store"), "").unwrap();

    fs::create_dir_all(base_path.join("001/000")).unwrap();
    fs::write(base_path.join("001/000/000.store"), "").unwrap();

    let max_id = find_max_id(base_path, "store").unwrap();

    assert_eq!(
        max_id,
        UintN::from(0x1000000u64),
        "Max should be 0x1000000 (16777216) from '001/000/000.store', not 0xffffff (16777215) from 'fff/fff.store'"
    );
}
