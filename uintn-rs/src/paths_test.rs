use crate::UintN;
use crate::paths::{PathError, find_max_id, find_min_id, get_files_ids};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[derive(Deserialize)]
struct TestCase {
    value: u64,
    path: String,
}
#[test]
fn test_to_file_path_golden() {
    let golden_data = r#"
    [
        { "value": 0, "path": "wal/000.wal" },
        { "value": 1, "path": "wal/001.wal" },
        { "value": 16, "path": "wal/010.wal" },
        { "value": 255, "path": "wal/0ff.wal" },
        { "value": 256, "path": "wal/100.wal" },
        { "value": 4095, "path": "wal/fff.wal" },
        { "value": 4096, "path": "wal/001/000.wal" },
        { "value": 65535, "path": "wal/00f/fff.wal" },
        { "value": 65536, "path": "wal/010/000.wal" },
        { "value": 16777215, "path": "wal/fff/fff.wal" },
        { "value": 16777216, "path": "wal/001/000/000.wal" },
        { "value": 4294967295, "path": "wal/0ff/fff/fff.wal" },
        { "value": 4294967296, "path": "wal/100/000/000.wal" },
        { "value": 1099511627775, "path": "wal/00f/fff/fff/fff.wal" },
        { "value": 1099511627776, "path": "wal/010/000/000/000.wal" },
        { "value": 281474976710655, "path": "wal/fff/fff/fff/fff.wal" },
        { "value": 281474976710656, "path": "wal/001/000/000/000/000.wal" },
        { "value": 72057594037927935, "path": "wal/0ff/fff/fff/fff/fff.wal" },
        { "value": 72057594037927936, "path": "wal/100/000/000/000/000.wal" },
        { "value": 18446744073709551615, "path": "wal/00f/fff/fff/fff/fff/fff.wal" }
    ]
    "#;

    let test_cases: Vec<TestCase> = serde_json::from_str(golden_data).unwrap();

    for tc in test_cases {
        let value = UintN::from(tc.value);
        let path = value.to_file_path("wal", "wal");
        assert_eq!(path, PathBuf::from(tc.path));
    }
}

#[test]
fn test_get_files_ids() {
    let dir = tempdir().unwrap();
    let base_path = dir.path();

    // Create a dummy file structure with 3-char chunks
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

    // Create a dummy file structure with 3-char chunks
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

    // Create a dummy file structure with 3-char chunks
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

    // Create a dummy file structure with a large number (3-char chunks)
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

    // Create a single file at root - this should be ID 0x01
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

    // Create files at different hierarchy levels
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

    // Create files matching the actual structure (with 3-char chunks)
    fs::write(base_path.join("001.store"), "").unwrap();
    fs::write(base_path.join("002.store"), "").unwrap();
    fs::write(base_path.join("00e.store"), "").unwrap();

    // Also create subdirectories with files
    fs::create_dir_all(base_path.join("001")).unwrap();
    fs::write(base_path.join("001/000.store"), "").unwrap();

    let min_id = find_min_id(base_path, "store").unwrap();
    let max_id = find_max_id(base_path, "store").unwrap();

    println!("Min ID: {:?}, Max ID: {:?}", min_id, max_id);

    // Min should be 0x01 (from 001.store)
    assert_eq!(
        min_id,
        UintN::from(0x01u64),
        "Min should be 0x01 from '001.store'"
    );

    // Max should be 0x001000 (from 001/000.store)
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

    // Create the lexicographically larger but numerically smaller file
    fs::create_dir_all(base_path.join("fff")).unwrap();
    fs::write(base_path.join("fff/fff.store"), "").unwrap();

    // Create the lexicographically smaller but numerically larger file
    fs::create_dir_all(base_path.join("001/000")).unwrap();
    fs::write(base_path.join("001/000/000.store"), "").unwrap();

    let max_id = find_max_id(base_path, "store").unwrap();

    // Should find the numerically largest: 0x1000000 = 16777216
    assert_eq!(
        max_id,
        UintN::from(0x1000000u64),
        "Max should be 0x1000000 (16777216) from '001/000/000.store', not 0xffffff (16777215) from 'fff/fff.store'"
    );
}
