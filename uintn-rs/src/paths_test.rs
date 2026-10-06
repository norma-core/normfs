use crate::UintN;
use serde::Deserialize;
use std::path::PathBuf;

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
