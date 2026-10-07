use super::size::{parse_bytes, parse_memory, SizeError};
use super::Args;
use clap::Parser;

#[test]
fn binary_and_decimal_units_are_distinct() {
    for (input, bytes) in [
        ("0", 0),
        ("262144", 262144),
        ("42B", 42),
        ("256KiB", 262144),
        ("512MiB", 536870912),
        ("32GiB", 34359738368),
        ("1TiB", 1 << 40),
        ("1PiB", 1 << 50),
        ("1EiB", 1 << 60),
        ("1KB", 1000),
        ("512MB", 512000000),
        ("32GB", 32000000000),
        ("1TB", 1000000000000),
        ("1PB", 1000000000000000),
        ("1EB", 1000000000000000000),
        (" 2 mIb ", 2097152),
        ("1.5GiB", 1610612736),
        ("0.001KB", 1),
        ("0.5KiB", 512),
    ] {
        assert_eq!(parse_bytes(input), Ok(bytes), "{input}");
        assert_eq!(parse_memory(input), Ok(bytes as usize), "{input}");
    }
}

#[test]
fn large_sizes_stay_exact_and_overflow_is_rejected() {
    assert_eq!(parse_bytes("9007199254740993B"), Ok(9007199254740993));
    assert_eq!(parse_bytes("18446744073709551615"), Ok(u64::MAX));
    assert_eq!(parse_bytes("18446744073709551615.0B"), Ok(u64::MAX));
    assert_eq!(parse_bytes("18446744073709551.615KB"), Ok(u64::MAX));
    for input in [
        "18446744073709551616",
        "18446744073709551.616KB",
        "16EiB",
        "340282366920938463463374607431768211455KiB",
        "340282366920938463463374607431768211456B",
        "0.0000000000000000000000000000000000000001B",
    ] {
        assert_eq!(parse_bytes(input), Err(SizeError::Overflow), "{input}");
    }
}

#[test]
fn invalid_sizes_never_round_or_silently_drop_units() {
    for input in ["", " ", "-1", "+1", ".5MiB", "1.", "1..5MiB", "NaN", "∞"] {
        assert_eq!(parse_bytes(input), Err(SizeError::InvalidNumber), "{input}");
    }
    for input in ["1XB", "1M", "1e3", "1MiBjunk", "1 2MiB", "1字节"] {
        assert_eq!(parse_bytes(input), Err(SizeError::UnknownUnit), "{input}");
    }
    for input in ["0.1B", "1.5", "0.1KiB"] {
        assert_eq!(
            parse_bytes(input),
            Err(SizeError::FractionalByte),
            "{input}"
        );
    }
}

#[test]
fn every_size_flag_accepts_units_and_preserves_defaults() {
    let defaults = Args::try_parse_from(["normfs-server"]).unwrap();
    let settings = normfs::NormFsSettings::default();
    assert_eq!(defaults.max_memory_usage, settings.max_memory_usage);
    assert_eq!(defaults.mem_page_size, settings.mem_page_size);
    assert_eq!(
        defaults.max_passive_memory_usage,
        settings.max_passive_memory_usage
    );
    assert_eq!(
        defaults.mem_passive_page_size,
        settings.mem_passive_page_size
    );
    assert_eq!(defaults.max_queue_disk_size, 32 * 1024 * 1024 * 1024);

    let args = Args::try_parse_from([
        "normfs-server",
        "--max-memory-usage",
        "512MiB",
        "--mem-page-size",
        "1.5MiB",
        "--max-passive-memory-usage",
        "4MB",
        "--mem-passive-page-size",
        "64KiB",
        "--max-queue-disk-size",
        "1TB",
    ])
    .unwrap();
    assert_eq!(args.max_memory_usage, 536870912);
    assert_eq!(args.mem_page_size, 1572864);
    assert_eq!(args.max_passive_memory_usage, 4000000);
    assert_eq!(args.mem_passive_page_size, 65536);
    assert_eq!(args.max_queue_disk_size, 1000000000000);
}

#[test]
fn unlimited_disk_still_conflicts_with_an_explicit_size() {
    assert!(Args::try_parse_from(["normfs-server", "--unlimited-disk"]).is_ok());
    let error = Args::try_parse_from([
        "normfs-server",
        "--unlimited-disk",
        "--max-queue-disk-size",
        "32GiB",
    ])
    .unwrap_err();
    assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
}

#[test]
fn cli_rejects_invalid_sizes_before_startup() {
    for flag in [
        "--max-memory-usage",
        "--mem-page-size",
        "--max-passive-memory-usage",
        "--mem-passive-page-size",
        "--max-queue-disk-size",
    ] {
        for input in ["16EiB", "1XB", "0.1B"] {
            let error = Args::try_parse_from(["normfs-server", flag, input]).unwrap_err();
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
        }
    }
}
