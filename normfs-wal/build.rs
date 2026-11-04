use std::env;
use std::path::{Path, PathBuf};

fn uintn_include_dir(manifest_dir: &Path) -> PathBuf {
    match env::var_os("DEP_NORMFS_UINTN_C_INCLUDE") {
        Some(dir) => PathBuf::from(dir),
        None => manifest_dir.join("../uintn-rs/c/include"),
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let uintn_include_dir = uintn_include_dir(&manifest_dir);
    let sources = [
        manifest_dir.join("c/src/wal_header.c"),
        manifest_dir.join("c/src/crc32c.c"),
        manifest_dir.join("c/src/wal_entry.c"),
        manifest_dir.join("c/src/wal_page.c"),
        manifest_dir.join("c/src/wal_scan.c"),
        manifest_dir.join("c/src/wal_pool.c"),
        manifest_dir.join("c/src/wal_ring.c"),
    ];

    println!("cargo:rerun-if-changed={}", include_dir.display());
    println!("cargo:rerun-if-changed={}", uintn_include_dir.display());
    for source in &sources {
        println!("cargo:rerun-if-changed={}", source.display());
    }

    let mut build = cc::Build::new();
    build
        .files(&sources)
        .include(&include_dir)
        .include(&uintn_include_dir)
        .flag("-std=c99")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .flag("-pedantic")
        .opt_level(3)
        .warnings(false);

    if env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("x86_64") {
        build.flag("-msse4.2");
    }

    build.compile("normfs_wal_c");
}
