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

    println!("cargo:rerun-if-changed={}", include_dir.display());
    println!("cargo:rerun-if-changed={}", uintn_include_dir.display());

    cc::Build::new()
        .file(manifest_dir.join("c/src/wal_header.c"))
        .include(&include_dir)
        .include(&uintn_include_dir)
        .flag("-std=c99")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .flag("-pedantic")
        .opt_level(3)
        .warnings(false)
        .compile("normfs_wal_c");
}
