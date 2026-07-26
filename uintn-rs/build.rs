use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");

    println!("cargo:rerun-if-changed={}", include_dir.display());

    cc::Build::new()
        .file(manifest_dir.join("c/src/varint.c"))
        .include(&include_dir)
        .flag("-std=c99")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .flag("-pedantic")
        .opt_level(3)
        .warnings(false)
        .compile("normfs_uintn_c");

    println!("cargo:include={}", include_dir.display());
}
