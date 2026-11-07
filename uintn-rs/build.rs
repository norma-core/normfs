use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let source = manifest_dir.join("c/src/varint.c");

    println!("cargo:rerun-if-changed={}", include_dir.display());
    println!("cargo:rerun-if-changed={}", source.display());

    c_build()
        .file(&source)
        .include(&include_dir)
        .opt_level(3)
        .compile("normfs_uintn_c");

    println!("cargo:include={}", include_dir.display());
}

/// cl has its own flags; /WX holds the C to the same no-warnings bar as -Werror.
fn c_build() -> cc::Build {
    let mut build = cc::Build::new();
    if build.get_compiler().is_like_msvc() {
        build
            .flag("/std:c11")
            .warnings(true)
            .warnings_into_errors(true);
    } else {
        build
            .warnings(false)
            .flag("-std=c99")
            .flag("-Wall")
            .flag("-Wextra")
            .flag("-Werror")
            .flag("-pedantic");
    }
    build
}
