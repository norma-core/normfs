use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let sources = [
        manifest_dir.join("c/src/seed.c"),
        manifest_dir.join("c/src/seed_sys.c"),
        manifest_dir.join("c/src/sha256.c"),
        manifest_dir.join("c/src/hmac_sha256.c"),
        manifest_dir.join("c/src/chacha20.c"),
        manifest_dir.join("c/src/kdf.c"),
    ];

    println!("cargo:rerun-if-changed={}", include_dir.display());
    for source in &sources {
        println!("cargo:rerun-if-changed={}", source.display());
    }

    cc::Build::new()
        .files(&sources)
        .include(&include_dir)
        .flag("-std=c99")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .flag("-pedantic")
        .opt_level(3)
        .warnings(false)
        .compile("normfs_crypto_c");
}
