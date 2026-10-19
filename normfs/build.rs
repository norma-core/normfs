use std::env;
use std::io::Result;
use std::path::{Path, PathBuf};

fn uintn_include_dir(manifest_dir: &Path) -> PathBuf {
    match env::var_os("DEP_NORMFS_UINTN_C_INCLUDE") {
        Some(dir) => PathBuf::from(dir),
        None => manifest_dir.join("../uintn-rs/c/include"),
    }
}

fn main() -> Result<()> {
    let out_dir = PathBuf::from("src/proto");

    prost_build::Config::new()
        .out_dir(&out_dir)
        .bytes(["."])
        .compile_protos(
            &["proto/normfs.proto", "proto/normfs_system.proto"],
            &["proto/"],
        )?;

    println!("cargo:rerun-if-changed=proto/normfs.proto");
    println!("cargo:rerun-if-changed=proto/normfs_system.proto");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let uintn_include_dir = uintn_include_dir(&manifest_dir);
    let sources = [
        manifest_dir.join("c/src/disk_monitor.c"),
        manifest_dir.join("c/src/disk_monitor_sys.c"),
        manifest_dir.join("c/src/frame.c"),
        manifest_dir.join("c/src/net_sys.c"),
    ];

    println!("cargo:rerun-if-changed={}", include_dir.display());
    println!("cargo:rerun-if-changed={}", uintn_include_dir.display());
    for source in &sources {
        println!("cargo:rerun-if-changed={}", source.display());
    }

    cc::Build::new()
        .files(&sources)
        .include(&include_dir)
        .include(&uintn_include_dir)
        .flag("-std=c99")
        .flag("-Wall")
        .flag("-Wextra")
        .flag("-Werror")
        .flag("-pedantic")
        .opt_level(3)
        .warnings(false)
        .compile("normfs_c");

    Ok(())
}
