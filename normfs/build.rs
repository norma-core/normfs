use std::env;
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let out_dir = PathBuf::from("src/proto");

    prost_build::Config::new()
        .out_dir(&out_dir)
        .bytes(["."])
        .compile_protos(&["proto/normfs.proto"], &["proto/"])?;

    println!("cargo:rerun-if-changed=proto/normfs.proto");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let sources = [
        manifest_dir.join("c/src/disk_monitor.c"),
        manifest_dir.join("c/src/disk_monitor_sys.c"),
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
        .compile("normfs_c");

    Ok(())
}
