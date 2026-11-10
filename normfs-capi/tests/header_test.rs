// Its own binary: spawning the compilers while other tests close and reopen
// a data_dir would let the children hold that directory's lock.
use std::path::Path;

#[test]
fn the_header_compiles_as_c_and_as_cpp() {
    let header = Path::new(env!("CARGO_MANIFEST_DIR")).join("include/normfs.h");
    for (compiler, lang) in [("cc", "c"), ("c++", "c++")] {
        let Ok(out) = std::process::Command::new(compiler)
            .args([
                "-x",
                lang,
                "-fsyntax-only",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-pedantic",
            ])
            .arg(&header)
            .output()
        else {
            if cfg!(windows) {
                continue;
            }
            panic!("no {compiler} to check the header with");
        };
        assert!(
            out.status.success(),
            "{compiler}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
