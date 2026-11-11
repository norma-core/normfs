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

#[test]
fn the_header_codes_match_the_library() {
    use normfs_capi::*;
    let codes = [
        ("NORMFS_OK", NORMFS_OK),
        ("NORMFS_ERR_INVALID", NORMFS_ERR_INVALID),
        ("NORMFS_ERR_NOT_FOUND", NORMFS_ERR_NOT_FOUND),
        ("NORMFS_ERR_EMPTY", NORMFS_ERR_EMPTY),
        ("NORMFS_ERR_TOO_LARGE", NORMFS_ERR_TOO_LARGE),
        ("NORMFS_ERR_CLOSED", NORMFS_ERR_CLOSED),
        ("NORMFS_ERR_ID_RANGE", NORMFS_ERR_ID_RANGE),
        ("NORMFS_ERR_IO", NORMFS_ERR_IO),
        ("NORMFS_ERR_PANIC", NORMFS_ERR_PANIC),
        ("NORMFS_ERR_BUSY", NORMFS_ERR_BUSY),
    ];
    // Both sides spelled out here, so a code added to only one of them fails too.
    let header = include_str!("../include/normfs.h");
    let enum_body = &header[header.find("enum normfs_status").unwrap()..];
    let enum_body = &enum_body[..enum_body.find("};").unwrap()];
    let in_rust = include_str!("../src/lib.rs")
        .matches("pub const NORMFS_")
        .count();
    assert_eq!(enum_body.matches("NORMFS_").count(), codes.len());
    assert_eq!(in_rust, codes.len());
    let mut src = String::from("#include \"normfs.h\"\n");
    for (name, value) in codes {
        // c99 has no _Static_assert, so a negative array size is the check.
        src += &format!("typedef char check_{name}[({name} == {value}) ? 1 : -1];\n");
    }
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("codes.c");
    std::fs::write(&file, src).unwrap();
    let include = Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let Ok(out) = std::process::Command::new("cc")
        .args([
            "-std=c99",
            "-fsyntax-only",
            "-Wall",
            "-Werror",
            "-pedantic",
            "-I",
        ])
        .arg(&include)
        .arg(&file)
        .output()
    else {
        if cfg!(windows) {
            return;
        }
        panic!("no cc to check the header with");
    };
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
