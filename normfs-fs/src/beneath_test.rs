use std::path::Path;

use crate::beneath::root_len;

#[cfg(unix)]
#[test]
fn root_len_walks_only_below_the_data_directory() {
    let root = Some(Path::new("/data/n"));
    assert_eq!(root_len(root, Path::new("/data/n/q/wal")).unwrap(), 7);
    assert_eq!(
        root_len(Some(Path::new("/data/n/")), Path::new("/data/n/q")).unwrap(),
        7
    );
    assert_eq!(root_len(root, Path::new("/data/n")).unwrap(), 0);
    assert_eq!(root_len(root, Path::new("/data")).unwrap(), 0);
    assert!(root_len(root, Path::new("/data/nn/q")).is_err());
    assert!(root_len(root, Path::new("/other")).is_err());
    assert_eq!(root_len(None, Path::new("/other")).unwrap(), 0);
}

#[test]
fn a_relative_data_directory_resolves_its_base_as_given() {
    let root = Some(Path::new("data"));
    assert_eq!(root_len(root, Path::new(".")).unwrap(), 0);
    assert_eq!(root_len(root, Path::new("")).unwrap(), 0);
}
