//! Reuse `uintn::paths` so all storage crates agree on the directory layout.

use std::io;
use std::path::Path;

use uintn::UintN;
use uintn::paths::{self, PathError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    Min,
    Max,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanResult {
    /// No file with the extension under the directory, or no directory.
    None,
    One(UintN),
    All(Vec<UintN>),
}

pub(crate) fn scan_ids(dir: &Path, ext: &str, which: Scan) -> io::Result<ScanResult> {
    match std::fs::metadata(dir) {
        Ok(m) if m.is_dir() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "scan root is not a directory",
            ));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ScanResult::None),
        Err(e) => return Err(e),
    }
    let one = |r: Result<UintN, PathError>| match r {
        Ok(id) => Ok(ScanResult::One(id)),
        Err(PathError::NoFilesFound) => Ok(ScanResult::None),
        Err(PathError::Io(e)) => Err(e),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
    };
    match which {
        Scan::Min => one(paths::find_min_id(dir, ext)),
        Scan::Max => one(paths::find_max_id(dir, ext)),
        Scan::All => match paths::get_files_ids(dir, ext) {
            Ok(ids) if ids.is_empty() => Ok(ScanResult::None),
            Ok(ids) => Ok(ScanResult::All(ids)),
            Err(PathError::NoFilesFound) => Ok(ScanResult::None),
            Err(PathError::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
    }
}
