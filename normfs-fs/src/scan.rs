//! Directory walks over the 3-hex-chunk layout `UintN::to_file_path` writes.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use uintn::UintN;
use walkdir::WalkDir;

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
        Scan::Min => one(find_min_id(dir, ext)),
        Scan::Max => one(find_max_id(dir, ext)),
        Scan::All => match get_files_ids(dir, ext) {
            Ok(ids) if ids.is_empty() => Ok(ScanResult::None),
            Ok(ids) => Ok(ScanResult::All(ids)),
            Err(PathError::NoFilesFound) => Ok(ScanResult::None),
            Err(PathError::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
    }
}

#[derive(Debug)]
pub enum PathError {
    Io(std::io::Error),
    UintN(uintn::Error),
    NoFilesFound,
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::Io(e) => write!(f, "IO error: {}", e),
            PathError::UintN(e) => write!(f, "UintN error: {}", e),
            PathError::NoFilesFound => write!(f, "No files found"),
        }
    }
}

impl std::error::Error for PathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PathError::Io(e) => Some(e),
            PathError::UintN(e) => Some(e),
            PathError::NoFilesFound => None,
        }
    }
}

impl From<std::io::Error> for PathError {
    fn from(err: std::io::Error) -> Self {
        PathError::Io(err)
    }
}

impl From<uintn::Error> for PathError {
    fn from(err: uintn::Error) -> Self {
        PathError::UintN(err)
    }
}

pub(crate) fn get_files_ids(base_path: &Path, extension: &str) -> Result<Vec<UintN>, PathError> {
    let mut ids = Vec::new();

    for entry in WalkDir::new(base_path) {
        let entry = match entry {
            Ok(entry) => entry,
            // A file or directory removed during the walk, as eviction does.
            Err(e) if e.io_error().map(io::Error::kind) == Some(io::ErrorKind::NotFound) => {
                continue;
            }
            Err(e) => return Err(PathError::Io(e.into())),
        };
        let path = entry.path();

        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some(extension) {
            let relative_path = match path.strip_prefix(base_path) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let mut hex_string = String::new();
            for component in relative_path.components() {
                let component_str = component.as_os_str().to_string_lossy();
                if let Some(stripped) = component_str.strip_suffix(&format!(".{}", extension)) {
                    hex_string.push_str(stripped);
                } else {
                    hex_string.push_str(&component_str);
                }
            }

            if let Ok(id) = UintN::from_hex_digits(&hex_string) {
                ids.push(id);
            }
        }
    }

    ids.sort();

    Ok(ids)
}

fn find_max_id_recursive(current_path: &Path, extension: &str) -> Result<PathBuf, PathError> {
    let max_from_dirs = fs::read_dir(current_path)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter_map(|p| {
            find_max_id_recursive(&p, extension).ok().map(|sub_path| {
                let mut result_path = PathBuf::new();
                if let Some(dir_name) = p.file_name() {
                    result_path.push(dir_name);
                    result_path.push(sub_path);
                }
                result_path
            })
        })
        .max_by(|a, b| {
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    let max_from_files = fs::read_dir(current_path)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some(extension))
        .filter_map(|p| p.file_name().map(PathBuf::from))
        .max_by(|a, b| {
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    match (max_from_dirs, max_from_files) {
        (Some(p1), Some(p2)) => {
            let id1 = path_to_id(&p1, extension).unwrap_or(UintN::zero());
            let id2 = path_to_id(&p2, extension).unwrap_or(UintN::zero());
            Ok(if id1 >= id2 { p1 } else { p2 })
        }
        (Some(p), None) => Ok(p),
        (None, Some(p)) => Ok(p),
        (None, None) => Err(PathError::NoFilesFound),
    }
}

fn find_min_id_recursive(current_path: &Path, extension: &str) -> Result<PathBuf, PathError> {
    let min_from_dirs = fs::read_dir(current_path)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .filter_map(|p| {
            find_min_id_recursive(&p, extension).ok().map(|sub_path| {
                let mut result_path = PathBuf::new();
                if let Some(dir_name) = p.file_name() {
                    result_path.push(dir_name);
                    result_path.push(sub_path);
                }
                result_path
            })
        })
        .min_by(|a, b| {
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    let min_from_files = fs::read_dir(current_path)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().and_then(|s| s.to_str()) == Some(extension))
        .filter_map(|p| p.file_name().map(PathBuf::from))
        .min_by(|a, b| {
            // Compare by parsing the IDs, not lexicographically
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    match (min_from_dirs, min_from_files) {
        (Some(p1), Some(p2)) => {
            let id1 = path_to_id(&p1, extension).unwrap_or(UintN::zero());
            let id2 = path_to_id(&p2, extension).unwrap_or(UintN::zero());
            Ok(if id1 <= id2 { p1 } else { p2 })
        }
        (Some(p), None) => Ok(p),
        (None, Some(p)) => Ok(p),
        (None, None) => Err(PathError::NoFilesFound),
    }
}

pub(crate) fn find_max_id(base_path: &Path, extension: &str) -> Result<UintN, PathError> {
    if !base_path.exists() {
        return Err(PathError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Base path does not exist",
        )));
    }

    let relative_path = find_max_id_recursive(base_path, extension)?;

    let mut hex_string = String::new();
    for component in relative_path.components() {
        let component_str = component.as_os_str().to_string_lossy();
        if let Some(stripped) = component_str.strip_suffix(&format!(".{}", extension)) {
            hex_string.push_str(stripped);
        } else {
            hex_string.push_str(&component_str);
        }
    }

    Ok(UintN::from_hex_digits(&hex_string)?)
}

pub(crate) fn find_min_id(base_path: &Path, extension: &str) -> Result<UintN, PathError> {
    if !base_path.exists() {
        return Err(PathError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Base path does not exist",
        )));
    }

    let relative_path = find_min_id_recursive(base_path, extension)?;

    let mut hex_string = String::new();
    for component in relative_path.components() {
        let component_str = component.as_os_str().to_string_lossy();
        if let Some(stripped) = component_str.strip_suffix(&format!(".{}", extension)) {
            hex_string.push_str(stripped);
        } else {
            hex_string.push_str(&component_str);
        }
    }

    Ok(UintN::from_hex_digits(&hex_string)?)
}

fn path_to_id(path: &Path, extension: &str) -> Result<UintN, PathError> {
    let mut hex_string = String::new();
    for component in path.components() {
        let component_str = component.as_os_str().to_string_lossy();
        if let Some(stripped) = component_str.strip_suffix(&format!(".{}", extension)) {
            hex_string.push_str(stripped);
        } else {
            hex_string.push_str(&component_str);
        }
    }
    Ok(UintN::from_hex_digits(&hex_string)?)
}
