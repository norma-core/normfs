//! Directory walks over the 3-hex-chunk layout `UintN::to_file_path` writes.

use std::io;
use std::path::{Path, PathBuf};

use uintn::UintN;

use crate::beneath::{self, At, Entry};

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

/// Lists through the walk, so a link under `dir` is neither entered nor
/// counted as a file.
fn list(dir: &Path, root: Option<&Path>) -> io::Result<Vec<Entry>> {
    beneath::list(&At::new(root, dir)?)
}

pub(crate) fn scan_ids(
    dir: &Path,
    ext: &str,
    which: Scan,
    root: Option<&Path>,
) -> io::Result<ScanResult> {
    match beneath::stat_as_given(&At::new(root, dir)?)? {
        Some(m) if m.is_dir() => {}
        Some(_) => {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "scan root is not a directory",
            ));
        }
        None => return Ok(ScanResult::None),
    }
    let one = |r: Result<UintN, PathError>| match r {
        Ok(id) => Ok(ScanResult::One(id)),
        Err(PathError::NoFilesFound) => Ok(ScanResult::None),
        Err(PathError::Io(e)) => Err(e),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
    };
    match which {
        Scan::Min => one(find_min_id(dir, ext, root)),
        Scan::Max => one(find_max_id(dir, ext, root)),
        Scan::All => match get_files_ids(dir, ext, root) {
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

pub(crate) fn get_files_ids(
    base_path: &Path,
    extension: &str,
    root: Option<&Path>,
) -> Result<Vec<UintN>, PathError> {
    let mut ids = Vec::new();
    let mut dirs = vec![PathBuf::new()];

    while let Some(rel) = dirs.pop() {
        let entries = match list(&base_path.join(&rel), root) {
            Ok(entries) => entries,
            // A directory removed during the walk, as eviction does.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                continue;
            }
            Err(e) => return Err(PathError::Io(e)),
        };
        for entry in entries {
            let relative_path = rel.join(&entry.name);
            if entry.stat.is_dir() {
                dirs.push(relative_path);
                continue;
            }
            if !entry.stat.is_file()
                || relative_path.extension().and_then(|s| s.to_str()) != Some(extension)
            {
                continue;
            }

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

fn find_max_id_recursive(
    current_path: &Path,
    extension: &str,
    root: Option<&Path>,
) -> Result<PathBuf, PathError> {
    let entries = list(current_path, root)?;
    let max_from_dirs = entries
        .iter()
        .filter(|e| e.stat.is_dir())
        .filter_map(|e| {
            find_max_id_recursive(&current_path.join(&e.name), extension, root)
                .ok()
                .map(|sub_path| Path::new(&e.name).join(sub_path))
        })
        .max_by(|a, b| {
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    let max_from_files = entries
        .iter()
        .map(|e| (e, PathBuf::from(&e.name)))
        .filter(|(e, p)| {
            e.stat.is_file() && p.extension().and_then(|s| s.to_str()) == Some(extension)
        })
        .map(|(_, p)| p)
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

fn find_min_id_recursive(
    current_path: &Path,
    extension: &str,
    root: Option<&Path>,
) -> Result<PathBuf, PathError> {
    let entries = list(current_path, root)?;
    let min_from_dirs = entries
        .iter()
        .filter(|e| e.stat.is_dir())
        .filter_map(|e| {
            find_min_id_recursive(&current_path.join(&e.name), extension, root)
                .ok()
                .map(|sub_path| Path::new(&e.name).join(sub_path))
        })
        .min_by(|a, b| {
            let id_a = path_to_id(a, extension).unwrap_or(UintN::zero());
            let id_b = path_to_id(b, extension).unwrap_or(UintN::zero());
            id_a.cmp(&id_b)
        });

    let min_from_files = entries
        .iter()
        .map(|e| (e, PathBuf::from(&e.name)))
        .filter(|(e, p)| {
            e.stat.is_file() && p.extension().and_then(|s| s.to_str()) == Some(extension)
        })
        .map(|(_, p)| p)
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

pub(crate) fn find_max_id(
    base_path: &Path,
    extension: &str,
    root: Option<&Path>,
) -> Result<UintN, PathError> {
    if beneath::stat_as_given(&At::new(root, base_path)?)?.is_none() {
        return Err(PathError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Base path does not exist",
        )));
    }

    let relative_path = find_max_id_recursive(base_path, extension, root)?;

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

pub(crate) fn find_min_id(
    base_path: &Path,
    extension: &str,
    root: Option<&Path>,
) -> Result<UintN, PathError> {
    if beneath::stat_as_given(&At::new(root, base_path)?)?.is_none() {
        return Err(PathError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Base path does not exist",
        )));
    }

    let relative_path = find_min_id_recursive(base_path, extension, root)?;

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
