use crate::wal_entry::WalEntryError;
use crate::wal_header::WalHeaderError;
use uintn::UintN;

#[derive(Debug)]
pub enum WalError {
    IoError(std::io::Error),
    WalHeaderError(WalHeaderError),
    WalEntryError(WalEntryError),
    PathError(uintn::paths::PathError),
    SendError,
    WalNotFound,
    WriterNotFound,
    WalEmpty(UintN),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::IoError(e) => write!(f, "IO error: {}", e),
            WalError::WalHeaderError(e) => write!(f, "WAL header error: {}", e),
            WalError::WalEntryError(e) => write!(f, "WAL entry error: {}", e),
            WalError::PathError(e) => write!(f, "Path error: {}", e),
            WalError::SendError => write!(f, "Failed to send WAL entry"),
            WalError::WalNotFound => write!(f, "WAL file not found"),
            WalError::WriterNotFound => write!(f, "WAL writer not found"),
            WalError::WalEmpty(id) => write!(f, "WAL file {} is empty", id),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalError::IoError(e) => Some(e),
            WalError::WalHeaderError(e) => Some(e),
            WalError::WalEntryError(e) => Some(e),
            WalError::PathError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<WalHeaderError> for WalError {
    fn from(e: WalHeaderError) -> Self {
        WalError::WalHeaderError(e)
    }
}

impl From<WalEntryError> for WalError {
    fn from(e: WalEntryError) -> Self {
        WalError::WalEntryError(e)
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::IoError(e)
    }
}

impl From<uintn::Error> for WalError {
    fn from(e: uintn::Error) -> Self {
        WalError::WalHeaderError(WalHeaderError::UintN(e))
    }
}
