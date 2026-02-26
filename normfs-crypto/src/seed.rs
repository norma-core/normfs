use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use getrandom::getrandom;
use zeroize::ZeroizeOnDrop;

pub const SEED_SIZE: usize = 32;
const SEED_FILE_NAME: &str = ".crypto_seed";

#[derive(Debug)]
pub enum SeedError {
    Io(io::Error),
    InvalidSeed,
    OsRng,
}

impl std::fmt::Display for SeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeedError::Io(e) => write!(f, "IO error: {}", e),
            SeedError::InvalidSeed => write!(f, "Invalid seed size or format"),
            SeedError::OsRng => write!(f, "Failed to generate random bytes from OS"),
        }
    }
}

impl std::error::Error for SeedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SeedError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for SeedError {
    fn from(e: io::Error) -> Self {
        SeedError::Io(e)
    }
}

#[derive(ZeroizeOnDrop)]
pub struct Seed {
    #[zeroize(skip)]
    bytes: [u8; SEED_SIZE],
}

impl Seed {
    pub fn generate() -> Result<Self, SeedError> {
        let mut bytes = [0u8; SEED_SIZE];
        getrandom(&mut bytes).map_err(|_| SeedError::OsRng)?;

        Ok(Self { bytes })
    }

    pub fn from_bytes(bytes: [u8; SEED_SIZE]) -> Self {
        Self { bytes }
    }

    pub fn load<P: AsRef<Path>>(data_dir: P) -> Result<Self, SeedError> {
        let seed_path = data_dir.as_ref().join(SEED_FILE_NAME);
        Self::load_from_file(&seed_path)
    }

    fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, SeedError> {
        let mut bytes = [0u8; SEED_SIZE];
        let mut file = fs::File::open(path)?;

        let bytes_read = file.read(&mut bytes)?;
        if bytes_read != SEED_SIZE {
            return Err(SeedError::InvalidSeed);
        }

        Ok(Self::from_bytes(bytes))
    }

    pub fn save<P: AsRef<Path>>(&self, data_dir: P) -> Result<(), SeedError> {
        let seed_path = data_dir.as_ref().join(SEED_FILE_NAME);
        self.save_to_file(&seed_path)
    }

    fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), SeedError> {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;

        file.write_all(&self.bytes)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn exists<P: AsRef<Path>>(data_dir: P) -> bool {
        data_dir.as_ref().join(SEED_FILE_NAME).exists()
    }

    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self, SeedError> {
        let data_dir = data_dir.as_ref();

        if Self::exists(data_dir) {
            Self::load(data_dir)
        } else {
            let seed = Self::generate()?;
            seed.save(data_dir)?;
            Ok(seed)
        }
    }

    pub fn as_bytes(&self) -> &[u8; SEED_SIZE] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_creates_random_seeds() {
        let seed1 = Seed::generate().unwrap();
        let seed2 = Seed::generate().unwrap();

        // Seeds should be different (astronomically unlikely to be same)
        assert_ne!(seed1.as_bytes(), seed2.as_bytes());
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [42u8; SEED_SIZE];
        let seed = Seed::from_bytes(bytes);

        assert_eq!(seed.as_bytes(), &bytes);
    }

    #[test]
    fn test_open_creates_new_if_not_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let seed = Seed::open(temp_dir.path()).unwrap();

        // Seed file should exist
        assert!(Seed::exists(temp_dir.path()));

        // Should be able to read the seed back
        let mut read_bytes = [0u8; SEED_SIZE];
        let seed_path = temp_dir.path().join(SEED_FILE_NAME);
        let mut file = fs::File::open(&seed_path).unwrap();
        file.read_exact(&mut read_bytes).unwrap();

        assert_eq!(seed.as_bytes(), &read_bytes);
    }

    #[test]
    fn test_open_loads_existing_seed() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create first seed
        let seed1 = Seed::open(temp_dir.path()).unwrap();
        let bytes1 = *seed1.as_bytes();

        // Open again should load the same seed
        let seed2 = Seed::open(temp_dir.path()).unwrap();
        let bytes2 = *seed2.as_bytes();

        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_seed_file_permissions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let seed = Seed::generate().unwrap();
        seed.save(temp_dir.path()).unwrap();

        let seed_path = temp_dir.path().join(SEED_FILE_NAME);
        let metadata = fs::metadata(&seed_path).unwrap();

        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();

        // Check that only owner has read/write permissions (0600)
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn test_exists() {
        let temp_dir = tempfile::tempdir().unwrap();

        assert!(!Seed::exists(temp_dir.path()));

        let seed = Seed::generate().unwrap();
        seed.save(temp_dir.path()).unwrap();

        assert!(Seed::exists(temp_dir.path()));
    }
}
