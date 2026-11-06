use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int};
use std::path::Path;

/// Must agree with NORMFS_SEED_SIZE in `c/include/normfs/seed.h`; the C layer
/// rejects a mismatch rather than trusting it.
pub const SEED_SIZE: usize = 32;

// Mirrors enum normfs_seed_status in c/include/normfs/seed.h.
const NORMFS_SEED_OK: c_int = 0;
const NORMFS_SEED_ERR_INVALID_ARG: c_int = 1;
const NORMFS_SEED_ERR_PATH_TOO_LONG: c_int = 2;
const NORMFS_SEED_ERR_OS_RNG: c_int = 3;
const NORMFS_SEED_ERR_INVALID_SEED: c_int = 4;
const NORMFS_SEED_ERR_IO: c_int = 5;

/// Two 4-byte fields in the same order as `struct normfs_seed_result`, so
/// neither side has padding the other does not.
#[repr(C)]
#[derive(Clone, Copy)]
struct CSeedResult {
    os_error: c_int,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_seed_generate(seed: *mut u8, seed_len: usize) -> CSeedResult;

    fn normfs_seed_load(
        data_dir: *const c_char,
        data_dir_len: usize,
        seed: *mut u8,
        seed_len: usize,
    ) -> CSeedResult;

    fn normfs_seed_save(
        data_dir: *const c_char,
        data_dir_len: usize,
        seed: *const u8,
        seed_len: usize,
    ) -> CSeedResult;

    fn normfs_seed_exists(data_dir: *const c_char, data_dir_len: usize) -> c_int;

    fn normfs_seed_zero(seed: *mut u8, seed_len: usize);
}

#[derive(Debug)]
pub enum SeedError {
    Io(io::Error),
    InvalidSeed,
    OsRng,
    /// A status this build does not know about, as in `VarintError`.
    UnknownStatus(c_int),
}

impl std::fmt::Display for SeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SeedError::Io(e) => write!(f, "IO error: {}", e),
            SeedError::InvalidSeed => write!(f, "Invalid seed size or format"),
            SeedError::OsRng => write!(f, "Failed to generate random bytes from OS"),
            SeedError::UnknownStatus(s) => {
                write!(f, "Unknown status {} from the C seed layer", s)
            }
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

/// Without the C layer's `errno`, `ErrorKind::NotFound` and `AlreadyExists`
/// would be lost at the FFI boundary, and both `Seed::open` and its callers
/// classify on exactly those.
fn os_error(raw: c_int) -> io::Error {
    if raw != 0 {
        io::Error::from_raw_os_error(raw)
    } else {
        io::Error::other("the C seed layer reported a failure without an errno")
    }
}

fn map_status(r: CSeedResult) -> Result<(), SeedError> {
    match r.status {
        NORMFS_SEED_OK => Ok(()),
        NORMFS_SEED_ERR_OS_RNG => Err(SeedError::OsRng),
        NORMFS_SEED_ERR_INVALID_SEED => Err(SeedError::InvalidSeed),
        NORMFS_SEED_ERR_IO => Err(SeedError::Io(os_error(r.os_error))),
        // Decided before any syscall runs, so there is no errno to carry.
        NORMFS_SEED_ERR_INVALID_ARG => Err(SeedError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "seed buffer of the wrong size",
        ))),
        NORMFS_SEED_ERR_PATH_TOO_LONG => Err(SeedError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "seed path exceeds the C layer's path limit",
        ))),
        other => Err(SeedError::UnknownStatus(other)),
    }
}

fn c_dir(data_dir: &Path) -> Result<CString, SeedError> {
    #[cfg(unix)]
    let bytes = std::os::unix::ffi::OsStrExt::as_bytes(data_dir.as_os_str());
    // The Windows C layer takes UTF-8 and widens it for the wide-char calls.
    #[cfg(windows)]
    let bytes = data_dir.to_str().map(str::as_bytes).ok_or_else(|| {
        SeedError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "data directory path is not valid UTF-8",
        ))
    })?;
    CString::new(bytes).map_err(|_| {
        SeedError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "data directory path contains an interior NUL",
        ))
    })
}

pub struct Seed {
    bytes: [u8; SEED_SIZE],
}

impl Drop for Seed {
    fn drop(&mut self) {
        // SAFETY: self.bytes is live and uniquely borrowed for the call.
        unsafe { normfs_seed_zero(self.bytes.as_mut_ptr(), SEED_SIZE) };
    }
}

// Required, not a formality: CryptoContext derives ZeroizeOnDrop over a
// `seed: Seed` field, and that derive needs each non-skipped field to be
// Zeroize or ZeroizeOnDrop. The Drop above is what makes it true.
impl zeroize::ZeroizeOnDrop for Seed {}

impl Seed {
    pub fn generate() -> Result<Self, SeedError> {
        // Built first and filled in place: a local array moved in afterwards
        // would leave a second, unwiped copy on the stack.
        let mut seed = Self {
            bytes: [0u8; SEED_SIZE],
        };

        // SAFETY: seed.bytes is a live 32-byte array, uniquely borrowed here.
        let r = unsafe { normfs_seed_generate(seed.bytes.as_mut_ptr(), SEED_SIZE) };
        map_status(r)?;

        Ok(seed)
    }

    // Public API, but `mod seed` is private in lib.rs and nothing in the crate
    // builds a seed from bytes any more; load fills the buffer over the FFI.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: [u8; SEED_SIZE]) -> Self {
        Self { bytes }
    }

    pub fn load<P: AsRef<Path>>(data_dir: P) -> Result<Self, SeedError> {
        let dir = c_dir(data_dir.as_ref())?;
        let mut seed = Self {
            bytes: [0u8; SEED_SIZE],
        };

        // SAFETY: `dir` is NUL-terminated by CString and outlives the call. It
        // and seed.bytes are distinct allocations, satisfying the C contract's
        // \separated precondition.
        let r = unsafe {
            normfs_seed_load(
                dir.as_ptr(),
                dir.as_bytes().len(),
                seed.bytes.as_mut_ptr(),
                SEED_SIZE,
            )
        };
        map_status(r)?;

        Ok(seed)
    }

    pub fn save<P: AsRef<Path>>(&self, data_dir: P) -> Result<(), SeedError> {
        let dir = c_dir(data_dir.as_ref())?;

        // SAFETY: as in load; the seed is only read here.
        let r = unsafe {
            normfs_seed_save(
                dir.as_ptr(),
                dir.as_bytes().len(),
                self.bytes.as_ptr(),
                SEED_SIZE,
            )
        };
        map_status(r)
    }

    pub fn exists<P: AsRef<Path>>(data_dir: P) -> bool {
        // Errors swallowed, as Path::exists swallows its own.
        let Ok(dir) = c_dir(data_dir.as_ref()) else {
            return false;
        };

        // SAFETY: `dir` is NUL-terminated and outlives the call.
        unsafe { normfs_seed_exists(dir.as_ptr(), dir.as_bytes().len()) == 1 }
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
    use std::fs;
    use std::io::Read;

    /// The production constant lives in c/include/normfs/seed.h now;
    /// test_seed.c pins that one against the literal.
    const SEED_FILE_NAME: &str = ".crypto_seed";

    #[test]
    fn test_generate_creates_random_seeds() {
        let seed1 = Seed::generate().unwrap();
        let seed2 = Seed::generate().unwrap();

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

        assert!(Seed::exists(temp_dir.path()));

        let mut read_bytes = [0u8; SEED_SIZE];
        let seed_path = temp_dir.path().join(SEED_FILE_NAME);
        let mut file = fs::File::open(&seed_path).unwrap();
        file.read_exact(&mut read_bytes).unwrap();

        assert_eq!(seed.as_bytes(), &read_bytes);
    }

    #[test]
    fn test_open_loads_existing_seed() {
        let temp_dir = tempfile::tempdir().unwrap();

        let seed1 = Seed::open(temp_dir.path()).unwrap();
        let bytes1 = *seed1.as_bytes();

        let seed2 = Seed::open(temp_dir.path()).unwrap();
        let bytes2 = *seed2.as_bytes();

        assert_eq!(bytes1, bytes2);
    }

    #[cfg(unix)]
    #[test]
    fn test_seed_file_permissions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let seed = Seed::generate().unwrap();
        seed.save(temp_dir.path()).unwrap();

        let seed_path = temp_dir.path().join(SEED_FILE_NAME);
        let metadata = fs::metadata(&seed_path).unwrap();

        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode();

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

    #[test]
    fn test_save_twice_fails() {
        let temp_dir = tempfile::tempdir().unwrap();

        Seed::generate().unwrap().save(temp_dir.path()).unwrap();

        // Seed::open relies on this surfacing as AlreadyExists, which is what
        // the errno round trip preserves.
        let err = Seed::generate()
            .unwrap()
            .save(temp_dir.path())
            .expect_err("a second save must not clobber the first seed");

        match err {
            SeedError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
            other => panic!("expected an Io error, got {:?}", other),
        }
    }

    #[test]
    fn test_load_missing_reports_not_found() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Matched rather than unwrapped: Seed has no Debug impl -- deriving one
        // would put the root secret in any log line that formatted it -- so
        // expect_err is unavailable here.
        match Seed::load(temp_dir.path()) {
            Err(SeedError::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
            Err(other) => panic!("expected an Io error, got {:?}", other),
            Ok(_) => panic!("there is no seed to load"),
        }
    }

    #[test]
    fn test_drop_wipes_the_seed() {
        use std::mem::ManuallyDrop;

        let mut seed = ManuallyDrop::new(Seed::from_bytes([0x5Au8; SEED_SIZE]));
        let ptr = seed.as_bytes().as_ptr();

        // SAFETY: ManuallyDrop keeps the storage alive across the drop, and
        // [u8; SEED_SIZE] has no drop glue and no invalid bit patterns, so
        // reading it back is well defined.
        unsafe {
            ManuallyDrop::drop(&mut seed);
            assert_eq!(
                std::slice::from_raw_parts(ptr, SEED_SIZE),
                &[0u8; SEED_SIZE]
            );
        }
    }
}
