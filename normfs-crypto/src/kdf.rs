use std::os::raw::c_int;

use zeroize::Zeroizing;

/// Must agree with NORMFS_KDF_KEY and NORMFS_KDF_NONCE in
/// `c/include/normfs/kdf.h`; the C rejects a mismatch rather than trusting it.
pub const AES_KEY_SIZE: usize = 32;
pub const GCM_NONCE_SIZE: usize = 12;

// Mirrors enum normfs_kdf_status in c/include/normfs/kdf.h.
const NORMFS_KDF_OK: c_int = 0;
const NORMFS_KDF_ERR_INVALID_ARG: c_int = 1;
const NORMFS_KDF_ERR_INFO_TOO_LONG: c_int = 2;

unsafe extern "C" {
    fn normfs_kdf_derive_file_key(
        seed: *const u8,
        seed_len: usize,
        info: *const u8,
        info_len: usize,
        out_key: *mut u8,
        out_key_len: usize,
        out_nonce: *mut u8,
        out_nonce_len: usize,
    ) -> c_int;
}

#[derive(Debug)]
pub enum KdfError {
    /// A length disagreed with the C layer's own constant.
    InvalidArg,
    /// The info string exceeded NORMFS_KDF_INFO_MAX. Unreachable for any queue
    /// path a caller can construct; the bound exists so the C arithmetic
    /// provably cannot wrap.
    InfoTooLong,
    /// A status this build does not know about, as in `SeedError`.
    UnknownStatus(c_int),
}

impl std::fmt::Display for KdfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KdfError::InvalidArg => write!(f, "Key derivation argument length mismatch"),
            KdfError::InfoTooLong => write!(f, "Key derivation info string too long"),
            KdfError::UnknownStatus(s) => {
                write!(f, "Unknown status {} from the C key derivation layer", s)
            }
        }
    }
}

impl std::error::Error for KdfError {}

fn map_status(status: c_int) -> Result<(), KdfError> {
    match status {
        NORMFS_KDF_OK => Ok(()),
        NORMFS_KDF_ERR_INVALID_ARG => Err(KdfError::InvalidArg),
        NORMFS_KDF_ERR_INFO_TOO_LONG => Err(KdfError::InfoTooLong),
        other => Err(KdfError::UnknownStatus(other)),
    }
}

/// HKDF-SHA256 over the seed, then ChaCha20 block 0 under the derived seed:
/// the key is keystream `[0,32)` and the nonce `[32,44)`.
///
/// `info` is built by the caller and passed whole. Its tail is `UintN`'s
/// `value_to_bytes`, whose width the enum variant chooses, and reproducing that
/// narrowing in C would be a second definition of an encoding already on disk.
pub(crate) fn derive_file_key(
    seed: &[u8; crate::seed::SEED_SIZE],
    info: &[u8],
) -> Result<(Zeroizing<[u8; AES_KEY_SIZE]>, [u8; GCM_NONCE_SIZE]), KdfError> {
    let mut key = Zeroizing::new([0u8; AES_KEY_SIZE]);
    let mut nonce = [0u8; GCM_NONCE_SIZE];

    // SAFETY: seed and info are live for the call; info.as_ptr() may dangle
    // when info is empty, which the C contract admits via its `info_len == 0 ||`
    // guard. key and nonce are distinct stack allocations, satisfying the
    // \separated preconditions the FFI cannot check.
    let status = unsafe {
        normfs_kdf_derive_file_key(
            seed.as_ptr(),
            crate::seed::SEED_SIZE,
            info.as_ptr(),
            info.len(),
            key.as_mut_ptr(),
            AES_KEY_SIZE,
            nonce.as_mut_ptr(),
            GCM_NONCE_SIZE,
        )
    };
    map_status(status)?;

    Ok((key, nonce))
}
