//! Store files built inside a slot of a [`PackPool`].
//!
//! A slot holds the WAL bytes of one file and, after them, the finished store
//! file: `auth ++ header ++ body`, contiguous, so it is written to disk or
//! sent to the bucket without being copied again. Compression runs in a zstd
//! context whose tables live in memory allocated with the pool, and AES-GCM
//! encrypts in place. Nothing on this path allocates per file beyond a few
//! hundred bytes of headers.

use bytes::BytesMut;
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_wal::{PackPool, PackSlot};
use std::ffi::c_int;
use std::io;
use std::sync::{Arc, Mutex};
use uintn::UintN;

use crate::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use crate::store_file::SealedFile;
use crate::store_header_v1::{STORE_HEADER_V1_MAX_SIZE, StoreHeaderV1};

const AUTH_SIZE: usize = FileAuthentication::SIZE;
const TAG_SIZE: usize = 16;

// Mirrors NORMFS_STORE_PACK_AUTH_SIZE, which the C layout reserves for it.
const _: () = assert!(AUTH_SIZE == 152);

#[repr(C)]
#[derive(Clone, Copy)]
struct Layout {
    file_at: usize,
    header_at: usize,
    body_at: usize,
    data_at: usize,
    data_cap: usize,
    encrypted: c_int,
}

#[repr(C)]
struct SizeResult {
    size: usize,
    status: c_int,
}

#[repr(C)]
struct LayoutResult {
    layout: Layout,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_store_pack_slot_size(input_cap: usize) -> SizeResult;
    fn normfs_store_pack_layout(
        slot_size: usize,
        input_cap: usize,
        header_len: usize,
        encrypted: c_int,
    ) -> LayoutResult;
    fn normfs_store_pack_file_len(layout: *const Layout, data_len: usize) -> SizeResult;
}

#[derive(Debug, PartialEq, Eq)]
pub enum PackError {
    InputTooLarge,
    SlotTooSmall,
    HeaderTooLarge,
    DataTooLarge,
    UnknownStatus(c_int),
}

impl PackError {
    fn check(status: c_int) -> Result<(), PackError> {
        match status {
            0 => Ok(()),
            1 => Err(PackError::InputTooLarge),
            2 => Err(PackError::SlotTooSmall),
            3 => Err(PackError::HeaderTooLarge),
            4 => Err(PackError::DataTooLarge),
            s => Err(PackError::UnknownStatus(s)),
        }
    }
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::InputTooLarge => write!(f, "pack slot input too large"),
            PackError::SlotTooSmall => write!(f, "pack slot too small for its input"),
            PackError::HeaderTooLarge => write!(f, "store header too large for a pack slot"),
            PackError::DataTooLarge => write!(f, "packed data overflows its slot"),
            PackError::UnknownStatus(s) => write!(f, "unknown pack slot status {s}"),
        }
    }
}

impl std::error::Error for PackError {}

impl From<PackError> for io::Error {
    fn from(e: PackError) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, e)
    }
}

fn slot_size(input_cap: usize) -> Result<usize, PackError> {
    let r = unsafe { normfs_store_pack_slot_size(input_cap) };
    PackError::check(r.status)?;
    Ok(r.size)
}

pub struct Packer {
    pool: Arc<PackPool>,
    input_cap: usize,
    /// One per slot, used only by whoever holds that slot.
    compressors: Vec<Mutex<Compressor>>,
}

impl Packer {
    /// `slots` files of up to `input_cap` WAL bytes each can be packed at once.
    pub fn new(slots: usize, input_cap: usize) -> io::Result<Self> {
        let compressors = (0..slots)
            .map(|_| Compressor::new(input_cap).map(Mutex::new))
            .collect::<io::Result<_>>()?;
        Ok(Packer {
            pool: PackPool::new(slots, slot_size(input_cap)?),
            input_cap,
            compressors,
        })
    }

    /// The largest file, header included, a slot takes.
    pub fn input_cap(&self) -> usize {
        self.input_cap
    }

    pub fn slots(&self) -> usize {
        self.pool.slots()
    }

    /// Waits for a slot. The caller writes the WAL bytes to the front of
    /// [`PackSlot::buf`], at most [`Packer::input_cap`] of them.
    pub async fn take(&self) -> PackSlot {
        self.pool.take().await
    }

    /// Seals the first `input_len` bytes of `slot` as store file `file_id`,
    /// the same bytes [`crate::store_file::build`] makes of them. The result
    /// holds the slot until it is dropped.
    #[allow(clippy::too_many_arguments)]
    pub fn seal(
        &self,
        mut slot: PackSlot,
        input_len: usize,
        queue: &QueueId,
        file_id: &UintN,
        compression: CompressionType,
        encryption: EncryptionType,
        entries_before: UintN,
        num_entries: UintN,
        crypto: &CryptoContext,
    ) -> io::Result<SealedFile> {
        assert!(
            input_len <= self.input_cap,
            "{input_len} bytes overflow the slot"
        );
        let header = StoreHeader::new(
            compression,
            encryption,
            entries_before.clone(),
            num_entries.clone(),
        );
        let mut header_bytes = BytesMut::with_capacity(STORE_HEADER_V1_MAX_SIZE);
        StoreHeaderV1::from_v0(&header)
            .and_then(|h| h.write_to_bytes(&mut header_bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let encrypted = encryption != EncryptionType::None;
        let index = slot.index();
        let buf = slot.buf();
        let r = unsafe {
            normfs_store_pack_layout(
                buf.len(),
                self.input_cap,
                header_bytes.len(),
                encrypted as c_int,
            )
        };
        PackError::check(r.status)?;
        let l = r.layout;
        let (input, out) = buf.split_at_mut(l.file_at);
        let input = &input[..input_len];
        let (body_at, data_at) = (l.body_at - l.file_at, l.data_at - l.file_at);
        let data = &mut out[data_at..data_at + l.data_cap];

        let data_len = match compression {
            CompressionType::None => {
                data[..input.len()].copy_from_slice(input);
                input.len()
            }
            CompressionType::Zstd => self.compressors[index]
                .lock()
                .unwrap()
                .compress(input, data)?,
            other => {
                return Err(io::Error::other(format!(
                    "Unsupported compression type: {other:?}"
                )));
            }
        };
        let r = unsafe { normfs_store_pack_file_len(&l, data_len) };
        PackError::check(r.status)?;
        let file_len = r.size;
        if encrypted {
            let (nonce, tag) = crypto
                .encrypt_in_place(queue, file_id, &mut out[data_at..data_at + data_len])
                .map_err(|e| io::Error::other(e.to_string()))?;
            out[body_at..data_at].copy_from_slice(&nonce);
            out[data_at + data_len..data_at + data_len + TAG_SIZE].copy_from_slice(&tag);
        }
        let body_len = file_len - body_at;
        out[AUTH_SIZE..body_at].copy_from_slice(&header_bytes);

        let header_signature = crypto.sign(&header_bytes);
        let content_signature = crypto.sign(&out[body_at..body_at + body_len]);
        let auth =
            FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());
        let mut auth_bytes = BytesMut::with_capacity(AUTH_SIZE);
        auth.write_to_bytes(&mut auth_bytes);
        out[..AUTH_SIZE].copy_from_slice(&auth_bytes);

        let at = l.file_at;
        let whole = slot.freeze().slice(at..at + file_len);
        Ok(SealedFile::contiguous(
            whole,
            header_bytes.len(),
            entries_before,
            num_entries,
            input_len,
        ))
    }
}

#[cfg(all(feature = "c-libs", not(feature = "pure-rust")))]
use static_zstd::Compressor;

#[cfg(feature = "pure-rust")]
use heap_zstd::Compressor;

/// A zstd context in a workspace sized once for the largest input, with the
/// parameters [`crate::compression::zstd_compress`] uses. The window is cut
/// to the input: a longer one finds nothing more in a file that size.
#[cfg(all(feature = "c-libs", not(feature = "pure-rust")))]
mod static_zstd {
    use std::io;
    use std::ptr::NonNull;
    use zstd_sys::*;

    const LEVEL: i32 = 10;
    const MAX_WINDOW_LOG: u32 = 27;

    pub struct Compressor {
        cctx: NonNull<ZSTD_CCtx>,
        _workspace: Box<[u64]>,
    }

    // The context is reached only through `&mut self`, and points into the
    // workspace this owns.
    unsafe impl Send for Compressor {}

    fn params(input_cap: usize) -> [(ZSTD_cParameter, i32); 12] {
        use ZSTD_cParameter::*;
        let window = usize::BITS - input_cap.saturating_sub(1).leading_zeros();
        let window = window.clamp(ZSTD_WINDOWLOG_MIN, MAX_WINDOW_LOG) as i32;
        let ldm_hash = (window - 7).max(ZSTD_LDM_HASHLOG_MIN as i32);
        // Level 10's table row, the tables cut to what the window can use.
        [
            (ZSTD_c_compressionLevel, LEVEL),
            (ZSTD_c_windowLog, window),
            (ZSTD_c_hashLog, 22.min(window + 1)),
            (ZSTD_c_chainLog, 21.min(window)),
            (ZSTD_c_searchLog, 5),
            (ZSTD_c_minMatch, 5),
            (ZSTD_c_targetLength, 16),
            (ZSTD_c_enableLongDistanceMatching, 1),
            (ZSTD_c_ldmHashLog, ldm_hash),
            (ZSTD_c_ldmMinMatch, 64),
            (ZSTD_c_ldmBucketSizeLog, 3),
            (ZSTD_c_ldmHashRateLog, window - ldm_hash),
        ]
    }

    fn check(code: usize) -> io::Result<usize> {
        if unsafe { ZSTD_isError(code) } != 0 {
            let name = unsafe { std::ffi::CStr::from_ptr(ZSTD_getErrorName(code)) };
            return Err(io::Error::other(format!(
                "zstd: {}",
                name.to_string_lossy()
            )));
        }
        Ok(code)
    }

    impl Compressor {
        pub fn new(input_cap: usize) -> io::Result<Self> {
            let params = params(input_cap);
            let need = unsafe {
                let p = ZSTD_createCCtxParams();
                if p.is_null() {
                    return Err(io::Error::other("zstd: cannot create parameters"));
                }
                let set = params
                    .iter()
                    .try_for_each(|&(k, v)| check(ZSTD_CCtxParams_setParameter(p, k, v)).map(drop));
                let need = ZSTD_estimateCCtxSize_usingCCtxParams(p);
                ZSTD_freeCCtxParams(p);
                set?;
                check(need)?
            };
            let mut workspace = vec![0u64; need.div_ceil(8)].into_boxed_slice();
            let cctx =
                unsafe { ZSTD_initStaticCCtx(workspace.as_mut_ptr().cast(), workspace.len() * 8) };
            let cctx =
                NonNull::new(cctx).ok_or_else(|| io::Error::other("zstd: workspace rejected"))?;
            for (k, v) in params {
                check(unsafe { ZSTD_CCtx_setParameter(cctx.as_ptr(), k, v) })?;
            }
            Ok(Compressor {
                cctx,
                _workspace: workspace,
            })
        }

        pub fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> io::Result<usize> {
            check(unsafe {
                ZSTD_compress2(
                    self.cctx.as_ptr(),
                    dst.as_mut_ptr().cast(),
                    dst.len(),
                    src.as_ptr().cast(),
                    src.len(),
                )
            })
        }
    }
}

/// ruzstd has no context to keep, so the pure-Rust build compresses into a
/// fresh buffer and copies it into the slot.
#[cfg(feature = "pure-rust")]
mod heap_zstd {
    use std::io;

    pub struct Compressor;

    impl Compressor {
        pub fn new(_input_cap: usize) -> io::Result<Self> {
            Ok(Compressor)
        }

        pub fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> io::Result<usize> {
            let out = crate::compression::zstd_compress(src)?;
            dst.get_mut(..out.len())
                .ok_or_else(|| io::Error::other("compressed file overflows the slot"))?
                .copy_from_slice(&out);
            Ok(out.len())
        }
    }
}

#[cfg(test)]
#[path = "pack_test.rs"]
mod tests;
