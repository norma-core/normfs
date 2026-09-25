use bytes::{Bytes, BytesMut};
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use std::io;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uintn::UintN;
use uuid::Uuid;

use crate::DiskUsage;
use crate::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use crate::store_header_v1::StoreHeaderV1;

/// A store file's bytes before they go anywhere: `auth ++ header ++ body`.
///
/// One builder whether the WAL bytes came from a `.wal` file or straight from
/// a memory page, so readers, recovery and offload never learn which.
pub struct SealedFile {
    pub auth: Bytes,
    pub header: Bytes,
    pub body: Bytes,
    pub entries_before: UintN,
    pub num_entries: UintN,
}

impl SealedFile {
    pub fn len(&self) -> usize {
        self.auth.len() + self.header.len() + self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_bytes(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(self.len());
        out.extend_from_slice(&self.auth);
        out.extend_from_slice(&self.header);
        out.extend_from_slice(&self.body);
        out.freeze()
    }

    pub fn last_entry_id(&self) -> Option<UintN> {
        if self.num_entries.is_zero() {
            return None;
        }
        let minus_one = self.num_entries.sub(&UintN::one()).ok()?;
        Some(self.entries_before.add(&minus_one))
    }
}

/// Seals `wal_bytes` -- a WAL header followed by framed entries -- as store
/// file `file_id` of `queue`. New files are always V1; readers dispatch on the
/// version word, so files already on disk keep being read as V0.
#[allow(clippy::too_many_arguments)]
pub fn build(
    queue: &QueueId,
    file_id: &UintN,
    compression: CompressionType,
    encryption: EncryptionType,
    entries_before: UintN,
    num_entries: UintN,
    wal_bytes: &Bytes,
    crypto: &CryptoContext,
) -> io::Result<SealedFile> {
    let body = compress_and_encrypt(queue, file_id, compression, encryption, wal_bytes, crypto)?;

    let header = StoreHeader::new(
        compression,
        encryption,
        entries_before.clone(),
        num_entries.clone(),
    );
    let header_v1 = StoreHeaderV1::from_v0(&header)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut header_bytes = BytesMut::new();
    header_v1
        .write_to_bytes(&mut header_bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let header_signature = crypto.sign(&header_bytes);
    let content_signature = crypto.sign(&body);
    let auth = FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());
    let mut auth_bytes = BytesMut::new();
    auth.write_to_bytes(&mut auth_bytes);

    Ok(SealedFile {
        auth: auth_bytes.freeze(),
        header: header_bytes.freeze(),
        body,
        entries_before,
        num_entries,
    })
}

/// Compress, then encrypt: `nonce ++ aes(zstd(data))`.
fn compress_and_encrypt(
    queue: &QueueId,
    file_id: &UintN,
    compression: CompressionType,
    encryption: EncryptionType,
    data: &Bytes,
    crypto: &CryptoContext,
) -> io::Result<Bytes> {
    let mut out = data.clone();

    match compression {
        CompressionType::None => {}
        CompressionType::Zstd => {
            let compressed = crate::compression::zstd_compress(data.as_ref())?;
            log::debug!(target: "normfs-store",
                "Compressed {} -> {} bytes for queue {}, file {}",
                data.len(), compressed.len(), queue, file_id);
            out = Bytes::from(compressed);
        }
        other => {
            return Err(io::Error::other(format!(
                "Unsupported compression type: {other:?}"
            )));
        }
    }

    if encryption != EncryptionType::None {
        let (nonce, ciphertext) = crypto
            .encrypt(queue, file_id, &out)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut sealed = BytesMut::with_capacity(nonce.len() + ciphertext.len());
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(&ciphertext);
        out = sealed.freeze();
    }

    Ok(out)
}

/// The directory is synced after the rename so the name survives a crash too.
pub async fn land_local(
    root: &Path,
    queue: &QueueId,
    file_id: &UintN,
    file: &SealedFile,
    fsync: bool,
    usage: &DiskUsage,
) -> io::Result<()> {
    let store_path = queue.to_store_path(root, file_id);
    let parent = store_path
        .parent()
        .ok_or_else(|| io::Error::other("store path has no parent"))?;
    fs::create_dir_all(parent).await?;

    let tmp_dir = root.join("tmp");
    fs::create_dir_all(&tmp_dir).await?;
    let temp_path = tmp_dir.join(format!("{}.tmp", Uuid::new_v4()));

    let mut out = fs::File::create(&temp_path).await?;
    out.write_all(&file.auth).await?;
    out.write_all(&file.header).await?;
    out.write_all(&file.body).await?;
    if fsync {
        out.sync_all().await?;
    }
    drop(out);

    usage
        .publish(queue, &temp_path, &store_path, file.len() as u64)
        .await?;
    if fsync {
        fs::File::open(parent).await?.sync_all().await?;
    }

    log::debug!(target: "normfs-store",
        "Landed store file for queue {}, file {}: {} bytes at {:?}",
        queue, file_id, file.len(), store_path);
    Ok(())
}
