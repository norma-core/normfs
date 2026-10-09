use bytes::{Bytes, BytesMut};
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_types::events::{FileFacts, SystemEvent, SystemEvents};
use std::io;
use uintn::UintN;

use crate::StoreError;
use crate::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use crate::store_header_v1::{AnyStoreHeader, StoreHeaderV1};

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
    /// Length of the WAL bytes the body was built from.
    pub raw_len: usize,
    /// `auth ++ header ++ body` in one buffer, when they were built that way.
    whole: Option<Bytes>,
}

impl SealedFile {
    /// A file whose three parts lie back to back in `whole`.
    pub(crate) fn contiguous(
        whole: Bytes,
        header_len: usize,
        entries_before: UintN,
        num_entries: UintN,
        raw_len: usize,
    ) -> Self {
        let auth_len = FileAuthentication::SIZE;
        SealedFile {
            auth: whole.slice(..auth_len),
            header: whole.slice(auth_len..auth_len + header_len),
            body: whole.slice(auth_len + header_len..),
            entries_before,
            num_entries,
            raw_len,
            whole: Some(whole),
        }
    }

    pub fn len(&self) -> usize {
        self.auth.len() + self.header.len() + self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The file as runs to write or send; one when it was built in one buffer.
    pub fn runs(&self) -> Vec<Bytes> {
        match &self.whole {
            Some(whole) => vec![whole.clone()],
            None => vec![self.auth.clone(), self.header.clone(), self.body.clone()],
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        if let Some(whole) = &self.whole {
            return whole.clone();
        }
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

    pub fn facts(&self, queue: &QueueId, file_id: &UintN) -> Result<FileFacts, StoreError> {
        let (auth, _) = FileAuthentication::from_bytes(&self.auth)?;
        let (header, _) = AnyStoreHeader::from_bytes(&self.header)?;
        Ok(FileFacts {
            raw_bytes: Some(self.raw_len as u64),
            ..facts_of(queue, file_id, &auth, &header, self.len() as u64)
        })
    }
}

/// Records a file that reached the local store.
pub fn report_stored(
    events: &dyn SystemEvents,
    queue: &QueueId,
    file_id: &UintN,
    file: &SealedFile,
) {
    match file.facts(queue, file_id) {
        Ok(facts) => events.emit(SystemEvent::FileStored(facts)),
        Err(e) => log::warn!(target: "normfs-store",
            "queue {}: store file {file_id} landed but its blocks do not parse: {e}", queue.short()),
    }
}

/// Enough of a store file's start for [`facts_of_head`]: the auth block and
/// a header of either version.
pub const HEAD_LEN: usize = 512;

/// [`SealedFile::facts`] for a whole store file read back from disk.
pub fn facts(queue: &QueueId, file_id: &UintN, file: &[u8]) -> Result<FileFacts, StoreError> {
    facts_of_head(queue, file_id, file, file.len() as u64)
}

/// [`facts`] from the first [`HEAD_LEN`] bytes of a file of `file_len`.
pub fn facts_of_head(
    queue: &QueueId,
    file_id: &UintN,
    head: &[u8],
    file_len: u64,
) -> Result<FileFacts, StoreError> {
    let (auth, auth_size) = FileAuthentication::from_bytes(head)?;
    let (header, _) = AnyStoreHeader::from_bytes(&head[auth_size..])?;
    Ok(facts_of(queue, file_id, &auth, &header, file_len))
}

fn facts_of(
    queue: &QueueId,
    file_id: &UintN,
    auth: &FileAuthentication,
    header: &AnyStoreHeader,
    file_bytes: u64,
) -> FileFacts {
    FileFacts {
        queue: queue.clone(),
        file_id: file_id.clone(),
        first_id: header.num_entries_before(),
        num_entries: header.num_entries(),
        file_bytes,
        raw_bytes: None,
        compression: header.compression(),
        encryption: header.encryption(),
        content_signature: auth.content_signature,
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
        raw_len: wal_bytes.len(),
        whole: None,
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
                data.len(), compressed.len(), queue.short(), file_id);
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
