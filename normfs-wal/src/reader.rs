use bytes::{Buf, Bytes, BytesMut};
use normfs_types::{DataSource, ReadEntry};
use std::io::SeekFrom;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc;
use uintn::{UintN, varint};
use xxhash_rust::xxh64;

use super::errors::WalError;
use super::wal_entry::WalEntryHeader;
use super::wal_entry_v1::{WAL_ENTRY_V1_CRC_SIZE, WAL_ENTRY_V1_MIN_SIZE, WalEntryV1};
use super::wal_header::{WalHeader, WalHeaderError};
use super::wal_header_v1::{
    AnyWalHeader, AnyWalHeaderError, WAL_HEADER_V1_VERSION, WalHeaderV1Error,
};

// Serving path: paid per concurrent reader, so kept small.
const WAL_READ_BUFFER_CAPACITY: usize = 64 * 1024;

// Recovery scans one file at a time and can afford a big block. Every refill is
// a blocking-pool round trip, so at 12 KiB entries 64 KiB -> 1 MiB is worth ~1.9x.
const WAL_SCAN_BLOCK_CAPACITY: usize = 1024 * 1024;

/// Covers the widest header. A shorter file yields fewer bytes and the header
/// decoder reports the truncation.
const WAL_HEADER_PROBE: usize = 128;

/// A contiguous window of unread bytes, so entries decode in place: no
/// per-entry allocation, zero-fill or copy.
///
/// `block` is the read granularity. The window grows past it for one oversized
/// entry — record sizes are `u32`, so no fixed window suffices.
struct BlockReader<R> {
    inner: R,
    buf: BytesMut,
    eof: bool,
    block: usize,
    /// Most bytes the source can hold. A record size is a `u32` off disk whose
    /// CRC is only checked once the bytes are in hand, so without this one
    /// corrupt prefix reserves up to 4 GiB before the frame is rejected.
    limit: usize,
}

impl<R: AsyncRead + Unpin> BlockReader<R> {
    fn new(inner: R, block: usize, limit: usize) -> Self {
        Self {
            inner,
            buf: BytesMut::with_capacity(block),
            eof: false,
            block,
            limit,
        }
    }

    /// Bytes read but not consumed; `BytesMut` derefs to exactly that range, so
    /// `consume` is a pointer bump.
    fn unread(&self) -> &[u8] {
        &self.buf
    }

    fn consume(&mut self, n: usize) {
        self.buf.advance(n);
    }

    fn at_eof(&self) -> bool {
        self.eof
    }

    /// Buffer at least `want` contiguous unread bytes, returning how many are
    /// available — short of `want` only at end of file.
    ///
    /// Pass the bytes actually needed, not a block: any read tops the window up
    /// to a full block, so this costs one read per block, not per entry.
    async fn fill(&mut self, want: usize) -> std::io::Result<usize> {
        while self.buf.len() < want && !self.eof {
            // `reserve` keeps the window contiguous; only the straddling tail
            // is copied, once per block.
            let target = want.max(self.block);
            if self.buf.capacity() < target {
                self.buf.reserve(target - self.buf.len());
            }
            if self.inner.read_buf(&mut self.buf).await? == 0 {
                self.eof = true;
            }
        }
        Ok(self.buf.len())
    }

    fn into_parts(self) -> (BytesMut, R) {
        (self.buf, self.inner)
    }
}

/// Where `next_v1_frame` left the window.
enum V1Frame {
    /// A whole frame of this many bytes sits at the front of the window.
    Ready(usize),
    /// The file ended cleanly on an entry boundary.
    End,
    /// Trailing bytes that cannot form a whole entry, or a malformed prefix.
    Truncated,
}

/// Make the next whole V1 entry — `[record_size varint32][record][crc32c]` —
/// contiguous at the front of the window, growing it if one entry needs more
/// than a block.
///
/// `End` and `Truncated` must stay distinct: a clean end of file finishes the
/// scan, a partial trailing entry discards itself and everything after it,
/// whose ids would be unknowable.
async fn next_v1_frame<R: AsyncRead + Unpin>(
    block: &mut BlockReader<R>,
) -> std::io::Result<V1Frame> {
    // Ask for the smallest possible entry so this returns without reading while
    // the window still holds bytes.
    block.fill(WAL_ENTRY_V1_MIN_SIZE).await?;
    if block.unread().is_empty() {
        return Ok(V1Frame::End);
    }

    let (record_size, prefix) = match varint::decode_u32(block.unread()) {
        Ok(result) => (result.value as usize, result.consumed),
        Err(_) => return Ok(V1Frame::Truncated),
    };

    let total = prefix + record_size + WAL_ENTRY_V1_CRC_SIZE;
    // A frame longer than the whole file, so the prefix is corrupt.
    if total > block.limit {
        return Ok(V1Frame::Truncated);
    }
    if block.unread().len() < total {
        block.fill(total).await?;
        if block.unread().len() < total {
            debug_assert!(block.at_eof());
            return Ok(V1Frame::Truncated);
        }
    }
    Ok(V1Frame::Ready(total))
}

pub async fn read_wal_header(
    fs: &normfs_fs::Fs,
    base_path: &Path,
    file_id: &UintN,
) -> Result<WalHeader, WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: reading header from file {}", file_id);

    let file = match fs.open_read(&file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_path.display());
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };

    if file.metadata().await?.len() == 0 {
        log::warn!("WAL reader: file {} is empty", file_path.display());
        return Err(WalError::WalEmpty(file_id.clone()));
    }

    let mut reader = BufReader::new(file.take(128));

    let (any_header, _) = match AnyWalHeader::from_reader(&mut reader).await {
        Ok(v) => v,
        Err(AnyWalHeaderError::V0(WalHeaderError::SliceTooShort))
        | Err(AnyWalHeaderError::V1(WalHeaderV1Error::Truncated)) => {
            log::warn!(
                "WAL reader: file {} has incomplete header",
                file_path.display()
            );
            return Err(WalError::WalEmpty(file_id.clone()));
        }
        Err(e) => return Err(e.into()),
    };
    let wal_header = WalHeader::from(&any_header);

    log::debug!(
        "WAL reader: successfully read header from file {}, entries_before: {}",
        file_id,
        wal_header.num_entries_before
    );

    Ok(wal_header)
}

pub async fn get_wal_range(
    fs: &normfs_fs::Fs,
    base_path: &Path,
    file_id: &UintN,
) -> Result<(WalHeader, Option<(UintN, UintN)>), WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: getting entry range from file {}", file_id);

    let file = match fs.open_read(&file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_id);
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };

    let file_len = file.metadata().await?.len();
    if file_len == 0 {
        log::warn!("WAL reader: file {} is empty", file_id);
        return Err(WalError::WalEmpty(file_id.clone()));
    }

    let mut block = BlockReader::new(file, WAL_SCAN_BLOCK_CAPACITY, file_len as usize);
    block.fill(WAL_HEADER_PROBE).await?;

    let (any_header, header_size) = match AnyWalHeader::from_bytes(block.unread()) {
        Ok(v) => v,
        Err(AnyWalHeaderError::V0(WalHeaderError::SliceTooShort))
        | Err(AnyWalHeaderError::V1(WalHeaderV1Error::Truncated)) => {
            return Err(WalError::WalEmpty(file_id.clone()));
        }
        Err(e) => return Err(e.into()),
    };
    block.consume(header_size);
    let wal_header = WalHeader::from(&any_header);

    // V1: entries store no id; iterate frames and derive id from the header's
    // num_entries_before plus a running index. A truncated or corrupt entry
    // stops the scan — the ids of everything after it would be unknowable.
    if any_header.version() == WAL_HEADER_V1_VERSION {
        let num_entries_before = wal_header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;
        let mut first_id: Option<UintN> = None;
        let mut last_id: Option<UintN> = None;
        let mut entry_count = 0u64;
        let mut index = 0u64;

        loop {
            let total = match next_v1_frame(&mut block).await? {
                V1Frame::Ready(total) => total,
                V1Frame::End | V1Frame::Truncated => break,
            };

            // Only `Copy` data leaves the borrow, so the window can advance
            // immediately after.
            let decoded =
                match WalEntryV1::iter_next(&block.unread()[..total], num_entries_before, index) {
                    Ok((_, entry_id, consumed)) => Some((entry_id, consumed)),
                    Err(_) => None,
                };

            match decoded {
                Some((entry_id, consumed)) => {
                    let id = UintN::from(entry_id);
                    if first_id.is_none() {
                        first_id = Some(id.clone());
                    }
                    last_id = Some(id);
                    entry_count += 1;
                    index += 1;
                    block.consume(consumed);
                }
                None => break,
            }
        }

        let range = first_id.and_then(|first| last_id.map(|last| (first, last)));
        log::debug!(
            "WAL reader: file {} contains {} valid V1 entries, range: {:?}",
            file_id,
            entry_count,
            range
        );
        return Ok((wal_header, range));
    }

    // V0 keeps its byte-oriented decoder. The window read past the header, so
    // seek back to the first entry — one seek against a scan measured in seconds.
    let (_, file) = block.into_parts();
    let mut file = file;
    file.seek(SeekFrom::Start(header_size as u64)).await?;
    let mut reader = BufReader::with_capacity(WAL_READ_BUFFER_CAPACITY, file);

    let mut first_id: Option<UintN> = None;
    let mut last_id: Option<UintN> = None;
    let mut entry_count = 0u64;

    while let Ok(entry_header) = WalEntryHeader::from_reader(&mut reader, &wal_header).await {
        if first_id.is_none() {
            first_id = Some(entry_header.entry_id.clone());
            log::trace!(
                "WAL reader: first entry ID in file {}: {}",
                file_id,
                entry_header.entry_id
            );
        }

        let record_size = match entry_header.record_size.to_u64() {
            Ok(s) => s,
            Err(_) => {
                log::warn!(
                    "WAL reader: invalid record size for entry {}",
                    entry_header.entry_id
                );
                break;
            }
        };

        let mut record_buffer = vec![0; record_size as usize];
        if record_size > 0 && reader.read_exact(&mut record_buffer).await.is_err() {
            log::warn!(
                "WAL reader: cropped entry {} in file {}",
                entry_header.entry_id,
                file_id
            );
            break;
        }

        let calculated_hash = xxh64::xxh64(&record_buffer, 0);
        if calculated_hash != entry_header.xxhash {
            log::warn!(
                "WAL reader: corrupted entry {} in file {} (hash mismatch)",
                entry_header.entry_id,
                file_id
            );
            break;
        }

        last_id = Some(entry_header.entry_id.clone());
        entry_count += 1;
    }

    let range = first_id.and_then(|first| last_id.map(|last| (first, last)));

    log::debug!(
        "WAL reader: file {} contains {} valid entries, range: {:?}",
        file_id,
        entry_count,
        range
    );

    Ok((wal_header, range))
}

pub fn get_wal_header(content: &Bytes) -> Result<WalHeader, AnyWalHeaderError> {
    let (header, _) = AnyWalHeader::from_bytes(content)?;
    Ok(WalHeader::from(&header))
}

pub struct WalContent {
    pub entries_before: UintN,
    pub num_entries: UintN,
    pub content: Bytes,
}

pub async fn get_wal_content(
    fs: &normfs_fs::Fs,
    base_path: &Path,
    file_id: &UintN,
) -> Result<WalContent, WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: getting content from file {}", file_id);

    let content = match fs.read_whole(&file_path).await {
        Ok(c) => c,
        Err(e) => {
            let e = std::io::Error::from(e);
            if e.kind() == std::io::ErrorKind::NotFound {
                log::warn!("WAL reader: file {} not found", file_id);
                return Err(WalError::WalNotFound);
            }
            return Err(e.into());
        }
    };

    if content.is_empty() {
        log::warn!("WAL reader: file {} has no content", file_id);
        return Err(WalError::WalEntryError(
            super::wal_entry::WalEntryError::SliceTooShort,
        ));
    }

    let (any_header, header_size) = AnyWalHeader::from_bytes(&content)?;
    let wal_header = WalHeader::from(&any_header);

    // V1: iterate frames from the byte buffer, deriving each id from
    // num_entries_before + running index; stop at the first corrupt entry.
    if any_header.version() == WAL_HEADER_V1_VERSION {
        let num_entries_before = wal_header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;
        let mut num_entries: u64 = 0;
        let mut cursor = header_size;
        let mut index = 0u64;
        let mut first_entry_id: Option<UintN> = None;
        let mut last_entry_id: Option<UintN> = None;

        while cursor < content.len() {
            match WalEntryV1::iter_next(&content[cursor..], num_entries_before, index) {
                Ok((_, entry_id, consumed)) => {
                    let id = UintN::from(entry_id);
                    if first_entry_id.is_none() {
                        first_entry_id = Some(id.clone());
                    }
                    last_entry_id = Some(id);
                    cursor += consumed;
                    num_entries += 1;
                    index += 1;
                }
                Err(_) => break,
            }
        }

        log::debug!(
            "WAL reader: file {} contains {} valid V1 entries, range: {:?} - {:?}, size: {} bytes",
            file_id,
            num_entries,
            first_entry_id,
            last_entry_id,
            content.len()
        );

        return Ok(WalContent {
            entries_before: wal_header.num_entries_before,
            num_entries: UintN::from(num_entries),
            content,
        });
    }

    let mut num_entries: u64 = 0;
    let mut cursor = header_size;
    let mut first_entry_id: Option<UintN> = None;
    let mut last_entry_id: Option<UintN> = None;

    loop {
        let remaining_bytes = &content[cursor..];
        if remaining_bytes.is_empty() {
            break;
        }

        match WalEntryHeader::from_bytes(remaining_bytes, &wal_header) {
            Ok(entry_header) => {
                if first_entry_id.is_none() {
                    first_entry_id = Some(entry_header.entry_id.clone());
                }
                last_entry_id = Some(entry_header.entry_id.clone());

                let entry_header_size = entry_header.size(&wal_header);
                cursor += entry_header_size;

                let record_size = match entry_header.record_size.to_u64() {
                    Ok(s) => s as usize,
                    Err(_) => {
                        log::warn!(
                            "WAL reader: invalid record size for entry {}",
                            entry_header.entry_id
                        );
                        break;
                    }
                };

                if content.len() < cursor + record_size {
                    log::warn!(
                        "WAL reader: cropped entry {} in file {}",
                        entry_header.entry_id,
                        file_id
                    );
                    break;
                }

                let record_buffer = &content[cursor..cursor + record_size];

                let calculated_hash = xxh64::xxh64(record_buffer, 0);
                if calculated_hash != entry_header.xxhash {
                    log::warn!(
                        "WAL reader: corrupted entry {} in file {} (hash mismatch)",
                        entry_header.entry_id,
                        file_id
                    );
                    break;
                }

                cursor += record_size;
                num_entries += 1;
            }
            Err(_) => {
                break;
            }
        }
    }

    log::debug!(
        "WAL reader: file {} contains {} valid entries, range: {:?} - {:?}, size: {} bytes",
        file_id,
        num_entries,
        first_entry_id,
        last_entry_id,
        content.len()
    );

    Ok(WalContent {
        entries_before: wal_header.num_entries_before,
        num_entries: UintN::from(num_entries),
        content,
    })
}

#[derive(Debug)]
pub enum ReadRangeResult {
    Complete, // All requested entries were read
    PartialRead {
        last_read_id: Option<UintN>,
        last_id_in_file: UintN,
    }, // Last entry ID that was read
    ChannelClosed, // Output channel was closed
}

pub async fn read_wal_file_range(
    fs: &normfs_fs::Fs,
    base_path: &Path,
    file_id: &UintN,
    from_id: &UintN,
    until_id: &Option<UintN>,
    step: usize,
    target: &mpsc::Sender<ReadEntry>,
    data_source: DataSource,
) -> Result<ReadRangeResult, WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!(
        "WAL reader: reading range [{} - {:?}] from file {}",
        from_id,
        until_id,
        file_id
    );

    let file = match fs.open_read(&file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_id);
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };

    let file_len = file.metadata().await?.len();
    if file_len == 0 {
        log::debug!("WAL reader: file {} is empty", file_id);
        return Ok(ReadRangeResult::PartialRead {
            last_read_id: None,
            last_id_in_file: from_id.clone(),
        });
    }

    let mut block = BlockReader::new(file, WAL_READ_BUFFER_CAPACITY, file_len as usize);
    block.fill(WAL_HEADER_PROBE).await?;
    // A header too short to decode is a file nothing has finished writing yet,
    // which is what the other three readers report it as.
    let (any_header, header_size) = match AnyWalHeader::from_bytes(block.unread()) {
        Ok(v) => v,
        Err(AnyWalHeaderError::V0(WalHeaderError::SliceTooShort))
        | Err(AnyWalHeaderError::V1(WalHeaderV1Error::Truncated)) => {
            return Ok(ReadRangeResult::PartialRead {
                last_read_id: None,
                last_id_in_file: from_id.clone(),
            });
        }
        Err(e) => return Err(e.into()),
    };
    block.consume(header_size);
    let wal_header = WalHeader::from(&any_header);

    // V1: frame-by-frame read from the file, deriving each id from
    // num_entries_before + running index. The same range/step filtering as V0,
    // but the id comes from the position rather than the entry bytes; the CRC
    // check lives inside iter_next, so a corrupt entry stops the scan.
    if any_header.version() == WAL_HEADER_V1_VERSION {
        let num_entries_before = wal_header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;
        let mut last_read_id: Option<UintN> = None;
        let mut last_processed_id: Option<UintN> = None;
        let mut found_complete = false;
        let mut entries_sent = 0u64;
        let mut entries_skipped = 0u64;
        let step = step.max(1);
        let mut index = 0u64;

        loop {
            if target.is_closed() {
                log::debug!("WAL reader: channel closed during V1 range read");
                return Ok(ReadRangeResult::ChannelClosed);
            }

            let total = match next_v1_frame(&mut block).await? {
                V1Frame::Ready(total) => total,
                V1Frame::End | V1Frame::Truncated => break,
            };

            // Only `Copy` data leaves the borrow; the record is located by
            // offset so it can be lifted out once delivery is decided.
            let decoded =
                match WalEntryV1::iter_next(&block.unread()[..total], num_entries_before, index) {
                    Ok((entry, id, consumed)) => {
                        let record_size = entry.record.len();
                        let record_offset = consumed - record_size - WAL_ENTRY_V1_CRC_SIZE;
                        Some((id, record_offset, record_size, consumed))
                    }
                    Err(_) => None,
                };
            let (id, record_offset, record_size, consumed) = match decoded {
                Some(v) => v,
                None => break,
            };

            let entry_id = UintN::from(id);
            index += 1;
            last_processed_id = Some(entry_id.clone());

            if let Some(until) = until_id
                && &entry_id > until
            {
                found_complete = true;
                break;
            }

            let in_range = &entry_id >= from_id
                && (until_id.is_none()
                    || until_id
                        .as_ref()
                        .map(|until| &entry_id <= until)
                        .unwrap_or(true));

            // Copied only for entries actually delivered, and before the
            // window advances past them.
            let record_bytes = if in_range && entry_id.in_step(from_id, step) {
                Some(Bytes::copy_from_slice(
                    &block.unread()[record_offset..record_offset + record_size],
                ))
            } else {
                None
            };
            block.consume(consumed);

            if in_range {
                if let Some(record_bytes) = record_bytes {
                    if target
                        .send(ReadEntry::new(entry_id.clone(), record_bytes, data_source))
                        .await
                        .is_err()
                    {
                        return Ok(ReadRangeResult::ChannelClosed);
                    }

                    if let Some(until) = until_id
                        && &entry_id >= until
                    {
                        found_complete = true;
                        last_read_id = Some(entry_id);
                        entries_sent += 1;
                        break;
                    }

                    last_read_id = Some(entry_id);
                    entries_sent += 1;
                }
            } else {
                entries_skipped += 1;
            }
        }

        let result = if found_complete
            || (last_processed_id.is_some()
                && until_id
                    .as_ref()
                    .map(|until| last_processed_id.as_ref().unwrap() >= until)
                    .unwrap_or(false))
        {
            log::debug!(
                "WAL reader: completed V1 range read, sent {} entries, skipped {}",
                entries_sent,
                entries_skipped
            );
            ReadRangeResult::Complete
        } else {
            let last_id_in_file = last_processed_id.unwrap_or_else(|| from_id.clone());
            log::debug!(
                "WAL reader: partial V1 range read, sent {} entries, last read: {:?}, last in file: {}, skipped {}",
                entries_sent,
                last_read_id,
                last_id_in_file,
                entries_skipped
            );
            ReadRangeResult::PartialRead {
                last_read_id,
                last_id_in_file,
            }
        };

        return Ok(result);
    }

    // V0 keeps its byte-oriented decoder. The window read past the header, so
    // seek back to the first entry rather than handing those bytes over.
    let (_, file) = block.into_parts();
    let mut file = file;
    file.seek(SeekFrom::Start(header_size as u64)).await?;
    let mut reader = BufReader::with_capacity(WAL_READ_BUFFER_CAPACITY, file);

    let mut last_read_id: Option<UintN> = None;
    let mut last_processed_id: Option<UintN> = None;
    let mut found_complete = false;
    let mut entries_sent = 0u64;
    let mut entries_skipped = 0u64;
    let step = step.max(1);

    loop {
        if target.is_closed() {
            log::debug!("WAL reader: channel closed during range read");
            return Ok(ReadRangeResult::ChannelClosed);
        }

        match WalEntryHeader::from_reader(&mut reader, &wal_header).await {
            Ok(entry_header) => {
                last_processed_id = Some(entry_header.entry_id.clone());
                // Check if we've passed the range (if until_id is set)
                if let Some(until) = until_id
                    && &entry_header.entry_id > until
                {
                    log::trace!(
                        "WAL reader: entry {} is past range end {}, stopping",
                        entry_header.entry_id,
                        until
                    );
                    found_complete = true;
                    break;
                }

                let record_size = match entry_header.record_size.to_u64() {
                    Ok(s) => s as usize,
                    Err(_) => {
                        log::warn!(
                            "WAL reader: invalid record size for entry {}",
                            entry_header.entry_id
                        );
                        break;
                    }
                };

                let mut record_buffer = vec![0; record_size];
                if record_size > 0 && reader.read_exact(&mut record_buffer).await.is_err() {
                    log::warn!(
                        "WAL reader: cropped entry {} in file {}",
                        entry_header.entry_id,
                        file_id
                    );
                    break;
                }

                let calculated_hash = xxh64::xxh64(&record_buffer, 0);
                if calculated_hash != entry_header.xxhash {
                    log::warn!(
                        "WAL reader: corrupted entry {} in file {} (hash mismatch)",
                        entry_header.entry_id,
                        file_id
                    );
                    break;
                }

                // Only send if within range
                let in_range = &entry_header.entry_id >= from_id
                    && (until_id.is_none()
                        || until_id
                            .as_ref()
                            .map(|until| &entry_header.entry_id <= until)
                            .unwrap_or(true));

                if in_range {
                    if entry_header.entry_id.in_step(from_id, step) {
                        let entry_id = entry_header.entry_id.clone();
                        let record_bytes = Bytes::from(record_buffer);

                        log::trace!("WAL reader: sending entry {} from range read", entry_id);

                        if target
                            .send(ReadEntry::new(entry_id.clone(), record_bytes, data_source))
                            .await
                            .is_err()
                        {
                            log::debug!(
                                "WAL reader: channel closed while sending entry {}",
                                entry_id
                            );
                            return Ok(ReadRangeResult::ChannelClosed);
                        }

                        // Check if we've reached the end of the requested range (if until_id is set)
                        if let Some(until) = until_id
                            && &entry_id >= until
                        {
                            log::trace!("WAL reader: reached end of range at entry {}", entry_id);
                            found_complete = true;
                            last_read_id = Some(entry_id);
                            entries_sent += 1;
                            break;
                        }

                        last_read_id = Some(entry_id);
                        entries_sent += 1;
                    }
                } else {
                    entries_skipped += 1;
                }
            }
            Err(_) => {
                log::trace!("WAL reader: reached end of file or corrupted header");
                break;
            }
        }
    }

    let result = if found_complete
        || (last_processed_id.is_some()
            && until_id
                .as_ref()
                .map(|until| last_processed_id.as_ref().unwrap() >= until)
                .unwrap_or(false))
    {
        log::debug!(
            "WAL reader: completed range read, sent {} entries, skipped {}",
            entries_sent,
            entries_skipped
        );
        ReadRangeResult::Complete
    } else {
        let last_id_in_file = last_processed_id.unwrap_or_else(|| from_id.clone());
        log::debug!(
            "WAL reader: partial range read, sent {} entries, last read: {:?}, last in file: {}, skipped {}",
            entries_sent,
            last_read_id,
            last_id_in_file,
            entries_skipped
        );
        ReadRangeResult::PartialRead {
            last_read_id,
            last_id_in_file,
        }
    };

    Ok(result)
}

pub async fn read_wal_bytes_range(
    content: &Bytes,
    from_id: &UintN,
    until_id: &Option<UintN>,
    step: usize,
    target: &mpsc::Sender<ReadEntry>,
    data_source: DataSource,
) -> Result<ReadRangeResult, WalError> {
    log::debug!(
        "WAL reader: reading bytes range [{} - {:?}, step {}] from content of size {} bytes",
        from_id,
        until_id,
        step,
        content.len()
    );
    if content.is_empty() {
        log::debug!(
            "WAL reader: content is empty for bytes range read, from {} to {:?} step {}",
            from_id,
            until_id,
            step
        );
        return Ok(ReadRangeResult::PartialRead {
            last_read_id: None,
            last_id_in_file: from_id.clone(),
        });
    }

    let (any_header, header_size) = match AnyWalHeader::from_bytes(content) {
        Ok(v) => v,
        Err(AnyWalHeaderError::V0(WalHeaderError::SliceTooShort))
        | Err(AnyWalHeaderError::V1(WalHeaderV1Error::Truncated)) => {
            log::debug!(
                "WAL reader: content has incomplete header for bytes range read, from {} to {:?} step {}",
                from_id,
                until_id,
                step
            );
            return Ok(ReadRangeResult::PartialRead {
                last_read_id: None,
                last_id_in_file: from_id.clone(),
            });
        }
        Err(e) => return Err(e.into()),
    };
    let wal_header = WalHeader::from(&any_header);

    // V1: iterate frames from the in-memory buffer with zero-copy record
    // slices, deriving each id from num_entries_before + running index.
    if any_header.version() == WAL_HEADER_V1_VERSION {
        let num_entries_before = wal_header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;
        let mut cursor = header_size;
        let mut last_read_id: Option<UintN> = None;
        let mut last_processed_id: Option<UintN> = None;
        let mut found_complete = false;
        let step = step.max(1);
        let mut entries_sent = 0u64;
        let mut entries_skipped = 0u64;
        let mut index = 0u64;

        loop {
            if target.is_closed() {
                return Ok(ReadRangeResult::ChannelClosed);
            }
            if cursor >= content.len() {
                break;
            }

            let (entry_id, record_size, consumed) =
                match WalEntryV1::iter_next(&content[cursor..], num_entries_before, index) {
                    Ok((entry, id, consumed)) => (UintN::from(id), entry.record.len(), consumed),
                    Err(_) => break,
                };
            index += 1;
            last_processed_id = Some(entry_id.clone());

            if let Some(until) = until_id
                && &entry_id > until
            {
                found_complete = true;
                break;
            }

            // record_offset is the varint length prefix; slice zero-copy.
            let record_offset = consumed - record_size - WAL_ENTRY_V1_CRC_SIZE;
            let record_bytes =
                content.slice(cursor + record_offset..cursor + record_offset + record_size);
            cursor += consumed;

            let in_range = &entry_id >= from_id
                && (until_id.is_none()
                    || until_id
                        .as_ref()
                        .map(|until| &entry_id <= until)
                        .unwrap_or(true));

            if in_range {
                if entry_id.in_step(from_id, step) {
                    if target
                        .send(ReadEntry::new(entry_id.clone(), record_bytes, data_source))
                        .await
                        .is_err()
                    {
                        return Ok(ReadRangeResult::ChannelClosed);
                    }

                    if let Some(until) = until_id
                        && &entry_id >= until
                    {
                        found_complete = true;
                        last_read_id = Some(entry_id);
                        entries_sent += 1;
                        break;
                    }

                    last_read_id = Some(entry_id);
                    entries_sent += 1;
                }
            } else {
                entries_skipped += 1;
            }
        }

        let result = if found_complete
            || (last_processed_id.is_some()
                && until_id
                    .as_ref()
                    .map(|until| last_processed_id.as_ref().unwrap() >= until)
                    .unwrap_or(false))
        {
            log::info!(
                "WAL reader: completed V1 bytes range read, sent {} entries, skipped {}, from {} to {:?} step {}",
                entries_sent,
                entries_skipped,
                from_id,
                until_id,
                step
            );
            ReadRangeResult::Complete
        } else {
            let last_id_in_file = last_processed_id.unwrap_or_else(|| from_id.clone());
            log::info!(
                "WAL reader: partial V1 bytes range read, sent {} entries, last read: {:?}, last in file: {}, skipped {}, from {} to {:?} step {}",
                entries_sent,
                last_read_id,
                last_id_in_file,
                entries_skipped,
                from_id,
                until_id,
                step
            );
            ReadRangeResult::PartialRead {
                last_read_id,
                last_id_in_file,
            }
        };

        return Ok(result);
    }

    let mut cursor = header_size;
    let mut last_read_id: Option<UintN> = None;
    let mut last_processed_id: Option<UintN> = None;
    let mut found_complete = false;
    let step = step.max(1);
    let mut entries_sent = 0u64;
    let mut entries_skipped = 0u64;

    loop {
        if target.is_closed() {
            log::debug!(
                "WAL reader: channel closed while reading from {} to {:?} step {}",
                from_id,
                until_id,
                step
            );
            return Ok(ReadRangeResult::ChannelClosed);
        }

        let remaining_bytes = &content[cursor..];
        if remaining_bytes.is_empty() {
            log::debug!(
                "WAL reader: reached end of content while reading from {} to {:?} step {}",
                from_id,
                until_id,
                step
            );
            break;
        }

        match WalEntryHeader::from_bytes(remaining_bytes, &wal_header) {
            Ok(entry_header) => {
                last_processed_id = Some(entry_header.entry_id.clone());
                // Check if we've passed the range (if until_id is set)
                if let Some(until) = until_id
                    && &entry_header.entry_id > until
                {
                    found_complete = true;
                    break;
                }

                let entry_header_size = entry_header.size(&wal_header);
                cursor += entry_header_size;

                let record_size = match entry_header.record_size.to_u64() {
                    Ok(s) => s as usize,
                    Err(_) => {
                        log::warn!(
                            "WAL reader: invalid record size for entry {} in bytes range",
                            entry_header.entry_id
                        );
                        break;
                    }
                };

                if content.len() < cursor + record_size {
                    log::warn!(
                        "WAL reader: cropped entry {} in bytes range",
                        entry_header.entry_id
                    );
                    break;
                }

                let record_buffer = content.slice(cursor..cursor + record_size);

                let calculated_hash = xxh64::xxh64(&record_buffer, 0);
                if calculated_hash != entry_header.xxhash {
                    log::warn!(
                        "WAL reader: corrupted entry {} in bytes range (hash mismatch)",
                        entry_header.entry_id
                    );
                    break;
                }

                // Only send if within range
                let in_range = &entry_header.entry_id >= from_id
                    && (until_id.is_none()
                        || until_id
                            .as_ref()
                            .map(|until| &entry_header.entry_id <= until)
                            .unwrap_or(true));

                if in_range {
                    if entry_header.entry_id.in_step(from_id, step) {
                        let entry_id = entry_header.entry_id.clone();
                        let record_bytes = record_buffer;

                        log::trace!(
                            "WAL reader: sending entry {} from bytes range read, from {} to {:?} step {}",
                            entry_id,
                            from_id,
                            until_id,
                            step
                        );

                        if target
                            .send(ReadEntry::new(entry_id.clone(), record_bytes, data_source))
                            .await
                            .is_err()
                        {
                            log::info!(
                                "WAL reader: channel closed while sending entry from bytes range {}, from {} to {:?} step {}",
                                entry_id,
                                from_id,
                                until_id,
                                step
                            );
                            return Ok(ReadRangeResult::ChannelClosed);
                        }

                        // Check if we've reached the end of the requested range (if until_id is set)
                        if let Some(until) = until_id
                            && &entry_id >= until
                        {
                            log::trace!(
                                "WAL reader: reached end of bytes range at entry {}, from {} to {} step {}",
                                entry_id,
                                from_id,
                                until,
                                step
                            );
                            found_complete = true;
                            last_read_id = Some(entry_id);
                            entries_sent += 1;
                            break;
                        }

                        last_read_id = Some(entry_id);
                        entries_sent += 1;
                    }
                } else {
                    entries_skipped += 1;
                    log::trace!(
                        "WAL reader: skipping entry {} outside bytes range, read from {} to {:?} step {}",
                        entry_header.entry_id,
                        from_id,
                        until_id,
                        step
                    );
                }

                cursor += record_size;
            }
            Err(_) => {
                log::trace!("WAL reader: reached end of bytes or corrupted header");
                break;
            }
        }
    }

    let result = if found_complete
        || (last_processed_id.is_some()
            && until_id
                .as_ref()
                .map(|until| last_processed_id.as_ref().unwrap() >= until)
                .unwrap_or(false))
    {
        log::info!(
            "WAL reader: completed bytes range read, sent {} entries, skipped {}, from {} to {:?} step {}",
            entries_sent,
            entries_skipped,
            from_id,
            until_id,
            step
        );
        ReadRangeResult::Complete
    } else {
        let last_id_in_file = last_processed_id.unwrap_or_else(|| from_id.clone());
        log::info!(
            "WAL reader: partial bytes range read, sent {} entries, last read: {:?}, last in file: {}, skipped {}, from {} to {:?} step {}",
            entries_sent,
            last_read_id,
            last_id_in_file,
            entries_skipped,
            from_id,
            until_id,
            step
        );
        ReadRangeResult::PartialRead {
            last_read_id,
            last_id_in_file,
        }
    };

    Ok(result)
}
