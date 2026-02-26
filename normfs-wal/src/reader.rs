use bytes::Bytes;
use normfs_types::{DataSource, ReadEntry};
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;
use uintn::UintN;
use xxhash_rust::xxh64;

use super::errors::WalError;
use super::wal_entry::WalEntryHeader;
use super::wal_header::{WalHeader, WalHeaderError};

pub async fn read_wal_header(base_path: &Path, file_id: &UintN) -> Result<WalHeader, WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: reading header from file {}", file_id);

    let file = match fs::File::open(&file_path).await {
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

    let (wal_header, _) = match WalHeader::from_reader(&mut reader).await {
        Ok(v) => v,
        Err(WalHeaderError::SliceTooShort) => {
            log::warn!(
                "WAL reader: file {} has incomplete header",
                file_path.display()
            );
            return Err(WalError::WalEmpty(file_id.clone()));
        }
        Err(e) => return Err(e.into()),
    };

    log::debug!(
        "WAL reader: successfully read header from file {}, entries_before: {}",
        file_id,
        wal_header.num_entries_before
    );

    Ok(wal_header)
}

pub async fn get_wal_range(
    base_path: &Path,
    file_id: &UintN,
) -> Result<(WalHeader, Option<(UintN, UintN)>), WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: getting entry range from file {}", file_id);

    let file = match fs::File::open(&file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_id);
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };

    if file.metadata().await?.len() == 0 {
        log::warn!("WAL reader: file {} is empty", file_id);
        return Err(WalError::WalEmpty(file_id.clone()));
    }

    let mut reader = BufReader::new(file);

    let (wal_header, _) = match WalHeader::from_reader(&mut reader).await {
        Ok(v) => v,
        Err(WalHeaderError::SliceTooShort) => {
            return Err(WalError::WalEmpty(file_id.clone()));
        }
        Err(e) => return Err(e.into()),
    };

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

pub fn get_wal_header(content: &Bytes) -> Result<WalHeader, WalHeaderError> {
    let (header, _) = WalHeader::from_bytes(content)?;
    Ok(header)
}

pub struct WalContent {
    pub entries_before: UintN,
    pub num_entries: UintN,
    pub content: Bytes,
}

pub async fn get_wal_content(base_path: &Path, file_id: &UintN) -> Result<WalContent, WalError> {
    let file_path = file_id.to_file_path(base_path.to_str().unwrap(), "wal");
    log::debug!("WAL reader: getting content from file {}", file_id);

    let content_bytes = match fs::read(&file_path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_id);
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };
    let content = Bytes::from(content_bytes);

    if content.is_empty() {
        log::warn!("WAL reader: file {} has no content", file_id);
        return Err(WalError::WalEntryError(
            super::wal_entry::WalEntryError::SliceTooShort,
        ));
    }

    let (wal_header, header_size) = WalHeader::from_bytes(&content)?;

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

    let file = match fs::File::open(&file_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::warn!("WAL reader: file {} not found", file_id);
            return Err(WalError::WalNotFound);
        }
        Err(e) => return Err(e.into()),
    };

    if file.metadata().await?.len() == 0 {
        log::debug!("WAL reader: file {} is empty", file_id);
        return Ok(ReadRangeResult::PartialRead {
            last_read_id: None,
            last_id_in_file: from_id.clone(),
        });
    }

    let mut reader = BufReader::new(file);
    let (wal_header, _) = WalHeader::from_reader(&mut reader).await?;

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

    let (wal_header, header_size) = match WalHeader::from_bytes(content) {
        Ok(v) => v,
        Err(WalHeaderError::SliceTooShort) => {
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
