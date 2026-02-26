#![allow(clippy::too_many_arguments)]

use bytes::Bytes;
use normfs_types::{DataSource, QueueId, ReadEntry};
use std::{collections::HashMap, path::PathBuf, sync::RwLock};
use tokio::{fs, sync::mpsc};
use uintn::{UintN, paths};
use writer::WalWriter;

mod ack_file_writer;
mod errors;
mod reader;
mod wal_entry;
mod wal_header;
mod writer;
mod writer_buffer;

pub use errors::*;
pub use reader::{ReadRangeResult, WalContent, get_wal_header};
pub use wal_header::{WalHeader, WalHeaderError};

#[cfg(test)]
mod wal_header_test;

#[cfg(test)]
mod wal_entry_test;

#[cfg(test)]
mod ack_file_writer_test;

#[cfg(test)]
mod reader_test;
#[cfg(test)]
mod writer_test;

#[derive(Debug, Clone)]
pub struct WalSettings {
    pub max_file_size: usize,
    pub write_buffer_size: usize,
    pub enable_fsync: bool,
    pub encryption_type: normfs_types::EncryptionType,
    pub compression_type: normfs_types::CompressionType,
}

impl Default for WalSettings {
    fn default() -> Self {
        Self {
            max_file_size: 128 * 1024 * 1024,     // 128MB
            write_buffer_size: 128 * 1024 * 1024, // 128MB
            enable_fsync: true,
            encryption_type: normfs_types::EncryptionType::Aes,
            compression_type: normfs_types::CompressionType::Zstd,
        }
    }
}

pub struct WalStore {
    root: PathBuf,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    wal_complete_sender: mpsc::UnboundedSender<WalFile>,
    writers: RwLock<HashMap<QueueId, WalWriter>>,
}

#[derive(Debug)]
pub struct WalFile {
    pub queue_id: QueueId,
    pub file_id: UintN,
    pub encryption_type: normfs_types::EncryptionType,
    pub compression_type: normfs_types::CompressionType,
}

pub struct QueueEnd {
    pub file_id: UintN,
    pub header: WalHeader,
    pub last_entry_id: Option<UintN>,
}

impl WalStore {
    pub fn new(
        root: impl AsRef<std::path::Path>,
        written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
        wal_complete_sender: mpsc::UnboundedSender<WalFile>,
    ) -> Self {
        let root_path = root.as_ref().to_path_buf();
        log::info!("WalStore: initializing at path: {:?}", root_path);

        Self {
            root: root_path,
            written_sender,
            wal_complete_sender,
            writers: RwLock::new(HashMap::new()),
        }
    }

    /// Get the latest WAL file ID for a queue.
    pub async fn find_last_file_id(&self, queue: &QueueId) -> Result<UintN, WalError> {
        let queue_path = queue.to_wal_dir(&self.root);
        tokio::fs::create_dir_all(&queue_path).await?;

        paths::find_max_id(&queue_path, "wal").map_err(WalError::PathError)
    }

    /// Get the last entry ID in a specific WAL file.
    /// Returns None if the file has no entries (only header) or doesn't exist.
    pub async fn get_file_end(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<UintN>, WalError> {
        let queue_path = queue_id.to_wal_dir(&self.root);

        match reader::get_wal_range(&queue_path, file_id).await {
            Ok((_, entries)) => Ok(entries.map(|(_, last)| last)),
            Err(WalError::WalEmpty(_)) => Ok(None),
            Err(WalError::WalNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn get_queue_start(&self, queue_id: &QueueId) -> Result<Option<UintN>, WalError> {
        let first_file_id = self.get_first_file_id(queue_id).await?;
        if let Some(firts_file_id) = first_file_id {
            let entries_before = self.get_entries_before(queue_id, &firts_file_id).await?;
            Ok(Some(entries_before))
        } else {
            Ok(None)
        }
    }

    pub async fn get_wal_file_content(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<WalContent, WalError> {
        log::debug!(
            "WalStore: getting content for queue '{}', file {}",
            queue_id,
            file_id
        );

        let queue_path = queue_id.to_wal_dir(&self.root);
        let content = reader::get_wal_content(&queue_path, file_id).await?;

        log::debug!(
            "WalStore: retrieved content for queue '{}', file {}, entries: {}, entries_before: {}",
            queue_id,
            file_id,
            content.num_entries,
            content.entries_before
        );

        Ok(content)
    }

    pub async fn read_wal_range(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        from_id: &UintN,
        until_id: &Option<UintN>,
        step: usize,
        tx: &mpsc::Sender<ReadEntry>,
        data_source: DataSource,
    ) -> Result<ReadRangeResult, WalError> {
        log::debug!(
            "WalStore: reading range [{} - {:?}] from file {} for queue '{}', step: {}",
            from_id,
            until_id,
            file_id,
            queue_id,
            step
        );

        let queue_path = queue_id.to_wal_dir(&self.root);
        let result = reader::read_wal_file_range(
            &queue_path,
            file_id,
            from_id,
            until_id,
            step,
            tx,
            data_source,
        )
        .await?;

        log::debug!(
            "WalStore: range read result for queue '{}': {:?}",
            queue_id,
            result
        );

        Ok(result)
    }

    pub async fn read_wal_content_range(
        &self,
        content: &Bytes,
        from_id: &UintN,
        until_id: &Option<UintN>,
        step: usize,
        tx: &mpsc::Sender<ReadEntry>,
        data_source: DataSource,
    ) -> Result<ReadRangeResult, WalError> {
        reader::read_wal_bytes_range(content, from_id, until_id, step, tx, data_source).await
    }

    pub async fn truncate(&self, queue_id: &QueueId) -> Result<(), WalError> {
        log::info!("WalStore: truncating WAL for queue '{}'", queue_id);

        let queue_path = queue_id.to_wal_dir(&self.root);

        tokio::fs::remove_dir_all(&queue_path).await?;
        tokio::fs::create_dir_all(&queue_path).await?;

        Ok(())
    }

    pub async fn has_writer(&self, queue_id: &QueueId) -> bool {
        let writers = self.writers.read().unwrap();
        writers.contains_key(queue_id)
    }

    pub async fn start_writer(
        &self,
        queue: &QueueId,
        file_id: &UintN,
        header: wal_header::WalHeader,
        settings: WalSettings,
        last_entry_id: Option<UintN>,
    ) -> Result<(), WalError> {
        log::info!(
            "WalStore: starting writer for queue '{}', file: {}, last_entry_id: {:?}, data size = {} bytes, id size = {} bytes",
            queue,
            file_id,
            last_entry_id,
            header.data_size_bytes,
            header.id_size_bytes
        );

        // Create WAL directory if needed
        let queue_fs_path = queue.to_wal_dir(&self.root);
        tokio::fs::create_dir_all(&queue_fs_path).await?;

        // Note: normfs core already performed recovery and determined the correct file_id to use.
        // The file_id provided here is either:
        // - A new file (if latest file had entries)
        // - The latest empty/header-only file (for reuse)
        // Old completed files will be processed asynchronously via process_old_files()

        let writer = WalWriter::new(
            queue,
            &self.root,
            file_id,
            header,
            settings,
            self.written_sender.clone(),
            self.wal_complete_sender.clone(),
            last_entry_id,
        )
        .await?;

        let mut writers = self.writers.write().unwrap();
        writers.insert(queue.clone(), writer);

        log::debug!(
            "WalStore: writer started successfully for queue '{}'",
            queue
        );

        Ok(())
    }

    /// Process old WAL files for a queue (files with id < current_file_id).
    /// Sends completion signals for these files so they can be persisted to Store.
    /// The current file being written to is NOT included (id < current_file_id).
    /// This is called asynchronously after start_writer to avoid blocking startup.
    pub async fn process_old_files(
        &self,
        queue: &QueueId,
        current_file_id: &UintN,
        compression_type: normfs_types::CompressionType,
        encryption_type: normfs_types::EncryptionType,
    ) -> Result<(), WalError> {
        log::info!(
            "WalStore: processing old files for queue '{}', current file: {} (not included)",
            queue,
            current_file_id
        );

        let queue_fs_path = queue.to_wal_dir(&self.root);

        match paths::get_files_ids(&queue_fs_path, "wal") {
            Ok(file_ids) => {
                let mut sent_count = 0;
                for id in file_ids {
                    // Only process files older than current - exclude current file being written to
                    if id < *current_file_id {
                        let id_str = id.to_string();
                        log::debug!(
                            "WalStore: sending completed file {} for queue '{}'",
                            id_str,
                            queue
                        );
                        let wal_file = WalFile {
                            queue_id: queue.clone(),
                            file_id: id,
                            encryption_type,
                            compression_type,
                        };
                        if self.wal_complete_sender.send(wal_file).is_err() {
                            log::error!(
                                "WalStore: failed to send completed wal file {} for queue '{}'",
                                id_str,
                                queue
                            );
                        } else {
                            sent_count += 1;
                        }
                    }
                }
                log::info!(
                    "WalStore: processed {} old files for queue '{}'",
                    sent_count,
                    queue
                );
            }
            Err(e) => {
                log::error!(
                    "WalStore: error listing wal files for queue '{}': {}",
                    queue,
                    e
                );
                return Err(WalError::PathError(e));
            }
        }

        Ok(())
    }

    pub fn enqueue(&self, queue: &QueueId, entry_id: UintN, data: Bytes) -> Result<(), WalError> {
        log::trace!(
            "WalStore: enqueuing entry {} for queue '{}', data size: {} bytes",
            entry_id,
            queue,
            data.len()
        );

        let writers = self.writers.read().unwrap();
        match writers.get(queue) {
            Some(writer) => {
                let entry_id_clone = entry_id.clone();
                writer.enqueue(entry_id, data)?;
                log::trace!(
                    "WalStore: entry {} enqueued for queue '{}'",
                    entry_id_clone,
                    queue
                );
                Ok(())
            }
            None => {
                log::error!("WalStore: no writer found for queue '{}'", queue);
                Err(WalError::WriterNotFound)
            }
        }
    }

    pub fn enqueue_batch(
        &self,
        queue: &QueueId,
        entries: Vec<(UintN, Bytes)>,
    ) -> Result<(), WalError> {
        log::trace!(
            "WalStore: enqueuing batch of {} entries for queue '{}'",
            entries.len(),
            queue
        );

        let writers = self.writers.read().unwrap();
        match writers.get(queue) {
            Some(writer) => {
                writer.enqueue_batch(entries)?;
                log::trace!("WalStore: batch enqueued for queue '{}'", queue);
                Ok(())
            }
            None => {
                log::error!("WalStore: no writer found for queue '{}'", queue);
                Err(WalError::WriterNotFound)
            }
        }
    }

    pub async fn close(&self) -> Result<(), WalError> {
        log::info!("WalStore: closing all writers");

        let writers = std::mem::take(&mut *self.writers.write().unwrap());
        let num_writers = writers.len();
        let mut close_handles = Vec::new();

        for (queue_id, writer) in writers {
            let queue_id_clone = queue_id.clone();
            let handle = tokio::spawn(async move {
                log::debug!("WalStore: closing writer for queue '{}'", queue_id_clone);
                writer.close().await
            });
            close_handles.push((queue_id, handle));
        }

        let mut errors = Vec::new();
        for (queue_id, handle) in close_handles {
            match handle.await {
                Ok(Ok(_)) => {
                    log::debug!(
                        "WalStore: successfully closed writer for queue '{}'",
                        queue_id
                    );
                }
                Ok(Err(e)) => {
                    log::error!(
                        "WalStore: error closing writer for queue '{}': {}",
                        queue_id,
                        e
                    );
                    errors.push(e);
                }
                Err(_) => {
                    log::error!(
                        "WalStore: task panic while closing writer for queue '{}'",
                        queue_id
                    );
                    errors.push(WalError::SendError);
                }
            }
        }

        if let Some(e) = errors.into_iter().next() {
            return Err(e);
        }

        log::info!("WalStore: successfully closed {} writers", num_writers);
        Ok(())
    }

    pub async fn delete_wal_file(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<(), WalError> {
        log::info!(
            "WalStore: deleting file {} for queue '{}'",
            file_id,
            queue_id
        );

        let file_path = queue_id.to_wal_path(&self.root, file_id);
        fs::remove_file(&file_path).await?;

        log::debug!(
            "WalStore: successfully deleted file {} for queue '{}'",
            file_id,
            queue_id
        );
        Ok(())
    }

    pub async fn get_entries_before(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<UintN, WalError> {
        log::debug!(
            "WalStore: getting entries_before for queue '{}', file {}",
            queue_id,
            file_id
        );

        let queue_path = queue_id.to_wal_dir(&self.root);
        let header = reader::read_wal_header(&queue_path, file_id).await?;

        log::debug!(
            "WalStore: file {} for queue '{}' has {} entries before",
            file_id,
            queue_id,
            header.num_entries_before
        );

        Ok(header.num_entries_before)
    }

    /// Get the header from a specific WAL file.
    /// Returns None if the file doesn't exist.
    pub async fn get_file_header(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<WalHeader>, WalError> {
        log::info!(
            "WalStore: getting header for queue '{}', file {}",
            queue_id,
            file_id
        );

        let queue_path = queue_id.to_wal_dir(&self.root);
        match reader::read_wal_header(&queue_path, file_id).await {
            Ok(header) => {
                log::info!(
                    "WalStore: file {} for queue '{}' - data_size={}, id_size={}, entries_before={}",
                    file_id,
                    queue_id,
                    header.data_size_bytes,
                    header.id_size_bytes,
                    header.num_entries_before
                );
                Ok(Some(header))
            }
            Err(WalError::WalNotFound) => {
                log::info!(
                    "WalStore: file {} not found for queue '{}'",
                    file_id,
                    queue_id
                );
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_first_file_id(&self, queue_id: &QueueId) -> Result<Option<UintN>, WalError> {
        log::debug!("WalStore: getting first file ID for queue '{}'", queue_id);

        let queue_path = queue_id.to_wal_dir(&self.root);
        tokio::fs::create_dir_all(&queue_path).await?;

        match paths::find_min_id(&queue_path, "wal") {
            Ok(id) => {
                log::debug!("WalStore: first file ID for queue '{}' is {}", queue_id, id);
                Ok(Some(id))
            }
            Err(paths::PathError::NoFilesFound) => {
                log::debug!("WalStore: no files found for queue '{}'", queue_id);
                Ok(None)
            }
            Err(e) => Err(WalError::PathError(e)),
        }
    }

    pub async fn get_last_file_id(&self, queue_id: &QueueId) -> Result<Option<UintN>, WalError> {
        log::debug!("WalStore: getting last file ID for queue '{}'", queue_id);

        let queue_path = queue_id.to_wal_dir(&self.root);
        tokio::fs::create_dir_all(&queue_path).await?;

        let last_file = match paths::find_max_id(&queue_path, "wal") {
            Err(paths::PathError::NoFilesFound) => {
                log::debug!("WalStore: no files found for queue '{}'", queue_id);
                return Ok(None);
            }
            Err(e) => {
                return Err(WalError::PathError(e));
            }
            Ok(file) => {
                log::debug!(
                    "WalStore: last file ID for queue '{}' is {}",
                    queue_id,
                    file
                );
                file
            }
        };

        Ok(Some(last_file))
    }

    /// Get the raw WAL bytes for a specific file.
    /// Returns None if the file doesn't exist.
    pub async fn get_wal_bytes(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<Bytes>, WalError> {
        log::debug!(
            "WalStore: getting WAL bytes for queue '{}', file {}",
            queue_id,
            file_id
        );

        let file_path = queue_id.to_wal_path(&self.root, file_id);

        match fs::read(&file_path).await {
            Ok(bytes) => {
                log::debug!(
                    "WalStore: read {} bytes for queue '{}', file {}",
                    bytes.len(),
                    queue_id,
                    file_id
                );
                Ok(Some(Bytes::from(bytes)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::debug!(
                    "WalStore: file {} not found for queue '{}'",
                    file_id,
                    queue_id
                );
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}
