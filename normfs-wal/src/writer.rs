use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::ack_file_writer::{AckFileWriter, AckFileWriterSettings};
use crate::wal_entry::WalEntryHeader;
use crate::wal_header::WalHeader;
use crate::writer_buffer::OrderedBuffer;
use crate::{WalError, WalFile, WalSettings};
use bytes::{Bytes, BytesMut};
use normfs_types::QueueId;
use tokio::sync::{mpsc, oneshot};
use uintn::UintN;

enum WriteRequest {
    Enqueue(UintN, Bytes),
    EnqueueBatch(Vec<(UintN, Bytes)>),
    Close(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct WalWriter {
    write_chan: mpsc::UnboundedSender<WriteRequest>,
}

struct WriterState {
    queue_path: PathBuf,
    queue_id: QueueId,
    file_id: UintN,
    header: WalHeader,
    settings: WalSettings,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    wal_complete_sender: mpsc::UnboundedSender<WalFile>,
    file_writer: AckFileWriter,
    has_written: bool,
    buffer: OrderedBuffer,
}

impl WalWriter {
    pub async fn new(
        queue: &QueueId,
        root: &Path,
        file_id: &UintN,
        header: WalHeader,
        settings: WalSettings,
        written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
        wal_complete_sender: mpsc::UnboundedSender<WalFile>,
        last_entry_id: Option<UintN>,
    ) -> Result<Self, WalError> {
        log::info!(
            "WAL writer: creating new writer for queue '{}', file: {}, last_entry_id: {:?}",
            queue,
            file_id,
            last_entry_id
        );

        let (tx, mut rx) = mpsc::unbounded_channel();

        let queue_fs_path = queue.to_wal_dir(root);
        tokio::fs::create_dir_all(&queue_fs_path).await?;

        let file_writer = new_file_writer(
            &queue_fs_path,
            file_id,
            &header,
            &settings,
            written_sender.clone(),
        )
        .await?;

        let mut state = WriterState {
            queue_path: queue_fs_path,
            queue_id: queue.clone(),
            file_id: file_id.clone(),
            header,
            settings,
            written_sender,
            wal_complete_sender,
            file_writer,
            has_written: false,
            buffer: OrderedBuffer::new(last_entry_id, queue.clone()),
        };

        let queue_log_str = queue.to_string();
        tokio::spawn(async move {
            log::debug!(
                "WAL writer: starting writer task for queue '{}'",
                queue_log_str
            );

            while let Some(req) = rx.recv().await {
                match req {
                    WriteRequest::Enqueue(entry_id, data) => {
                        if let Err(e) = state.write(entry_id, data).await {
                            log::error!(
                                "WAL writer: error writing entry for queue '{}': {}",
                                state.queue_id,
                                e
                            );
                        }
                    }
                    WriteRequest::EnqueueBatch(entries) => {
                        if let Err(e) = state.write_batch(entries).await {
                            log::error!(
                                "WAL writer: error writing batch for queue '{}': {}",
                                state.queue_id,
                                e
                            );
                        }
                    }
                    WriteRequest::Close(responder) => {
                        log::info!("WAL writer: closing writer for queue '{}'", state.queue_id);
                        if let Err(e) = state.file_writer.close().await {
                            log::error!(
                                "WAL writer: error closing file writer for queue '{}': {}",
                                state.queue_id,
                                e
                            );
                        }
                        let _ = responder.send(());
                        break;
                    }
                }
            }

            log::debug!(
                "WAL writer: writer task ended for queue '{}'",
                state.queue_id
            );
        });

        Ok(Self { write_chan: tx })
    }

    pub fn enqueue(&self, entry_id: UintN, data: Bytes) -> Result<(), WalError> {
        log::trace!("WAL writer: enqueuing entry {} for write", entry_id);

        self.write_chan
            .send(WriteRequest::Enqueue(entry_id, data))
            .map_err(|_| WalError::SendError)
    }

    pub fn enqueue_batch(&self, entries: Vec<(UintN, Bytes)>) -> Result<(), WalError> {
        log::trace!(
            "WAL writer: enqueuing batch of {} entries for write",
            entries.len()
        );

        self.write_chan
            .send(WriteRequest::EnqueueBatch(entries))
            .map_err(|_| WalError::SendError)
    }

    pub async fn close(&self) -> Result<(), WalError> {
        let (tx, rx) = oneshot::channel();
        self.write_chan
            .send(WriteRequest::Close(tx))
            .map_err(|_| WalError::SendError)?;
        rx.await.map_err(|_| WalError::SendError)
    }
}

impl WriterState {
    async fn write(&mut self, entry_id: UintN, data: Bytes) -> Result<(), WalError> {
        log::debug!(
            "WAL writer: writing entry {} to queue '{}', data size: {} bytes",
            entry_id,
            self.queue_id,
            data.len()
        );

        let entries = if self.buffer.can_write(&entry_id) {
            // direct write
            log::debug!(
                "WAL writer: entry {} is in order for queue '{}', proceeding to write",
                entry_id,
                self.queue_id
            );
            vec![(entry_id.clone(), data)]
        } else {
            // buffer and get ready entries
            log::debug!(
                "WAL writer: entry {} is out of order for queue '{}', buffering",
                entry_id,
                self.queue_id
            );
            self.buffer.wait_for_order((entry_id.clone(), data))
        };

        if entries.is_empty() {
            log::debug!(
                "WAL writer: no entries ready to write for queue '{}'",
                self.queue_id
            );
            return Ok(());
        }

        for (entry_id, data) in entries {
            if (self.has_written && !self.file_writer.can_add(data.len()).await)
                || !self.header.can_hold_entry(&entry_id, data.len())
            {
                log::debug!(
                    "WAL writer: need to rotate file for queue '{}', entry {}, data size: {}",
                    self.queue_id,
                    entry_id,
                    data.len()
                );
                self.rotate(entry_id.clone(), data.len()).await?;
            } else {
                log::debug!(
                    "WAL writer: current file {} for queue '{}' can hold entry {}, data size: {}",
                    self.file_id,
                    self.queue_id,
                    entry_id,
                    data.len()
                );
            }

            let entry_header = WalEntryHeader::new(entry_id.clone(), &data);
            let mut entry_buf = BytesMut::new();
            entry_header.write_to_bytes(&mut entry_buf, &self.header)?;
            entry_buf.extend_from_slice(&data);
            let entry_buf = entry_buf.freeze();

            log::debug!(
                "WAL writer: writing entry {} to file {} for queue '{}', total size: {} bytes",
                entry_id,
                self.file_id,
                self.queue_id,
                entry_buf.len()
            );

            self.file_writer
                .write(self.queue_id.clone(), entry_id.clone(), entry_buf)
                .await;

            // Update last written entry ID
            self.has_written = true;

            log::debug!(
                "WAL writer: successfully wrote entry {} to queue '{}'",
                entry_id,
                self.queue_id
            );
        }

        Ok(())
    }

    async fn write_batch(&mut self, entries: Vec<(UintN, Bytes)>) -> Result<(), WalError> {
        log::debug!(
            "WAL writer: writing batch of {} entries to queue '{}'",
            entries.len(),
            self.queue_id
        );

        for (entry_id, data) in entries {
            self.write(entry_id, data).await?;
        }

        log::debug!(
            "WAL writer: completed batch write to queue '{}'",
            self.queue_id
        );

        Ok(())
    }

    async fn rotate(
        &mut self,
        next_entry_id: UintN,
        next_data_size: usize,
    ) -> Result<(), WalError> {
        log::info!(
            "WAL writer: rotating file for queue '{}', current file: {}, next entry: {}",
            self.queue_id,
            self.file_id,
            next_entry_id
        );

        self.file_writer.close().await?;
        let _ = self.wal_complete_sender.send(WalFile {
            queue_id: self.queue_id.clone(),
            file_id: self.file_id.clone(),
            encryption_type: self.settings.encryption_type,
            compression_type: self.settings.compression_type,
        });

        let old_file_id = self.file_id.clone();
        self.file_id = self.file_id.increment();
        self.header = self.header.resize(&next_entry_id, next_data_size);
        self.header.num_entries_before = next_entry_id.clone();

        log::debug!(
            "WAL writer: creating new file {} for queue '{}', entries before: {}, id size bytes: {}, data size bytes: {}",
            self.file_id,
            self.queue_id,
            self.header.num_entries_before,
            self.header.id_size_bytes,
            self.header.data_size_bytes
        );

        self.file_writer = new_file_writer(
            &self.queue_path,
            &self.file_id,
            &self.header,
            &self.settings,
            self.written_sender.clone(),
        )
        .await?;
        self.has_written = false;

        log::info!(
            "WAL writer: successfully rotated from file {} to {} for queue '{}'",
            old_file_id,
            self.file_id,
            self.queue_id
        );

        Ok(())
    }
}

async fn new_file_writer(
    queue_path: &Path,
    file_id: &UintN,
    header: &WalHeader,
    settings: &WalSettings,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
) -> Result<AckFileWriter, WalError> {
    let file_path = file_id.to_file_path(queue_path.to_str().unwrap(), "wal");

    let mut header_buf = BytesMut::new();
    header.write_to_bytes(&mut header_buf);

    let writer = AckFileWriter::new(
        file_path,
        AckFileWriterSettings {
            max_buffer_size: settings.write_buffer_size,
            max_file_size: settings.max_file_size as u64,
            write_interval: Duration::from_millis(50),
            fsync: settings.enable_fsync,
        },
        written_sender,
        header_buf.freeze(),
    )
    .await?;

    Ok(writer)
}
