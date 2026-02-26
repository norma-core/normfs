use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use normfs_types::QueueId;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use uintn::UintN;

#[derive(Debug, Clone)]
pub struct AckFileWriterSettings {
    pub max_buffer_size: usize,
    pub max_file_size: u64,
    pub write_interval: Duration,
    pub fsync: bool,
}

impl Default for AckFileWriterSettings {
    fn default() -> Self {
        Self {
            max_buffer_size: 128 * 1024 * 1024, // 128MB
            max_file_size: 128 * 1024 * 1024,   // 128MB
            write_interval: Duration::from_millis(20),
            fsync: true,
        }
    }
}

#[derive(Debug)]
struct WriterState {
    buffer: BytesMut,
    acks: Vec<(QueueId, UintN)>,
    current_size: u64,
}

#[derive(Debug)]
pub struct AckFileWriter {
    settings: AckFileWriterSettings,
    state: Arc<Mutex<WriterState>>,
    writer_handle: Mutex<Option<JoinHandle<()>>>,
    shutdown_tx: mpsc::Sender<()>,
    buffer_full_notify: Arc<Notify>,
}

const MAX_RETRIES: u32 = 1000;
const RETRY_DELAY: Duration = Duration::from_millis(10);

impl AckFileWriter {
    pub async fn new(
        path: impl AsRef<Path>,
        settings: AckFileWriterSettings,
        ack_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
        header: Bytes,
    ) -> std::io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())
            .await?;

        let initial_size = header.len() as u64;
        file.write_all(&header).await?;

        let path = path.as_ref().to_path_buf();

        let state = Arc::new(Mutex::new(WriterState {
            buffer: BytesMut::with_capacity(settings.max_buffer_size),
            acks: Vec::new(),
            current_size: initial_size,
        }));
        let buffer_full_notify = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let writer_handle = tokio::spawn(writer_task(
            path.clone(),
            Arc::new(Mutex::new(file)),
            state.clone(),
            settings.clone(),
            shutdown_rx,
            buffer_full_notify.clone(),
            ack_sender,
        ));

        Ok(Self {
            settings,
            state,
            writer_handle: Mutex::new(Some(writer_handle)),
            shutdown_tx,
            buffer_full_notify,
        })
    }

    pub async fn can_add(&self, size: usize) -> bool {
        let state = self.state.lock().await;
        state.current_size + (size as u64) <= self.settings.max_file_size
    }

    pub async fn write(&self, queue_id: QueueId, entry_id: UintN, entry: Bytes) {
        let mut state = self.state.lock().await;

        state.buffer.extend_from_slice(&entry);
        state.acks.push((queue_id, entry_id));
        state.current_size += entry.len() as u64;

        if state.buffer.len() >= self.settings.max_buffer_size {
            self.buffer_full_notify.notify_one();
        }
    }

    pub async fn close(&mut self) -> std::io::Result<()> {
        let _ = self.shutdown_tx.send(()).await;
        if let Some(handle) = self.writer_handle.lock().await.take() {
            handle.await.map_err(std::io::Error::other)?;
        }
        Ok(())
    }
}

async fn writer_task(
    path: PathBuf,
    file: Arc<Mutex<File>>,
    state: Arc<Mutex<WriterState>>,
    settings: AckFileWriterSettings,
    mut shutdown_rx: mpsc::Receiver<()>,
    buffer_full_notify: Arc<Notify>,
    ack_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
) {
    let mut interval = tokio::time::interval(settings.write_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                flush_buffer(&path, &file, &state, &ack_sender, settings.fsync).await;
                break;
            }
            _ = buffer_full_notify.notified() => {
                flush_buffer(&path, &file, &state, &ack_sender, settings.fsync).await;
            }
            _ = interval.tick() => {
                flush_buffer(&path, &file, &state, &ack_sender, settings.fsync).await;
            }
        }
    }
}

async fn flush_buffer(
    path: &Path,
    file: &Arc<Mutex<File>>,
    state: &Arc<Mutex<WriterState>>,
    ack_sender: &mpsc::UnboundedSender<(QueueId, UintN)>,
    fsync: bool,
) {
    let (data_to_write, acks_to_send) = {
        let mut state_guard = state.lock().await;
        if state_guard.buffer.is_empty() {
            return;
        }
        let data = state_guard.buffer.split().freeze();
        let mut acks = std::mem::take(&mut state_guard.acks);

        let acks_to_send = if let Some(last_ack) = acks.pop() {
            vec![last_ack]
        } else {
            vec![]
        };

        (data, acks_to_send)
    };

    if data_to_write.is_empty() {
        return;
    }

    log::debug!(
        target: "normfs",
        "Writing to file {}, block size: {}",
        path.display(),
        data_to_write.len()
    );

    let mut write_successful = false;
    for attempt in 0..MAX_RETRIES {
        let mut file_guard = file.lock().await;
        if let Err(e) = file_guard.write_all(&data_to_write).await {
            log::error!(
                target: "normfs",
                "Failed to write to file (attempt {}/{}): {}",
                attempt + 1,
                MAX_RETRIES,
                e
            );
            if attempt < MAX_RETRIES - 1 {
                tokio::time::sleep(RETRY_DELAY).await;
            }
        } else {
            if fsync && let Err(e) = file_guard.sync_all().await {
                log::error!(
                    target: "normfs",
                    "Failed to sync file (attempt {}/{}): {}",
                    attempt + 1,
                    MAX_RETRIES,
                    e
                );
                if attempt < MAX_RETRIES - 1 {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                continue;
            }
            write_successful = true;
            break;
        }
    }

    if write_successful {
        for (queue_id, entry_id) in acks_to_send {
            if let Err(e) = ack_sender.send((queue_id, entry_id)) {
                log::debug!(target: "normfs", "FATAL: Failed to send ack for written data: {}. The application integrity is compromised.", e);
            }
        }
    } else {
        log::error!(target: "normfs", "All write attempts failed. Returning data to buffer to prevent loss.");
        let mut state_guard = state.lock().await;
        let mut new_buffer =
            BytesMut::with_capacity(data_to_write.len() + state_guard.buffer.len());
        new_buffer.extend_from_slice(&data_to_write);
        new_buffer.extend_from_slice(&state_guard.buffer);
        state_guard.buffer = new_buffer;

        // Return acks as well
        let mut new_acks = acks_to_send;
        new_acks.append(&mut state_guard.acks);
        state_guard.acks = new_acks;
    }
}

impl Drop for AckFileWriter {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
        if let Ok(mut guard) = self.writer_handle.try_lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }
}
