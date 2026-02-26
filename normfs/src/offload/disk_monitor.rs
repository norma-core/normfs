use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use uintn::{paths, UintN};

use crate::Error;
use normfs_cloud::offloader::QueueOffloader;
use normfs_cloud::S3Client;
use normfs_store::StoreError;
use normfs_types::QueueId;
use normfs_wal::WalSettings;

#[derive(Debug, Clone)]
pub struct DiskMonitorConfig {
    /// Maximum size in bytes for a queue (store + wal combined)
    pub max_size: usize,
    /// Monitoring interval
    pub check_interval: Duration,
    /// WAL settings to validate minimum size
    pub wal_settings: WalSettings,
}

impl DiskMonitorConfig {
    pub fn validate(&self) -> Result<(), Error> {
        // Queue size cannot be less than wal file size * 3
        let min_size = self.wal_settings.max_file_size * 3;
        if self.max_size < min_size {
            return Err(Error::Store(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Queue max_size ({}) must be at least 3x WAL file size ({})",
                    self.max_size, min_size
                ),
            ))));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct QueueMonitor {
    queue_id: String,
    config: DiskMonitorConfig,
    root_path: PathBuf,
    offloader: Option<QueueOffloader>,
}

impl QueueMonitor {
    async fn new(
        queue_id: String,
        config: DiskMonitorConfig,
        root_path: PathBuf,
        client: Option<Arc<S3Client>>,
        prefix: Option<&str>,
    ) -> Self {
        let offloader = if let (Some(client), Some(prefix)) = (client, prefix) {
            Some(QueueOffloader::new(queue_id.clone(), root_path.clone(), client, prefix).await)
        } else {
            None
        };

        Self {
            queue_id,
            config,
            root_path,
            offloader,
        }
    }

    async fn get_queue_size(&self) -> Result<usize, Error> {
        let mut total_size = 0;

        // Calculate WAL folder size
        let wal_path = self.root_path.join(&self.queue_id).join("wal");
        if wal_path.exists() {
            total_size += Self::get_directory_size(&wal_path).await?;
        }

        // Calculate store folder size
        let store_path = self.root_path.join(&self.queue_id).join("store");
        if store_path.exists() {
            total_size += Self::get_directory_size(&store_path).await?;
        }

        Ok(total_size)
    }

    async fn get_directory_size(path: &Path) -> Result<usize, Error> {
        let mut total_size = 0;
        let mut dirs_to_process = vec![path.to_path_buf()];

        // Iterative approach using a stack to avoid recursion
        while let Some(current_dir) = dirs_to_process.pop() {
            let mut entries = tokio::fs::read_dir(&current_dir)
                .await
                .map_err(|e| Error::Store(StoreError::Io(e)))?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| Error::Store(StoreError::Io(e)))?
            {
                let metadata = entry
                    .metadata()
                    .await
                    .map_err(|e| Error::Store(StoreError::Io(e)))?;

                if metadata.is_file() {
                    total_size += metadata.len() as usize;
                } else if metadata.is_dir() {
                    // Add subdirectory to the stack for processing
                    dirs_to_process.push(entry.path());
                }
            }
        }

        Ok(total_size)
    }

    async fn cleanup_oldest_files(&self, current_size: usize) -> Result<(), Error> {
        let target_size = self.config.max_size;
        let mut size_to_free = current_size.saturating_sub(target_size);
        let mut total_freed = 0usize;

        if size_to_free == 0 {
            return Ok(());
        }

        log::info!(
            target: "normfs::disk_monitor",
            "Queue '{}' size {} exceeds limit {}, need to free {} bytes",
            self.queue_id,
            current_size,
            self.config.max_size,
            size_to_free
        );

        // Get the latest offloaded ID if we have an offloader
        let latest_offloaded_id = if let Some(ref offloader) = self.offloader {
            offloader.get_latest_offloaded_id().await
        } else {
            None
        };

        let store_path = self.root_path.join(&self.queue_id).join("store");
        let wal_path = self.root_path.join(&self.queue_id).join("wal");

        // Find initial minimum ID across both store and wal
        let store_min_id = if store_path.exists() {
            paths::find_min_id(&store_path, "store").ok()
        } else {
            None
        };

        let wal_min_id = if wal_path.exists() {
            paths::find_min_id(&wal_path, "wal").ok()
        } else {
            None
        };

        // Start with the minimum ID across both directories
        let mut current_id = match (store_min_id, wal_min_id) {
            (Some(sid), Some(wid)) => sid.min(wid),
            (Some(sid), None) => sid,
            (None, Some(wid)) => wid,
            (None, None) => {
                log::warn!(
                    target: "normfs::disk_monitor",
                    "No files found to delete for queue '{}'",
                    self.queue_id
                );
                return Ok(());
            }
        };

        while size_to_free > 0 {
            let store_file_path = current_id.to_file_path(store_path.to_str().unwrap(), "store");
            let wal_file_path = current_id.to_file_path(wal_path.to_str().unwrap(), "wal");

            if !store_file_path.exists() && !wal_file_path.exists() {
                log::info!(
                    target: "normfs::disk_monitor",
                    "No more files to delete for queue '{}', stopping cleanup at id {}",
                    self.queue_id,
                    current_id
                );
                break;
            }

            // Check if file has been offloaded before deleting
            let can_delete = if let Some(ref latest_offloaded) = latest_offloaded_id {
                current_id <= *latest_offloaded
            } else {
                // No offloader or no files offloaded yet, can delete if no S3 config
                self.offloader.is_none()
            };

            if !can_delete {
                log::warn!(
                    target: "normfs::disk_monitor",
                    "Skipping deletion of file {} - not yet offloaded to S3",
                    current_id
                );
                break;
            }

            if store_file_path.exists() {
                match tokio::fs::metadata(&store_file_path).await {
                    Ok(metadata) => {
                        let file_size = metadata.len() as usize;

                        log::info!(
                            target: "normfs::disk_monitor",
                            "Deleting store file {} (size: {} bytes) for queue '{}'",
                            current_id,
                            file_size,
                            self.queue_id
                        );

                        match tokio::fs::remove_file(&store_file_path).await {
                            Ok(_) => {
                                size_to_free = size_to_free.saturating_sub(file_size);
                                total_freed += file_size;
                                let remaining_size = current_size.saturating_sub(total_freed);
                                log::info!(
                                    target: "normfs::disk_monitor",
                                    "Deleted store file {}, queue '{}' size now: {} bytes",
                                    current_id,
                                    self.queue_id,
                                    remaining_size
                                );
                            }
                            Err(e) => {
                                log::error!(
                                    target: "normfs::disk_monitor",
                                    "Failed to delete store file {}: {}",
                                    current_id,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            target: "normfs::disk_monitor",
                            "Failed to get metadata for store file {}: {}",
                            current_id,
                            e
                        );
                    }
                }
            } else if wal_file_path.exists() {
                // Get file size and delete wal file
                match tokio::fs::metadata(&wal_file_path).await {
                    Ok(metadata) => {
                        let file_size = metadata.len() as usize;

                        log::warn!(
                            target: "normfs::disk_monitor",
                            "Deleting WAL file {} (size: {} bytes) for queue '{}'",
                            current_id,
                            file_size,
                            self.queue_id
                        );

                        match tokio::fs::remove_file(&wal_file_path).await {
                            Ok(_) => {
                                size_to_free = size_to_free.saturating_sub(file_size);
                                total_freed += file_size;
                                let remaining_size = current_size.saturating_sub(total_freed);
                                log::info!(
                                    target: "normfs::disk_monitor",
                                    "Deleted WAL file {}, queue '{}' size now: {} bytes",
                                    current_id,
                                    self.queue_id,
                                    remaining_size
                                );
                            }
                            Err(e) => {
                                log::error!(
                                    target: "normfs::disk_monitor",
                                    "Failed to delete WAL file {}: {}",
                                    current_id,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::error!(
                            target: "normfs::disk_monitor",
                            "Failed to get metadata for WAL file {}: {}",
                            current_id,
                            e
                        );
                    }
                }
            }

            current_id = current_id.increment();
        }

        Ok(())
    }

    async fn check_and_cleanup(&self) -> Result<(), Error> {
        let current_size = self.get_queue_size().await?;

        if current_size > self.config.max_size {
            self.cleanup_oldest_files(current_size).await?;
        } else {
            log::info!(
                target: "normfs::disk_monitor",
                "Queue '{}' size {} is within limit {}",
                self.queue_id,
                current_size,
                self.config.max_size
            );
        }

        Ok(())
    }
}

pub struct DiskMonitor {
    monitors: Arc<RwLock<std::collections::HashMap<QueueId, QueueMonitor>>>,
    root_path: PathBuf,
    _handle: Option<tokio::task::JoinHandle<()>>,
    client: Option<Arc<S3Client>>,
    prefix: Option<String>,
}

impl DiskMonitor {
    pub async fn new(
        root_path: impl AsRef<Path>,
        client: Option<Arc<S3Client>>,
        prefix: Option<String>,
    ) -> Result<Self, Error> {
        let monitors: Arc<RwLock<std::collections::HashMap<QueueId, QueueMonitor>>> =
            Arc::new(RwLock::new(std::collections::HashMap::new()));
        let monitors_clone = monitors.clone();

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let monitors = monitors_clone.read().await;
                        for (queue_id, monitor) in monitors.iter() {
                            if let Err(e) = monitor.check_and_cleanup().await {
                                log::error!(
                                    target: "normfs::disk_monitor",
                                    "Error checking queue '{}': {}",
                                    queue_id,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            monitors,
            root_path: root_path.as_ref().to_path_buf(),
            _handle: Some(handle),
            client,
            prefix,
        })
    }

    pub async fn enqueue_for_offload(
        &self,
        queue_id: &QueueId,
        file_id: UintN,
    ) -> Result<(), Error> {
        let monitors = self.monitors.read().await;
        if let Some(monitor) = monitors.get(queue_id) {
            if let Some(ref offloader) = monitor.offloader {
                if let Err(e) = offloader.enqueue_file(file_id.clone()).await {
                    log::error!(
                        target: "normfs::disk_monitor",
                        "Failed to enqueue file for offload: queue={}, file_id={:?}, error={}",
                        queue_id, file_id, e
                    );
                }
                Ok(())
            } else {
                log::debug!(
                    target: "normfs::disk_monitor",
                    "No offloader configured for queue '{}', skipping offload for file {:?}",
                    queue_id, file_id
                );
                Ok(())
            }
        } else {
            log::warn!(
                target: "normfs::disk_monitor",
                "Queue '{}' not found in disk monitor, cannot enqueue file {:?} for offload",
                queue_id, file_id
            );
            Err(Error::Store(StoreError::FileNotFound))
        }
    }

    pub async fn add_queue(
        &self,
        queue_id: &QueueId,
        config: DiskMonitorConfig,
    ) -> Result<(), Error> {
        config.validate()?;

        let monitor = QueueMonitor::new(
            queue_id.to_string(),
            config,
            self.root_path.clone(),
            self.client.clone(),
            self.prefix.as_deref(),
        )
        .await;

        // Do an initial check
        monitor.check_and_cleanup().await?;

        let mut monitors = self.monitors.write().await;
        let has_offloader = monitor.offloader.is_some();
        monitors.insert(queue_id.clone(), monitor);

        log::info!(
            target: "normfs::disk_monitor",
            "Added disk monitoring for queue '{}' with max_size: {} (S3 offloading: {})",
            queue_id,
            monitors.get(queue_id).unwrap().config.max_size,
            if has_offloader { "enabled" } else { "disabled" }
        );

        Ok(())
    }
}
