use normfs_cloud::CloudDownloader;
use normfs_store::{PersistStore, StoreError};
use normfs_types::QueueId;
use normfs_wal::{WalError, WalStore};
use std::sync::Arc;
use uintn::UintN;

#[derive(Debug)]
pub enum LookupError {
    Store(StoreError),
    Wal(WalError),
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupError::Store(e) => write!(f, "Store lookup error: {}", e),
            LookupError::Wal(e) => write!(f, "WAL lookup error: {}", e),
        }
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LookupError::Store(e) => Some(e),
            LookupError::Wal(e) => Some(e),
        }
    }
}

impl From<StoreError> for LookupError {
    fn from(e: StoreError) -> Self {
        LookupError::Store(e)
    }
}

impl From<WalError> for LookupError {
    fn from(e: WalError) -> Self {
        LookupError::Wal(e)
    }
}

/// Walk backward from `start_file_id` to find a file with valid entry data,
/// checking both Store and WAL (mirrors recovery's continue_queue logic).
/// Returns (effective_file_id, start, end) or None if no file with data found.
async fn find_valid_file_backward(
    queue: &QueueId,
    start_file_id: &UintN,
    min_file_id: &UintN,
    store: &PersistStore,
    wal: &WalStore,
) -> Result<Option<(UintN, UintN, Option<UintN>)>, LookupError> {
    let mut search_id = start_file_id.clone();
    loop {
        // Check store
        if let Some((start, end)) = store.get_file_range(queue, &search_id).await? {
            return Ok(Some((search_id, start, Some(end))));
        }
        // Check WAL
        match wal.get_entries_before(queue, &search_id).await {
            Ok(start) => return Ok(Some((search_id, start, None))),
            Err(WalError::WalNotFound | WalError::WalEmpty(_)) => {
                if search_id <= *min_file_id {
                    return Ok(None);
                }
                search_id = search_id
                    .decrement()
                    .map_err(|_| LookupError::Wal(WalError::WalNotFound))?;
            }
            Err(e) => return Err(LookupError::Wal(e)),
        }
    }
}

pub async fn find_file_with_s3(
    queue: &QueueId,
    target_id: &UintN,
    store: &PersistStore,
    wal: &WalStore,
    cloud_downloader: Option<&Arc<CloudDownloader>>,
) -> Result<Option<UintN>, LookupError> {
    log::debug!(target: "normfs-lookup", "Finding file for queue '{}', target ID: {}", queue, target_id);

    let (store_first_id, store_last_id, wal_first_id, wal_last_id) = tokio::join!(
        store.get_first_file_id(queue),
        store.get_last_file_id(queue),
        wal.get_first_file_id(queue),
        wal.get_last_file_id(queue)
    );

    let store_first_id = store_first_id?;
    let store_last_id = store_last_id?;
    let wal_first_id = wal_first_id?;
    let wal_last_id = wal_last_id?;

    log::debug!(target: "normfs-lookup",
        "File IDs for queue '{}' - Store: {:?} to {:?}, WAL: {:?} to {:?}",
        queue, store_first_id, store_last_id, wal_first_id, wal_last_id);

    // Check S3 for first file ID if available
    let s3_first_id = if let Some(s3) = cloud_downloader {
        match s3.find_min_id(queue).await {
            Ok(id) => {
                if id.is_some() {
                    log::debug!(target: "normfs-lookup", "S3 first file ID for queue '{}': {:?}", queue, id);
                }
                id
            }
            Err(e) => {
                log::warn!(target: "normfs-lookup", "Failed to get S3 first file ID for queue '{}': {}", queue, e);
                None
            }
        }
    } else {
        None
    };

    // Determine the absolute first and last file IDs
    let first_file_id = match (&store_first_id, &wal_first_id, &s3_first_id) {
        (Some(s), Some(w), Some(s3)) => Some(s.min(w).min(s3).clone()),
        (Some(s), Some(w), None) => Some(s.min(w).clone()),
        (Some(s), None, Some(s3)) => Some(s.min(s3).clone()),
        (None, Some(w), Some(s3)) => Some(w.min(s3).clone()),
        (Some(s), None, None) => Some(s.clone()),
        (None, Some(w), None) => Some(w.clone()),
        (None, None, Some(s3)) => Some(s3.clone()),
        (None, None, None) => {
            log::debug!(target: "normfs-lookup", "No files found for queue '{}'", queue);
            return Ok(None);
        }
    }
    .unwrap();

    let last_file_id = match (&store_last_id, &wal_last_id) {
        (Some(s), Some(w)) => Some(s.max(w).clone()),
        (Some(s), None) => Some(s.clone()),
        (None, Some(w)) => Some(w.clone()),
        (None, None) => return Ok(None),
    }
    .unwrap();

    log::debug!(target: "normfs-lookup",
        "Absolute file range for queue '{}': {} to {}",
        queue, first_file_id, last_file_id);

    // get range for first/last file from store
    let (store_start_range, store_end_range) = tokio::join!(
        store.get_file_range(queue, &first_file_id),
        store.get_file_range(queue, &last_file_id),
    );

    let store_start_range = store_start_range?;
    let _store_end_range = store_end_range?;

    let (first_file_start, first_file_end) = match store_start_range {
        Some((start, end)) => (start, Some(end)),
        None => {
            // Not in store, check WAL
            match wal.get_entries_before(queue, &first_file_id).await {
                Ok(start) => (start, None),
                Err(WalError::WalNotFound) => {
                    // Not in WAL, check S3 if available
                    if let Some(s3) = cloud_downloader {
                        match s3.get_file_range(queue, &first_file_id).await {
                            Ok(Some((start, end))) => {
                                log::debug!(target: "normfs-lookup",
                                    "Found first file range in S3 for queue '{}', file {}: {} to {}",
                                    queue, first_file_id, start, end);
                                (start, Some(end))
                            }
                            Ok(None) => {
                                log::warn!(target: "normfs-lookup",
                                    "First file {} not found in S3 for queue '{}'",
                                    first_file_id, queue);
                                return Ok(None);
                            }
                            Err(e) => {
                                log::warn!(target: "normfs-lookup",
                                    "S3 lookup failed for first file {} in queue '{}': {}. Using local data only.",
                                    first_file_id, queue, e);
                                return Ok(None);
                            }
                        }
                    } else {
                        return Err(LookupError::Wal(WalError::WalNotFound));
                    }
                }
                Err(WalError::WalEmpty(_)) => {
                    // File exists locally but is empty, S3 won't have it
                    return Ok(None);
                }
                Err(e) => return Err(LookupError::Wal(e)),
            }
        }
    };

    if target_id < &first_file_start {
        log::debug!(target: "normfs-lookup",
            "Target ID {} is before first file start {} for queue '{}', returning first file",
            target_id, first_file_start, queue);
        return Ok(Some(first_file_id));
    }

    if let Some(end) = &first_file_end {
        if target_id <= end {
            log::debug!(target: "normfs-lookup",
                "Target ID {} is in first file (start: {}, end: {}) for queue '{}'",
                target_id, first_file_start, end, queue);
            return Ok(Some(first_file_id));
        }
    }

    // Walk backward from last_file_id to find effective last file with data.
    // Handles empty/missing trailing WAL or Store files (same pattern as recovery).
    let (last_file_id, last_file_start, last_file_end) =
        match find_valid_file_backward(queue, &last_file_id, &first_file_id, store, wal).await? {
            Some((file_id, start, end)) => (file_id, start, end),
            None => return Ok(None), // no files with data
        };

    if target_id > &last_file_start {
        log::debug!(target: "normfs-lookup",
            "Target ID {} is after last file start {} for queue '{}', returning last file",
            target_id, last_file_start, queue);
        return Ok(Some(last_file_id));
    }

    if let Some(end) = &last_file_end {
        if target_id > end {
            log::debug!(target: "normfs-lookup",
                "Target ID {} is after last file end {} for queue '{}', no file found",
                target_id, end, queue);
            return Ok(None);
        }
    }

    log::debug!(target: "normfs-lookup",
        "Starting binary search for target ID {} in queue '{}'",
        target_id, queue);

    return binary_search(
        queue,
        target_id,
        &first_file_id,
        &last_file_id,
        first_file_end,
        last_file_start,
        store,
        wal,
        cloud_downloader,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn binary_search(
    queue: &QueueId,
    target_id: &UintN,
    first_file_id: &UintN,
    last_file_id: &UintN,
    mut left_end: Option<UintN>,
    mut right_start: UintN,
    store: &PersistStore,
    wal: &WalStore,
    cloud_downloader: Option<&Arc<CloudDownloader>>,
) -> Result<Option<UintN>, LookupError> {
    let mut left_file_id = first_file_id.clone();
    let mut right_file_id = last_file_id.clone();
    let mut iteration = 0;

    loop {
        iteration += 1;
        log::debug!(target: "normfs-lookup",
            "Binary search iteration {} for queue '{}' - Left: {}, Right: {}, left end: {:?}, right start: {}",
            iteration, queue, left_file_id, right_file_id, left_end, right_start);

        // Base case: if left and right are the same
        if left_file_id == right_file_id {
            log::debug!(target: "normfs-lookup",
                "Binary search converged to file {} for target ID {} in queue '{}'",
                left_file_id, target_id, queue);
            return Ok(Some(left_file_id));
        }

        // Check if files are adjacent
        let next_left = left_file_id.increment();
        if next_left == right_file_id {
            // Files are adjacent, determine which one contains the target
            if let Some(left_end_val) = &left_end {
                if target_id <= left_end_val {
                    log::debug!(target: "normfs-lookup",
                        "Target ID {} is in left file {} (adjacent case) for queue '{}'",
                        target_id, left_file_id, queue);
                    return Ok(Some(left_file_id));
                }
            } else if target_id < &right_start {
                log::debug!(target: "normfs-lookup",
                    "Target ID {} is in left file {} (adjacent case, no left_end) for queue '{}'",
                    target_id, left_file_id, queue);
                return Ok(Some(left_file_id));
            }

            log::debug!(target: "normfs-lookup",
                "Target ID {} is in right file {} (adjacent case) for queue '{}'",
                target_id, right_file_id, queue);
            return Ok(Some(right_file_id));
        }

        // Calculate midpoint file ID using the middle method
        let mid_file_id = left_file_id.middle(&right_file_id);

        // Get the range for the middle file
        let mid_range = store.get_file_range(queue, &mid_file_id).await?;

        let (mid_start, mid_end) = match mid_range {
            Some((start, end)) => (start, Some(end)),
            None => {
                // Not in local store, check if it's a WAL file
                match wal.get_entries_before(queue, &mid_file_id).await {
                    Ok(start) => (start, None),
                    Err(WalError::WalNotFound) => {
                        // Not in WAL, check S3 if available
                        if let Some(s3) = cloud_downloader {
                            match s3.get_file_range(queue, &mid_file_id).await {
                                Ok(Some((start, end))) => {
                                    log::debug!(target: "normfs-lookup",
                                        "Found file range in S3 for queue '{}', file {}: {} to {}",
                                        queue, mid_file_id, start, end);
                                    (start, Some(end))
                                }
                                Ok(None) => {
                                    log::warn!(target: "normfs-lookup",
                                        "File {} not found in S3 for queue '{}'",
                                        mid_file_id, queue);
                                    return Err(LookupError::Store(StoreError::FileNotFound));
                                }
                                Err(e) => {
                                    log::warn!(target: "normfs-lookup",
                                        "S3 lookup failed for file {} in queue '{}': {}. Continuing without this file.",
                                        mid_file_id, queue, e);
                                    continue;
                                }
                            }
                        } else {
                            // No S3 and file doesn't exist — gap in file IDs.
                            // Walk backward to find nearest valid file.
                            let min_search = left_file_id.increment();
                            match find_valid_file_backward(
                                queue,
                                &mid_file_id,
                                &min_search,
                                store,
                                wal,
                            )
                            .await?
                            {
                                Some((valid_id, valid_start, valid_end)) => {
                                    right_file_id = valid_id;
                                    right_start = valid_start;
                                    left_end = valid_end;
                                    continue;
                                }
                                None => {
                                    return Ok(Some(left_file_id));
                                }
                            }
                        }
                    }
                    Err(WalError::WalEmpty(_)) => {
                        // Mid file is empty — walk backward to find nearest valid file.
                        // If none found between left and mid, target must be in left file.
                        let min_search = left_file_id.increment();
                        match find_valid_file_backward(queue, &mid_file_id, &min_search, store, wal)
                            .await?
                        {
                            Some((valid_id, valid_start, valid_end)) => {
                                // Use valid file as new right boundary
                                right_file_id = valid_id;
                                right_start = valid_start;
                                left_end = valid_end;
                                continue;
                            }
                            None => {
                                // No valid files between left and mid, target is in left file
                                return Ok(Some(left_file_id));
                            }
                        }
                    }
                    Err(e) => return Err(LookupError::Wal(e)),
                }
            }
        };

        // Determine which half to search
        if target_id < &mid_start {
            // Target is in the left half
            log::debug!(target: "normfs-lookup",
                "Target ID {} < mid start {}, searching left half for queue '{}'",
                target_id, mid_start, queue);
            right_file_id = mid_file_id;
            right_start = mid_start;
        } else if let Some(mid_end_val) = &mid_end {
            if target_id <= mid_end_val {
                // Target is in the middle file
                log::debug!(target: "normfs-lookup",
                    "Target ID {} found in mid file {} (start: {}, end: {}) for queue '{}'",
                    target_id, mid_file_id, mid_start, mid_end_val, queue);
                return Ok(Some(mid_file_id));
            } else {
                // Target is in the right half
                log::debug!(target: "normfs-lookup",
                    "Target ID {} > mid end {}, searching right half for queue '{}'",
                    target_id, mid_end_val, queue);
                left_file_id = mid_file_id;
                left_end = mid_end;
            }
        } else {
            // We don't know the end of the middle file, so we need to check the next file
            let next_mid = mid_file_id.increment();
            if next_mid <= right_file_id {
                // Get the start of the next file to determine if target is in mid or after
                let next_range = store.get_file_range(queue, &next_mid).await?;
                let next_start = match next_range {
                    Some((start, _)) => start,
                    None => {
                        // Not in local store, check WAL first
                        match wal.get_entries_before(queue, &next_mid).await {
                            Ok(start) => start,
                            Err(WalError::WalNotFound) => {
                                // Not in WAL, check S3 if available
                                if let Some(s3) = cloud_downloader {
                                    match s3.get_file_range(queue, &next_mid).await {
                                        Ok(Some((start, _))) => start,
                                        Ok(None) => {
                                            log::warn!(target: "normfs-lookup",
                                                "Next file {} not found in S3 for queue '{}'",
                                                next_mid, queue);
                                            return Err(LookupError::Store(
                                                StoreError::FileNotFound,
                                            ));
                                        }
                                        Err(e) => {
                                            log::warn!(target: "normfs-lookup",
                                                "S3 lookup failed for next file {} in queue '{}': {}. Assuming target is in current file.",
                                                next_mid, queue, e);
                                            return Ok(Some(mid_file_id));
                                        }
                                    }
                                } else {
                                    return Err(LookupError::Wal(WalError::WalNotFound));
                                }
                            }
                            Err(WalError::WalEmpty(_)) => {
                                // File exists locally but is empty, S3 won't have it
                                return Err(LookupError::Store(StoreError::FileNotFound));
                            }
                            Err(e) => return Err(LookupError::Wal(e)),
                        }
                    }
                };

                if target_id < &next_start {
                    // Target is in the middle file
                    log::debug!(target: "normfs-lookup",
                        "Target ID {} is in mid file {} (no end, next start: {}) for queue '{}'",
                        target_id, mid_file_id, next_start, queue);
                    return Ok(Some(mid_file_id));
                } else {
                    // Target is in the right half
                    log::debug!(target: "normfs-lookup",
                        "Target ID {} >= next start {}, searching right half for queue '{}'",
                        target_id, next_start, queue);
                    left_file_id = mid_file_id;
                    left_end = mid_end;
                }
            } else {
                // Middle file is the last file, so target must be in it
                log::debug!(target: "normfs-lookup",
                    "Mid file {} is the last file, target ID {} must be in it for queue '{}'",
                    mid_file_id, target_id, queue);
                return Ok(Some(mid_file_id));
            }
        }
    }
}
