use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use normfs_fs::{Fs, FsConfig};
use normfs_store::{local_store, Layer, SealedFile, SealedFileSink};
use normfs_types::QueueId;
use uintn::UintN;

use crate::bucket_check::BucketCheck;
use crate::memory_pointers::MemoryPointers;
use crate::{NormFS, NormFsSettings, Persist, QueueSettings};

struct Nowhere;

impl SealedFileSink for Nowhere {
    fn land<'a>(
        &'a self,
        _queue: &'a QueueId,
        _file_id: &'a UintN,
        _file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

#[tokio::test]
async fn the_first_free_file_holds_after_a_file_that_did_not_build() {
    let bucket = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 4096;
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist::STORE);
    let fs = NormFS::new(bucket.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    for _ in 0..5 {
        fs.enqueue(&queue, Bytes::from(vec![1; 1000]))
            .await
            .unwrap();
    }
    fs.flush_queue(&queue).await.unwrap();
    fs.close().await.unwrap();

    let files = Fs::new(FsConfig::default()).unwrap();
    let cloud = Layer::new(
        Arc::new(local_store(
            files.clone(),
            bucket.path(),
            false,
            Arc::default(),
        )),
        None,
        false,
    );
    assert_eq!(
        cloud.last_file_id(&queue).await.unwrap(),
        Some(UintN::from(2u64))
    );
    let local = tempfile::tempdir().unwrap();
    let pointers = MemoryPointers::open(files, local.path()).await.unwrap();
    let check = BucketCheck::new(
        Arc::new(Nowhere),
        Arc::new(cloud),
        Arc::new(pointers),
        Some(UintN::from(4u64)),
    );

    let planned = UintN::from(2u64);
    assert_eq!(
        check.file_id(&queue, &planned).await.unwrap(),
        UintN::from(3u64)
    );
    // The writer keeps its own id when a file fails to build, and asks again.
    assert_eq!(
        check.file_id(&queue, &planned).await.unwrap(),
        UintN::from(3u64)
    );
    assert_eq!(
        check.file_id(&queue, &UintN::from(4u64)).await.unwrap(),
        UintN::from(4u64)
    );
}
