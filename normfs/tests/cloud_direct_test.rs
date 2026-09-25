//! A cloud-direct queue (`Persist::CLOUD`) lands each sealed page as one
//! object in the bucket and keeps nothing on local disk but the pointer that
//! names its last file. Runs only against a reachable S3: set
//! `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
//! (`S3_BUCKET` defaults to `normfs-test`).

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use normfs::{
    CloudSettings, DataSource, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition,
};
use normfs_cloud::S3Client;
use normfs_wal::WalSettings;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

const PAGE: usize = 4096;
const RECORD_LEN: usize = 1000;
const PER_PAGE: u64 = 4;

fn cloud_settings() -> Option<CloudSettings> {
    let endpoint = std::env::var("S3_ENDPOINT_URL").ok()?;
    Some(CloudSettings {
        endpoint,
        bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "normfs-test".to_string()),
        region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var("AWS_ACCESS_KEY_ID").ok()?,
        secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok()?,
        // A prefix per run, so two runs never see each other's objects.
        prefix: format!(
            "cloud-direct-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    })
}

/// `None` skips the test: no S3 configured.
async fn s3() -> Option<CloudSettings> {
    let settings = cloud_settings()?;
    let client = S3Client::new(
        url::Url::parse(&settings.endpoint).unwrap(),
        settings.bucket.clone(),
        settings.region.clone(),
        settings.access_key.clone(),
        settings.secret_key.clone(),
    )
    .unwrap();
    client.create_bucket().await.expect("bucket");
    Some(settings)
}

fn settings(cloud: CloudSettings) -> NormFsSettings {
    NormFsSettings {
        mem_page_size: PAGE,
        max_memory_usage: 64 * 1024,
        wal_settings: WalSettings {
            write_interval: Duration::from_millis(20),
            flush_retry_delay: Duration::from_millis(10),
            ..Default::default()
        },
        cloud_settings: Some(cloud),
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::CLOUD),
        ..NormFsSettings::all_active()
    }
}

async fn open(root: &Path, settings: NormFsSettings) -> NormFS {
    NormFS::new(root.to_path_buf(), settings).await.unwrap()
}

async fn write(fs: &NormFS, queue: &normfs::QueueId, count: u64) {
    for _ in 0..count {
        fs.enqueue(queue, Bytes::from(vec![0x5A; RECORD_LEN]))
            .await
            .unwrap();
    }
}

async fn read_all(fs: &NormFS, queue: &normfs::QueueId, from: u64, count: u64) -> Vec<DataSource> {
    let (tx, mut rx) = mpsc::channel(count as usize + 1);
    fs.read(
        queue,
        ReadPosition::Absolute(UintN::from(from)),
        count,
        1,
        tx,
    )
    .await
    .unwrap();
    let mut sources = Vec::new();
    for i in from..from + count {
        let entry = timeout(Duration::from_secs(10), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived"))
            .expect("stream ended early");
        assert_eq!(entry.id.to_u64().unwrap(), i);
        assert_eq!(entry.data.len(), RECORD_LEN);
        sources.push(entry.source);
    }
    sources
}

fn nothing_local(root: &Path, queue: &normfs::QueueId) {
    assert!(!queue.to_wal_dir(root).exists());
    assert!(!queue.to_store_dir(root).exists());
}

#[tokio::test]
async fn a_full_page_becomes_one_object_and_nothing_touches_the_disk() {
    let Some(cloud) = s3().await else { return };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), settings(cloud.clone())).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        // More pages than memory holds, so early records must come back
        // from the bucket.
        write(&fs, &queue, 40 * PER_PAGE).await;
        fs.flush_queue(&queue).await.unwrap();
        nothing_local(temp.path(), &queue);

        let sources = read_all(&fs, &queue, 0, 40 * PER_PAGE).await;
        assert_eq!(sources[0], DataSource::Cloud);
        assert_eq!(*sources.last().unwrap(), DataSource::Memory);
        fs.close().await.unwrap();
    }
    let pointers = std::fs::read_to_string(temp.path().join(".memory_pointers")).unwrap();
    let line = pointers
        .lines()
        .find(|l| l.starts_with(queue.as_str()))
        .expect("the queue has a pointer");
    let cols: Vec<_> = line.split('\t').collect();
    assert_eq!(cols[1], (40 * PER_PAGE - 1).to_string(), "last id landed");
    assert_eq!(cols[2], "40", "last file landed");
}

#[tokio::test]
async fn a_restart_resumes_after_the_last_landed_file() {
    let Some(cloud) = s3().await else { return };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), settings(cloud.clone())).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    {
        let fs = open(temp.path(), settings(cloud.clone())).await;
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 2);
        let id = fs
            .enqueue(&queue, Bytes::from(vec![1u8; RECORD_LEN]))
            .await
            .unwrap();
        assert_eq!(id.to_u64().unwrap(), 3);
        fs.flush_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    // A third life with nothing in memory reads both objects, across the
    // boundary, and the ids are continuous.
    let fs = open(temp.path(), settings(cloud)).await;
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    assert_eq!(read_all(&fs, &queue, 0, 4).await, [DataSource::Cloud; 4]);
    nothing_local(temp.path(), &queue);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_lost_pointer_is_reconciled_from_the_bucket() {
    let Some(cloud) = s3().await else { return };
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), settings(cloud.clone())).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2 * PER_PAGE + 1).await;
        fs.flush_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }
    std::fs::remove_file(temp.path().join(".memory_pointers")).unwrap();

    let fs = open(temp.path(), settings(cloud)).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(
        fs.get_last_id(&queue).unwrap().to_u64().unwrap(),
        2 * PER_PAGE
    );
    let id = fs
        .enqueue(&queue, Bytes::from(vec![2u8; RECORD_LEN]))
        .await
        .unwrap();
    assert_eq!(id.to_u64().unwrap(), 2 * PER_PAGE + 1);
    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(
        read_all(&fs, &queue, 0, 2 * PER_PAGE + 2).await.len(),
        (2 * PER_PAGE + 2) as usize
    );
    fs.close().await.unwrap();
}
