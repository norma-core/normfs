use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, Persist, QueueSettings};
use std::time::Duration;

#[tokio::test]
async fn cloud_recovery_lands_nothing_while_listing_fails() {
    cloud_recovery_failure(false, 503).await;
}

#[tokio::test]
async fn cloud_recovery_lands_nothing_while_the_latest_range_fails() {
    cloud_recovery_failure(true, 503).await;
}

#[tokio::test]
async fn cloud_recovery_lands_nothing_while_the_latest_object_is_missing() {
    cloud_recovery_failure(true, 404).await;
}

async fn cloud_recovery_failure(list_succeeds: bool, object_status: u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let temp = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist::CLOUD);
    settings.wal_settings.flush_max_retries = 2;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(1);
    settings.cloud_settings = Some(normfs::CloudSettings {
        endpoint,
        bucket: "test".into(),
        region: "us-east-1".into(),
        access_key: "test".into(),
        secret_key: "test".into(),
        prefix: "test".into(),
    });
    let fs = NormFS::new(temp.path().to_path_buf(), settings.clone())
        .await
        .unwrap();
    let queue = fs.resolve("review");
    fs.close().await.unwrap();
    std::fs::write(
        temp.path().join(".memory_pointers"),
        format!("{}\t3\t1\n", queue.as_str()),
    )
    .unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let key = queue.to_cloud_key("test/", &uintn::UintN::from(2u64));
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = requests.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&buf[..n]);
            }
            let request = String::from_utf8_lossy(&request).to_string();
            let listing = request.contains("list-type=");
            seen.lock().unwrap().push(request);
            let (status, body) = if listing && list_succeeds {
                (200, format!("<ListBucketResult><Contents><Key>{key}</Key><Size>256</Size></Contents></ListBucketResult>"))
            } else {
                (object_status, String::new())
            };
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(fs.get_last_id(&queue).unwrap(), uintn::UintN::from(3u64));
    fs.enqueue(&queue, Bytes::from_static(b"accepted"))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), fs.flush_queue(&queue))
            .await
            .is_err(),
        "a stale pointer must not authorize reuse of an unchecked object key"
    );
    let requests = requests.lock().unwrap().clone();
    assert!(
        requests.len() >= 2,
        "the bucket is asked again before the first file"
    );
    assert!(requests.iter().all(|r| !r.starts_with("PUT")));
    assert!(fs.close().await.is_err());
}

#[tokio::test]
async fn batch_larger_than_pool_completes() {
    let temp = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 4096;
    settings.max_memory_usage = 8192;
    settings.wal_settings.write_interval = Duration::from_millis(10);
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("review");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        fs.enqueue_batch(&queue, vec![Bytes::from(vec![7; 1000]); 20]),
    )
    .await;
    let ids = result
        .expect("batch stalled although the WAL disk is writable")
        .unwrap();
    assert_eq!(ids, (0..20u64).map(uintn::UintN::from).collect::<Vec<_>>());
    fs.close().await.unwrap();
    let reopened = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    assert_records(&reopened, &queue, 20, &vec![7; 1000]).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn close_reports_store_failure() {
    let temp = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist::STORE);
    settings.wal_settings.flush_max_retries = 1;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(1);
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("review");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"accepted"))
        .await
        .unwrap();
    let store = queue.to_store_dir(temp.path());
    std::fs::create_dir_all(store.parent().unwrap()).unwrap();
    std::fs::write(&store, b"blocks the store directory").unwrap();
    assert!(matches!(
        fs.close().await,
        Err(normfs::Error::Store(normfs::StoreError::CloseIncomplete))
    ));
    assert!(fs.close().await.is_err());
    std::fs::remove_file(store).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fs.close().await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let reopened = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    assert_records(&reopened, &queue, 1, b"accepted").await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn close_budget_covers_sealed_pages() {
    let temp = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 4096;
    settings.max_memory_usage = 8192;
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist::STORE);
    settings.wal_settings.flush_max_retries = 1;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(1);
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("review");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let store = queue.to_store_dir(temp.path());
    std::fs::create_dir_all(store.parent().unwrap()).unwrap();
    std::fs::write(&store, b"blocks the store directory").unwrap();
    for _ in 0..5 {
        fs.enqueue(&queue, Bytes::from(vec![7; 1000]))
            .await
            .unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(2), fs.close_queue(&queue)).await;
    assert!(matches!(
        result.expect("close exceeded its retry budget"),
        Err(normfs::Error::Wal(normfs::WalError::CloseIncomplete))
    ));
    assert!(!queue.to_fs_path(temp.path()).join("closed").exists());
    assert!(fs.ensure_queue_exists_for_write(&queue).await.is_err());
    std::fs::remove_file(store).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fs.close_queue(&queue).await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    fs.close().await.unwrap();
    let reopened = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    assert_records(&reopened, &queue, 5, &vec![7; 1000]).await;
    reopened.close().await.unwrap();
}

async fn assert_records(fs: &NormFS, queue: &normfs::QueueId, count: u64, payload: &[u8]) {
    fs.ensure_queue_exists_for_read(queue).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(count as usize + 1);
    fs.read(
        queue,
        normfs::ReadPosition::Absolute(uintn::UintN::zero()),
        count,
        1,
        tx,
    )
    .await
    .unwrap();
    for id in 0..count {
        let record = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.id, uintn::UintN::from(id));
        assert_eq!(record.data.as_ref(), payload);
    }
}

#[tokio::test]
async fn a_cloud_outage_is_a_cloud_error_not_a_missing_queue() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let temp = tempfile::tempdir().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.queue_settings = QueueSettings::all_active().with_default_persist(Persist::CLOUD);
    settings.cloud_settings = Some(normfs::CloudSettings {
        endpoint,
        bucket: "test".into(),
        region: "us-east-1".into(),
        access_key: "test".into(),
        secret_key: "test".into(),
        prefix: "test".into(),
    });
    let queue = normfs_types::QueueIdResolver::new("0000").resolve("/outage/q");
    std::fs::write(
        temp.path().join(".memory_pointers"),
        format!("{}\t3\t1\n", queue.as_str()),
    )
    .unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();

    // The start-up listing finds nothing past the pointer; every request
    // after it fails.
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let (status, body) = if std::mem::take(&mut first) {
                (200, "<ListBucketResult></ListBucketResult>")
            } else {
                (503, "")
            };
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });

    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(4);
    let err = fs
        .read(
            &queue,
            normfs::ReadPosition::Absolute(uintn::UintN::zero()),
            1,
            1,
            tx,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, normfs::Error::Cloud(_)), "got {err:?}");
    fs.close().await.unwrap();
}
