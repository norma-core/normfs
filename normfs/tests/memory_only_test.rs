use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition};
use std::time::Duration;
use tempfile::TempDir;
use uintn::UintN;

fn memory_only_settings() -> NormFsSettings {
    NormFsSettings {
        queue_settings: QueueSettings::default().with_default_persist(Persist::MEMORY),
        memory_pointers_flush_interval: Duration::from_millis(10),
        ..NormFsSettings::default()
    }
}

#[tokio::test]
async fn memory_only_writes_nothing_to_disk_and_restarts_empty() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_path_buf();
    let settings = memory_only_settings();

    {
        let fs = NormFS::new(root.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        let first = fs
            .enqueue(&queue, Bytes::from_static(b"one"))
            .await
            .unwrap();
        let second = fs
            .enqueue(&queue, Bytes::from_static(b"two"))
            .await
            .unwrap();

        assert_eq!(first.to_u64().unwrap(), 0);
        assert_eq!(second.to_u64().unwrap(), 1);
        assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 1);

        fs.close().await.unwrap();

        assert!(root.join(".crypto_seed").exists());
        assert!(!root.join(".memory_pointers").exists());
        assert!(!queue.to_wal_dir(&root).exists());
        assert!(!queue.to_store_dir(&root).exists());
    }

    {
        let fs = NormFS::new(root.clone(), settings).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        assert!(matches!(fs.get_last_id(&queue), Err(Error::QueueEmpty)));
        let next = fs
            .enqueue(&queue, Bytes::from_static(b"three"))
            .await
            .unwrap();
        assert_eq!(next.to_u64().unwrap(), 0);

        fs.close().await.unwrap();
    }
}

#[tokio::test]
async fn memory_only_reader_does_not_fallback_to_disk_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_path_buf();
    let settings = memory_only_settings();

    {
        let fs = NormFS::new(root.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        fs.enqueue(&queue, Bytes::from_static(b"live"))
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let subscribed = fs
            .read(&queue, ReadPosition::Absolute(UintN::zero()), 1, 1, tx)
            .await
            .unwrap();
        assert!(!subscribed);
        let entry = rx.recv().await.unwrap();
        assert_eq!(entry.id.to_u64().unwrap(), 0);
        assert_eq!(&entry.data[..], b"live");

        fs.close().await.unwrap();
    }

    {
        let fs = NormFS::new(root.clone(), settings).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_read(&queue).await.unwrap();
        assert!(matches!(fs.get_last_id(&queue), Err(Error::QueueEmpty)));

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let subscribed = fs
            .read(&queue, ReadPosition::Absolute(UintN::zero()), 1, 1, tx)
            .await
            .unwrap();
        assert!(!subscribed);
        assert!(rx.try_recv().is_err());

        fs.close().await.unwrap();
    }
}
