use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, PersistenceMode, ReadPosition};
use std::time::Duration;
use tempfile::TempDir;
use uintn::UintN;

fn memory_only_settings() -> NormFsSettings {
    NormFsSettings {
        persistence_mode: PersistenceMode::MemoryOnly,
        memory_pointers_flush_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

#[tokio::test]
async fn memory_only_persists_latest_pointer_without_wal_or_store() {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path().to_path_buf();
    let settings = memory_only_settings();

    {
        let fs = NormFS::new(root.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        let first = fs.enqueue(&queue, Bytes::from_static(b"one")).unwrap();
        let second = fs.enqueue(&queue, Bytes::from_static(b"two")).unwrap();

        assert_eq!(first.to_u64().unwrap(), 0);
        assert_eq!(second.to_u64().unwrap(), 1);
        assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 1);

        fs.close().await.unwrap();

        assert!(root.join(".crypto_seed").exists());
        assert!(root.join(".memory_pointers").exists());
        assert!(!queue.to_wal_dir(&root).exists());
        assert!(!queue.to_store_dir(&root).exists());
    }

    {
        let fs = NormFS::new(root.clone(), settings).await.unwrap();
        let queue = fs.resolve("rover/events");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 1);
        let next = fs.enqueue(&queue, Bytes::from_static(b"three")).unwrap();
        assert_eq!(next.to_u64().unwrap(), 2);

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
        fs.enqueue(&queue, Bytes::from_static(b"live")).unwrap();

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
        assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 0);

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let err = fs
            .read(&queue, ReadPosition::Absolute(UintN::zero()), 1, 1, tx)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound));
        assert!(rx.try_recv().is_err());

        fs.close().await.unwrap();
    }
}
