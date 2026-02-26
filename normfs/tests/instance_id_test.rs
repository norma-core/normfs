use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use tempfile::TempDir;
use uintn::UintN;

#[tokio::test]
async fn test_instance_id_persists_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    // Create first NormFS instance
    let normfs1 = NormFS::new(temp_path.clone(), NormFsSettings::default())
        .await
        .unwrap();
    let instance_id_1 = normfs1.get_instance_id().to_string();
    println!("First instance ID: {}", instance_id_1);
    normfs1.close().await.unwrap();

    // Create second NormFS instance with same path
    let normfs2 = NormFS::new(temp_path.clone(), NormFsSettings::default())
        .await
        .unwrap();
    let instance_id_2 = normfs2.get_instance_id().to_string();
    println!("Second instance ID: {}", instance_id_2);

    assert_eq!(
        instance_id_1, instance_id_2,
        "Instance IDs should match when using same directory"
    );

    normfs2.close().await.unwrap();
}

#[tokio::test]
async fn test_queue_paths_use_instance_id() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    // Create NormFS and write some data
    let normfs = NormFS::new(temp_path.clone(), NormFsSettings::default())
        .await
        .unwrap();
    let instance_id = normfs.get_instance_id().to_string();

    let queue_id = normfs.resolve("test-queue");

    normfs
        .ensure_queue_exists_for_write(&queue_id)
        .await
        .unwrap();

    // Write an entry
    normfs.enqueue(&queue_id, Bytes::from("test data")).unwrap();
    normfs.close().await.unwrap();

    // Verify the WAL file was created in the correct path structure: instance_id/queue_name/wal/
    let expected_wal_path = temp_path.join(&instance_id).join("test-queue").join("wal");
    assert!(
        expected_wal_path.exists(),
        "WAL directory should exist at: {:?}",
        expected_wal_path
    );

    // Check that at least one WAL file exists
    let entries = std::fs::read_dir(&expected_wal_path).unwrap();
    let wal_files: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "wal")
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !wal_files.is_empty(),
        "At least one WAL file should exist in: {:?}",
        expected_wal_path
    );

    println!(
        "Found {} WAL file(s) in correct path: {:?}",
        wal_files.len(),
        expected_wal_path
    );
}

#[tokio::test]
async fn test_queue_data_readable_after_restart() {
    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    // First session: write data
    {
        let normfs = NormFS::new(temp_path.clone(), NormFsSettings::default())
            .await
            .unwrap();
        let queue_id = normfs.resolve("persistent-queue");

        normfs
            .ensure_queue_exists_for_write(&queue_id)
            .await
            .unwrap();

        // Write 10 entries
        for i in 0..10 {
            let data = Bytes::from(format!("entry-{}", i));
            normfs.enqueue(&queue_id, data).unwrap();
        }

        normfs.close().await.unwrap();
    }

    // Second session: read data back
    {
        let normfs = NormFS::new(temp_path.clone(), NormFsSettings::default())
            .await
            .unwrap();
        let queue_id = normfs.resolve("persistent-queue");

        normfs
            .ensure_queue_exists_for_write(&queue_id)
            .await
            .unwrap();

        // Read entries
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        normfs
            .read(&queue_id, ReadPosition::Absolute(UintN::zero()), 10, 1, tx)
            .await
            .unwrap();

        let mut entries_read = 0;
        while let Some(entry) = rx.recv().await {
            let expected_data = format!("entry-{}", entries_read);
            assert_eq!(
                String::from_utf8_lossy(&entry.data),
                expected_data,
                "Entry {} should have correct data",
                entries_read
            );
            entries_read += 1;
            if entries_read >= 10 {
                break;
            }
        }

        assert_eq!(entries_read, 10, "Should have read all 10 entries");

        normfs.close().await.unwrap();
    }
}
