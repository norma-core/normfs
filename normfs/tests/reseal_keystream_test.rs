use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, QueueConfig, QueueSettings};
use normfs_crypto::CryptoContext;
use normfs_types::{CompressionType, QueueId};
use uintn::UintN;

fn settings(compression: CompressionType) -> NormFsSettings {
    let config = QueueConfig {
        compression_type: compression,
        ..QueueConfig::active()
    };
    NormFsSettings {
        queue_settings: QueueSettings::new(Vec::new(), config).unwrap(),
        ..NormFsSettings::all_active()
    }
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One start of the instance, which migrates the WAL file a previous start
/// left behind into the store.
async fn start_and_migrate(path: &Path, compression: CompressionType, queue: &QueueId) {
    let fs = NormFS::new(path.to_path_buf(), settings(compression))
        .await
        .unwrap();
    fs.ensure_queue_exists_for_write(queue).await.unwrap();
    let wal = wal_path(path, queue);
    wait_for("the leftover WAL file to be migrated", || !wal.exists()).await;
    fs.close().await.unwrap();
}

fn file_one() -> UintN {
    UintN::from(1u64)
}

fn wal_path(base: &Path, queue: &QueueId) -> PathBuf {
    file_one().to_file_path(&queue.to_wal_dir(base).to_string_lossy(), "wal")
}

/// The nonce, the ciphertext with its tag, and the plaintext they decrypt to.
fn open_sealed(path: &Path, queue: &QueueId) -> (Bytes, Bytes, Bytes) {
    let file = std::fs::read(queue.to_store_path(path, &file_one())).unwrap();
    let (header, at) = normfs_store::parser::parse_store_header(&file).unwrap();
    assert!(header.is_encrypted());
    let body = Bytes::copy_from_slice(&file[at..]);
    let (nonce, ciphertext) = (body.slice(..12), body.slice(12..));
    let plaintext = CryptoContext::open(path)
        .unwrap()
        .decrypt(queue, &file_one(), &nonce, &ciphertext)
        .unwrap();
    (nonce, ciphertext, plaintext)
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b).map(|(x, y)| x ^ y).collect()
}

/// A crash between the store `put` and the WAL delete -- or a delete that
/// fails, which is only logged -- leaves both files, and the next start
/// migrates the WAL file again under the compression configured *then*.
#[tokio::test]
async fn resealing_a_leftover_wal_file_does_not_reuse_the_keystream() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let path = path.as_path();

    let queue = {
        let fs = NormFS::new(path.to_path_buf(), settings(CompressionType::Zstd))
            .await
            .unwrap();
        let queue = fs.resolve("reseal");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        for i in 0..64 {
            fs.enqueue(&queue, Bytes::from(format!("record {i:04} ").repeat(32)))
                .await
                .unwrap();
        }
        fs.close().await.unwrap();
        queue
    };
    let wal_bytes = std::fs::read(wal_path(path, &queue)).unwrap();

    start_and_migrate(path, CompressionType::Zstd, &queue).await;
    let (nonce_a, ct_a, pt_a) = open_sealed(path, &queue);

    std::fs::write(wal_path(path, &queue), &wal_bytes).unwrap();
    start_and_migrate(path, CompressionType::None, &queue).await;
    let (nonce_b, ct_b, pt_b) = open_sealed(path, &queue);

    assert_ne!(pt_a, pt_b, "the two seals must encrypt different bytes");
    let n = pt_a.len().min(pt_b.len());
    assert!(
        nonce_a != nonce_b || xor(&ct_a[..n], &ct_b[..n]) != xor(&pt_a[..n], &pt_b[..n]),
        "store file 1 was sealed twice under one key and nonce: the two ciphertexts \
         XOR to the XOR of the two plaintexts"
    );
}
