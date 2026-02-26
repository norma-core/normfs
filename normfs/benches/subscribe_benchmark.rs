use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use uintn::UintN;

fn subscribe_latency_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("subscribe_latency", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let temp_dir = tempdir().unwrap();
                let path = temp_dir.path().to_path_buf();
                let settings = NormFsSettings::default();
                let normfs = Arc::new(NormFS::new(path, settings).await.unwrap());
                let queue_name = normfs.resolve("test_queue");
                normfs
                    .ensure_queue_exists_for_write(&queue_name)
                    .await
                    .unwrap();
                let mut total_duration = Duration::from_secs(0);

                for _ in 0..iters {
                    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

                    // Start subscription from the end (offset=0, backward, limit=0 for subscribe)
                    let normfs_clone = normfs.clone();
                    let queue_name_clone = queue_name.clone();
                    let subscribe_task = tokio::spawn(async move {
                        let _ = normfs_clone
                            .read(
                                &queue_name_clone,
                                ReadPosition::ShiftFromTail(UintN::zero()),
                                0,
                                1,
                                tx,
                            )
                            .await;
                    });

                    // Wait a bit for subscription to be active
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    let start_time = Instant::now();
                    normfs.enqueue(&queue_name, Bytes::from("hello")).unwrap();

                    if let Some(_entry) = rx.recv().await {
                        let end_time = Instant::now();
                        total_duration += end_time.duration_since(start_time);
                    }

                    subscribe_task.abort();
                }

                normfs.close().await.unwrap();
                total_duration
            })
        });
    });
}

criterion_group!(benches, subscribe_latency_benchmark);
criterion_main!(benches);
