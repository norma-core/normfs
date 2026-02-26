use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use uintn::UintN;

const BLOCK_SIZE: usize = 12 * 1024; // 12KB
const TOTAL_SIZE: usize = 50 * 1024 * 1024 * 1024; // 50GB
const TOTAL_BLOCKS: usize = TOTAL_SIZE / BLOCK_SIZE;
const PROGRESS_INTERVAL: usize = 10_000; // Print progress every 10k blocks

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run_benchmark().await {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let bench_dir = PathBuf::from("/tmp/normfs-bench");

    if !bench_dir.exists() {
        eprintln!(
            "Error: Benchmark data directory does not exist: {:?}",
            bench_dir
        );
        eprintln!("Please run the write benchmark first to generate data.");
        std::process::exit(1);
    }

    println!("NormFS Read Benchmark");
    println!("========================");
    println!("Block size: {} KB", BLOCK_SIZE / 1024);
    println!("Total size: {} GB", TOTAL_SIZE / (1024 * 1024 * 1024));
    println!("Total blocks: {}", TOTAL_BLOCKS);
    println!("Data directory: {:?}", bench_dir);
    println!();

    let settings = NormFsSettings::default();
    let normfs = NormFS::new(bench_dir.clone(), settings).await?;

    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("write_bench_queue");

    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    println!("Reading {} total blocks", TOTAL_BLOCKS);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<normfs::ReadEntry>(1000);

    let start_offset = UintN::from(0u64);
    let limit = TOTAL_BLOCKS as u64;

    println!("Starting read benchmark...");
    let start_time = Instant::now();
    let mut last_progress_time = start_time;
    let mut last_progress_blocks = 0;

    let normfs_clone = normfs.clone();
    let read_task = tokio::spawn(async move {
        normfs_clone
            .read(
                &queue_name,
                ReadPosition::Absolute(start_offset.clone()),
                limit,
                1,
                tx,
            )
            .await
    });

    let mut entries_read = 0u64;
    let mut expected_id: Option<UintN> = None;
    let mut bytes_read = 0usize;

    while let Some(entry) = rx.recv().await {
        // On first entry, initialize expected_id
        if expected_id.is_none() {
            println!("Queue starts at entry ID: {}", entry.id);
            expected_id = Some(entry.id.clone());
        }

        if let Some(ref exp_id) = expected_id {
            if &entry.id != exp_id {
                panic!(
                    "Order corruption detected! Expected entry ID: {}, but got: {}",
                    exp_id, entry.id
                );
            }
        }

        if entry.data.len() != BLOCK_SIZE {
            panic!(
                "Unexpected data size! Expected: {} bytes, but got: {} bytes for entry ID: {}",
                BLOCK_SIZE,
                entry.data.len(),
                entry.id
            );
        }

        expected_id = Some(entry.id.increment());
        entries_read += 1;
        bytes_read += entry.data.len();

        if entries_read.is_multiple_of(PROGRESS_INTERVAL as u64)
            || entries_read == TOTAL_BLOCKS as u64
        {
            let current_time = Instant::now();
            let total_elapsed = current_time.duration_since(start_time);
            let interval_elapsed = current_time.duration_since(last_progress_time);

            let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
            let total_gb = total_mb / 1024.0;
            let progress_pct = entries_read as f64 / TOTAL_BLOCKS as f64 * 100.0;

            let interval_bytes = (entries_read - last_progress_blocks) as usize * BLOCK_SIZE;
            let total_speed_mbps = total_mb / total_elapsed.as_secs_f64();
            let interval_speed_mbps =
                (interval_bytes as f64 / (1024.0 * 1024.0)) / interval_elapsed.as_secs_f64();

            println!(
                "[{:6.2}%] Read: {:.2} GB | Total speed: {:.2} MB/s | Current speed: {:.2} MB/s | Entries: {} | Elapsed: {:.1}s",
                progress_pct,
                total_gb,
                total_speed_mbps,
                interval_speed_mbps,
                entries_read,
                total_elapsed.as_secs_f64()
            );

            last_progress_time = current_time;
            last_progress_blocks = entries_read;
        }
    }

    read_task.await??;

    let total_elapsed = start_time.elapsed();
    let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
    let avg_speed_mbps = total_mb / total_elapsed.as_secs_f64();

    println!();
    println!("Read benchmark completed!");
    println!("========================");
    println!("Total time: {:.2} seconds", total_elapsed.as_secs_f64());
    println!("Average speed: {:.2} MB/s", avg_speed_mbps);
    println!("Total entries read: {}", entries_read);
    println!("Total bytes read: {:.2} GB", total_mb / 1024.0);
    println!("Data integrity: ✓ All entry IDs are in correct sequential order");
    println!();

    if entries_read != TOTAL_BLOCKS as u64 {
        eprintln!(
            "WARNING: Expected {} entries but read {}",
            TOTAL_BLOCKS, entries_read
        );
    }

    println!("Closing NormFS...");
    let normfs = Arc::try_unwrap(normfs).unwrap_or_else(|arc| {
        panic!(
            "Failed to unwrap Arc, {} references remaining",
            Arc::strong_count(&arc)
        );
    });
    normfs.close().await?;

    println!("NormFS closed successfully.");

    Ok(())
}
