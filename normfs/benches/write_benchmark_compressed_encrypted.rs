use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

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
    let bench_dir = PathBuf::from("/tmp/normfs-bench-compressed-encrypted");

    // Create directory if it doesn't exist
    std::fs::create_dir_all(&bench_dir)?;

    println!("NormFS Write Benchmark (Compression + Encryption)");
    println!("==================================================");
    println!("Block size: {} KB", BLOCK_SIZE / 1024);
    println!("Total size: {} GB", TOTAL_SIZE / (1024 * 1024 * 1024));
    println!("Total blocks: {}", TOTAL_BLOCKS);
    println!("Compression: ENABLED (Zstd)");
    println!("Encryption: ENABLED (ChaCha20-Poly1305)");
    println!("Data directory: {:?}", bench_dir);
    println!();

    let settings = NormFsSettings {
        max_disk_usage_per_queue: Some(1024 * 1024 * 1024), // 1GB per queue
        ..Default::default()
    };
    let normfs = NormFS::new(bench_dir.clone(), settings).await?;

    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("write_bench_queue_compressed_encrypted");

    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    // Create a 12KB block of random data (hard to compress)
    let mut block_data = vec![0u8; BLOCK_SIZE];
    fastrand::seed(42); // Use fixed seed for reproducibility
    for byte in block_data.iter_mut() {
        *byte = fastrand::u8(..);
    }
    let block = Bytes::from(block_data);

    println!("Starting write benchmark...");
    let start_time = Instant::now();
    let mut last_progress_time = start_time;
    let mut last_progress_blocks = 0;

    for i in 0..TOTAL_BLOCKS {
        normfs.enqueue(&queue_name, block.clone())?;

        if (i + 1) % PROGRESS_INTERVAL == 0 || i == TOTAL_BLOCKS - 1 {
            let current_time = Instant::now();
            let total_elapsed = current_time.duration_since(start_time);
            let interval_elapsed = current_time.duration_since(last_progress_time);

            let total_bytes_written = (i + 1) * BLOCK_SIZE;
            let interval_bytes = (i + 1 - last_progress_blocks) * BLOCK_SIZE;

            let total_mb = total_bytes_written as f64 / (1024.0 * 1024.0);
            let total_gb = total_mb / 1024.0;
            let progress_pct = (i + 1) as f64 / TOTAL_BLOCKS as f64 * 100.0;

            let total_speed_mbps = total_mb / total_elapsed.as_secs_f64();
            let interval_speed_mbps =
                (interval_bytes as f64 / (1024.0 * 1024.0)) / interval_elapsed.as_secs_f64();

            println!(
                "[{:6.2}%] Written: {:.2} GB | Total speed: {:.2} MB/s | Current speed: {:.2} MB/s | Elapsed: {:.1}s",
                progress_pct,
                total_gb,
                total_speed_mbps,
                interval_speed_mbps,
                total_elapsed.as_secs_f64()
            );

            last_progress_time = current_time;
            last_progress_blocks = i + 1;
        }
    }

    let total_elapsed = start_time.elapsed();
    let total_mb = TOTAL_SIZE as f64 / (1024.0 * 1024.0);
    let avg_speed_mbps = total_mb / total_elapsed.as_secs_f64();

    println!();
    println!("Write benchmark completed!");
    println!("==================================================");
    println!("Total time: {:.2} seconds", total_elapsed.as_secs_f64());
    println!("Average speed: {:.2} MB/s", avg_speed_mbps);
    println!("Total blocks written: {}", TOTAL_BLOCKS);
    println!();

    println!("Closing NormFS...");
    let normfs = Arc::try_unwrap(normfs).unwrap_or_else(|arc| {
        panic!(
            "Failed to unwrap Arc, {} references remaining",
            Arc::strong_count(&arc)
        );
    });
    normfs.close().await?;

    println!("NormFS closed successfully.");
    println!("Data persisted at: {:?}", bench_dir);
    println!("This data can be used for read benchmarks.");

    Ok(())
}
