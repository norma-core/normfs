use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uintn::UintN;

const NUM_CLIENTS: usize = 1000;
const PUBLISH_INTERVAL_MS: u64 = 10; // 10ms
const NUM_PUBLISHES: usize = 3000; // 30 seconds of publishing
const POLL_INTERVAL_MS: u64 = 1; // Poll every 1ms
const PROGRESS_INTERVAL: usize = 100; // Print progress every N publishes

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run_benchmark().await {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let bench_dir = PathBuf::from("/tmp/normfs-bench-parallel-tail");

    // Clean up and recreate
    if bench_dir.exists() {
        std::fs::remove_dir_all(&bench_dir)?;
    }
    std::fs::create_dir_all(&bench_dir)?;

    println!("NormFS Parallel Tail Read Benchmark");
    println!("====================================");
    println!("Parallel clients: {}", NUM_CLIENTS);
    println!("Publish interval: {}ms", PUBLISH_INTERVAL_MS);
    println!("Number of publishes: {}", NUM_PUBLISHES);
    println!("Client poll interval: {}ms", POLL_INTERVAL_MS);
    println!("Data directory: {:?}", bench_dir);
    println!();

    let settings = NormFsSettings::default();
    let normfs = NormFS::new(bench_dir.clone(), settings).await?;
    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("parallel_tail_queue");

    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    println!("Queue started. Publishing initial entry...");

    // Publish initial entry
    let initial_data = format!("entry-{}", 0);
    normfs.enqueue(&queue_name, Bytes::from(initial_data))?;

    println!("Starting {} parallel clients...", NUM_CLIENTS);

    // Shared state for tracking which entry each client has seen
    let last_seen = Arc::new(Mutex::new(vec![0usize; NUM_CLIENTS]));
    let read_latencies = Arc::new(Mutex::new(Vec::new()));

    // Spawn client tasks
    let mut client_handles = Vec::new();
    for client_id in 0..NUM_CLIENTS {
        let normfs_clone = normfs.clone();
        let queue_name_clone = queue_name.clone();
        let last_seen_clone = last_seen.clone();
        let read_latencies_clone = read_latencies.clone();

        let handle = tokio::spawn(async move {
            loop {
                let read_start = Instant::now();

                // Read latest 1 entry from tail
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                let read_result = normfs_clone
                    .read(
                        &queue_name_clone,
                        ReadPosition::ShiftFromTail(UintN::zero()),
                        1, // Read only 1 entry
                        1, // Batch size 1
                        tx,
                    )
                    .await;

                if read_result.is_ok() {
                    if let Some(entry) = rx.recv().await {
                        let read_duration = read_start.elapsed();

                        // Track read latency
                        let mut latencies = read_latencies_clone.lock().await;
                        latencies.push(read_duration);
                        drop(latencies);

                        // Parse entry number from data
                        if let Ok(data_str) = std::str::from_utf8(&entry.data) {
                            if let Some(num_str) = data_str.strip_prefix("entry-") {
                                if let Ok(entry_num) = num_str.parse::<usize>() {
                                    let mut seen = last_seen_clone.lock().await;
                                    seen[client_id] = entry_num;
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
        });

        client_handles.push(handle);
    }

    println!("All clients started. Waiting 2 seconds for stabilization...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\nStarting publish benchmark...\n");

    let mut propagation_latencies = Vec::new();

    let bench_start = Instant::now();

    for i in 1..=NUM_PUBLISHES {
        let entry_data = format!("entry-{}", i);

        let publish_time = Instant::now();
        normfs.enqueue(&queue_name, Bytes::from(entry_data))?;

        // Wait for all clients to see this entry
        let mut all_seen = false;
        while !all_seen {
            tokio::time::sleep(Duration::from_millis(10)).await;

            let seen = last_seen.lock().await;
            all_seen = seen.iter().all(|&v| v >= i);
        }

        let propagation_time = publish_time.elapsed();
        propagation_latencies.push(propagation_time);

        // Print progress every PROGRESS_INTERVAL entries
        if i % PROGRESS_INTERVAL == 0 || i == NUM_PUBLISHES {
            let elapsed = bench_start.elapsed();
            let progress_pct = (i as f64 / NUM_PUBLISHES as f64) * 100.0;
            println!(
                "[{:6.2}%] Published {} / {} entries | Elapsed: {:.1}s | Last propagation: {:.2}ms",
                progress_pct,
                i,
                NUM_PUBLISHES,
                elapsed.as_secs_f64(),
                propagation_time.as_secs_f64() * 1000.0
            );
        }

        // Wait for next publish interval
        if i < NUM_PUBLISHES {
            tokio::time::sleep(Duration::from_millis(PUBLISH_INTERVAL_MS)).await;
        }
    }

    // Abort all client tasks
    for handle in client_handles {
        handle.abort();
    }

    println!("\n================================================");
    println!("Benchmark Results");
    println!("================================================\n");

    // Propagation latency stats
    let avg_propagation: Duration =
        propagation_latencies.iter().sum::<Duration>() / propagation_latencies.len() as u32;
    let min_propagation = propagation_latencies.iter().min().unwrap();
    let max_propagation = propagation_latencies.iter().max().unwrap();

    println!(
        "Propagation Latency (time for all {} clients to receive entry):",
        NUM_CLIENTS
    );
    println!("  Average: {:.2}ms", avg_propagation.as_secs_f64() * 1000.0);
    println!("  Min: {:.2}ms", min_propagation.as_secs_f64() * 1000.0);
    println!("  Max: {:.2}ms", max_propagation.as_secs_f64() * 1000.0);
    println!();

    // Read latency stats
    let latencies = read_latencies.lock().await;
    let total_reads = latencies.len();

    if !latencies.is_empty() {
        let avg_read: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let mut sorted_latencies = latencies.clone();
        sorted_latencies.sort();

        let p50 = sorted_latencies[sorted_latencies.len() / 2];
        let p95 = sorted_latencies[(sorted_latencies.len() as f64 * 0.95) as usize];
        let p99 = sorted_latencies[(sorted_latencies.len() as f64 * 0.99) as usize];
        let min_read = sorted_latencies.iter().min().unwrap();
        let max_read = sorted_latencies.iter().max().unwrap();

        println!("Individual Read Latency (single tail read):");
        println!("  Total reads: {}", total_reads);
        println!("  Average: {:.2}µs", avg_read.as_micros());
        println!("  Min: {:.2}µs", min_read.as_micros());
        println!("  p50: {:.2}µs", p50.as_micros());
        println!("  p95: {:.2}µs", p95.as_micros());
        println!("  p99: {:.2}µs", p99.as_micros());
        println!("  Max: {:.2}µs", max_read.as_micros());
        println!();
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
