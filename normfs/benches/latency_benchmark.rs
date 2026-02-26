use bytes::Bytes;
use core::time::Duration;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::sleep;
use uintn::UintN;

#[tokio::main]
async fn main() {
    if let Err(e) = run_benchmark().await {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize NormFS with temporary directory
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().to_path_buf();
    let settings = NormFsSettings::default();
    let normfs = NormFS::new(temp_path, settings).await?;

    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("latency_test_queue");

    // Start the queue
    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    // Channel for latency measurements
    let (latency_tx, mut latency_rx) = mpsc::unbounded_channel::<Duration>();

    // Statistics tracking
    let mut latencies = Vec::new();
    let test_duration = Duration::from_secs(120); // Run test for 120 seconds
    let start_time = Instant::now();

    // Writer coroutine - writes timestamps to the queue
    let writer_normfs = normfs.clone();
    let writer_queue = queue_name.clone();
    let writer_handle = tokio::spawn(async move {
        let mut count = 0u64;
        while start_time.elapsed() < test_duration {
            // Get current timestamp in nanoseconds
            let timestamp_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            // Write timestamp to queue
            let data = Bytes::from(timestamp_ns.to_le_bytes().to_vec());
            if let Err(e) = writer_normfs.enqueue(&writer_queue, data) {
                eprintln!("Write error: {:?}", e);
                break;
            }

            count += 1;

            // Write at approximately 1000 messages per second
            sleep(Duration::from_micros(1000)).await;
        }
        println!("Writer finished. Wrote {} messages", count);
    });

    // Reader coroutine - reads entries and measures latency
    let reader_normfs = normfs.clone();
    let reader_queue = queue_name.clone();
    let reader_handle = tokio::spawn(async move {
        let mut last_read_id: Option<UintN> = None;
        let mut count = 0u64;

        while start_time.elapsed() < test_duration {
            // Determine the starting point for reading
            let from_id = match &last_read_id {
                Some(id) => id.increment(),
                None => UintN::one(),
            };

            // Create channel for receiving entries
            let (tx, mut rx) = mpsc::channel::<normfs::ReadEntry>(1000);

            // Start reading in a separate task - read up to 1000 entries
            let read_normfs = reader_normfs.clone();
            let read_queue = reader_queue.clone();
            let read_task = tokio::spawn(async move {
                if let Err(e) = read_normfs
                    .read(
                        &read_queue,
                        ReadPosition::Absolute(from_id.clone()),
                        1000,
                        1,
                        tx,
                    )
                    .await
                {
                    eprintln!("Read error: {:?}", e);
                }
            });

            // Process received entries
            while let Some(entry) = rx.recv().await {
                // Extract timestamp from data
                if entry.data.len() == 8 {
                    let mut timestamp_bytes = [0u8; 8];
                    timestamp_bytes.copy_from_slice(&entry.data);
                    let write_timestamp_ns = u64::from_le_bytes(timestamp_bytes);

                    // Calculate latency
                    let current_timestamp_ns = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if current_timestamp_ns >= write_timestamp_ns {
                        let latency_ns = current_timestamp_ns - write_timestamp_ns;
                        let latency = Duration::from_nanos(latency_ns);

                        // Send latency measurement
                        let _ = latency_tx.send(latency);

                        count += 1;
                    }
                }

                last_read_id = Some(entry.id.clone());
            }

            // Wait for read task to complete
            let _ = read_task.await;

            // Sleep before next read cycle
            sleep(Duration::from_nanos(1)).await;
        }

        println!("Reader finished. Read {} messages", count);
    });

    // Collector coroutine - collects and analyzes latency measurements
    let collector_handle = tokio::spawn(async move {
        while let Some(latency) = latency_rx.recv().await {
            latencies.push(latency);

            // Print periodic updates
            if latencies.len() % 1000 == 0 {
                let avg_latency = calculate_average(&latencies);
                let p50 = calculate_percentile(&mut latencies, 0.50);
                let p99 = calculate_percentile(&mut latencies, 0.99);

                println!(
                    "Samples: {}, Avg: {:?}, P50: {:?}, P99: {:?}",
                    latencies.len(),
                    avg_latency,
                    p50,
                    p99
                );
            }
        }

        // Final statistics
        if !latencies.is_empty() {
            println!("\n=== Final Latency Statistics ===");
            println!("Total samples: {}", latencies.len());
            println!("Average latency: {:?}", calculate_average(&latencies));
            println!("Min latency: {:?}", latencies.iter().min().unwrap());
            println!("Max latency: {:?}", latencies.iter().max().unwrap());
            println!(
                "P50 latency: {:?}",
                calculate_percentile(&mut latencies, 0.50)
            );
            println!(
                "P90 latency: {:?}",
                calculate_percentile(&mut latencies, 0.90)
            );
            println!(
                "P95 latency: {:?}",
                calculate_percentile(&mut latencies, 0.95)
            );
            println!(
                "P99 latency: {:?}",
                calculate_percentile(&mut latencies, 0.99)
            );
            println!(
                "P99.9 latency: {:?}",
                calculate_percentile(&mut latencies, 0.999)
            );
        }
    });

    // Wait for writer and reader to complete
    let _ = tokio::join!(writer_handle, reader_handle);

    // Wait for collector to finish (latency_tx is already moved into reader_handle)
    let _ = collector_handle.await;

    // Cleanup
    let normfs = Arc::try_unwrap(normfs).unwrap_or_else(|arc| {
        panic!(
            "Failed to unwrap Arc, {} references remaining",
            Arc::strong_count(&arc)
        );
    });
    normfs.close().await?;

    Ok(())
}

fn calculate_average(latencies: &[Duration]) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }

    let total_nanos: u128 = latencies.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((total_nanos / latencies.len() as u128) as u64)
}

fn calculate_percentile(latencies: &mut [Duration], percentile: f64) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }

    latencies.sort();
    let index = ((latencies.len() as f64 - 1.0) * percentile) as usize;
    latencies[index]
}
