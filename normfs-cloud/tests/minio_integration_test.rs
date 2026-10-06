use bytes::Bytes;
use normfs_cloud::{CloudSettings, S3Client};
use std::env;

/// Helper function to get cloud settings from standard AWS environment variables
fn get_cloud_settings() -> Option<CloudSettings> {
    let endpoint = env::var("S3_ENDPOINT_URL").ok()?;
    let bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "stations".to_string());
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key = env::var("AWS_ACCESS_KEY_ID").ok()?;
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok()?;
    let prefix = env::var("S3_PREFIX").unwrap_or_else(|_| "cloud".to_string());

    Some(CloudSettings {
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        prefix,
    })
}

/// Helper to skip test if S3/MinIO is not configured
fn skip_if_no_s3() -> Option<CloudSettings> {
    match get_cloud_settings() {
        Some(settings) => {
            println!("✓ S3/MinIO settings found");
            println!("  Endpoint: {}", settings.endpoint);
            println!("  Bucket: {}", settings.bucket);
            println!("  Region: {}", settings.region);
            println!("  Prefix: {}", settings.prefix);
            Some(settings)
        }
        None => {
            println!("⊘ Skipping S3 integration test - not configured");
            println!("  Set these environment variables to enable:");
            println!("    S3_ENDPOINT_URL (e.g., http://localhost:9000)");
            println!("    AWS_ACCESS_KEY_ID");
            println!("    AWS_SECRET_ACCESS_KEY");
            println!("    S3_BUCKET (optional, default: stations)");
            println!("    AWS_REGION (optional, default: us-east-1)");
            println!("    S3_PREFIX (optional, default: cloud)");
            None
        }
    }
}

/// Helper to create S3Client from CloudSettings
fn create_client(settings: &CloudSettings) -> Result<S3Client, Box<dyn std::error::Error>> {
    let endpoint = url::Url::parse(&settings.endpoint)?;
    S3Client::new(
        endpoint,
        settings.bucket.clone(),
        settings.region.clone(),
        settings.access_key.clone(),
        settings.secret_key.clone(),
    )
}

#[tokio::test]
async fn test_s3_client_creation() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: S3Client creation");
    let client = create_client(&settings);
    assert!(
        client.is_ok(),
        "Failed to create S3Client: {:?}",
        client.err()
    );
    println!("✓ S3Client created successfully");
}

#[tokio::test]
async fn test_put_get_basic() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Basic put and get");

    let client = create_client(&settings).unwrap();

    let test_key = format!(
        "{}/test-put-basic-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );
    let test_data = b"Hello MinIO from normfs-cloud!";

    println!("Putting object to key: {}", test_key);
    let put_result = client
        .put_object(&test_key, Bytes::from_static(test_data))
        .await;
    assert!(put_result.is_ok(), "Put failed: {:?}", put_result.err());
    let status = put_result.unwrap();
    println!("✓ Put successful (status: {})", status);
    assert!(
        status == 200 || status == 201,
        "Expected 200/201 status, got {}",
        status
    );

    println!("Getting object from key: {}", test_key);
    let get_result = client.get_object(&test_key).await;
    assert!(get_result.is_ok(), "Get failed: {:?}", get_result.err());

    let downloaded_data = get_result.unwrap();
    assert!(downloaded_data.is_some(), "Expected data but got None");
    let downloaded_data = downloaded_data.unwrap();
    assert_eq!(
        downloaded_data,
        Bytes::from(&test_data[..]),
        "Downloaded data doesn't match"
    );
    println!(
        "✓ Get successful, data matches ({} bytes)",
        downloaded_data.len()
    );
}

#[tokio::test]
async fn test_put_get_large_file() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Large file put and get");

    let client = create_client(&settings).unwrap();

    let test_key = format!(
        "{}/test-large-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );

    // Create 10MB of data
    let data_size = 10 * 1024 * 1024;
    let test_data = vec![42u8; data_size];

    println!("Putting {} MB file...", data_size / (1024 * 1024));
    let put_result = client
        .put_object(&test_key, Bytes::from(test_data.clone()))
        .await;
    assert!(put_result.is_ok(), "Put failed: {:?}", put_result.err());
    println!("✓ Put successful");

    println!("Getting {} MB file...", data_size / (1024 * 1024));
    let get_result = client.get_object(&test_key).await;
    assert!(get_result.is_ok(), "Get failed: {:?}", get_result.err());

    let downloaded_data = get_result.unwrap();
    assert!(downloaded_data.is_some(), "Expected data but got None");
    let downloaded_data = downloaded_data.unwrap();
    assert_eq!(
        downloaded_data.len(),
        data_size,
        "Downloaded size doesn't match"
    );
    assert_eq!(
        downloaded_data,
        Bytes::from(test_data),
        "Downloaded data doesn't match"
    );
    println!("✓ Get successful, {} bytes verified", downloaded_data.len());
}

#[tokio::test]
async fn test_put_object_stream_sends_every_chunk() {
    let Some(settings) = skip_if_no_s3() else {
        return;
    };
    let client = create_client(&settings).unwrap();
    let key = format!(
        "{}/test-stream-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );

    // Not a multiple of the read size, so the last chunk is a short one.
    let data: Vec<u8> = (0..3 * 1024 * 1024 + 4321u32)
        .map(|i| (i % 251) as u8)
        .collect();
    let status = client
        .put_object_stream(&key, std::io::Cursor::new(data.clone()), data.len() as u64)
        .await
        .unwrap();
    assert_eq!(status, 200);

    assert_eq!(
        client.head_object(&key).await.unwrap(),
        Some(data.len() as u64)
    );
    let got = client.get_object(&key).await.unwrap().expect("object");
    assert_eq!(got, Bytes::from(data));
}

#[tokio::test]
async fn test_put_get_multiple_files() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Multiple files put and get");

    let client = create_client(&settings).unwrap();

    let num_files = 5;
    let mut keys = Vec::new();

    // Put multiple files
    println!("Putting {} files...", num_files);
    for i in 0..num_files {
        let key = format!(
            "{}/test-multi-{}-{}.dat",
            settings.prefix,
            i,
            uuid::Uuid::new_v4()
        );
        let data = format!("Test data for file {}", i);

        let put_result = client.put_object(&key, Bytes::from(data.clone())).await;
        assert!(
            put_result.is_ok(),
            "Put {} failed: {:?}",
            i,
            put_result.err()
        );
        keys.push((key, data));
        println!("  ✓ Put file {}/{}", i + 1, num_files);
    }

    // Get and verify each file
    println!("Getting {} files...", num_files);
    for (i, (key, expected_data)) in keys.iter().enumerate() {
        let get_result = client.get_object(key).await;
        assert!(
            get_result.is_ok(),
            "Get {} failed: {:?}",
            i,
            get_result.err()
        );

        let downloaded_data = get_result.unwrap();
        assert!(
            downloaded_data.is_some(),
            "Expected data but got None for file {}",
            i
        );
        let downloaded_data = downloaded_data.unwrap();
        assert_eq!(
            downloaded_data.as_ref(),
            expected_data.as_bytes(),
            "Data mismatch for file {}",
            i
        );
        println!("  ✓ Got and verified file {}/{}", i + 1, num_files);
    }
    println!("✓ All files verified");
}

#[tokio::test]
async fn test_get_nonexistent_file() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Get nonexistent file");

    let client = create_client(&settings).unwrap();

    let nonexistent_key = format!(
        "{}/nonexistent-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );

    println!("Attempting to get nonexistent key: {}", nonexistent_key);
    let get_result = client.get_object(&nonexistent_key).await;
    assert!(
        get_result.is_ok(),
        "Get should succeed but return None for nonexistent file"
    );

    let data = get_result.unwrap();
    assert!(
        data.is_none(),
        "Expected None for nonexistent file, got Some"
    );
    println!("✓ Correctly returned None for nonexistent file");
}

#[tokio::test]
async fn test_head_object() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Head object");

    let client = create_client(&settings).unwrap();

    let test_key = format!("{}/test-head-{}.dat", settings.prefix, uuid::Uuid::new_v4());
    let test_data = b"Data for head test";

    // Put object first
    println!("Putting object...");
    client
        .put_object(&test_key, Bytes::from_static(test_data))
        .await
        .unwrap();
    println!("✓ Put successful");

    // Head object to check size
    println!("Checking object metadata with HEAD...");
    let head_result = client.head_object(&test_key).await;
    assert!(head_result.is_ok(), "Head failed: {:?}", head_result.err());

    let size = head_result.unwrap();
    assert!(size.is_some(), "Expected size but got None");
    let size = size.unwrap();
    assert_eq!(
        size,
        test_data.len() as u64,
        "Size mismatch: expected {}, got {}",
        test_data.len(),
        size
    );
    println!("✓ HEAD successful, size verified: {} bytes", size);
}

#[tokio::test]
async fn test_head_nonexistent() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Head nonexistent object");

    let client = create_client(&settings).unwrap();

    let nonexistent_key = format!(
        "{}/nonexistent-head-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );

    println!("Checking nonexistent object with HEAD...");
    let head_result = client.head_object(&nonexistent_key).await;
    assert!(head_result.is_ok(), "Head should succeed but return None");

    let size = head_result.unwrap();
    assert!(size.is_none(), "Expected None for nonexistent object");
    println!("✓ HEAD correctly returned None for nonexistent object");
}

#[tokio::test]
async fn test_put_with_special_characters() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Put with special characters in key");

    let client = create_client(&settings).unwrap();

    // Test with various special characters (URL-safe)
    let test_key = format!(
        "{}/test-special_chars-2024.01.15-v1.0-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );
    let test_data = b"Data with special chars in key";

    println!("Putting object with special chars in key: {}", test_key);
    let put_result = client
        .put_object(&test_key, Bytes::from_static(test_data))
        .await;
    assert!(put_result.is_ok(), "Put failed: {:?}", put_result.err());
    println!("✓ Put successful");

    let get_result = client.get_object(&test_key).await;
    assert!(get_result.is_ok(), "Get failed: {:?}", get_result.err());
    assert_eq!(get_result.unwrap().unwrap(), Bytes::from(&test_data[..]));
    println!("✓ Get successful");
}

#[tokio::test]
async fn test_concurrent_puts() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Concurrent puts");

    let client = create_client(&settings).unwrap();
    let client = std::sync::Arc::new(client);

    let num_concurrent = 10;
    let mut handles = Vec::new();

    println!("Starting {} concurrent puts...", num_concurrent);

    for i in 0..num_concurrent {
        let client = client.clone();
        let settings = settings.clone();

        let handle = tokio::spawn(async move {
            let key = format!(
                "{}/test-concurrent-{}-{}.dat",
                settings.prefix,
                i,
                uuid::Uuid::new_v4()
            );
            let data = format!("Concurrent put {}", i);

            client
                .put_object(&key, Bytes::from(data.clone()))
                .await
                .unwrap();
            (key, data)
        });

        handles.push(handle);
    }

    // Wait for all puts to complete
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.unwrap();
        results.push(result);
    }
    println!("✓ All {} puts completed", num_concurrent);

    // Verify all files
    println!("Verifying {} files...", num_concurrent);
    for (i, (key, expected_data)) in results.iter().enumerate() {
        let downloaded = client.get_object(key).await.unwrap().unwrap();
        assert_eq!(
            downloaded.as_ref(),
            expected_data.as_bytes(),
            "Data mismatch for concurrent file {}",
            i
        );
    }
    println!("✓ All files verified");
}

#[tokio::test]
async fn test_get_object_range() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };

    println!("\nTest: Get object range");

    let client = create_client(&settings).unwrap();

    let test_key = format!(
        "{}/test-range-{}.dat",
        settings.prefix,
        uuid::Uuid::new_v4()
    );
    let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Put object
    println!("Putting object ({} bytes)...", test_data.len());
    client
        .put_object(&test_key, Bytes::from_static(test_data))
        .await
        .unwrap();
    println!("✓ Put successful");

    // Get partial range
    let start = 10u64;
    let end = 19u64;
    println!("Getting range {}-{}...", start, end);
    let range_result = client.get_object_range(&test_key, start, Some(end)).await;
    assert!(
        range_result.is_ok(),
        "Range get failed: {:?}",
        range_result.err()
    );

    let range_data = range_result.unwrap();
    assert!(range_data.is_some(), "Expected range data but got None");
    let range_data = range_data.unwrap();

    let expected = &test_data[start as usize..=end as usize];
    assert_eq!(
        range_data,
        Bytes::from(expected),
        "Range data doesn't match"
    );
    println!(
        "✓ Range get successful: {:?} ({} bytes)",
        String::from_utf8_lossy(&range_data),
        range_data.len()
    );
}

#[tokio::test]
async fn test_create_bucket_is_idempotent() {
    let settings = match skip_if_no_s3() {
        Some(s) => s,
        None => return,
    };
    let client = create_client(&settings).unwrap();
    client.create_bucket().await.expect("first create");
    client.create_bucket().await.expect("second create");
}

/// A downloader over a prefix no other run shares, and the queue under it.
async fn fresh_queue(
    settings: &CloudSettings,
    list_page_size: Option<usize>,
) -> (
    std::sync::Arc<S3Client>,
    normfs_cloud::CloudDownloader,
    normfs_types::QueueId,
) {
    let mut client = create_client(settings).unwrap();
    if let Some(keys) = list_page_size {
        client = client.with_list_page_size(keys);
    }
    client.create_bucket().await.unwrap();
    let client = std::sync::Arc::new(client);
    let prefix = format!("{}/ids-{}", settings.prefix, uuid::Uuid::new_v4());
    let downloader = normfs_cloud::CloudDownloader::new(client.clone(), &prefix);
    let queue = normfs_types::QueueIdResolver::new("0123456789abcdef").resolve("q");
    (client, downloader, queue)
}

async fn put_ids(
    client: &S3Client,
    downloader: &normfs_cloud::CloudDownloader,
    queue: &normfs_types::QueueId,
    ids: impl IntoIterator<Item = u64>,
) {
    for id in ids {
        let status = client
            .put_object(
                &downloader.key(queue, &uintn::UintN::from(id)),
                Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
        assert_eq!(status, 200);
    }
}

#[tokio::test]
async fn test_find_ids_across_directory_levels() {
    let Some(settings) = skip_if_no_s3() else {
        return;
    };
    let (client, downloader, queue) = fresh_queue(&settings, None).await;

    // fff.store, 001/000.store, 002/fff.store, 001/000/000.store
    put_ids(
        &client,
        &downloader,
        &queue,
        [0xfff, 0x1000, 0x2fff, 0x1000000],
    )
    .await;
    assert_eq!(
        downloader.find_max_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0x1000000u64))
    );
    assert_eq!(
        downloader.find_min_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0xfffu64))
    );

    let (client, downloader, queue) = fresh_queue(&settings, None).await;
    put_ids(&client, &downloader, &queue, [0x2fff, 0x1000]).await;
    assert_eq!(
        downloader.find_min_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0x1000u64))
    );
    assert_eq!(
        downloader.find_max_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0x2fffu64))
    );

    // 001/000/000.store sorts before 002/000.store but is the larger id.
    let (client, downloader, queue) = fresh_queue(&settings, None).await;
    put_ids(&client, &downloader, &queue, [0x1000000, 0x2000]).await;
    assert_eq!(
        downloader.find_min_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0x2000u64))
    );
}

#[tokio::test]
async fn test_find_ids_ignores_keys_outside_the_layout() {
    let Some(settings) = skip_if_no_s3() else {
        return;
    };
    let (client, downloader, queue) = fresh_queue(&settings, None).await;

    put_ids(&client, &downloader, &queue, [5]).await;
    let key = downloader.key(&queue, &uintn::UintN::from(5u64));
    let queue_prefix = key.strip_suffix("005.store").unwrap();
    for stray in [
        "ffff.store",
        "zzz.store",
        "fff.wal",
        "fff/",
        "g01/000.store",
    ] {
        client
            .put_object(&format!("{queue_prefix}{stray}"), Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    assert_eq!(
        downloader.find_max_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(5u64))
    );
}

#[tokio::test]
async fn test_find_ids_follows_every_listing_page() {
    let Some(settings) = skip_if_no_s3() else {
        return;
    };
    let (client, downloader, queue) = fresh_queue(&settings, Some(4)).await;

    put_ids(&client, &downloader, &queue, 1..=11).await;
    put_ids(&client, &downloader, &queue, (1..=11).map(|d| d << 12)).await;
    assert_eq!(
        downloader.find_max_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(11u64 << 12))
    );
    assert_eq!(
        downloader.find_min_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(1u64))
    );
}

#[tokio::test]
async fn test_find_ids_under_a_prefix_the_server_would_encode() {
    let Some(mut settings) = skip_if_no_s3() else {
        return;
    };
    settings.prefix = format!("{} with space+plus", settings.prefix);
    let (client, downloader, queue) = fresh_queue(&settings, None).await;

    put_ids(&client, &downloader, &queue, [1, 0x1000]).await;
    assert_eq!(
        downloader.find_max_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(0x1000u64))
    );
    assert_eq!(
        downloader.find_min_id(&queue).await.unwrap(),
        Some(uintn::UintN::from(1u64))
    );
}
