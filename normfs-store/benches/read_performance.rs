use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use normfs_crypto::CryptoContext;
use normfs_store::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use std::time::Duration;
use uintn::UintN;

const ENTRY_SIZE: usize = 1024; // 1KB per entry
const TOTAL_SIZE: usize = 128 * 1024 * 1024; // 128MB
const NUM_ENTRIES: usize = TOTAL_SIZE / ENTRY_SIZE; // 131,072 entries

struct BenchConfig {
    compression: bool,
    encryption: bool,
    verify_signatures: bool,
}

impl BenchConfig {
    fn name(&self) -> String {
        format!(
            "compress={}_encrypt={}_verify={}",
            if self.compression { "yes" } else { "no" },
            if self.encryption { "yes" } else { "no" },
            if self.verify_signatures { "yes" } else { "no" }
        )
    }
}

fn create_test_data(entry_size: usize, num_entries: usize) -> Bytes {
    use std::fs::File;
    use std::io::Read;

    let mut data = BytesMut::with_capacity(entry_size * num_entries);

    // Read random data from /dev/urandom for realistic, incompressible data
    let mut urandom = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    let total_size = entry_size * num_entries;

    let mut buffer = vec![0u8; 1024 * 1024]; // Read 1MB at a time
    let mut bytes_read = 0;

    while bytes_read < total_size {
        let to_read = std::cmp::min(buffer.len(), total_size - bytes_read);
        urandom
            .read_exact(&mut buffer[..to_read])
            .expect("Failed to read random data");
        data.extend_from_slice(&buffer[..to_read]);
        bytes_read += to_read;
    }

    data.freeze()
}

fn create_store_file(crypto_ctx: &CryptoContext, config: &BenchConfig, wal_data: &Bytes) -> Bytes {
    // Step 1: Compress if needed
    let data_to_encrypt = if config.compression {
        let compressed = zstd::encode_all(wal_data.as_ref(), 3).unwrap();
        Bytes::from(compressed)
    } else {
        wal_data.clone()
    };

    // Step 2: Encrypt if needed
    let resolver = QueueIdResolver::new(crypto_ctx.instance_id_hex());
    let queue_id = resolver.resolve("benchmark_queue");
    let file_id = UintN::from(1u64);

    let final_data = if config.encryption {
        let (nonce, ciphertext) = crypto_ctx
            .encrypt(&queue_id, &file_id, &data_to_encrypt)
            .unwrap();
        let mut result = BytesMut::with_capacity(nonce.len() + ciphertext.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&ciphertext);
        result.freeze()
    } else {
        data_to_encrypt
    };

    // Step 3: Create StoreHeader
    let compression_type = if config.compression {
        CompressionType::Gzip // Using Gzip as marker (actual compression is zstd)
    } else {
        CompressionType::None
    };

    let encryption_type = if config.encryption {
        EncryptionType::Aes
    } else {
        EncryptionType::None
    };

    let header = StoreHeader::new(
        compression_type,
        encryption_type,
        UintN::from(0u64),
        UintN::from(NUM_ENTRIES as u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    // Step 4: Sign header and content
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&final_data);

    // Step 5: Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Step 6: Assemble complete store file
    let mut complete_file = BytesMut::new();
    file_auth.write_to_bytes(&mut complete_file);
    complete_file.extend_from_slice(&header_bytes);
    complete_file.extend_from_slice(&final_data);

    complete_file.freeze()
}

fn benchmark_extract(
    crypto_ctx: &CryptoContext,
    store_bytes: &Bytes,
    config: &BenchConfig,
) -> Result<Bytes, normfs_store::StoreError> {
    let resolver = QueueIdResolver::new(crypto_ctx.instance_id_hex());
    let queue_id = resolver.resolve("benchmark_queue");
    let file_id = UintN::from(1u64);

    // Parse FileAuthentication
    let (file_auth, auth_size) = FileAuthentication::from_bytes(store_bytes)?;

    // Parse StoreHeader
    let content_after_auth = &store_bytes[auth_size..];
    let (store_header, header_size) = StoreHeader::from_bytes(content_after_auth)?;

    // Verify signatures if enabled
    if config.verify_signatures {
        let header_bytes = &content_after_auth[..header_size];
        crypto_ctx
            .verify(header_bytes, &file_auth.header_signature)
            .map_err(|_| normfs_store::StoreError::SignatureVerificationFailed)?;

        let content_bytes = &content_after_auth[header_size..];
        crypto_ctx
            .verify(content_bytes, &file_auth.content_signature)
            .map_err(|_| normfs_store::StoreError::SignatureVerificationFailed)?;
    }

    let encrypted_content = &content_after_auth[header_size..];

    // Decrypt if needed
    let wal_content_bytes = if store_header.is_encrypted() {
        if encrypted_content.len() < 12 {
            return Err(normfs_store::StoreError::Decrypt);
        }

        let encrypted_bytes = Bytes::from(encrypted_content.to_vec());
        let nonce = encrypted_bytes.slice(0..12);
        let ciphertext = encrypted_bytes.slice(12..);

        crypto_ctx
            .decrypt(&queue_id, &file_id, &nonce, &ciphertext)
            .map_err(|_| normfs_store::StoreError::Decrypt)?
    } else {
        Bytes::from(encrypted_content.to_vec())
    };

    // Decompress if needed
    let wal_bytes = if store_header.is_compressed() {
        Bytes::from(zstd::decode_all(wal_content_bytes.as_ref())?)
    } else {
        wal_content_bytes
    };

    Ok(wal_bytes)
}

fn bench_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_read_performance");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    // Create test data
    println!(
        "Creating test data: {} entries of {} bytes = {} MB",
        NUM_ENTRIES,
        ENTRY_SIZE,
        TOTAL_SIZE / (1024 * 1024)
    );
    let test_data = create_test_data(ENTRY_SIZE, NUM_ENTRIES);

    // Create crypto context
    let temp_dir = tempfile::tempdir().unwrap();
    let crypto_ctx = Arc::new(CryptoContext::open(temp_dir.path()).unwrap());

    // All 8 combinations of settings
    let configs = vec![
        BenchConfig {
            compression: false,
            encryption: false,
            verify_signatures: false,
        },
        BenchConfig {
            compression: false,
            encryption: false,
            verify_signatures: true,
        },
        BenchConfig {
            compression: false,
            encryption: true,
            verify_signatures: false,
        },
        BenchConfig {
            compression: false,
            encryption: true,
            verify_signatures: true,
        },
        BenchConfig {
            compression: true,
            encryption: false,
            verify_signatures: false,
        },
        BenchConfig {
            compression: true,
            encryption: false,
            verify_signatures: true,
        },
        BenchConfig {
            compression: true,
            encryption: true,
            verify_signatures: false,
        },
        BenchConfig {
            compression: true,
            encryption: true,
            verify_signatures: true,
        },
    ];

    for config in configs {
        let name = config.name();
        println!("Creating store file for: {}", name);

        let store_bytes = create_store_file(&crypto_ctx, &config, &test_data);

        println!(
            "  Store file size: {:.2} MB (compression ratio: {:.2}x)",
            store_bytes.len() as f64 / (1024.0 * 1024.0),
            TOTAL_SIZE as f64 / store_bytes.len() as f64
        );

        group.throughput(Throughput::Bytes(TOTAL_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::new("extract_wal_bytes", &name),
            &store_bytes,
            |b, store_bytes| {
                b.iter(|| {
                    let result = benchmark_extract(&crypto_ctx, store_bytes, &config);
                    assert!(result.is_ok(), "Extraction failed");
                    result.unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_read_performance);
criterion_main!(benches);
