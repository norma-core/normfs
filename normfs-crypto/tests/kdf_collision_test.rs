use bytes::Bytes;
use normfs_crypto::CryptoContext;
use normfs_types::{EncryptionType, QueueIdResolver};
use uintn::UintN;

/// `/inst/q` with file id 0x3141 and `/inst/qA` with file id 0x31 both encode
/// to the bytes `/inst/qA1` once the queue path and the little-endian file id
/// are concatenated without lengths.
fn colliding_pair() -> (
    (normfs_types::QueueId, UintN),
    (normfs_types::QueueId, UintN),
) {
    let resolver = QueueIdResolver::new("inst");
    (
        (resolver.resolve("/inst/q"), UintN::from(0x3141u64)),
        (resolver.resolve("/inst/qA"), UintN::from(0x31u64)),
    )
}

#[test]
fn aes_v1_derives_one_key_for_two_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ctx = CryptoContext::open(temp_dir.path()).unwrap();
    let ((queue_a, file_a), (queue_b, file_b)) = colliding_pair();

    let plaintext = Bytes::from("same plaintext");
    let (nonce_a, ct_a) = ctx
        .encrypt(&queue_a, &file_a, EncryptionType::Aes, &plaintext)
        .unwrap();
    let (nonce_b, ct_b) = ctx
        .encrypt(&queue_b, &file_b, EncryptionType::Aes, &plaintext)
        .unwrap();

    assert_eq!(nonce_a, nonce_b);
    assert_eq!(ct_a, ct_b);

    let secret = Bytes::from("file B holds a different secret");
    let (_, ct_b2) = ctx
        .encrypt(&queue_b, &file_b, EncryptionType::Aes, &secret)
        .unwrap();

    let recovered: Vec<u8> = ct_a
        .iter()
        .zip(ct_b2.iter())
        .zip(plaintext.iter())
        .map(|((x, y), p)| x ^ y ^ p)
        .collect();
    assert_eq!(&recovered[..], &secret[..recovered.len()]);
}

#[test]
fn aes_v2_derives_a_distinct_key_per_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ctx = CryptoContext::open(temp_dir.path()).unwrap();
    let ((queue_a, file_a), (queue_b, file_b)) = colliding_pair();

    let plaintext = Bytes::from("same plaintext");
    let (nonce_a, ct_a) = ctx
        .encrypt(&queue_a, &file_a, EncryptionType::AesV2, &plaintext)
        .unwrap();
    let (nonce_b, ct_b) = ctx
        .encrypt(&queue_b, &file_b, EncryptionType::AesV2, &plaintext)
        .unwrap();

    assert_ne!(nonce_a, nonce_b);
    assert_ne!(ct_a, ct_b);
}

#[test]
fn aes_v2_ignores_the_uintn_variant() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ctx = CryptoContext::open(temp_dir.path()).unwrap();
    let resolver = QueueIdResolver::new("inst");
    let queue_id = resolver.resolve("q");
    let plaintext = Bytes::from("payload");

    let narrow = UintN::from(7u64);
    let wide = UintN::U128(7).clone();

    let (nonce_narrow, ct_narrow) = ctx
        .encrypt(&queue_id, &narrow, EncryptionType::AesV2, &plaintext)
        .unwrap();
    let (nonce_wide, ct_wide) = ctx
        .encrypt(&queue_id, &wide, EncryptionType::AesV2, &plaintext)
        .unwrap();

    assert_eq!(nonce_narrow, nonce_wide);
    assert_eq!(ct_narrow, ct_wide);
}
