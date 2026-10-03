use super::*;
use crate::compression::zstd_decompress;
use crate::store_file;
use bytes::Bytes;
use normfs_types::QueueIdResolver;

const CAP: usize = 64 * 1024;

struct Fixture {
    _dir: tempfile::TempDir,
    crypto: CryptoContext,
    queue: QueueId,
    packer: Packer,
}

fn fixture(slots: usize) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    Fixture {
        crypto: CryptoContext::open(dir.path()).unwrap(),
        queue: QueueIdResolver::new("inst").resolve("cam"),
        packer: Packer::new(slots, CAP).unwrap(),
        _dir: dir,
    }
}

/// Text-like bytes that compress, or noise that does not.
fn input(len: usize, compressible: bool) -> Vec<u8> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..len)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let b = (x >> 33) as u8;
            if compressible { b'a' + b % 8 } else { b }
        })
        .collect()
}

impl Fixture {
    async fn seal(
        &self,
        data: &[u8],
        file_id: u64,
        compression: CompressionType,
        encryption: EncryptionType,
    ) -> SealedFile {
        let mut slot = self.packer.take().await;
        slot.buf()[..data.len()].copy_from_slice(data);
        self.packer
            .seal(
                slot,
                data.len(),
                &self.queue,
                &UintN::from(file_id),
                compression,
                encryption,
                UintN::from(100u64),
                UintN::from(5u64),
                &self.crypto,
            )
            .unwrap()
    }

    fn build(
        &self,
        data: &[u8],
        file_id: u64,
        compression: CompressionType,
        encryption: EncryptionType,
    ) -> SealedFile {
        store_file::build(
            &self.queue,
            &UintN::from(file_id),
            compression,
            encryption,
            UintN::from(100u64),
            UintN::from(5u64),
            &Bytes::copy_from_slice(data),
            &self.crypto,
        )
        .unwrap()
    }

    fn open(&self, file: &SealedFile, file_id: u64) -> Vec<u8> {
        let (auth, _) = FileAuthentication::from_bytes(&file.auth).unwrap();
        self.crypto
            .verify(&file.header, &auth.header_signature)
            .unwrap();
        self.crypto
            .verify(&file.body, &auth.content_signature)
            .unwrap();
        let (header, _) = crate::store_header_v1::AnyStoreHeader::from_bytes(&file.header).unwrap();
        let body = if header.is_encrypted() {
            self.crypto
                .decrypt(
                    &self.queue,
                    &UintN::from(file_id),
                    &file.body.slice(..12),
                    &file.body.slice(12..),
                )
                .unwrap()
        } else {
            file.body.clone()
        };
        if header.is_compressed() {
            zstd_decompress(&body).unwrap()
        } else {
            body.to_vec()
        }
    }
}

#[tokio::test]
async fn an_uncompressed_file_is_the_same_bytes_either_way() {
    let f = fixture(1);
    let data = input(CAP, false);
    for encryption in [EncryptionType::None, EncryptionType::Aes] {
        let sealed = f.seal(&data, 3, CompressionType::None, encryption).await;
        let built = f.build(&data, 3, CompressionType::None, encryption);
        assert_eq!(sealed.to_bytes(), built.to_bytes(), "{encryption:?}");
        assert_eq!(sealed.raw_len, built.raw_len);
    }
}

#[tokio::test]
async fn a_compressed_file_reads_back_as_its_input() {
    let f = fixture(1);
    for (len, compressible) in [(CAP, true), (CAP, false), (1000, true), (1, false)] {
        let data = input(len, compressible);
        for encryption in [EncryptionType::None, EncryptionType::Aes] {
            let sealed = f.seal(&data, 9, CompressionType::Zstd, encryption).await;
            let built = f.build(&data, 9, CompressionType::Zstd, encryption);
            assert_eq!(sealed.header, built.header);
            assert_eq!(
                f.open(&sealed, 9),
                data,
                "{len} {compressible} {encryption:?}"
            );
            if compressible {
                assert!(sealed.body.len() < len / 2);
            }
        }
    }
}

#[tokio::test]
async fn the_parts_of_a_sealed_file_are_one_buffer() {
    let f = fixture(1);
    let sealed = f
        .seal(
            &input(5000, true),
            1,
            CompressionType::Zstd,
            EncryptionType::Aes,
        )
        .await;
    let whole = sealed.to_bytes();
    assert_eq!(whole.len(), sealed.len());
    assert_eq!(whole.as_ptr(), sealed.auth.as_ptr());
    assert_eq!(
        &whole[..],
        &[&sealed.auth[..], &sealed.header[..], &sealed.body[..]].concat()[..]
    );
}

#[tokio::test]
async fn a_file_holds_its_slot_until_dropped() {
    let f = fixture(1);
    let sealed = f
        .seal(
            &input(100, true),
            1,
            CompressionType::Zstd,
            EncryptionType::None,
        )
        .await;
    let body = sealed.body.clone();
    drop(sealed);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), f.packer.take())
            .await
            .is_err(),
        "the body still holds the slot"
    );
    drop(body);
    let _slot = f.packer.take().await;
}
