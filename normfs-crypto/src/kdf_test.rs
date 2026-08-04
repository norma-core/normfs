//! The C key derivation against the Rust it replaces, byte for byte.
//!
//! The scheme is deterministic and there are encrypted files on disk behind it,
//! so the question is not whether the C implements HKDF and ChaCha20 — the RFC
//! vectors in `c/tests/` settle that — but whether it reproduces exactly what
//! `rand_chacha`'s `ChaCha20Rng` produced. That is an argument about a crate's
//! internals, and this makes it a checked fact instead.
//!
//! The reference below is why `hkdf` is still a dev-dependency. Do not delete
//! it when the port lands: it is the only thing standing between a refactor and
//! silently undecryptable data.

use hkdf::Hkdf;
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use sha2::Sha256;

use crate::kdf::{derive_file_key, AES_KEY_SIZE, GCM_NONCE_SIZE};
use crate::seed::SEED_SIZE;

/// The derivation exactly as `CryptoContext::derive_rng` performed it before
/// this change: HKDF-SHA256 with an absent salt, then a ChaCha20Rng seeded from
/// the 32-byte OKM, drawing the key first and the nonce second.
fn derive_reference(
    seed: &[u8; SEED_SIZE],
    info: &[u8],
) -> ([u8; AES_KEY_SIZE], [u8; GCM_NONCE_SIZE]) {
    let hkdf = Hkdf::<Sha256>::new(None, seed);

    let mut rng_seed = [0u8; 32];
    hkdf.expand(info, &mut rng_seed).unwrap();

    let mut rng = ChaCha20Rng::from_seed(rng_seed);
    let mut key = [0u8; AES_KEY_SIZE];
    let mut nonce = [0u8; GCM_NONCE_SIZE];
    rng.fill_bytes(&mut key);
    rng.fill_bytes(&mut nonce);

    (key, nonce)
}

fn assert_agrees(seed: &[u8; SEED_SIZE], info: &[u8]) {
    let (want_key, want_nonce) = derive_reference(seed, info);
    let (got_key, got_nonce) = derive_file_key(seed, info).unwrap();

    assert_eq!(
        got_key.as_ref(),
        &want_key,
        "key diverged for info {:02x?}",
        info
    );
    assert_eq!(
        got_nonce, want_nonce,
        "nonce diverged for info {:02x?}",
        info
    );
}

fn seeds() -> Vec<[u8; SEED_SIZE]> {
    vec![[0u8; SEED_SIZE], [0xFFu8; SEED_SIZE], {
        let mut s = [0u8; SEED_SIZE];
        for (i, b) in s.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7).wrapping_add(3);
        }
        s
    }]
}

/// The queue-path halves of `info`. The 63/64/65-byte cases straddle the HMAC
/// block boundary, which is where a length-handling bug would hide.
fn paths() -> Vec<String> {
    vec![
        String::from("/"),
        String::from("/a"),
        String::from("/trailing/"),
        // What production actually passes: "/" + 64 hex chars + "/" + name.
        format!("/{}/orders", "3f1a9c".repeat(10) + "abcd"),
        String::from("/очередь"),
        String::from("/日本語/キュー"),
        String::from("/emoji/🚀"),
        format!("/{}", "x".repeat(62)),
        format!("/{}", "x".repeat(63)),
        format!("/{}", "x".repeat(64)),
        // Past any plausible path limit, to prove the C's info bound did not
        // newly reject a queue that works today.
        format!("/{}", "x".repeat(5000)),
    ]
}

/// `file_id.value_to_bytes()` is little-endian at a width the `UintN` variant
/// chooses, so these are the byte-length transitions.
fn file_id_tails() -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = Vec::new();
    for n in [0u64, 1, 254, 255, 256, 65534, 65535, 65536, 0xFFFF_FFFF] {
        v.push(uintn::UintN::from(n).value_to_bytes().to_vec());
    }
    v.push(uintn::UintN::from(u64::MAX).value_to_bytes().to_vec());
    v.push(uintn::UintN::from(u128::MAX).value_to_bytes().to_vec());
    v
}

#[test]
fn c_matches_rust_across_the_corpus() {
    for seed in seeds() {
        for path in paths() {
            for tail in file_id_tails() {
                let mut info = path.as_bytes().to_vec();
                info.extend_from_slice(&tail);
                assert_agrees(&seed, &info);
            }
        }
    }
}

/// The same numeric file id at five widths must give five different keys. If
/// this ever passes with fewer than five distinct keys, the width has been
/// dropped somewhere and the corpus above would no longer notice.
#[test]
fn uintn_width_changes_the_key() {
    use uintn::UintN;

    let seed = [0x22u8; SEED_SIZE];
    let widths = [
        UintN::U8(42),
        UintN::U16(42),
        UintN::U32(42),
        UintN::U64(42),
        UintN::U128(42),
    ];

    let mut keys = Vec::new();
    for id in widths {
        let mut info = b"/inst/queue".to_vec();
        info.extend_from_slice(&id.value_to_bytes());
        assert_agrees(&seed, &info);

        let (key, _) = derive_file_key(&seed, &info).unwrap();
        keys.push(*key);
    }

    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            assert_ne!(keys[i], keys[j], "widths {} and {} collided", i, j);
        }
    }
}

/// A seeded sweep over lengths the hand-written corpus misses, in the style of
/// test_crc32c.c's length sweep.
#[test]
fn c_matches_rust_over_random_info() {
    let seed = [0x5Au8; SEED_SIZE];
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    for len in 0..300usize {
        let info: Vec<u8> = (0..len).map(|_| (next() & 0xFF) as u8).collect();
        assert_agrees(&seed, &info);
    }
}
