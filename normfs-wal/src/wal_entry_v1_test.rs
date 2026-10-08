use super::wal_entry::WalEntryHeader;
use super::wal_entry_v1::{
    WAL_ENTRY_V1_CRC_SIZE, WAL_ENTRY_V1_MIN_SIZE, WalEntryV1, WalEntryV1Error, crc32c,
    derive_entry_id, encoded_len,
};
use super::wal_header::WalHeader;
use super::wal_header_v1::WalHeaderV1;
use bytes::BytesMut;
use uintn::UintN;

fn pseudo_random(len: usize) -> Vec<u8> {
    let mut state: u32 = 0x9E37_79B9;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xFF) as u8
        })
        .collect()
}

fn roundtrip(record_size: usize, expected_prefix: usize) {
    let record = pseudo_random(record_size);
    let entry = WalEntryV1::new(&record);
    let expected = expected_prefix + record_size + WAL_ENTRY_V1_CRC_SIZE;

    assert_eq!(entry.size().unwrap(), expected);
    assert_eq!(encoded_len(record_size as u32), expected);

    let mut buffer = BytesMut::new();
    let written = entry.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, expected);
    assert_eq!(buffer.len(), expected);

    let (decoded, consumed) = WalEntryV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded.record, &record[..]);
    assert_eq!(consumed, expected);
}

#[test]
fn test_wal_entry_v1_roundtrip_across_varint_widths() {
    roundtrip(0, 1);
    roundtrip(1, 1);
    roundtrip(127, 1);
    roundtrip(128, 2);
    roundtrip(16_383, 2);
    roundtrip(16_384, 3);
}

#[test]
fn test_wal_entry_v1_empty_record() {
    let entry = WalEntryV1::new(&[]);

    let mut buffer = BytesMut::new();
    let written = entry.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, WAL_ENTRY_V1_MIN_SIZE);
    assert_eq!(buffer[0], 0);

    let (decoded, consumed) = WalEntryV1::from_bytes(&buffer).unwrap();
    assert!(decoded.record.is_empty());
    assert_eq!(consumed, WAL_ENTRY_V1_MIN_SIZE);
}

#[test]
fn test_wal_entry_v1_entries_pack_back_to_back() {
    let sizes = [0usize, 5, 130, 17];
    let records: Vec<Vec<u8>> = sizes.iter().map(|s| pseudo_random(*s)).collect();

    let mut buffer = BytesMut::new();
    for record in &records {
        WalEntryV1::new(record).write_to_bytes(&mut buffer).unwrap();
    }

    let mut cursor = 0usize;
    for record in &records {
        let (decoded, consumed) = WalEntryV1::from_bytes(&buffer[cursor..]).unwrap();
        assert_eq!(decoded.record, &record[..]);
        cursor += consumed;
    }
    assert_eq!(cursor, buffer.len());
}

#[test]
fn test_wal_entry_v1_rejects_malformed_varints() {
    let record = pseudo_random(300);
    let mut buffer = BytesMut::new();
    let written = WalEntryV1::new(&record)
        .write_to_bytes(&mut buffer)
        .unwrap();

    for len in 0..written {
        assert_eq!(
            WalEntryV1::from_bytes(&buffer[..len]),
            Err(WalEntryV1Error::Truncated),
            "length {} should be truncated",
            len
        );
    }

    // Record size padded to two bytes instead of the canonical one.
    assert_eq!(
        WalEntryV1::from_bytes(&[0x85, 0x00, 1, 2, 3, 4, 5, 6, 7]),
        Err(WalEntryV1Error::NonCanonical)
    );

    // Record size needs a sixth byte to be represented.
    assert_eq!(
        WalEntryV1::from_bytes(&[0xff, 0xff, 0xff, 0xff, 0x10, 1, 2, 3, 4]),
        Err(WalEntryV1Error::Overflow)
    );
}

#[test]
fn test_wal_entry_v1_detects_corruption() {
    let record = pseudo_random(64);
    let mut buffer = BytesMut::new();
    let written = WalEntryV1::new(&record)
        .write_to_bytes(&mut buffer)
        .unwrap();

    // A single flipped bit anywhere in the framed entry is rejected. The CRC
    // covers the length prefix as well as the record, so a corrupted length is
    // caught too.
    for i in 0..written {
        buffer[i] ^= 0x01;
        assert!(
            WalEntryV1::from_bytes(&buffer).is_err(),
            "flipping a bit at offset {} should be detected",
            i
        );
        buffer[i] ^= 0x01;
    }

    assert!(WalEntryV1::from_bytes(&buffer).is_ok());

    // Corrupting only the record body reports a CRC mismatch specifically.
    buffer[5] ^= 0x80;
    assert_eq!(
        WalEntryV1::from_bytes(&buffer),
        Err(WalEntryV1Error::CrcMismatch)
    );
}

#[test]
fn test_wal_entry_v1_ignores_trailing_bytes() {
    let record = pseudo_random(10);
    let mut buffer = BytesMut::new();
    let written = WalEntryV1::new(&record)
        .write_to_bytes(&mut buffer)
        .unwrap();
    buffer.extend_from_slice(&[0xAA, 0xBB]);

    let (decoded, consumed) = WalEntryV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded.record, &record[..]);
    assert_eq!(consumed, written);
}

#[test]
fn test_wal_entry_v1_crc32c_check_vector() {
    assert_eq!(crc32c(0, b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(0, b""), 0);

    // Seeding one call with the previous result checksums the concatenation.
    let data = pseudo_random(128);
    for split in 0..=data.len() {
        assert_eq!(
            crc32c(crc32c(0, &data[..split]), &data[split..]),
            crc32c(0, &data)
        );
    }

    // The C fast path folds three interleaved streams and rejoins them with
    // the shift matrices, in 3072-byte chunks and then 768-byte ones; under
    // 768 it stays serial. 7000 bytes reaches both chunk sizes, so folding the
    // same buffer in small chunks checks them against the serial path, which
    // is what every test above is too short to reach.
    let big = pseudo_random(7000);
    let mut acc = 0u32;
    for chunk in big.chunks(100) {
        acc = crc32c(acc, chunk);
    }
    assert_eq!(acc, crc32c(0, &big));
}

#[test]
fn test_wal_entry_v1_entry_id_derivation() {
    let header = WalHeaderV1::new(8, 4, 100).unwrap();

    assert_eq!(header.entry_id(0).unwrap(), 100);
    assert_eq!(header.entry_id(7).unwrap(), 107);

    assert_eq!(derive_entry_id(0, 0).unwrap(), 0);
    assert_eq!(derive_entry_id(u64::MAX, 0).unwrap(), u64::MAX);

    assert_eq!(
        derive_entry_id(u64::MAX, 1),
        Err(WalEntryV1Error::IdOverflow)
    );

    let saturated = WalHeaderV1::new(8, 4, u64::MAX).unwrap();
    assert_eq!(saturated.entry_id(1), Err(WalEntryV1Error::IdOverflow));
}

#[test]
fn test_wal_entry_v1_iter_next_derives_ids_across_buffer() {
    // Entries packed back to back, iterated with a running index. The id of the
    // i-th entry is num_entries_before + i.
    let base = 1000u64;
    let sizes = [0usize, 5, 130, 17, 200];
    let records: Vec<Vec<u8>> = sizes.iter().map(|s| pseudo_random(*s)).collect();

    let mut buffer = BytesMut::new();
    for record in &records {
        WalEntryV1::new(record).write_to_bytes(&mut buffer).unwrap();
    }

    let mut cursor = 0usize;
    for (index, record) in (0u64..).zip(&records) {
        let (decoded, entry_id, consumed) =
            WalEntryV1::iter_next(&buffer[cursor..], base, index).unwrap();
        assert_eq!(decoded.record, &record[..]);
        assert_eq!(entry_id, base + index);
        // The same entry decoded standalone consumes the same number of bytes.
        let (_, consumed_decode) = WalEntryV1::from_bytes(&buffer[cursor..]).unwrap();
        assert_eq!(consumed, consumed_decode);
        cursor += consumed;
    }
    assert_eq!(cursor, buffer.len());
}

#[test]
fn test_wal_entry_v1_iter_next_stops_on_truncation_and_corruption() {
    let record = pseudo_random(120);
    let mut buffer = BytesMut::new();
    let written = WalEntryV1::new(&record)
        .write_to_bytes(&mut buffer)
        .unwrap();

    // Every proper prefix is truncated.
    for len in 0..written {
        assert_eq!(
            WalEntryV1::iter_next(&buffer[..len], 0, 0),
            Err(WalEntryV1Error::Truncated),
            "length {} should be truncated",
            len
        );
    }

    // A flipped record byte is a CRC mismatch, regardless of index/base.
    buffer[3] ^= 0x01;
    assert_eq!(
        WalEntryV1::iter_next(&buffer, 42, 7),
        Err(WalEntryV1Error::CrcMismatch)
    );
}

#[test]
fn test_wal_entry_v1_iter_next_reports_id_overflow() {
    // The entry decodes fine, but num_entries_before + index wraps u64.
    let record = pseudo_random(8);
    let mut buffer = BytesMut::new();
    WalEntryV1::new(&record)
        .write_to_bytes(&mut buffer)
        .unwrap();

    // Largest non-overflowing id.
    let (_, entry_id, _) = WalEntryV1::iter_next(&buffer, u64::MAX, 0).unwrap();
    assert_eq!(entry_id, u64::MAX);

    assert_eq!(
        WalEntryV1::iter_next(&buffer, u64::MAX, 1),
        Err(WalEntryV1Error::IdOverflow)
    );
}

#[test]
fn test_wal_entry_v0_is_unchanged() {
    // From Go test: entry_id 456, record "hello world", V0 framing.
    let wal_header = WalHeader::new(2, 4, UintN::from(123u64)).unwrap();
    let go_bytes: [u8; 22] = [
        0, 0, 0, 0, 0, 0, 0, 0, 200, 1, 0, 0, 11, 0, 104, 105, 30, 178, 52, 103, 171, 69,
    ];

    let parsed = WalEntryHeader::from_bytes(&go_bytes, &wal_header).unwrap();
    assert_eq!(parsed.entry_id, UintN::from(456u16));
    assert_eq!(parsed.record_size, UintN::from(11u16));
    assert_eq!(parsed.xxhash, 5020219685658847592);

    let mut buffer = BytesMut::new();
    WalEntryHeader::new(UintN::from(456u16), b"hello world")
        .write_to_bytes(&mut buffer, &wal_header)
        .unwrap();
    assert_eq!(buffer.as_ref(), &go_bytes[..]);

    // The same record costs 28 bytes of framing in V0 and 5 in V1.
    assert_eq!(parsed.size(&wal_header), 22);
    assert_eq!(
        WalEntryV1::new(b"hello world").size().unwrap() - 11,
        WAL_ENTRY_V1_MIN_SIZE
    );
}
