use bytes::Bytes;
use prost::Message;

use super::request::{decode_request, RequestError};
use crate::proto::{
    ClientRequest, Id, Offset, PingRequest, ReadRequest, SetupRequest, WriteRequest,
};

fn write(id: u64, queue: &str, packets: usize) -> WriteRequest {
    WriteRequest {
        write_id: id,
        queue_id: queue.to_string(),
        packets: (0..packets)
            .map(|i| Bytes::from(vec![i as u8; i % 300]))
            .collect(),
    }
}

fn read(id: u64, queue: &str, encodings: usize, raw: Option<Vec<u8>>) -> ReadRequest {
    ReadRequest {
        read_id: id,
        queue_id: queue.to_string(),
        step: id % 3,
        accept_encodings: (0..encodings).map(|i| (i % 3) as i32 - 1).collect(),
        offset: raw.map(|raw| Offset {
            id: Some(Id { raw: raw.into() }),
            r#type: 1,
        }),
        limit: id.wrapping_mul(0x9e37_79b9_7f4a_7c15),
    }
}

fn requests() -> Vec<ClientRequest> {
    vec![
        ClientRequest::default(),
        ClientRequest {
            setup: Some(SetupRequest { version: 1 }),
            ..Default::default()
        },
        ClientRequest {
            ping: Some(PingRequest {
                sequence: u64::MAX,
                client_timestamp_ns: 1_760_000_000_000_000_000,
            }),
            ..Default::default()
        },
        ClientRequest {
            write: Some(write(7, "robot/камера", 3)),
            ..Default::default()
        },
        ClientRequest {
            write: Some(write(8, "q", 1000)),
            ..Default::default()
        },
        ClientRequest {
            read: Some(read(9, "q", 0, None)),
            ..Default::default()
        },
        ClientRequest {
            read: Some(read(10, "q", 50, Some(vec![1, 2, 3, 0xff]))),
            ..Default::default()
        },
        ClientRequest {
            read: Some(ReadRequest {
                offset: Some(Offset {
                    id: Some(Id::default()),
                    r#type: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        ClientRequest {
            setup: Some(SetupRequest { version: 2 }),
            ping: Some(PingRequest::default()),
            write: Some(write(u64::MAX, "", 65)),
            read: Some(read(1 << 40, "x", 9, Some(vec![0; 16]))),
        },
    ]
}

fn assert_same(buf: &[u8]) {
    let expected = ClientRequest::decode(buf).expect("prost accepts the buffer");
    assert_eq!(decode_request(buf).unwrap(), expected);
}

#[test]
fn test_matches_prost() {
    for request in requests() {
        assert_same(&request.encode_to_vec());
    }
}

#[test]
fn test_unknown_fields_are_skipped() {
    let mut buf = requests().last().unwrap().encode_to_vec();
    // field 99 varint, field 98 fixed64, field 97 bytes, field 96 fixed32
    buf.extend_from_slice(&[0x98, 0x06, 0x96, 0x01]);
    buf.extend_from_slice(&[0x91, 0x06, 1, 2, 3, 4, 5, 6, 7, 8]);
    buf.extend_from_slice(&[0x8a, 0x06, 2, 0xaa, 0xbb]);
    buf.extend_from_slice(&[0x85, 0x06, 1, 2, 3, 4]);
    // field 95 a group holding field 1 and an empty group 94
    buf.extend_from_slice(&[0xfb, 0x05, 0x08, 0x01, 0xf3, 0x05, 0xf4, 0x05, 0xfc, 0x05]);
    assert_same(&buf);
}

#[test]
fn test_unpacked_encodings_match_prost() {
    // ReadRequest { accept_encodings: [1, 0, 1] } written unpacked, field 4 wire type 0
    let read = [0x20, 0x01, 0x20, 0x00, 0x20, 0x01];
    let mut buf = vec![0x5a, read.len() as u8];
    buf.extend_from_slice(&read);
    assert_same(&buf);
}

#[test]
fn test_rejects_what_prost_rejects() {
    let full = requests().last().unwrap().encode_to_vec();
    for cut in 1..full.len() {
        match ClientRequest::decode(&full[..cut]) {
            Ok(expected) => assert_eq!(decode_request(&full[..cut]), Ok(expected), "cut at {cut}"),
            Err(_) => assert!(decode_request(&full[..cut]).is_err(), "cut at {cut}"),
        }
    }
    // field 0
    assert_eq!(
        decode_request(&[0x00, 0x00]),
        Err(RequestError::BadFieldNum)
    );
    // write queue id that is not UTF-8
    assert_eq!(
        decode_request(&[0x52, 0x03, 0x12, 0x01, 0xff]),
        Err(RequestError::InvalidUtf8)
    );
    // an end without a start, a group never closed, wire types 6 and 7
    for tail in [
        &[0xfc, 0x05][..],
        &[0xfb, 0x05, 0x08, 0x01],
        &[0xfe, 0x05, 0x01],
        &[0xff, 0x05, 0x01],
    ] {
        let buf = [&full[..], tail].concat();
        assert!(ClientRequest::decode(&buf[..]).is_err());
        assert!(decode_request(&buf).is_err());
    }
}

#[test]
fn test_repeated_write_is_refused() {
    let first = ClientRequest {
        write: Some(write(7, "q", 1)),
        ..Default::default()
    };
    let second = ClientRequest {
        write: Some(write(7, "q", 2)),
        ..Default::default()
    };
    let mut buf = first.encode_to_vec();
    buf.extend(second.encode_to_vec());
    assert_eq!(
        ClientRequest::decode(&buf[..])
            .unwrap()
            .write
            .unwrap()
            .packets
            .len(),
        3
    );
    assert_eq!(decode_request(&buf), Err(RequestError::Repeated));
}

#[test]
fn test_repeated_offset_or_id_is_refused() {
    let offset = |raw: u8| Offset {
        id: Some(Id {
            raw: Bytes::from(vec![raw]),
        }),
        r#type: 0,
    };
    let in_read = |inner: Vec<u8>| {
        let mut buf = vec![0x5a, inner.len() as u8];
        buf.extend(inner);
        buf
    };

    let mut two_offsets = read(1, "q", 0, None).encode_to_vec();
    for raw in [1, 2] {
        two_offsets.extend(
            ReadRequest {
                offset: Some(offset(raw)),
                ..Default::default()
            }
            .encode_to_vec(),
        );
    }
    assert_eq!(
        decode_request(&in_read(two_offsets)),
        Err(RequestError::Repeated)
    );

    let mut two_ids = offset(1).encode_to_vec();
    two_ids.extend(offset(2).encode_to_vec());
    let mut with_offset = read(1, "q", 0, None).encode_to_vec();
    with_offset.extend([0x52, two_ids.len() as u8]);
    with_offset.extend(two_ids);
    assert_eq!(
        decode_request(&in_read(with_offset)),
        Err(RequestError::Repeated)
    );
}

#[test]
fn test_differs_from_prost() {
    // setup { version: 1 } then an empty setup: prost merges, the readers would keep the last
    let twice = [0x0a, 0x02, 0x08, 0x01, 0x0a, 0x00];
    assert_eq!(
        ClientRequest::decode(&twice[..])
            .unwrap()
            .setup
            .unwrap()
            .version,
        1
    );
    assert_eq!(decode_request(&twice), Err(RequestError::Repeated));

    // ping sent as a varint is skipped as an unknown field
    let ping = [0x10, 0x01];
    assert!(ClientRequest::decode(&ping[..]).is_err());
    assert_eq!(decode_request(&ping), Ok(ClientRequest::default()));

    // accept_encodings packed, then unpacked
    let mixed = [0x5a, 0x05, 0x22, 0x01, 0x01, 0x20, 0x00];
    assert!(ClientRequest::decode(&mixed[..]).is_ok());
    assert_eq!(decode_request(&mixed), Err(RequestError::BadWireType));

    // an empty packed block, then an unpacked value
    let empty_packed = [0x5a, 0x04, 0x22, 0x00, 0x20, 0x01];
    assert!(ClientRequest::decode(&empty_packed[..]).is_ok());
    assert_eq!(decode_request(&empty_packed), Err(RequestError::Truncated));

    // a packed block that ends inside a varint
    let cut_varint = [0x5a, 0x04, 0x22, 0x02, 0x01, 0x80];
    assert!(ClientRequest::decode(&cut_varint[..]).is_err());
    assert_eq!(
        decode_request(&cut_varint)
            .unwrap()
            .read
            .unwrap()
            .accept_encodings,
        [1]
    );

    // a limit of 2^64: prost refuses it, gremlin keeps the low 64 bits
    let wide = [
        0x5a, 0x0e, 0x12, 0x01, 0x71, 0x58, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
        0x02,
    ];
    assert!(ClientRequest::decode(&wide[..]).is_err());
    assert_eq!(decode_request(&wide).unwrap().read.unwrap().limit, 0);

    // a key past 32 bits: prost refuses it, gremlin reads it as field 1, setup
    let wide_key = [0x8a, 0x80, 0x80, 0x80, 0x10, 0x02, 0x08, 0x01];
    assert!(ClientRequest::decode(&wide_key[..]).is_err());
    assert_eq!(decode_request(&wide_key).unwrap().setup.unwrap().version, 1);

    // packed blocks [1], [] and [0]: both read [1, 0] since gremlin.c skips empty blocks
    let gap = [0x5a, 0x08, 0x22, 0x01, 0x01, 0x22, 0x00, 0x22, 0x01, 0x00];
    assert_eq!(
        ClientRequest::decode(&gap[..])
            .unwrap()
            .read
            .unwrap()
            .accept_encodings,
        [1, 0]
    );
    assert_eq!(
        decode_request(&gap).unwrap().read.unwrap().accept_encodings,
        [1, 0]
    );

    // group 95 closed by an end for field 94: prost refuses it, gremlin skips it
    let crossed = [0xfb, 0x05, 0xf4, 0x05];
    assert!(ClientRequest::decode(&crossed[..]).is_err());
    assert_eq!(decode_request(&crossed), Ok(ClientRequest::default()));
}
