use bytes::Bytes;
use prost::Message;

use super::response::encode_response;
use crate::proto::{
    Id, PingRequest, PingResponse, ReadResponse, ServerResponse, SetupResponse, WriteResponse,
};

fn id(n: usize) -> Id {
    Id {
        raw: Bytes::from(vec![n as u8; n % 20]),
    }
}

fn responses() -> Vec<ServerResponse> {
    vec![
        ServerResponse::default(),
        ServerResponse {
            setup: Some(SetupResponse {
                version: 1,
                instance_id_bytes: Bytes::from_static(&[0; 16]),
                instance_id: "станция-1".to_string(),
            }),
            ..Default::default()
        },
        ServerResponse {
            ping: Some(PingResponse {
                local_stamp_ns: 1_760_000_000_000_000_000,
                monotonic_stamp_ns: u64::MAX,
                request: Some(PingRequest {
                    sequence: 7,
                    client_timestamp_ns: 0,
                }),
            }),
            ..Default::default()
        },
        ServerResponse {
            ping: Some(PingResponse::default()),
            ..Default::default()
        },
        ServerResponse {
            write: Some(WriteResponse {
                write_id: 9,
                result: 1,
                ids: (0..300).map(id).collect(),
            }),
            ..Default::default()
        },
        ServerResponse {
            read: Some(ReadResponse {
                read_id: 3,
                result: 1,
                id: Some(id(8)),
                data: Bytes::from(vec![0xab; 70_000]),
                data_source: 4,
                encoding: 1,
            }),
            ..Default::default()
        },
        ServerResponse {
            read: Some(ReadResponse {
                result: 2,
                ..Default::default()
            }),
            ..Default::default()
        },
        ServerResponse {
            setup: Some(SetupResponse::default()),
            ping: Some(PingResponse::default()),
            write: Some(WriteResponse {
                write_id: u64::MAX,
                result: 0,
                ids: vec![Id::default(), id(3)],
            }),
            read: Some(ReadResponse {
                read_id: 1 << 40,
                id: Some(Id::default()),
                ..Default::default()
            }),
        },
    ]
}

#[test]
fn test_matches_prost() {
    for resp in responses() {
        let out = encode_response(&resp, 8, u64::MAX).unwrap();
        assert_eq!(&out[..8], &[0; 8]);
        assert_eq!(&out[8..], &resp.encode_to_vec()[..]);
    }
}

#[test]
fn test_negative_enum_decodes_to_the_same_value() {
    let resp = ServerResponse {
        read: Some(ReadResponse {
            result: -1,
            ..Default::default()
        }),
        ..Default::default()
    };
    let out = encode_response(&resp, 0, u64::MAX).unwrap();
    // five byte varint instead of prost's ten, the same i32 once decoded
    assert_eq!(out.len() + 5, resp.encoded_len());
    assert_eq!(ServerResponse::decode(&out[..]).unwrap(), resp);
}

#[test]
fn test_too_large_is_refused_before_encoding() {
    let resp = ServerResponse {
        read: Some(ReadResponse {
            data: Bytes::from(vec![0; 100]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let size = resp.encoded_len();
    assert_eq!(encode_response(&resp, 0, size as u64).unwrap().len(), size);
    assert_eq!(
        encode_response(&resp, 0, size as u64 - 1),
        Err(super::response::ResponseError::TooLarge(size))
    );
}
