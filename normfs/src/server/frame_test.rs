use super::frame::{
    decode_header, encode_header, FrameError, FRAME_HEADER_SIZE, FRAME_MAX_PAYLOAD,
};

#[test]
fn test_header_is_little_endian_u64() {
    for len in [0u64, 1, 0x1234_5678, FRAME_MAX_PAYLOAD] {
        let header = encode_header(len).unwrap();
        assert_eq!(header, len.to_le_bytes());
        assert_eq!(decode_header(&header).unwrap(), len);
    }
}

#[test]
fn test_too_large_carries_the_length() {
    assert_eq!(
        encode_header(FRAME_MAX_PAYLOAD + 1),
        Err(FrameError::TooLarge(FRAME_MAX_PAYLOAD + 1))
    );
    let header: [u8; FRAME_HEADER_SIZE] = [0xff; FRAME_HEADER_SIZE];
    assert_eq!(decode_header(&header), Err(FrameError::TooLarge(u64::MAX)));
}
