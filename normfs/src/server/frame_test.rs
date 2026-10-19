use super::frame::{encode_header, FrameError, FRAME_MAX_PAYLOAD};

#[test]
fn test_header_is_little_endian_u64() {
    for len in [0u64, 1, 0x1234_5678, FRAME_MAX_PAYLOAD] {
        assert_eq!(encode_header(len).unwrap(), len.to_le_bytes());
    }
}

#[test]
fn test_too_large_carries_the_length() {
    assert_eq!(
        encode_header(FRAME_MAX_PAYLOAD + 1),
        Err(FrameError::TooLarge(FRAME_MAX_PAYLOAD + 1))
    );
}
