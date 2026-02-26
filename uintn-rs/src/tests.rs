use super::*;

#[test]
fn test_zero() {
    let zero = UintN::zero();
    assert_eq!(zero, UintN::U8(0));
    assert!(zero.is_zero());
}

#[test]
fn test_one() {
    let one = UintN::one();
    assert_eq!(one, UintN::U8(1));
    assert!(!one.is_zero());
}

#[test]
fn test_from_conversions() {
    assert_eq!(UintN::from(0u8), UintN::U8(0));
    assert_eq!(UintN::from(255u8), UintN::U8(255));

    assert_eq!(UintN::from(0u16), UintN::U8(0));
    assert_eq!(UintN::from(255u16), UintN::U8(255));
    assert_eq!(UintN::from(256u16), UintN::U16(256));
    assert_eq!(UintN::from(65535u16), UintN::U16(65535));

    assert_eq!(UintN::from(0u32), UintN::U8(0));
    assert_eq!(UintN::from(255u32), UintN::U8(255));
    assert_eq!(UintN::from(256u32), UintN::U16(256));
    assert_eq!(UintN::from(65535u32), UintN::U16(65535));
    assert_eq!(UintN::from(65536u32), UintN::U32(65536));

    assert_eq!(UintN::from(0u64), UintN::U8(0));
    assert_eq!(UintN::from(4294967296u64), UintN::U64(4294967296));

    assert_eq!(UintN::from(0u128), UintN::U8(0));
    assert_eq!(
        UintN::from(18446744073709551616u128),
        UintN::U128(18446744073709551616)
    );
}

#[test]
fn test_increment() {
    let val = UintN::U8(254);
    let inc = val.increment();
    assert_eq!(inc, UintN::U8(255));

    let val = UintN::U8(255);
    let inc = val.increment();
    assert_eq!(inc, UintN::U16(256));

    let val = UintN::U16(65535);
    let inc = val.increment();
    assert_eq!(inc, UintN::U32(65536));

    let val = UintN::U32(4294967295);
    let inc = val.increment();
    assert_eq!(inc, UintN::U64(4294967296));

    let val = UintN::U64(u64::MAX);
    let inc = val.increment();
    assert_eq!(inc, UintN::U128(18446744073709551616));

    let val = UintN::U128(u128::MAX);
    let inc = val.increment();
    assert!(matches!(inc, UintN::Big(_)));
}

#[test]
fn test_decrement() {
    let val = UintN::U8(1);
    let dec = val.decrement().unwrap();
    assert_eq!(dec, UintN::U8(0));

    let val = UintN::U8(0);
    assert!(val.decrement().is_err());

    let val = UintN::U16(256);
    let dec = val.decrement().unwrap();
    assert_eq!(dec, UintN::U8(255));

    let val = UintN::U32(65536);
    let dec = val.decrement().unwrap();
    assert_eq!(dec, UintN::U16(65535));
}

#[test]
fn test_add_same_types() {
    let a = UintN::U8(100);
    let b = UintN::U8(50);
    assert_eq!(a.add(&b), UintN::U8(150));

    let a = UintN::U8(200);
    let b = UintN::U8(100);
    assert_eq!(a.add(&b), UintN::U16(300));

    let a = UintN::U16(30000);
    let b = UintN::U16(20000);
    assert_eq!(a.add(&b), UintN::U16(50000));

    let a = UintN::U16(40000);
    let b = UintN::U16(30000);
    assert_eq!(a.add(&b), UintN::U32(70000));
}

#[test]
fn test_add_mixed_types() {
    let a = UintN::U8(100);
    let b = UintN::U16(1000);
    assert_eq!(a.add(&b), UintN::U16(1100));

    let a = UintN::U8(100);
    let b = UintN::U32(100000);
    assert_eq!(a.add(&b), UintN::U32(100100));

    let a = UintN::U16(1000);
    let b = UintN::U64(1000000000);
    assert_eq!(a.add(&b), UintN::U32(1000001000));
}

#[test]
fn test_add_with_zero() {
    let a = UintN::U32(12345);
    let zero = UintN::zero();
    assert_eq!(a.add(&zero), a);
}

#[test]
fn test_sub_same_types() {
    let a = UintN::U8(100);
    let b = UintN::U8(50);
    assert_eq!(a.sub(&b).unwrap(), UintN::U8(50));

    let a = UintN::U16(1000);
    let b = UintN::U16(500);
    assert_eq!(a.sub(&b).unwrap(), UintN::U16(500));

    let a = UintN::U8(50);
    let b = UintN::U8(100);
    assert!(a.sub(&b).is_err());
}

#[test]
fn test_sub_mixed_types() {
    let a = UintN::U16(1000);
    let b = UintN::U8(100);
    assert_eq!(a.sub(&b).unwrap(), UintN::U16(900));

    let a = UintN::U32(100000);
    let b = UintN::U16(1000);
    assert_eq!(a.sub(&b).unwrap(), UintN::U32(99000));

    let a = UintN::U8(50);
    let b = UintN::U16(100);
    assert!(a.sub(&b).is_err());
}

#[test]
fn test_sub_with_zero() {
    let a = UintN::U64(12345);
    let zero = UintN::zero();
    assert_eq!(a.sub(&zero).unwrap(), a);
}

#[test]
fn test_ordering() {
    let a = UintN::U8(100);
    let b = UintN::U8(200);
    assert!(a < b);
    assert!(b > a);

    let a = UintN::U8(255);
    let b = UintN::U16(255);
    assert!(a.eq(&b));

    let a = UintN::U8(255);
    let b = UintN::U16(256);
    assert!(a < b);

    let a = UintN::U32(1000);
    let b = UintN::U16(999);
    assert!(a > b);
}

#[test]
fn test_display() {
    assert_eq!(format!("{}", UintN::U8(123)), "123");
    assert_eq!(format!("{}", UintN::U16(12345)), "12345");
    assert_eq!(format!("{}", UintN::U32(1234567)), "1234567");
    assert_eq!(format!("{}", UintN::U64(123456789)), "123456789");
    assert_eq!(
        format!("{}", UintN::U128(123456789012345)),
        "123456789012345"
    );
}

#[test]
fn test_to_u64() {
    assert_eq!(UintN::U8(123).to_u64().unwrap(), 123u64);
    assert_eq!(UintN::U16(12345).to_u64().unwrap(), 12345u64);
    assert_eq!(UintN::U32(1234567).to_u64().unwrap(), 1234567u64);
    assert_eq!(UintN::U64(123456789).to_u64().unwrap(), 123456789u64);
    assert_eq!(UintN::U128(123456789).to_u64().unwrap(), 123456789u64);

    let big_u128 = UintN::U128(u128::MAX);
    assert!(big_u128.to_u64().is_err());
}

#[test]
fn test_to_u128() {
    assert_eq!(UintN::U8(123).to_u128().unwrap(), 123u128);
    assert_eq!(UintN::U16(12345).to_u128().unwrap(), 12345u128);
    assert_eq!(UintN::U32(1234567).to_u128().unwrap(), 1234567u128);
    assert_eq!(UintN::U64(123456789).to_u128().unwrap(), 123456789u128);
    assert_eq!(UintN::U128(u128::MAX).to_u128().unwrap(), u128::MAX);
}

#[test]
fn test_shrink_to_fit() {
    let val = UintN::U32(255);
    assert_eq!(val.shrink_to_fit(), UintN::U8(255));

    let val = UintN::U64(65535);
    assert_eq!(val.shrink_to_fit(), UintN::U16(65535));

    let val = UintN::U128(4294967295);
    assert_eq!(val.shrink_to_fit(), UintN::U32(4294967295));

    let val = UintN::U128(4294967296);
    assert_eq!(val.shrink_to_fit(), UintN::U64(4294967296));
}

#[test]
fn test_get_type() {
    assert_eq!(UintN::U8(0).get_type(), UintNType::U8);
    assert_eq!(UintN::U16(0).get_type(), UintNType::U16);
    assert_eq!(UintN::U32(0).get_type(), UintNType::U32);
    assert_eq!(UintN::U64(0).get_type(), UintNType::U64);
    assert_eq!(UintN::U128(0).get_type(), UintNType::U128);
}

#[test]
fn test_forever_incrementing_id() {
    let mut id = UintN::zero();

    for i in 0..300 {
        match i {
            0..=254 => assert!(matches!(id, UintN::U8(_))),
            255 => assert_eq!(id, UintN::U8(255)),
            256 => assert_eq!(id, UintN::U16(256)),
            _ => {}
        }

        if i < 299 {
            id = id.increment();
        }
    }

    assert_eq!(id, UintN::U16(299));
}

#[test]
fn test_large_increments() {
    let mut val = UintN::U64(u64::MAX - 1);
    val = val.increment();
    assert_eq!(val, UintN::U64(u64::MAX));

    val = val.increment();
    assert_eq!(val, UintN::U128(u64::MAX as u128 + 1));
}

#[test]
fn test_add_overflow_to_big() {
    let a = UintN::U128(u128::MAX - 10);
    let b = UintN::U128(20);
    let result = a.add(&b);
    assert!(matches!(result, UintN::Big(_)));
}

#[test]
fn test_read_value_from_slice() {
    // u8
    let data = [42u8];
    let val = UintN::read_value_from_slice(&data, 1).unwrap();
    assert_eq!(val, UintN::U8(42));

    // u16
    let data = 1000u16.to_le_bytes();
    let val = UintN::read_value_from_slice(&data, 2).unwrap();
    assert_eq!(val, UintN::from(1000u16));

    // u32
    let data = 100000u32.to_le_bytes();
    let val = UintN::read_value_from_slice(&data, 4).unwrap();
    assert_eq!(val, UintN::from(100000u32));

    // u64
    let data = 10000000000u64.to_le_bytes();
    let val = UintN::read_value_from_slice(&data, 8).unwrap();
    assert_eq!(val, UintN::from(10000000000u64));

    // u128
    let data = 10000000000000000000u128.to_le_bytes();
    let val = UintN::read_value_from_slice(&data, 16).unwrap();
    assert_eq!(val, UintN::from(10000000000000000000u128));

    // Big
    let big_val =
        dashu_int::UBig::from_str_radix("1234567890123456789012345678901234567890", 10).unwrap();
    let data = big_val.to_le_bytes();
    let val = UintN::read_value_from_slice(&data, data.len()).unwrap();
    assert_eq!(val.to_ubig(), big_val);
}

#[test]
fn test_read_from_slice() {
    // u64
    let value: u64 = 123456789;
    let value_bytes = value.to_le_bytes();
    let len_bytes = (value_bytes.len() as u64).to_le_bytes();

    let mut buffer = Vec::new();
    buffer.extend_from_slice(&len_bytes);
    buffer.extend_from_slice(&value_bytes);

    let (read_val, bytes_read) = UintN::read_from_slice(&buffer).unwrap();

    assert_eq!(bytes_read, 8 + value_bytes.len());
    assert_eq!(read_val, UintN::from(value));
}
