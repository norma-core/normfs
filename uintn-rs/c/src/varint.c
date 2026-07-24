#include "uintn/varint.h"

/*@ assigns \nothing;
    ensures \result == normfs_uintn_varint32_size_logic(value);
    ensures 1 <= \result <= 5;
*/
size_t
normfs_uintn_varint32_size_ffi(uint32_t value)
{
	return normfs_uintn_varint32_size(value);
}

/*@ assigns \nothing;
    ensures \result == normfs_uintn_varint64_size_logic(value);
    ensures 1 <= \result <= 10;
*/
size_t
normfs_uintn_varint64_size_ffi(uint64_t value)
{
	return normfs_uintn_varint64_size(value);
}

/*@ requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.written == normfs_uintn_varint32_size_logic(value);
    ensures \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE ==>
              \result.written == 0;
    ensures \result.written <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            value < 0x80 ==>
              out[0] == (uint8_t)value;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x80 <= value < 0x4000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(value / 128);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x4000 <= value < 0x200000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(value / 0x4000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x200000 <= value < 0x10000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(value / 0x200000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x10000000 <= value ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(value / 0x10000000);
*/
struct normfs_uintn_varint_encode_result
normfs_uintn_varint32_encode_ffi(uint32_t value, uint8_t *out, size_t out_len)
{
	return normfs_uintn_varint32_encode(value, out, out_len);
}

/*@ requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.written == normfs_uintn_varint64_size_logic(value);
    ensures \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE ==>
              \result.written == 0;
    ensures \result.written <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            value < 0x80 ==>
              out[0] == (uint8_t)value;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x80 <= value < 0x4000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(value / 128);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x4000 <= value < 0x200000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(value / 0x4000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x200000 <= value < 0x10000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(value / 0x200000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x10000000 <= value < 0x800000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(value / 0x10000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x800000000 <= value < 0x40000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(value / 0x800000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x40000000000 <= value < 0x2000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(value / 0x40000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x2000000000000 <= value < 0x100000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(value / 0x2000000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x100000000000000 <= value < 0x8000000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(128 + (value / 0x2000000000000) % 128) &&
              out[8] == (uint8_t)(value / 0x100000000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x8000000000000000 <= value ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(128 + (value / 0x2000000000000) % 128) &&
              out[8] == (uint8_t)(128 + (value / 0x100000000000000) % 128) &&
              out[9] == (uint8_t)(value / 0x8000000000000000);
*/
struct normfs_uintn_varint_encode_result
normfs_uintn_varint64_encode_ffi(uint64_t value, uint8_t *out, size_t out_len)
{
	return normfs_uintn_varint64_encode(value, out, out_len);
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_TRUNCATED ||
            \result.status == NORMFS_UINTN_VARINT_ERR_OVERFLOW ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              1 <= \result.consumed <= 5 && \result.consumed <= len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.consumed == normfs_uintn_varint32_size_logic(\result.value);
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
struct normfs_uintn_varint32_decode_result
normfs_uintn_varint32_decode_ffi(const uint8_t *buf, size_t len)
{
	return normfs_uintn_varint32_decode(buf, len);
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_TRUNCATED ||
            \result.status == NORMFS_UINTN_VARINT_ERR_OVERFLOW ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              1 <= \result.consumed <= 10 && \result.consumed <= len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.consumed == normfs_uintn_varint64_size_logic(\result.value);
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
struct normfs_uintn_varint64_decode_result
normfs_uintn_varint64_decode_ffi(const uint8_t *buf, size_t len)
{
	return normfs_uintn_varint64_decode(buf, len);
}
