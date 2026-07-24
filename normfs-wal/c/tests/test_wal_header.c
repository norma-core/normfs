#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "normfs/wal_header.h"

static void
roundtrip(uint64_t data_size, uint64_t id_size, uint64_t entries,
    size_t expected_size)
{
	struct normfs_wal_header_v1 header;
	struct normfs_wal_header_encode_result encoded;
	struct normfs_wal_header_decode_result decoded;
	uint8_t buf[NORMFS_WAL_HEADER_V1_MAX_SIZE];

	header.data_size_bytes = data_size;
	header.id_size_bytes = id_size;
	header.num_entries_before = entries;

	assert(normfs_wal_header_v1_size(&header) == expected_size);

	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_OK);
	assert(encoded.written == expected_size);
	assert(buf[0] == NORMFS_WAL_HEADER_V1);

	decoded = normfs_wal_header_v1_decode(buf, encoded.written);
	assert(decoded.status == NORMFS_WAL_HEADER_OK);
	assert(decoded.consumed == expected_size);
	assert(decoded.header.data_size_bytes == data_size);
	assert(decoded.header.id_size_bytes == id_size);
	assert(decoded.header.num_entries_before == entries);
}

static void
test_roundtrip_across_varint_widths(void)
{
	roundtrip(1u, 1u, 0u, 4u);
	roundtrip(2u, 4u, 127u, 4u);
	roundtrip(8u, 4u, 128u, 5u);
	roundtrip(8u, 8u, 16383u, 5u);
	roundtrip(8u, 8u, 16384u, 6u);
	roundtrip(16u, 16u, 9876543210u, 8u);
	roundtrip(8u, 4u, 0x7fffffffffffffffull, 12u);
	roundtrip(8u, 4u, 0xffffffffffffffffull, 13u);
}

static void
test_encode_reports_short_output_buffer(void)
{
	struct normfs_wal_header_v1 header = {8u, 4u, 0xffffffffffffffffull};
	struct normfs_wal_header_encode_result encoded;
	uint8_t buf[NORMFS_WAL_HEADER_V1_MAX_SIZE];
	size_t len;

	for (len = 0u; len < NORMFS_WAL_HEADER_V1_MAX_SIZE; len++) {
		encoded = normfs_wal_header_v1_encode(&header, buf, len);
		assert(encoded.status == NORMFS_WAL_HEADER_ERR_NO_SPACE);
		assert(encoded.written == 0u);
	}

	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_OK);
}

static void
test_encode_rejects_invalid_field_sizes(void)
{
	struct normfs_wal_header_v1 header = {3u, 4u, 0u};
	struct normfs_wal_header_encode_result encoded;
	uint8_t buf[NORMFS_WAL_HEADER_V1_MAX_SIZE];

	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE);
	assert(encoded.written == 0u);

	header.data_size_bytes = 8u;
	header.id_size_bytes = 5u;
	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE);
	assert(encoded.written == 0u);

	header.id_size_bytes = 0u;
	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE);
}

static void
test_decode_rejects_truncated_input(void)
{
	struct normfs_wal_header_v1 header = {8u, 4u, 0xffffffffffffffffull};
	struct normfs_wal_header_encode_result encoded;
	struct normfs_wal_header_decode_result decoded;
	uint8_t buf[NORMFS_WAL_HEADER_V1_MAX_SIZE];
	size_t len;

	encoded = normfs_wal_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_WAL_HEADER_OK);

	for (len = 0u; len < encoded.written; len++) {
		decoded = normfs_wal_header_v1_decode(buf, len);
		assert(decoded.status == NORMFS_WAL_HEADER_ERR_TRUNCATED);
		assert(decoded.consumed == 0u);
		assert(decoded.header.data_size_bytes == 0u);
		assert(decoded.header.id_size_bytes == 0u);
		assert(decoded.header.num_entries_before == 0u);
	}
}

static void
test_decode_rejects_non_canonical_varint(void)
{
	const uint8_t buf[] = {1u, 0x88u, 0x00u, 4u, 0u};
	struct normfs_wal_header_decode_result decoded;

	decoded = normfs_wal_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_NON_CANONICAL);
	assert(decoded.consumed == 0u);
}

static void
test_decode_rejects_overflowing_varint(void)
{
	const uint8_t buf[] = {
	    1u, 8u, 4u,
	    0xffu, 0xffu, 0xffu, 0xffu, 0xffu,
	    0xffu, 0xffu, 0xffu, 0xffu, 0x02u
	};
	struct normfs_wal_header_decode_result decoded;

	decoded = normfs_wal_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_OVERFLOW);
	assert(decoded.consumed == 0u);
}

static void
test_decode_rejects_unsupported_version(void)
{
	const uint8_t two[] = {2u, 8u, 4u, 0u};
	const uint8_t large[] = {0xffu, 0x01u, 8u, 4u, 0u};
	struct normfs_wal_header_decode_result decoded;

	decoded = normfs_wal_header_v1_decode(two, sizeof(two));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION);
	assert(decoded.consumed == 0u);

	decoded = normfs_wal_header_v1_decode(large, sizeof(large));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION);
}

static void
test_decode_rejects_invalid_field_sizes(void)
{
	const uint8_t bad_data_size[] = {1u, 3u, 4u, 0u};
	const uint8_t bad_id_size[] = {1u, 8u, 7u, 0u};
	struct normfs_wal_header_decode_result decoded;

	decoded = normfs_wal_header_v1_decode(bad_data_size, sizeof(bad_data_size));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE);
	assert(decoded.consumed == 0u);
	assert(decoded.header.data_size_bytes == 3u);

	decoded = normfs_wal_header_v1_decode(bad_id_size, sizeof(bad_id_size));
	assert(decoded.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE);
	assert(decoded.consumed == 0u);
	assert(decoded.header.data_size_bytes == 8u);
	assert(decoded.header.id_size_bytes == 7u);
}

static void
test_peek_version_separates_v0_from_v1(void)
{
	/* From Go test: Go WAL Header 1 (dataSize: 2, idSize: 4, entries: 123) */
	const uint8_t v0[] = {
	    0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    2u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    4u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    8u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    123u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
	};
	const uint8_t v1[] = {1u, 8u, 4u, 0u};
	const uint8_t v2[] = {2u, 8u, 4u, 0u};
	struct normfs_wal_header_version_result version;

	version = normfs_wal_header_peek_version(v0, sizeof(v0));
	assert(version.status == NORMFS_WAL_HEADER_OK);
	assert(version.version == NORMFS_WAL_HEADER_V0);
	assert(version.consumed == 0u);

	version = normfs_wal_header_peek_version(v1, sizeof(v1));
	assert(version.status == NORMFS_WAL_HEADER_OK);
	assert(version.version == NORMFS_WAL_HEADER_V1);
	assert(version.consumed == 1u);

	version = normfs_wal_header_peek_version(v2, sizeof(v2));
	assert(version.status == NORMFS_WAL_HEADER_OK);
	assert(version.version == 2u);
	assert(version.consumed == 1u);

	version = normfs_wal_header_peek_version(v1, 0u);
	assert(version.status == NORMFS_WAL_HEADER_ERR_TRUNCATED);
	assert(version.version == 0u);
	assert(version.consumed == 0u);
}

static void
test_decode_ignores_trailing_bytes(void)
{
	const uint8_t buf[] = {1u, 8u, 4u, 123u, 0xaau, 0xbbu};
	struct normfs_wal_header_decode_result decoded;

	decoded = normfs_wal_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_WAL_HEADER_OK);
	assert(decoded.consumed == 4u);
	assert(decoded.header.num_entries_before == 123u);
}

int
main(void)
{
	test_roundtrip_across_varint_widths();
	test_encode_reports_short_output_buffer();
	test_encode_rejects_invalid_field_sizes();
	test_decode_rejects_truncated_input();
	test_decode_rejects_non_canonical_varint();
	test_decode_rejects_overflowing_varint();
	test_decode_rejects_unsupported_version();
	test_decode_rejects_invalid_field_sizes();
	test_peek_version_separates_v0_from_v1();
	test_decode_ignores_trailing_bytes();

	printf("wal_header: all tests passed\n");
	return 0;
}
