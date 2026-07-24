#include <assert.h>
#include <stdio.h>

#include "normfs/store_header.h"

static void
roundtrip(uint64_t compression, uint64_t encryption, uint64_t entries_before,
    uint64_t entries, size_t expected_size)
{
	struct normfs_store_header_v1 header;
	struct normfs_store_header_encode_result encoded;
	struct normfs_store_header_decode_result decoded;
	uint8_t buf[NORMFS_STORE_HEADER_V1_MAX_SIZE];

	header.compression = compression;
	header.encryption = encryption;
	header.num_entries_before = entries_before;
	header.num_entries = entries;

	assert(normfs_store_header_v1_size(&header) == expected_size);

	encoded = normfs_store_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_STORE_HEADER_OK);
	assert(encoded.written == expected_size);
	assert(buf[0] == NORMFS_STORE_HEADER_V1);

	decoded = normfs_store_header_v1_decode(buf, encoded.written);
	assert(decoded.status == NORMFS_STORE_HEADER_OK);
	assert(decoded.consumed == expected_size);
	assert(decoded.header.compression == compression);
	assert(decoded.header.encryption == encryption);
	assert(decoded.header.num_entries_before == entries_before);
	assert(decoded.header.num_entries == entries);
}

static void
test_roundtrip_across_varint_widths(void)
{
	roundtrip(0u, 0u, 0u, 0u, 5u);
	roundtrip(3u, 1u, 127u, 127u, 5u);
	roundtrip(2u, 1u, 128u, 127u, 6u);
	roundtrip(2u, 1u, 16383u, 16384u, 8u);
	roundtrip(3u, 1u, 9876543210u, 1000u, 10u);
	roundtrip(0u, 0u, 0xffffffffffffffffull, 0xffffffffffffffffull, 23u);
}

static void
test_encode_reports_short_output_buffer(void)
{
	struct normfs_store_header_v1 header = {
	    0u, 0u, 0xffffffffffffffffull, 0xffffffffffffffffull
	};
	struct normfs_store_header_encode_result encoded;
	uint8_t buf[NORMFS_STORE_HEADER_V1_MAX_SIZE];
	size_t len;

	for (len = 0u; len < 23u; len++) {
		encoded = normfs_store_header_v1_encode(&header, buf, len);
		assert(encoded.status == NORMFS_STORE_HEADER_ERR_NO_SPACE);
		assert(encoded.written == 0u);
	}

	encoded = normfs_store_header_v1_encode(&header, buf, 23u);
	assert(encoded.status == NORMFS_STORE_HEADER_OK);
	assert(encoded.written == 23u);
}

static void
test_decode_rejects_truncated_input(void)
{
	struct normfs_store_header_v1 header = {3u, 1u, 9876543210u, 1000u};
	struct normfs_store_header_encode_result encoded;
	struct normfs_store_header_decode_result decoded;
	uint8_t buf[NORMFS_STORE_HEADER_V1_MAX_SIZE];
	size_t len;

	encoded = normfs_store_header_v1_encode(&header, buf, sizeof(buf));
	assert(encoded.status == NORMFS_STORE_HEADER_OK);

	for (len = 0u; len < encoded.written; len++) {
		decoded = normfs_store_header_v1_decode(buf, len);
		assert(decoded.status == NORMFS_STORE_HEADER_ERR_TRUNCATED);
		assert(decoded.consumed == 0u);
		assert(decoded.header.compression == 0u);
		assert(decoded.header.encryption == 0u);
		assert(decoded.header.num_entries_before == 0u);
		assert(decoded.header.num_entries == 0u);
	}
}

static void
test_decode_rejects_non_canonical_varint(void)
{
	const uint8_t buf[] = {1u, 0u, 0u, 0x83u, 0x00u, 0u};
	struct normfs_store_header_decode_result decoded;

	decoded = normfs_store_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_STORE_HEADER_ERR_NON_CANONICAL);
	assert(decoded.consumed == 0u);
}

static void
test_decode_rejects_overflowing_varint(void)
{
	const uint8_t buf[] = {
	    1u, 0u, 0u,
	    0xffu, 0xffu, 0xffu, 0xffu, 0xffu,
	    0xffu, 0xffu, 0xffu, 0xffu, 0x02u,
	    0u
	};
	struct normfs_store_header_decode_result decoded;

	decoded = normfs_store_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_STORE_HEADER_ERR_OVERFLOW);
	assert(decoded.consumed == 0u);
}

static void
test_decode_rejects_unsupported_version(void)
{
	const uint8_t two[] = {2u, 0u, 0u, 0u, 0u};
	const uint8_t large[] = {0xffu, 0x01u, 0u, 0u, 0u, 0u};
	struct normfs_store_header_decode_result decoded;

	decoded = normfs_store_header_v1_decode(two, sizeof(two));
	assert(decoded.status == NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION);
	assert(decoded.consumed == 0u);

	decoded = normfs_store_header_v1_decode(large, sizeof(large));
	assert(decoded.status == NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION);
}

static void
test_peek_version_separates_v0_from_v1(void)
{
	const uint8_t v0[] = {
	    0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    2u, 0u, 0u, 0u, 0u, 0u, 0u, 0u,
	    1u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
	};
	const uint8_t v1[] = {1u, 3u, 1u, 0u, 0u};
	struct normfs_store_header_version_result version;

	version = normfs_store_header_peek_version(v0, sizeof(v0));
	assert(version.status == NORMFS_STORE_HEADER_OK);
	assert(version.version == NORMFS_STORE_HEADER_V0);
	assert(version.consumed == 0u);

	version = normfs_store_header_peek_version(v1, sizeof(v1));
	assert(version.status == NORMFS_STORE_HEADER_OK);
	assert(version.version == NORMFS_STORE_HEADER_V1);
	assert(version.consumed == 1u);

	version = normfs_store_header_peek_version(v1, 0u);
	assert(version.status == NORMFS_STORE_HEADER_ERR_TRUNCATED);
	assert(version.version == 0u);
	assert(version.consumed == 0u);
}

static void
test_decode_ignores_trailing_bytes(void)
{
	const uint8_t buf[] = {1u, 3u, 1u, 10u, 20u, 0xaau, 0xbbu};
	struct normfs_store_header_decode_result decoded;

	decoded = normfs_store_header_v1_decode(buf, sizeof(buf));
	assert(decoded.status == NORMFS_STORE_HEADER_OK);
	assert(decoded.consumed == 5u);
	assert(decoded.header.compression == 3u);
	assert(decoded.header.encryption == 1u);
	assert(decoded.header.num_entries_before == 10u);
	assert(decoded.header.num_entries == 20u);
}

int
main(void)
{
	test_roundtrip_across_varint_widths();
	test_encode_reports_short_output_buffer();
	test_decode_rejects_truncated_input();
	test_decode_rejects_non_canonical_varint();
	test_decode_rejects_overflowing_varint();
	test_decode_rejects_unsupported_version();
	test_peek_version_separates_v0_from_v1();
	test_decode_ignores_trailing_bytes();

	printf("store_header: all tests passed\n");
	return 0;
}
