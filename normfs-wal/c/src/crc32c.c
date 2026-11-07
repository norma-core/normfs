#include "normfs/crc32c.h"

/*
 * Frama-C sees the fast path's contract but not its body: the intrinsics have
 * no meaning it can interpret. The contract is therefore assumed, and the
 * dispatcher below is proven against it. The assumption is
 * exactly "the CRC32 instruction computes CRC32C as its ISA specifies", and
 * test_crc32c.c checks it on every build against a reference the test derives
 * from the polynomial itself.
 */
#if defined(__FRAMAC__)
#define NORMFS_CRC32C_HW 1
#elif (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) || \
    defined(_MSC_VER)

#if defined(_WIN32) && (defined(_M_ARM64) || defined(__aarch64__))
#error "Windows on arm64 is not a supported target"
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define NORMFS_CRC32C_ARM 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET
#define NORMFS_CRC32C_STEP8(c, w) __crc32cd((c), (w))
#define NORMFS_CRC32C_STEP1(c, b) __crc32cb((c), (b))
#include <arm_acle.h>
#include <string.h>
/*
 * CRC32 is optional in ARMv8.0 and only mandatory from ARMv8.1, so a build for
 * generic armv8-a leaves __ARM_FEATURE_CRC32 undefined. The fast path is
 * compiled for +crc regardless: a core without the instruction is not a
 * supported target.
 */
#elif defined(__aarch64__) && \
    (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 11))
#define NORMFS_CRC32C_ARM 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET __attribute__((target("+crc")))
#define NORMFS_CRC32C_STEP8(c, w) __crc32cd((c), (w))
#define NORMFS_CRC32C_STEP1(c, b) __crc32cb((c), (b))
#include <arm_acle.h>
#include <string.h>
#elif defined(__aarch64__)
#error "aarch64 builds of crc32c.c before GCC 11 require -march=armv8-a+crc"
#elif defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#if !defined(__SSE4_2__)
#error "x86_64 builds of crc32c.c require -msse4.2"
#endif
#define NORMFS_CRC32C_X86 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET
#define NORMFS_CRC32C_STEP8(c, w) ((uint32_t)_mm_crc32_u64((uint64_t)(c), (w)))
#define NORMFS_CRC32C_STEP1(c, b) ((uint32_t)_mm_crc32_u8((c), (b)))
#include <nmmintrin.h>
#include <string.h>
/* cl needs no -msse4.2 for the CRC32 intrinsics; every Windows target is little-endian. */
#elif defined(_MSC_VER) && defined(_M_X64)
#define NORMFS_CRC32C_X86 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET
#define NORMFS_CRC32C_STEP8(c, w) ((uint32_t)_mm_crc32_u64((uint64_t)(c), (w)))
#define NORMFS_CRC32C_STEP1(c, b) ((uint32_t)_mm_crc32_u8((c), (b)))
#include <nmmintrin.h>
#include <string.h>
#endif

#endif

#if !defined(NORMFS_CRC32C_HW)
#error "no CRC32 instruction for this target, which is not a supported one"
#endif

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t normfs_crc32c_hw(uint32_t crc, const uint8_t *data, size_t len);

/*
 * Columns of the GF(2) operator "append N zero bytes" to the raw CRC state:
 * normfs_crc32c_shift_N[i] is the state after folding N zero bytes from state
 * 1u << i (x^(8N) mod P applied to basis bit i, reflected convention, no
 * inversion). Used to join independently folded streams. Not part of the
 * public API; exported without a header declaration so test_crc32c can
 * re-derive them from the polynomial by repeated squaring and check them
 * entry by entry.
 */
const uint32_t normfs_crc32c_shift_256[32] = {
	0xDCB17AA4U, 0xBC8E83B9U, 0x7CF17183U, 0xF9E2E306U,
	0xF629B0FDU, 0xE9BF170BU, 0xD69258E7U, 0xA8C8C73FU,
	0x547DF88FU, 0xA8FBF11EU, 0x541B94CDU, 0xA837299AU,
	0x558225C5U, 0xAB044B8AU, 0x53E4E1E5U, 0xA7C9C3CAU,
	0x4A7FF165U, 0x94FFE2CAU, 0x2C13B365U, 0x582766CAU,
	0xB04ECD94U, 0x6571EDD9U, 0xCAE3DBB2U, 0x902BC195U,
	0x25BBF5DBU, 0x4B77EBB6U, 0x96EFD76CU, 0x2833D829U,
	0x5067B052U, 0xA0CF60A4U, 0x4472B7B9U, 0x88E56F72U
};

const uint32_t normfs_crc32c_shift_512[32] = {
	0xBD6F81F8U, 0x7F337501U, 0xFE66EA02U, 0xF921A2F5U,
	0xF7AF331BU, 0xEAB210C7U, 0xD088577FU, 0xA4FCD80FU,
	0x4C15C6EFU, 0x982B8DDEU, 0x35BB6D4DU, 0x6B76DA9AU,
	0xD6EDB534U, 0xA8371C99U, 0x55824FC3U, 0xAB049F86U,
	0x53E549FDU, 0xA7CA93FAU, 0x4A795105U, 0x94F2A20AU,
	0x2C0932E5U, 0x581265CAU, 0xB024CB94U, 0x65A5E1D9U,
	0xCB4BC3B2U, 0x937BF195U, 0x231B95DBU, 0x46372BB6U,
	0x8C6E576CU, 0x1D30D829U, 0x3A61B052U, 0x74C360A4U
};

const uint32_t normfs_crc32c_shift_1024[32] = {
	0xFE314258U, 0xF98EF241U, 0xF6F19273U, 0xE80F5217U,
	0xD5F2D2DFU, 0xAE09D34FU, 0x59FFD06FU, 0xB3FFA0DEU,
	0x6213374DU, 0xC4266E9AU, 0x8DA0ABC5U, 0x1EAD217BU,
	0x3D5A42F6U, 0x7AB485ECU, 0xF5690BD8U, 0xEF3E6141U,
	0xDB90B473U, 0xB2CD1E17U, 0x60764ADFU, 0xC0EC95BEU,
	0x84355D8DU, 0x0D86CDEBU, 0x1B0D9BD6U, 0x361B37ACU,
	0x6C366F58U, 0xD86CDEB0U, 0xB535CB91U, 0x6F87E1D3U,
	0xDF0FC3A6U, 0xBBF3F1BDU, 0x720B958BU, 0xE4172B16U
};

const uint32_t normfs_crc32c_shift_2048[32] = {
	0xF7506984U, 0xEB4CA5F9U, 0xD3753D03U, 0xA3060CF7U,
	0x43E06F1FU, 0x87C0DE3EU, 0x0A6DCA8DU, 0x14DB951AU,
	0x29B72A34U, 0x536E5468U, 0xA6DCA8D0U, 0x48552751U,
	0x90AA4EA2U, 0x24B8EBB5U, 0x4971D76AU, 0x92E3AED4U,
	0x202B2B59U, 0x405656B2U, 0x80ACAD64U, 0x04B52C39U,
	0x096A5872U, 0x12D4B0E4U, 0x25A961C8U, 0x4B52C390U,
	0x96A58720U, 0x28A778B1U, 0x514EF162U, 0xA29DE2C4U,
	0x40D7B379U, 0x81AF66F2U, 0x06B2BB15U, 0x0D65762AU
};

#if defined(NORMFS_CRC32C_HW) && !defined(__FRAMAC__)

/*
 * Image of v under the operator whose columns are m. Branchless: a bit-test
 * loop mispredicts about half the time on random states.
 */
static uint32_t
normfs_crc32c_shift(uint32_t v, const uint32_t m[32])
{
	uint32_t r = 0u;
	uint32_t i;

	for (i = 0u; i < 32u; i++)
		r ^= m[i] & (0u - ((v >> i) & 1u));

	return r;
}

/* Serial fold over the raw CRC state: no inversion on entry or exit. */
NORMFS_CRC32C_HW_TARGET static uint32_t
normfs_crc32c_hw_serial(uint32_t c, const uint8_t *data, size_t len)
{
	size_t i = 0u;

	while (len - i >= 8u) {
		uint64_t word;
		memcpy(&word, data + i, sizeof(word));
		c = NORMFS_CRC32C_STEP8(c, word);
		i += 8u;
	}

	while (i < len) {
		c = NORMFS_CRC32C_STEP1(c, data[i]);
		i++;
	}

	return c;
}

/*
 * One 3n-byte block as three independent n-byte streams: the crc
 * instruction's latency hides behind the other two chains, so the block runs
 * at instruction throughput instead. n must be a multiple of 8. Streams b and
 * c start from state 0, which makes their folds the constant terms of the
 * affine block maps, so the shift operators join the three results exactly.
 */
NORMFS_CRC32C_HW_TARGET static uint32_t
normfs_crc32c_hw_block3(uint32_t c0, const uint8_t *p, size_t n,
    const uint32_t m2n[32], const uint32_t mn[32])
{
	uint32_t a = c0;
	uint32_t b = 0u;
	uint32_t c = 0u;
	size_t i;

	for (i = 0u; i < n; i += 8u) {
		uint64_t wa;
		uint64_t wb;
		uint64_t wc;

		memcpy(&wa, p + i, sizeof(wa));
		memcpy(&wb, p + n + i, sizeof(wb));
		memcpy(&wc, p + 2u * n + i, sizeof(wc));
		a = NORMFS_CRC32C_STEP8(a, wa);
		b = NORMFS_CRC32C_STEP8(b, wb);
		c = NORMFS_CRC32C_STEP8(c, wc);
	}

	return normfs_crc32c_shift(a, m2n) ^ normfs_crc32c_shift(b, mn) ^ c;
}

/* The only place the 0xFFFFFFFF inversion convention lives on this path. */
NORMFS_CRC32C_HW_TARGET uint32_t
normfs_crc32c_hw(uint32_t crc, const uint8_t *data, size_t len)
{
	uint32_t c = ~crc;
	size_t i = 0u;

	while (len - i >= 3072u) {
		c = normfs_crc32c_hw_block3(c, data + i, 1024u,
		    normfs_crc32c_shift_2048, normfs_crc32c_shift_1024);
		i += 3072u;
	}

	while (len - i >= 768u) {
		c = normfs_crc32c_hw_block3(c, data + i, 256u,
		    normfs_crc32c_shift_512, normfs_crc32c_shift_256);
		i += 768u;
	}

	return ~normfs_crc32c_hw_serial(c, data + i, len - i);
}

#endif

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t
normfs_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
	return normfs_crc32c_hw(crc, data, len);
}
