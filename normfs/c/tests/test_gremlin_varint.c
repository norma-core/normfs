/*
 * WP proves gremlin_varint_size_cascade; gcc and clang build
 * gremlin_varint_size_clz, which rests on __builtin_clzll. Compare them.
 */
#include <stdint.h>
#include <stdio.h>

#include "gremlin.h"

static int
same(uint64_t v)
{
	if (gremlin_varint_size_clz(v) == gremlin_varint_size_cascade(v))
		return 1;
	fprintf(stderr, "gremlin_varint: FAIL size of %llu\n", (unsigned long long)v);
	return 0;
}

int
main(void)
{
	uint64_t x = 0x9e3779b97f4a7c15u;
	int k, i;

	if (!same(0) || !same(UINT64_MAX))
		return 1;
	for (k = 1; k < 64; k++) {
		uint64_t b = (uint64_t)1 << k;
		if (!same(b - 1) || !same(b) || !same(b + 1))
			return 1;
	}
	for (i = 0; i < 1000000; i++) {
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		if (!same(x >> (i % 64)))
			return 1;
	}
	return 0;
}
