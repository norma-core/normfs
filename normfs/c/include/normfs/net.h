#ifndef NORMFS_NET_H
#define NORMFS_NET_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/frame.h"

enum normfs_net_status {
	NORMFS_NET_OK = 0,
	NORMFS_NET_WOULD_BLOCK = 1,
	NORMFS_NET_EOF = 2,
	NORMFS_NET_ERR_IO = 3,
	NORMFS_NET_ERR_TOO_LARGE = 4,
	NORMFS_NET_ERR_BAD_LENGTH = 5,
	NORMFS_NET_YIELD = 6
};

/* Syscalls one read or write call makes before it hands control back. */
#define NORMFS_NET_BATCH 16

/* os_error is the errno of a failed syscall, 0 otherwise. */
struct normfs_net_result {
	uint64_t value;
	int os_error;
	int status;
};

/* Where the read side of a connection is: inside a header or inside a payload. */
struct normfs_net_reader {
	uint8_t header[NORMFS_FRAME_HEADER_SIZE];
	size_t header_have;
	uint64_t payload_len;
	uint64_t payload_have;
	int in_payload;
};

void normfs_net_reader_init(struct normfs_net_reader *r);

/*
 * Reads a frame header from a non-blocking socket. OK carries the payload
 * length in value; a zero length is a keepalive and the reader is ready for
 * the next header. WOULD_BLOCK keeps the bytes read so far.
 */
struct normfs_net_result
normfs_net_read_header(struct normfs_net_reader *r, int fd);

/*
 * Reads the payload announced by the last header into buf, whose length must
 * equal it. OK once the whole payload is in, after which the reader expects a
 * header again. YIELD after NORMFS_NET_BATCH reads that left more to come.
 */
struct normfs_net_result
normfs_net_read_payload(struct normfs_net_reader *r, int fd, uint8_t *buf,
    size_t len);

/* Writes buf[*off .. len) until done, the socket would block or a batch of
 * writes is spent (YIELD); *off advances. */
struct normfs_net_result
normfs_net_write(int fd, const uint8_t *buf, size_t len, size_t *off);

#endif /* NORMFS_NET_H */
