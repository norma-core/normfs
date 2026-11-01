#ifndef NORMFS_NET_H
#define NORMFS_NET_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/frame.h"
#include "normfs/net_sys.h"

enum normfs_net_status {
	NORMFS_NET_OK = 0,
	NORMFS_NET_WOULD_BLOCK = 1,
	NORMFS_NET_EOF = 2,
	NORMFS_NET_ERR_IO = 3,
	NORMFS_NET_ERR_TOO_LARGE = 4,
	NORMFS_NET_ERR_BAD_LENGTH = 5,
	NORMFS_NET_YIELD = 6,
	NORMFS_NET_FULL = 7
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

/*@ predicate normfs_net_reader_ok(struct normfs_net_reader *r) =
      r->header_have <= NORMFS_FRAME_HEADER_SIZE &&
      r->payload_len <= NORMFS_FRAME_MAX_PAYLOAD &&
      r->payload_have <= r->payload_len &&
      (r->in_payload == 0 || r->in_payload == 1) &&
      (r->in_payload == 1 ==> r->header_have == 0) &&
      (r->in_payload == 0 ==> r->payload_have == 0);

    predicate normfs_net_header_read{L}(struct normfs_net_reader *r, integer fd) =
      \forall integer i; 0 <= i < r->header_have ==>
        r->header[i] == net_in(fd, net_pos(fd) - r->header_have + i);

    predicate normfs_net_payload_read{L}(uint8_t *buf, integer have, integer fd) =
      \forall integer i; 0 <= i < have ==>
        buf[i] == net_in(fd, net_pos(fd) - have + i);

    predicate normfs_net_reported(struct normfs_net_result res, integer last) =
      (res.status == NORMFS_NET_WOULD_BLOCK ==> last == NORMFS_NET_SYS_WOULD_BLOCK) &&
      (res.status == NORMFS_NET_EOF ==> last == 0) &&
      (res.status == NORMFS_NET_ERR_IO ==> last == -1 && res.os_error > 0) &&
      (res.status != NORMFS_NET_ERR_IO ==> res.os_error == 0);
*/

void normfs_net_reader_init(struct normfs_net_reader *r);

/*
 * Reads a frame header from a non-blocking socket. OK carries the payload
 * length in value; a zero length is a keepalive and the reader is ready for
 * the next header. WOULD_BLOCK keeps the bytes read so far.
 */
struct normfs_net_result
normfs_net_read_header(struct normfs_net_reader *r, int fd);

/*
 * Reads the payload announced by the last header into buf, which holds len
 * bytes, the ones read so far at its start. OK once the whole payload is in,
 * after which the reader expects a header again; FULL when buf is full and
 * more is to come. YIELD after NORMFS_NET_BATCH reads that left more to come.
 */
struct normfs_net_result
normfs_net_read_payload(struct normfs_net_reader *r, int fd, uint8_t *buf,
    size_t len);

/* Writes buf[*off .. len) until done, the socket would block or a batch of
 * writes is spent (YIELD); *off advances. */
struct normfs_net_result
normfs_net_write(int fd, const uint8_t *buf, size_t len, size_t *off);

#endif /* NORMFS_NET_H */
