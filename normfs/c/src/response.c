#include "normfs/response.h"

/* response.rs mirrors these structs and asserts the same sizes. */
typedef char normfs_response_sizes_ok[
    sizeof(normfs_Id) == 24 && sizeof(normfs_SetupResponse) == 48 &&
    sizeof(normfs_PingRequest) == 24 && sizeof(normfs_PingResponse) == 32 &&
    sizeof(normfs_WriteResponse) == 40 && sizeof(normfs_ReadResponse) == 56 &&
    sizeof(normfs_ServerResponse) == 40 ? 1 : -1];

/*@ requires normfs_ServerResponse_valid(m);
    requires normfs_ServerResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->setup->_size, m->ping->_size,
            *(m->ping != \null ? &m->ping->request->_size : &m->_size),
            m->write->_size,
            m->write->ids[0 .. (m->write != \null ? m->write->ids_count : 0) - 1]->_size,
            m->read->_size, *(m->read != \null ? &m->read->id->_size : &m->_size);
    ensures \result == \old(normfs_ServerResponse_tsize(m));
    ensures normfs_ServerResponse_sized(m);
    ensures normfs_ServerResponse_dsame{Pre,Post}(m);
*/
size_t
normfs_response_size(const normfs_ServerResponse *m)
{
	return normfs_ServerResponse_size(m);
}

/*@ requires normfs_ServerResponse_valid(m) && normfs_ServerResponse_sized(m);
    requires len == 0 || \valid(buf + (0 .. len - 1));
    requires normfs_ServerResponse_bufsep(m, buf, 0, len - 1);
    assigns buf[0 .. len - 1];
    ensures \result == NORMFS_RESPONSE_OK || \result == NORMFS_RESPONSE_ERR_BAD_LENGTH;
    ensures \result == NORMFS_RESPONSE_OK <==> m->_size == len && len <= 0x7fffffffffffffff;
    ensures \result != NORMFS_RESPONSE_OK ==>
              \forall integer i; 0 <= i < len ==> buf[i] == \old(buf[i]);
*/
int
normfs_response_encode(const normfs_ServerResponse *m, uint8_t *buf, size_t len)
{
	if (m->_size != len || len > 0x7fffffffffffffffu)
		return NORMFS_RESPONSE_ERR_BAD_LENGTH;
	normfs_ServerResponse_encode_at(m, buf, 0);
	return NORMFS_RESPONSE_OK;
}
