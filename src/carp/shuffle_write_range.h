/*
 * shuffle_write_range.h  shuffle_write for carp range store
 */

#include "../preload_internal.h"

// carp/range: the shuffle key/value should match the preload inkey/invalue
int shuffle_write_range(shuffle_ctx_t* ctx, const char* skey,
                        unsigned char skey_len, char* svalue,
                        unsigned char svalue_len, int epoch);
