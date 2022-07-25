#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "common.h"
#include "range_common.h"

#define MSGFMT_MAX_BUFSIZE 255

#define ABORT_FILENAME \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define ABORT(msg) msg_abort(errno, msg, __func__, ABORT_FILENAME, __LINE__)

#include <vector>

#define RANGE_IS_INIT(x) (x->range_state == range_state_t::RS_INIT)
#define RANGE_IS_READY(x) (x->range_state == range_state_t::RS_READY)
#define RANGE_IS_RENEGO(x) (x->range_state == range_state_t::RS_RENEGO)
#define RANGE_IS_BLOCKED(x) (x->range_state == range_state_t::RS_BLOCKED)
#define RANGE_IS_ACK(x) (x->range_state == range_state_t::RS_ACK)

#define RANGE_LEFT_OOB_FULL(x) (x->oob_count_left == DEFAULT_OOBSZ)
#define RANGE_RIGHT_OOB_FULL(x) (x->oob_count_right == DEFAULT_OOBSZ)

#define RANGE_OOB_FULL(x) \
  (x->oob_count_left + x->oob_count_right == RANGE_TOTAL_OOB_SZ)

#define RANGE_BUF_OOB(buf) \
  (buf_type_t::RB_BUF_LEFT == buf) || (buf_type_t::RB_BUF_RIGHT == buf)

/* abort with an error message: forward decl */
void msg_abort(int err, const char* msg, const char* func, const char* file,
               int line);

/* Allowed transitions:
 * INIT -> RENEGO
 * READY -> RENEGO
 */
enum class range_state_t {
  RS_INIT,
  RS_READY, /* oob buffers have space and we're ready to shuffle */
  RS_RENEGO, /* currently in the middle of an active renegotn */
 /* we need a renegotiation but one hasn't been triggered
  * for some reason. writer sets this as soon as OOB buffers max out */
  RS_ACK,
  RS_BLOCKED, /* don't really need this but verify */
};

typedef struct preload_ctx preload_ctx_t;

void range_init_negotiation(preload_ctx_t* pctx);

void range_handle_reneg_pivots(char* buf, unsigned int buf_sz, int src_rank);

void range_handle_reneg_acks(char* buf, unsigned int buf_sz);
