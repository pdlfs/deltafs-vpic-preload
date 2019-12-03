#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include "common.h"

#define MSGFMT_MAX_BUFSIZE 255

#define ABORT_FILENAME \
  (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#define ABORT(msg) msg_abort(errno, msg, __func__, ABORT_FILENAME, __LINE__)

#include <vector>

// TODO: make this configurable
#define RANGE_BUFSZ 1000
// TODO: Can shorten this by using indirect ptr?
#define RANGE_MAX_PSZ 255
#define RANGE_MAX_OOB_THRESHOLD 8
/* Total  for left + right buffers */
#define RANGE_TOTAL_OOB_THRESHOLD 2 * RANGE_MAX_OOB_THRESHOLD
#define RANGE_NUM_PIVOTS 4

#define RANGE_IS_INIT(x) (x->range_state == range_state_t::RS_INIT)
#define RANGE_IS_READY(x) (x->range_state == range_state_t::RS_READY)
#define RANGE_IS_RENEGO(x) (x->range_state == range_state_t::RS_RENEGO)
#define RANGE_IS_BLOCKED(x) (x->range_state == range_state_t::RS_BLOCKED)
#define RANGE_IS_ACK(x) (x->range_state == range_state_t::RS_ACK)

#define RANGE_LEFT_OOB_FULL(x) (x->oob_count_left == RANGE_MAX_OOB_THRESHOLD)
#define RANGE_RIGHT_OOB_FULL(x) (x->oob_count_right == RANGE_MAX_OOB_THRESHOLD)

#define RANGE_OOB_FULL(x) \
  (x->oob_count_left + x->oob_count_right == RANGE_TOTAL_OOB_THRESHOLD)

#define RANGE_BUF_OOB(buf) \
  (buf_type_t::RB_BUF_LEFT == buf) || (buf_type_t::RB_BUF_RIGHT == buf)

/* abort with an error message: forward decl */
void msg_abort(int err, const char* msg, const char* func, const char* file,
               int line);

typedef struct particle_mem {
  float indexed_prop;       // property for range query
  char buf[RANGE_MAX_PSZ];  // other data
  int buf_sz;
} particle_mem_t;

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

enum class buf_type_t { RB_NO_BUF, RB_BUF_LEFT, RB_BUF_RIGHT, RB_UNDECIDED };

typedef struct range_ctx {
  /* range data structures */

  /* Current/next negotiation round number
   * (use range_state to check if you're in a negotiation round */
  std::atomic<int> nneg_round_num;

  /* assert 0 <= (pvt - ack) <= 1 */
  std::atomic<int> pvt_round_num;
  std::atomic<int> ack_round_num;

  int ts_writes_received;
  int ts_writes_shuffled;

  /* must grab this every time you read/write what exactly?
   * In the common case, this lock is expected to be uncontended
   * hence not expensive to acquire
   */
  std::mutex bin_access_m;

  /*  START Shared variables protected by bin_access_m */
  range_state_t range_state;
  range_state_t range_state_prev;

  std::vector<float> rank_bins;
  std::vector<float> rank_bin_count;
  float range_min, range_max;
  /*  END Shared variables protected by bin_access_m */

  std::mutex snapshot_access_m;
  /* START Shared variables protected by snapshot_acces_m */
  std::vector<float> rank_bins_ss;
  std::vector<float> rank_bin_count_ss;
  std::vector<float> oob_buffer_left_ss;
  std::vector<float> oob_buffer_right_ss;
  float range_min_ss, range_max_ss;
  /* END Shared variables protected by snapshot_acces_m */

  /* OOB buffers are never handled by reneg threads
   * and therefore don't need a lock */
  std::vector<particle_mem_t> oob_buffer_left;
  /* OOB buffers are preallocated to MAX to avoid resize calls
   * thus we use counters to track actual size */
  int oob_count_left;

  std::vector<particle_mem_t> oob_buffer_right;
  int oob_count_right;

  /* "infinitely" extensible queue for when you don't know what
   * to do with a particle; to be used sparingly for corner cases
   * at some point the rank will come to its senses and flush this
   * queue (read: finish negotiation or flush fixed queues)
   */
  std::vector<particle_mem_t> contingency_queue;

  float my_pivots[RANGE_NUM_PIVOTS];
  float pivot_width;

  /* Store pivots from all ranks during a negotiation */
  std::vector<float> all_pivots;
  std::vector<float> all_pivot_widths;
  std::atomic<int> ranks_responded;

  std::vector<bool> ranks_acked;
  std::atomic<int> ranks_acked_count;

  std::vector<bool> ranks_acked_next;
  std::atomic<int> ranks_acked_count_next;

  std::condition_variable block_writes_cv;
} range_ctx_t;

typedef struct preload_ctx preload_ctx_t;

void range_init_negotiation(preload_ctx_t* pctx);

/* get_local_pivots: Take the bins stored in bin_snapshots and OOB buffers
 * and store their pivots in rctx->my_pivots. Return nothing
 * XXX TODO: Also need to snapshot OOB buffers (if only indexable_prob) in
 * TODO: don't really have to snapshot buffers, if we can snapshot the idx
 * addition to bins.
 * @param rctx range_ctx
 * @return None
 * */
void get_local_pivots(range_ctx_t* rctx);

void range_handle_reneg_begin(char* buf, unsigned int buf_sz);

void range_handle_reneg_pivots(char* buf, unsigned int buf_sz, int src_rank);

void range_handle_reneg_acks(char* buf, unsigned int buf_sz);
