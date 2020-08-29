#pragma once

#include <time.h>
#include "data_buffer.h"
#include "preload_range.h"
#include "preload_shuffle.h"
#include "range_utils.h"
#include "rtp/rtp_internal.h"
#include "rtp_state_mgr.h"
#include "xn_shuffle.h"


/*
 * Edge cases:
 *
 * You receive RENEG_BEGIN for Round R+1 while you're still on R
 * - Set a flag to indicate R+1 has begun, and get to it after handling R
 * - Transition directly
 *
 * You receive pivots for R before receiving RENEG_BEGIN for R
 * - ?
 *
 * You receive pivots for R+1 while you're still on R
 * - Can only happen to higher level nodes
 * - BUFFER?
 */

namespace pdlfs {
/**
 * @brief Buffer for an RTP instance to store pivots for different stages
 * Most ranks will not need a Stage 2 or a Stage 3, but this allocation is
 * simpler.
 */

/**
 * @brief Benchmarking utility.
 */
class RenegBench {
 private:
  struct timespec round_start;
  struct timespec activated;
  struct timespec pvt_bcast;
  struct timespec round_end;

  bool is_root;

 public:
  RenegBench();

  void rec_start();

  void rec_active();

  void rec_pvt_bcast();

  void rec_finished();

  void print_stderr();
};

struct reneg_ctx {
  RenegBench reneg_bench;

  xn_ctx_t *xn_sctx; /* shuffler to use for data */
  nexus_ctx_t nxp;   /* extracted from sctx */

  pivot_ctx_t *pvt_ctx;

  /* All data below is protected by this mutex - this is also
   * shared between the main thread and multiple message handlers.
   * Message handlers will be serialized by the delivery thread
   * so concurrency there is not an issue */
  pthread_mutex_t reneg_mutex = PTHREAD_MUTEX_INITIALIZER;

  RtpStateMgr state_mgr;
  DataBuffer data_buffer;

  int round_num;
  int my_rank;
  int num_ranks;

  int fanout[4];
  int peers[4][FANOUT_MAX];
  int num_peers[4];
  int root[4];
  int pvtcnt[4];
};

struct reneg_opts {
  int fanout_s1;
  int fanout_s2;
  int fanout_s3;
};

typedef struct reneg_ctx *reneg_ctx_t;

/**
 * @brief
 *
 * @param rctx The RTP context
 * @param sctx The shuffle (3-hop only) context
 * @param pvt_ctx The pivot context
 * @param ro Config options for RTP
 *
 * @return retcode
 */
int reneg_init(reneg_ctx_t rctx, shuffle_ctx_t *sctx, pivot_ctx_t *pvt_ctx,
               struct reneg_opts ro);

/**
 * @brief Handler for all RTP messages. Multiplexes to internal handlers.
 *
 * @param rctx
 * @param buf
 * @param buf_sz
 * @param src The source rank
 *
 * @return
 */
int reneg_handle_msg(reneg_ctx_t rctx, char *buf, unsigned int buf_sz, int src);

/**
 * @brief
 *
 * @param rctx
 *
 * @return
 */
int reneg_init_round(reneg_ctx_t rctx);

/**
 * @brief Destroy
 *
 * @param rctx
 *
 * @return retcode
 */
int reneg_destroy(reneg_ctx_t rctx);

} // namespace pdlfs
