#pragma once

#include <time.h>
#include "preload_range.h"
#include "preload_shuffle.h"
#include "xn_shuffle.h"

#define FANOUT_MAX 128
// XXX: This is probably defined elsewhere
#define PIVOTS_MAX 4

/* This is not configurable. RTP is designed for 3 stages 
 * Stage 1 - leaf stage (all shared memory, ideally)
 * Stage 3 - to final root
 * */
#define STAGES_MAX 3

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

enum RenegState {
  /* Bootstrapping state, no RTP messages can be gracefully handled in this
   * state, will move to READY once bootstrapping is complete
   */
  INIT,
  /* Ready to either trigger a round locally, or respond to another RTP msg */
  READY,
  /* Ready/starting round, but change state to block main thread */
  READYBLOCK, /* Ready to activate, just changed to block main */
  PVTSND      /* Has been activated */
};

class RenegStateMgr {
 private:
  RenegState current_state;
  RenegState prev_state;

  int cur_round_num;
  bool next_round_started;

 public:
  RenegStateMgr();

  RenegState get_state();

  RenegState update_state(RenegState new_state);

  void mark_next_round_start(int round_num);

  bool get_next_round_start();
};

/**
 * @brief Buffer for an RTP instance to store pivots for different stages
 * Most ranks will not need a Stage 2 or a Stage 3, but this allocation is
 * simpler.
 */
class DataBuffer {
 private:
  /* This simple storage format has 2*512KB of theoretical
   * footprint. (2* 4 * 128 * 256 * 4B). But no overhead will
   * be incurred for ranks that aren't actually using those
   * stages. (Virtual Memory ftw)
   */
  float data_store[2][STAGES_MAX + 1][FANOUT_MAX][PIVOTS_MAX];
  int data_len[2][STAGES_MAX + 1];

  int num_pivots;
  int cur_store_idx;

 public:
  DataBuffer();

  /**
   * @brief Store pivots for the current round
   *
   * @param stage
   * @param data
   * @param dlen
   * @param isnext true if data is for the next round, false o/w
   *
   * @return errno if < 0, else num_items in store for the stage
   */
  int store_data(int stage, float *data, int dlen, bool isnext);

  /**
   * @brief 
   *
   * @param stage
   * @param isnext true if data is for the next round, false o/w
   *
   * @return 
   */
  int get_num_items(int stage, bool isnext);

  /**
   * @brief Clear all data for current round, set next round data as cur
   *
   * @return errno or 0
   */
  int advance_round();

  /**
   * @brief Clear ALL data (both current round and next). Use with caution.
   *
   * @return 
   */
  int clear_all_data();

};

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

  /* All data in this section is shared with other
   * threads, and must be accessed undedr this mutex
   *
   * The data mutex is shared between the main thread, whihc is
   * read-only, and the delivery thread, which may both read
   * and write
   */
  /* BEGIN data_mutex */
  pthread_mutex_t *data_mutex = NULL;
  float *data = NULL;
  int *data_len = NULL;
  /* END data_mutex */

  /* All data below is protected by this mutex - this is also
   * shared between the main thread and multiple message handlers.
   * Message handlers will be serialized by the delivery thread
   * so concurrency there is not an issue */
  pthread_mutex_t reneg_mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t reneg_cv = PTHREAD_COND_INITIALIZER;

  RenegStateMgr state_mgr;
  DataBuffer data_buffer;

  int round_num;
  int my_rank;
  int num_ranks;

  int fanout_s1;
  int fanout_s2;
  int fanout_s3;

  int peers_s1[FANOUT_MAX];
  int peers_s2[FANOUT_MAX];
  int peers_s3[FANOUT_MAX];

  int num_peers_s1;
  int num_peers_s2;
  int num_peers_s3;

  int root_s1;
  int root_s2;
  int root_s3;
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
 * @param data The array from where to grab, and to update the rank's pivots
 * This array is shared with the regular shuffle thread, so may only be accessed
 * under data_mutex.
 * @param data_len Pointer to the pivot length int
 * @param data_max Max memory allocated for data
 * @param data_mutex Mutex that protects all the above
 * @param ro Config options for RTP
 *
 * @return retcode
 */
int reneg_init(reneg_ctx_t rctx, shuffle_ctx_t *sctx, float *data,
               int *data_len, int data_max, pthread_mutex_t *data_mutex,
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