#pragma once

#include <time.h>
#include "preload_range.h"
#include "preload_shuffle.h"
#include "xn_shuffle.h"

#define FANOUT_MAX 128
// XXX: This is probably defined elsewhere
#define PIVOTS_MAX 4
#define STAGES_MAX 3

enum RenegState {
  RENEG_INIT,
  RENEG_READY,      /* Ready to activate */
  RENEG_READYBLOCK, /* Ready to activate, just changed to block main */
  RENEG_R1SND,      /* Has been activated */
  RENEG_R2SND,
  RENEG_R3SND,
  RENEG_RECVWAIT,
  RENEG_FINISHED
};

class RenegStateMgr {
 private:
  RenegState current_state;
  RenegState prev_state;

 public:
  RenegStateMgr();

  RenegState get_state();

  RenegState update_state(RenegState new_state);
};

class DataBuffer {
 private:
  float data_store[STAGES_MAX + 1][FANOUT_MAX][PIVOTS_MAX];
  int data_len[STAGES_MAX + 1];

  int num_pivots;

 public:
  DataBuffer();

  int store_data(int stage, float *data, int dlen);

  int get_num_items(int stage);

  int clear_all_data();
};

class RenegBench {
 private:
  struct timespec round_start;
  struct timespec activated;
  struct timespec pvt_bcast;
  struct timespec round_end;

  bool is_root;

  uint64_t tv_to_us(const struct timespec *tv) {
    uint64_t t;
    t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
    t += tv->tv_nsec / 1000;
    return t;
  }

  uint64_t calc_diff_us(const struct timespec *a, const struct timespec *b) {
    uint64_t a_us = tv_to_us(a);
    uint64_t b_us = tv_to_us(b);

    return b_us - a_us;
  }

 public:
  RenegBench() { is_root = false; }

  void rec_start() { clock_gettime(CLOCK_MONOTONIC, &round_start); }

  void rec_active() { clock_gettime(CLOCK_MONOTONIC, &activated); }

  void rec_pvt_bcast() {
    is_root = true;
    clock_gettime(CLOCK_MONOTONIC, &activated);
  }

  void rec_finished() { clock_gettime(CLOCK_MONOTONIC, &round_end); }

  void print_stderr() {
    uint64_t start_to_end = calc_diff_us(&round_start, &round_end);

    uint64_t start_to_active = calc_diff_us(&round_start, &activated);

    if (is_root) {
      uint64_t active_to_pvt = calc_diff_us(&activated, &pvt_bcast);
      uint64_t pvt_to_end = calc_diff_us(&pvt_bcast, &round_end);

      fprintf(stderr,
              "[[ BENCHMARK_RTP_ROOT ]] Time taken: "
              "%lu us/%lu us/%lu us (%lu us)\n",
              start_to_active, active_to_pvt, pvt_to_end, start_to_end);
    } else {
      uint64_t active_to_end = calc_diff_us(&activated, &round_end);

      fprintf(stderr,
              "[[ BENCHMARK_RTP ]] Time taken: "
              "%lu us/%lu us (%lu us)\n",
              start_to_active, active_to_end, start_to_end);
    }
  }
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
 * @param rctx
 * @param sctx
 * @param data
 * @param data_len
 * @param data_max
 * @param data_mutex
 * @param ro
 *
 * @return retcode
 */
int reneg_init(reneg_ctx_t rctx, shuffle_ctx_t *sctx, float *data,
               int *data_len, int data_max, pthread_mutex_t *data_mutex,
               struct reneg_opts ro);

/**
 * @brief
 *
 * @param rctx
 * @param buf
 * @param buf_sz
 * @param src
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
