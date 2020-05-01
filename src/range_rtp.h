#pragma once

#include "preload_range.h"
#include "preload_shuffle.h"
#include "xn_shuffle.h"

#define FANOUT_MAX 128
// XXX: This is probably defined elsewhere
#define PIVOTS_MAX 4
#define STAGES_MAX 3

enum RenegState {
  RENEG_INIT,
  RENEG_READY,
  RENEG_R1SND,
  RENEG_R2SND,
  RENEG_R3SND,
  RENEG_RECVWAIT
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

struct reneg_ctx {
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
