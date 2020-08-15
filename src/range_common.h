#pragma once

#include <pthread.h>
#include <vector>
//
// TODO: make this configurable
#define RANGE_BUFSZ 1000
// TODO: Can shorten this by using indirect ptr?
#define RANGE_MAX_PSZ 255
#define RANGE_MAX_OOB_THRESHOLD 2000
/* Total  for left + right buffers */
#define RANGE_TOTAL_OOB_THRESHOLD 2 * RANGE_MAX_OOB_THRESHOLD

// TODO: irrelevant, replace with MAX_PIVOTS
#define RANGE_NUM_PIVOTS 4

#define RANGE_MAX_PIVOTS 256


typedef struct particle_mem {
  float indexed_prop;       // property for range query
  char buf[RANGE_MAX_PSZ];  // other data
  int buf_sz;
} particle_mem_t;


typedef struct snapshot_state {
  std::vector<float> rank_bins;
  std::vector<float> rank_bin_count;
  std::vector<float> oob_buffer_left;
  std::vector<float> oob_buffer_right;
  float range_min, range_max;
} snapshot_state_t;

enum MainThreadState {
  MT_INIT,
  MT_READY,
  MT_BLOCK,
};

class MainThreadStateMgr {
 private:
  MainThreadState current_state;
  MainThreadState prev_state;

 public:
  MainThreadStateMgr();

  MainThreadState get_state();

  MainThreadState get_prev_state();

  MainThreadState update_state(MainThreadState new_state);
};

typedef struct pivot_ctx {
  /* The whole structure, except for the snapshot, is protected by
   * this mutex */
  pthread_mutex_t pivot_access_m = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t pivot_update_cv = PTHREAD_COND_INITIALIZER;

  MainThreadStateMgr mts_mgr;

  std::vector<float> rank_bins;
  std::vector<float> rank_bin_count;
  float range_min, range_max;
  /*  END Shared variables protected by bin_access_m */

  std::vector<particle_mem_t> oob_buffer_left;
  std::vector<particle_mem_t> oob_buffer_right;
  /* OOB buffers are preallocated to MAX to avoid resize calls
   * thus we use counters to track actual size */
  int oob_count_left;
  int oob_count_right;

  float my_pivots[RANGE_MAX_PIVOTS];
  float pivot_width;

  pthread_mutex_t snapshot_access_m = PTHREAD_MUTEX_INITIALIZER;
  snapshot_state snapshot;
} pivot_ctx_t;

int pivot_ctx_init(pivot_ctx *pvt_ctx);

int pivot_ctx_reset(pivot_ctx *pvt_ctx);

/**
 * @brief 
 *
 * @param pvt_ctx Calculate pivots from the current pivot_ctx state.
 * This also modifies OOB buffers (sorts them), but their order shouldn't
 * be relied upon anyway.
 *
 * @return 
 */
int pivot_calculate(pivot_ctx_t *pvt_ctx, const int num_pivots);
/**
 * @brief Take a snapshot of the pivot_ctx state
 *
 * @param pvt_ctx
 *
 * @return 
 */
int pivot_state_snapshot(pivot_ctx *pvt_ctx);

/**
 * @brief Calculate pivot state from snapshot. Exists mostly as legacy
 *
 * @param pvt_ctx
 *
 * @return 
 */
int pivot_calculate_from_snapshot(pivot_ctx_t *pvt_ctx, const int num_pivots);
