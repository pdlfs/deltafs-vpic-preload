#pragma once

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <vector>

#include "oob_buffer.h"
#include "range_constants.h"

typedef struct rb_item {
  int rank;
  float bin_val;
  float bin_other;
  bool is_start;
} rb_item_t;  // rank-bin item

bool rb_item_lt(const rb_item_t& a, const rb_item_t& b);

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

/* TODO: remove BUF_LEFT and BUF_WRITE, after naive impl is ported */
enum class buf_type_t {
  RB_NO_BUF,
  RB_BUF_OOB,
  RB_UNDECIDED,
  RB_BUF_LEFT,
  RB_BUF_RIGHT
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
  /* both of these should be ints, TODO: change count to int and validate */
  std::vector<float> rank_bin_count;
  std::vector<uint64_t> rank_bin_count_aggr;
  float range_min, range_max;
  /*  END Shared variables protected by bin_access_m */

  pdlfs::OobBuffer oob_buffer;

  float my_pivots[pdlfs::kMaxPivots];
  int my_pivot_count;
  float pivot_width;

  pthread_mutex_t snapshot_access_m = PTHREAD_MUTEX_INITIALIZER;
  snapshot_state snapshot;

  int last_reneg_counter = 0;
} pivot_ctx_t;

int pivot_ctx_init(pivot_ctx_t* pvt_ctx);

int pivot_ctx_reset(pivot_ctx_t* pvt_ctx);

/**
 * @brief Calculate pivots from the current pivot_ctx state.
 * This also modifies OOB buffers (sorts them), but their order shouldn't
 * be relied upon anyway.
 *
 * SAFE version computes "token pivots" in case no mass is there to
 * actually compute pivots. This ensures that merging calculations
 * do not fail.
 *
 * XXX: a more semantically appropriate fix would be to define addition
 * and resampling for zero-pivots
 *
 * @param pvt_ctx pivot context
 *
 * @return
 */
int pivot_calculate_safe(pivot_ctx_t* pvt_ctx, const int num_pivots);

/**
 * @brief Calculate pivots from the current pivot_ctx state.
 * This also modifies OOB buffers (sorts them), but their order shouldn't
 * be relied upon anyway.
 *
 * @param pvt_ctx pivot context
 *
 * @return
 */
int pivot_calculate(pivot_ctx_t* pvt_ctx, const size_t num_pivots);
/**
 * @brief Take a snapshot of the pivot_ctx state
 *
 * @param pvt_ctx
 *
 * @return
 */
int pivot_state_snapshot(pivot_ctx* pvt_ctx);

/**
 * @brief Calculate pivot state from snapshot. Exists mostly as legacy
 *
 * @param pvt_ctx
 *
 * @return
 */
int pivot_calculate_from_snapshot(pivot_ctx_t* pvt_ctx,
                                  const size_t num_pivots);

/**
 * @brief Update pivots after renegotiation. This *does not* manipulate the
 * state manager. State manager needs to be directly controlled by the
 * renegotiation provider because of synchronization implications
 *
 * @param pvt_ctx
 * @param pivots
 * @param num_pivots
 * @return
 */
int pivot_update_pivots(pivot_ctx_t* pvt_ctx, float* pivots, int num_pivots);

static inline int print_vector(char* buf, int buf_sz, std::vector<uint64_t>& v,
                               int vlen = 0, bool truncate = true) {
  vlen = (vlen == -1) ? v.size() : vlen;

  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%lu ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, std::vector<int>& v,
                               int vlen = -1, bool truncate = true) {
  vlen = (vlen == -1) ? v.size() : vlen;

  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%d ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, std::vector<float>& v,
                               int vlen = -1, bool truncate = true) {
  vlen = (vlen == -1) ? v.size() : vlen;

  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.1f ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, uint64_t* v, int vlen,
                               bool truncate = true) {
  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%lu ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, float* v, int vlen,
                               bool truncate = true) {
  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.1f ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static bool float_eq(float a, float b) {
  return fabs(a - b) < pdlfs::kFloatCompThreshold;
}

static bool float_gt(float a, float b) {
  return a > b + pdlfs::kFloatCompThreshold;
}

static bool float_gte(float a, float b) {
  return a > b - pdlfs::kFloatCompThreshold;
}

static bool float_lt(float a, float b) {
  return a < b - pdlfs::kFloatCompThreshold;
}

static bool float_lte(float a, float b) {
  const float kFloatCompThreshold = 1e-5;
  return a < b + pdlfs::kFloatCompThreshold;
}
