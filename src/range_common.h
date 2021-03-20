#pragma once

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <vector>

#include "carp/oob_buffer.h"
#include "range_constants.h"

typedef struct rb_item {
  int rank;
  double bin_val;
  double bin_other;
  bool is_start;
} rb_item_t;  // rank-bin item

enum class buf_type_t {
  RB_NO_BUF,
  RB_BUF_OOB,
  RB_UNDECIDED,
  RB_BUF_LEFT,
  RB_BUF_RIGHT
};

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
  MT_REMAIN_BLOCKED,
};

class MainThreadStateMgr {
 private:
  MainThreadState current_state_;
  MainThreadState prev_state_;
  bool first_block_;

 public:
  MainThreadStateMgr();
  MainThreadState GetState();
  MainThreadState GetPrevState();
  MainThreadState UpdateState(MainThreadState new_state);
  void Reset();
  bool FirstBlock() const;
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
  double range_min, range_max;
  /*  END Shared variables protected by bin_access_m */

  pdlfs::carp::OobBuffer* oob_buffer;

  double my_pivots[pdlfs::kMaxPivots];
  int my_pivot_count;
  double pivot_width;

  int last_reneg_counter = 0;
} ppivot_ctx_t;

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

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.lf ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, std::vector<double>& v,
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

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.1lf ", v[vidx]);
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

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.4f ", v[vidx]);
  }

  buf_idx +=
      snprintf(buf + buf_idx, buf_sz - buf_idx, "%s", truncated ? "... " : "");
  return buf_idx;
}

static inline int print_vector(char* buf, int buf_sz, double* v, int vlen,
                               bool truncate = true) {
  bool truncated = false;

  if (truncate && vlen > 16) {
    vlen = 16;
    truncated = true;
  }

  int buf_idx = 0;

  for (int vidx = 0; vidx < vlen; vidx++) {
    if (buf_idx >= buf_sz - 32) return buf_idx;

    buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, "%.4lf ", v[vidx]);
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
