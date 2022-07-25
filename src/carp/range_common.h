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

enum MainThreadState {
  MT_INIT,
  MT_READY,
  MT_READYBLOCK,
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
