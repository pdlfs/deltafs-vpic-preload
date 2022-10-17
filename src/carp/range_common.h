#pragma once

#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "range_constants.h"

typedef struct rb_item {
  int rank;
  double bin_val;
  double bin_other;
  bool is_start;
} rb_item_t;  // rank-bin item

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
  bool first_block_;

 public:
  MainThreadStateMgr();
  MainThreadState GetState();
  MainThreadState UpdateState(MainThreadState new_state);
  void Reset();
  bool FirstBlock() const;
};

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

namespace pdlfs {
namespace carp {
template <typename T>
std::string vec_to_str(const std::vector<T>& vec) {
  std::ostringstream vecstr;
  vecstr.precision(3);

  for (size_t vecidx = 0; vecidx < vec.size(); vecidx++) {
    if (vecidx % 10 == 0) {
      vecstr << "\n\t";
    }

    vecstr << vec[vecidx] << ", ";
  }

  return vecstr.str();
}

static void deduplicate_sorted_vector(std::vector<float>& vec) {
  if (vec.size() == 0) return;

  std::sort(vec.begin(), vec.end());
  size_t out_idx = 1;

  float last_copied = vec[0];

  for (size_t in_idx = 1; in_idx < vec.size(); in_idx++) {
    float cur = vec[in_idx];
    float prev = vec[in_idx - 1];

    assert(cur >= prev);
    assert(cur >= last_copied);

    if (cur - last_copied > 1e-7) {
      // arbitrary comparison threshold
      vec[out_idx++] = cur;
      last_copied = cur;
    }
  }

  vec.resize(out_idx);
}
}  // namespace carp
}  // namespace pdlfs
