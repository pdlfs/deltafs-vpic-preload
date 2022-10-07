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
