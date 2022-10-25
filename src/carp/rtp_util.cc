#include <algorithm>

#include "carp/rtp.h"

namespace pdlfs {

static uint64_t tv_to_us(const struct timespec* tv) {
  uint64_t t;
  t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
  t += tv->tv_nsec / 1000;
  return t;
}

static uint64_t calc_diff_us(const struct timespec* a,
                             const struct timespec* b) {
  uint64_t a_us = tv_to_us(a);
  uint64_t b_us = tv_to_us(b);

  return b_us - a_us;
}

namespace carp {
int RTPUtil::ComputeTreeFanout(int world_sz, int* fanout_arr) {
  int rv = 0;

  /* Init fanout of the tree for each stage */
  if (world_sz % 4) {
    ABORT("RTP world size must be a multiple of 4");
  }

  /* base setup */
  fanout_arr[1] = 1;
  fanout_arr[2] = 1;
  fanout_arr[3] = world_sz;

  int wsz_remain = world_sz;
  double root = pow(wsz_remain, 1.0 / 3);
  int f1_cand = (int)root;

  while (f1_cand < wsz_remain) {
    if (wsz_remain % f1_cand == 0) {
      fanout_arr[1] = f1_cand;
      wsz_remain /= f1_cand;
      break;
    }

    f1_cand++;
  }

  root = pow(wsz_remain, 1.0 / 2);
  int f2_cand = (int)root;

  while (f2_cand < wsz_remain) {
    if (wsz_remain % f2_cand == 0) {
      fanout_arr[2] = f2_cand;
      wsz_remain /= f2_cand;
      break;
    }

    f2_cand++;
  }

  fanout_arr[3] = world_sz / (fanout_arr[1] * fanout_arr[2]);
  assert(fanout_arr[3] * fanout_arr[2] * fanout_arr[1] == world_sz);

  return rv;
}

RenegBench::RenegBench() : is_root_{false} {}

void RenegBench::MarkStart() { clock_gettime(CLOCK_MONOTONIC, &round_start_); }

void RenegBench::MarkActive() { clock_gettime(CLOCK_MONOTONIC, &activated_); }

void RenegBench::MarkStageComplete(int stage_num) {
  assert(stage_num <= 3);
  clock_gettime(CLOCK_MONOTONIC, &stage_completed_[stage_num]);
}

void RenegBench::MarkPvtBcast() {
  is_root_ = true;
  clock_gettime(CLOCK_MONOTONIC, &pvt_bcast_);
}

void RenegBench::MarkFinished() { clock_gettime(CLOCK_MONOTONIC, &round_end_); }

void RenegBench::PrintStats() {
  if (!is_root_) return;

  uint64_t ts_tmp;
#define PRINT_TS(ts, msg)                 \
  ts_tmp = calc_diff_us(&round_start_, &ts); \
  logf(LOG_INFO, "%lu us: %s", ts_tmp, msg);

  logf(LOG_INFO, "\n\nRTP Benchmark... printing timeline");
  PRINT_TS(activated_, "RTP_BEGIN received");
  PRINT_TS(stage_completed_[1], "Stage 1 completed");
  PRINT_TS(stage_completed_[2], "Stage 2 completed");
  PRINT_TS(stage_completed_[3], "Stage 3 completed");
  PRINT_TS(pvt_bcast_, "Pivot Broadcast begun");
  PRINT_TS(round_end_, "Round completed on RTP root\n");

  // logf(LOG_INFO,
       // "[[ BENCHMARK_RTP_ROOT ]] Time taken: "
       // "%lu us/%lu us/%lu us (%lu us)\n",
       // start_to_active, active_to_pvt, pvt_to_end, start_to_end);
}
}  // namespace carp
}  // namespace pdlfs
