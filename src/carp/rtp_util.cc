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
