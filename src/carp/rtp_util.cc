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

void RenegBench::MarkPvtBcast() {
  is_root_ = true;
  clock_gettime(CLOCK_MONOTONIC, &activated_);
}

void RenegBench::MarkFinished() { clock_gettime(CLOCK_MONOTONIC, &round_end_); }

void RenegBench::PrintStderr() {
  uint64_t start_to_end = calc_diff_us(&round_start_, &round_end_);

  uint64_t start_to_active = calc_diff_us(&round_start_, &activated_);

  if (is_root_) {
    uint64_t active_to_pvt = calc_diff_us(&activated_, &pvt_bcast_);
    uint64_t pvt_to_end = calc_diff_us(&pvt_bcast_, &round_end_);

    fprintf(stderr,
            "[[ BENCHMARK_RTP_ROOT ]] Time taken: "
            "%lu us/%lu us/%lu us (%lu us)\n",
            start_to_active, active_to_pvt, pvt_to_end, start_to_end);
  } else {
    uint64_t active_to_end = calc_diff_us(&activated_, &round_end_);

    fprintf(stderr,
            "[[ BENCHMARK_RTP ]] Time taken: "
            "%lu us/%lu us (%lu us)\n",
            start_to_active, active_to_end, start_to_end);
  }
}
}  // namespace carp
}  // namespace pdlfs
