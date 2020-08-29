#include <algorithm>

#include "rtp/rtp.h"

namespace pdlfs {

static uint64_t tv_to_us(const struct timespec *tv) {
  uint64_t t;
  t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
  t += tv->tv_nsec / 1000;
  return t;
}

static uint64_t calc_diff_us(const struct timespec *a,
                             const struct timespec *b) {
  uint64_t a_us = tv_to_us(a);
  uint64_t b_us = tv_to_us(b);

  return b_us - a_us;
}

RenegBench::RenegBench() : is_root{false} {}

void RenegBench::rec_start() { clock_gettime(CLOCK_MONOTONIC, &round_start); }

void RenegBench::rec_active() { clock_gettime(CLOCK_MONOTONIC, &activated); }

void RenegBench::rec_pvt_bcast() {
  is_root = true;
  clock_gettime(CLOCK_MONOTONIC, &activated);
}

void RenegBench::rec_finished() { clock_gettime(CLOCK_MONOTONIC, &round_end); }

void RenegBench::print_stderr() {
  uint64_t start_to_end = calc_diff_us(&round_start, &round_end);

  uint64_t start_to_active = calc_diff_us(&round_start, &activated);

  if (is_root) {
    uint64_t active_to_pvt = calc_diff_us(&activated, &pvt_bcast);
    uint64_t pvt_to_end = calc_diff_us(&pvt_bcast, &round_end);

    fprintf(stderr,
            "[[ BENCHMARK_RTP_ROOT ]] Time taken: "
            "%lu us/%lu us/%lu us (%lu us)\n",
            start_to_active, active_to_pvt, pvt_to_end, start_to_end);
  } else {
    uint64_t active_to_end = calc_diff_us(&activated, &round_end);

    fprintf(stderr,
            "[[ BENCHMARK_RTP ]] Time taken: "
            "%lu us/%lu us (%lu us)\n",
            start_to_active, active_to_end, start_to_end);
  }
}
} // namespace
