#include "range_rtp.h"

RenegStateMgr::RenegStateMgr()
    : current_state{RENEG_INIT}, prev_state{RENEG_INIT} {}

RenegState RenegStateMgr::get_state() { return this->current_state; }

RenegState RenegStateMgr::update_state(RenegState new_state) {
  this->prev_state = this->current_state;
  this->current_state = new_state;
}

DataBuffer::DataBuffer() {
  memset(data_len, 0, sizeof(data_len));
  // XXX: revisit
  this->num_pivots = PIVOTS_MAX;
}

int DataBuffer::store_data(int stage, float *data, int dlen) {
  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (data_len[stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != num_pivots) {
    return -3;
  }

  int idx = data_len[stage];
  memcpy(data_store[stage][idx], data, dlen * sizeof(float));
  data_len[stage]++;

  return 0;
}

int DataBuffer::get_num_items(int stage) {
  if (stage < 1 || stage > STAGES_MAX) {
    return -1;
  }

  return data_len[stage];
}

int DataBuffer::clear_all_data() {
  memset(data_len, 0, sizeof(data_len));
  return 0;
}

static uint64_t tv_to_us(const struct timespec *tv) {
  uint64_t t;
  t = static_cast<uint64_t>(tv->tv_sec) * 1000000;
  t += tv->tv_nsec / 1000;
  return t;
}

static uint64_t calc_diff_us(const struct timespec *a, const struct timespec *b) {
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
