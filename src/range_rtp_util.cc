#include "range_rtp.h"

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

RenegStateMgr::RenegStateMgr()
    : current_state{INIT},
      prev_state{INIT},
      next_round_started{false},
      cur_round_num{0} {}

RenegState RenegStateMgr::get_state() { return this->current_state; }

RenegState RenegStateMgr::update_state(RenegState new_state) {
  RenegState cur_state = this->current_state;

  if (cur_state == RenegState::INIT && new_state == RenegState::READY) {
    // pass
  } else if (cur_state == RenegState::READY &&
             new_state == RenegState::READYBLOCK) {
    // pass
  } else if (cur_state == RenegState::READY &&
             new_state == RenegState::PVTSND) {
    // pass
  } else if (cur_state == RenegState::READYBLOCK &&
             new_state == RenegState::PVTSND) {
    // pass
  } else if (cur_state == RenegState::PVTSND &&
             new_state == RenegState::READY) {
    /* READY or READYBLOCK represent state machine entering the next state */
    this->next_round_started = false;
    this->cur_round_num++;
  } else if (cur_state == RenegState::PVTSND &&
             new_state == RenegState::READYBLOCK) {
    this->next_round_started = false;
    this->cur_round_num++;
  } else {
    ABORT("RenegStateMgr::update_state: unexpected transition");
  }

  this->prev_state = this->current_state;
  this->current_state = new_state;
}

void RenegStateMgr::mark_next_round_start(int round_num) {
  if (round_num != this->cur_round_num + 1) {
    ABORT("RenegStateMgr::mark_next_round_start: wrong round_num");
  }

  this->next_round_started = true;
}

bool RenegStateMgr::get_next_round_start() { return this->next_round_started; }

DataBuffer::DataBuffer() {
  memset(data_len, 0, sizeof(data_len));
  // XXX: revisit
  this->num_pivots = PIVOTS_MAX;
  this->cur_store_idx = 0;
}

int DataBuffer::store_data(int stage, float *data, int dlen, bool isnext) {
  int sidx = this->cur_store_idx;
  if (isnext) sidx = !sidx;

  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (data_len[sidx][stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != num_pivots) {
    return -3;
  }

  int idx = data_len[sidx][stage];
  memcpy(data_store[sidx][stage][idx], data, dlen * sizeof(float));
  data_len[sidx][stage]++;

  return data_len[sidx][stage];
}

int DataBuffer::get_num_items(int stage, bool isnext) {
  if (stage < 1 || stage > STAGES_MAX) {
    return -1;
  }

  int sidx = this->cur_store_idx;
  if (isnext) sidx = !sidx;

  return data_len[sidx][stage];
}

int DataBuffer::advance_round() {
  this->cur_store_idx = !(this->cur_store_idx);
  return 0;
}

int DataBuffer::clear_all_data() {
  memset(data_len, 0, sizeof(data_len));
  return 0;
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
