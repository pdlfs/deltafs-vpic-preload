//
// Created by Ankush J on 8/28/20.
//

#include "rtp/rtp_state_mgr.h"

#include "rtp/rtp.h"

namespace pdlfs {

RtpStateMgr::RtpStateMgr()
    : current_state{INIT},
      prev_state{INIT},
      next_round_started{false},
      cur_round_num{0} {}

RenegState RtpStateMgr::get_state() { return this->current_state; }

RenegState RtpStateMgr::update_state(RenegState new_state) {
  RenegState cur_state = this->current_state;

#define IS_TRANS(a, b) (cur_state == (a) && new_state == (b))

  if (IS_TRANS(INIT, READY)) {
    // allow
  } else if (IS_TRANS(READY, READYBLOCK)) {
    // allow
  } else if (IS_TRANS(READY, PVTSND)) {
    // allow
  } else if (IS_TRANS(READYBLOCK, PVTSND)) {
    // allow
  } else if (IS_TRANS(PVTSND, READY)) {
    /* READY or READYBLOCK represent state machine entering the next round */
    this->next_round_started = false;
    this->cur_round_num++;
  } else if (IS_TRANS(PVTSND, READYBLOCK)) {
    this->next_round_started = false;
    this->cur_round_num++;
  } else {
    ABORT("RtpStateMgr::update_state: unexpected transition");
  }

  this->prev_state = this->current_state;
  this->current_state = new_state;

  return this->prev_state;

#undef IS_TRANS
}

void RtpStateMgr::mark_next_round_start(int round_num) {
  if (round_num != this->cur_round_num + 1) {
    ABORT("RtpStateMgr::mark_next_round_start: wrong round_num");
  }

  this->next_round_started = true;
}

bool RtpStateMgr::get_next_round_start() { return this->next_round_started; }
}  // namespace pdlfs