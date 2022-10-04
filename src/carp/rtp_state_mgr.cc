//
// Created by Ankush J on 8/28/20.
//

#include "common.h"
#include "carp/rtp_state_mgr.h"
#include "carp/rtp.h"

namespace pdlfs {

RtpStateMgr::RtpStateMgr()
    : current_state_{INIT},
      prev_state_{INIT},
      next_round_started_{false},
      cur_round_num_{0} {}

RenegState RtpStateMgr::GetState() { return this->current_state_; }

RenegState RtpStateMgr::UpdateState(RenegState new_state) {
  RenegState cur_state = this->current_state_;

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
    this->next_round_started_ = false;
    this->cur_round_num_++;
  } else if (IS_TRANS(PVTSND, READYBLOCK)) {
    this->next_round_started_ = false;
    this->cur_round_num_++;
  } else {
    ABORT("RtpStateMgr::UpdateState: unexpected transition");
  }

  this->prev_state_ = this->current_state_;
  this->current_state_ = new_state;

  return this->prev_state_;

#undef IS_TRANS
}

void RtpStateMgr::MarkNextRoundStart(int round_num) {
  if (round_num != this->cur_round_num_ + 1) {
    ABORT("RtpStateMgr::MarkNextRoundStart: wrong round_num");
  }

  this->next_round_started_ = true;
}

bool RtpStateMgr::GetNextRoundStart() { return this->next_round_started_; }
}  // namespace pdlfs
