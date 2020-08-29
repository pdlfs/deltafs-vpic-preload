//
// Created by Ankush J on 8/28/20.
//

#include "rtp/rtp.h"
#include "rtp/rtp_state_mgr.h"

namespace pdlfs {

RtpStateMgr::RtpStateMgr()
    : current_state{INIT},
      prev_state{INIT},
      next_round_started{false},
      cur_round_num{0} {}
      
RenegState RtpStateMgr::get_state() { return this->current_state; }

RenegState RtpStateMgr::update_state(RenegState new_state) {
  RenegState cur_state = this->current_state;

  if (cur_state == INIT && new_state == READY) {
    // pass
  } else if (cur_state == READY &&
      new_state == READYBLOCK) {
    // pass
  } else if (cur_state == READY &&
      new_state == PVTSND) {
    // pass
  } else if (cur_state == READYBLOCK &&
      new_state == PVTSND) {
    // pass
  } else if (cur_state == PVTSND &&
      new_state == READY) {
    /* READY or READYBLOCK represent state machine entering the next state */
    this->next_round_started = false;
    this->cur_round_num++;
  } else if (cur_state == PVTSND &&
      new_state == READYBLOCK) {
    this->next_round_started = false;
    this->cur_round_num++;
  } else {
    ABORT("RtpStateMgr::update_state: unexpected transition");
  }

  this->prev_state = this->current_state;
  this->current_state = new_state;

  return this->prev_state;
}

void RtpStateMgr::mark_next_round_start(int round_num) {
  if (round_num != this->cur_round_num + 1) {
    ABORT("RtpStateMgr::mark_next_round_start: wrong round_num");
  }

  this->next_round_started = true;
}

bool RtpStateMgr::get_next_round_start() { return this->next_round_started; }
} // namespace pdlfs