#pragma once

namespace pdlfs {
enum RenegState {
  /* Bootstrapping state, no RTP messages can be gracefully handled in this
   * state, will move to READY once bootstrapping is complete
   */
  INIT,
  /* Ready to either trigger a round locally, or respond to another RTP msg */
  READY,
  /* Ready/starting round, but change state to block main thread */
  READYBLOCK, /* Ready to activate, just changed to block main */
  PVTSND      /* Has been activated */
};

class RtpStateMgr {
 private:
  RenegState current_state_;
  RenegState prev_state_;

  int cur_round_num_;
  bool next_round_started_;

 public:
  RtpStateMgr();
  RenegState GetState();
  RenegState UpdateState(RenegState new_state);
  void MarkNextRoundStart(int round_num);
  bool GetNextRoundStart();
};
} // namespace pdlfs
