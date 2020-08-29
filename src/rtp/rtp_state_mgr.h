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
  RenegState current_state;
  RenegState prev_state;

  int cur_round_num;
  bool next_round_started;

 public:
  RtpStateMgr();

  RenegState get_state();

  RenegState update_state(RenegState new_state);

  void mark_next_round_start(int round_num);

  bool get_next_round_start();
};
} // namespace pdlfs
