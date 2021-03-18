#pragma once

#include <time.h>

#include "carp/rtp_internal.h"
#include "data_buffer.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/status.h"
#include "preload_range.h"
#include "preload_shuffle.h"
#include "range_utils.h"
#include "rtp_state_mgr.h"
#include "xn_shuffle.h"

/*
 * Edge cases:
 *
 * You receive RENEG_BEGIN for Round R+1 while you're still on R
 * - Set a flag to indicate R+1 has begun, and get to it after handling R
 * - Transition directly
 *
 * You receive pivots for R before receiving RENEG_BEGIN for R
 * - ?
 *
 * You receive pivots for R+1 while you're still on R
 * - Can only happen to higher level nodes
 * - BUFFER?
 */

namespace pdlfs {
namespace carp {
class Carp;
struct CarpOptions;
/**
 * @brief Benchmarking utility.
 */
class RenegBench {
 private:
  struct timespec round_start_;
  struct timespec activated_;
  struct timespec pvt_bcast_;
  struct timespec round_end_;

  bool is_root_;

 public:
  RenegBench();
  void MarkStart();
  void MarkActive();
  void MarkPvtBcast();
  void MarkFinished();
  void PrintStderr();
};

class RTPTest;

class RTP {
 public:
  explicit RTP(Carp* carp, const CarpOptions& opts);
  Status InitRound();
  Status HandleMessage(char* buf, unsigned int bufsz, int src);
  int NumRounds() const;

 private:
  Status InitTopology();
  Status BroadcastBegin();
  Status SendToRank(const char* buf, int bufsz, int rank);
  Status SendToAll(int stage, const char* buf, int bufsz,
                   bool exclude_self = false);
  Status SendToChildren(const char* buf, int bufsz, bool exclude_self = false);

  Status HandleBegin(char* buf, unsigned int bufsz, int src);
  Status HandlePivots(char* buf, unsigned int bufsz, int src);
  Status HandlePivotBroadcast(char* buf, unsigned int bufsz, int src);

  Status ReplayBegin();
  void ComputeAggregatePivots(int stage_num, int num_merged,
                              double* merged_pivots, double& merged_width);

  Carp* carp_;

  xn_ctx_t* sh_;
  port::Mutex mutex_;

  RtpStateMgr state_;
  /**
   * @brief Buffer for an RTP instance to store pivots for different stages
   * Most ranks will not need a Stage 2 or a Stage 3, but this allocation is
   * simpler.
   */
  DataBuffer data_buffer_;

  int round_num_;
  int my_rank_;
  int num_ranks_;

  int fanout_[4];
  int peers_[4][FANOUT_MAX];
  int root_[4];
  int pvtcnt_[4];

  RenegBench reneg_bench_;
  friend class RTPTest;
};
}  // namespace carp
}  // namespace pdlfs
