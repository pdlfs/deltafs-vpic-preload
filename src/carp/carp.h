//
// Created by Ankush J on 3/5/21.
//

#pragma once

#include <float.h>
#include <msgfmt.h>
#include <pdlfs-common/status.h>
#include <range_constants.h>
#include <range_utils.h>

#include "carp_utils.h"
#include "oob_buffer.h"
#include "pdlfs-common/mutexlock.h"
#include "policy.h"
#include "preload_shuffle.h"
/* XXX: temporarily for range-utils, refactor ultimately */
#include "range_backend/mock_backend.h"
#include "rtp.h"

namespace pdlfs {
/* forward declaration */
struct rtp_ctx;

namespace carp {

struct CarpOptions {
  uint32_t my_rank;
  uint32_t num_ranks;
  uint32_t oob_sz;
  uint64_t reneg_intvl;
  int rtp_pvtcnt[4];
  shuffle_ctx_t* sctx;
  const char* reneg_policy;
  uint32_t dynamic_intvl;
  float dynamic_thresh;
  std::string mount_path; // for stat_trigger only
  Env* env;
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
        rtp_(this, options_),
        epoch_(0),
        cv_(&mutex_),
        range_min_(0),
        range_max_(0),
        rank_bins_(options_.num_ranks + 1, 0),
        rank_counts_(options_.num_ranks, 0),
        rank_counts_aggr_(options_.num_ranks, 0),
        oob_buffer_(options.oob_sz),
        my_pivot_count_(0),
        my_pivot_width_(0),
        policy_(nullptr) {
    MutexLock ml(&mutex_);
    // necessary to set mts_state_ to READY
    Reset();

#define POLICY_IS(s) (strncmp(options_.reneg_policy, s, strlen(s)) == 0)

    if (options_.reneg_policy == nullptr) {
      policy_ = new InvocationPeriodic(*this, options_);
    } else if(POLICY_IS("InvocationDynamic")) {
      policy_ = new InvocationDynamic(*this, options_);
    } else if (POLICY_IS("InvocationPeriodic")) {
      policy_ = new InvocationPeriodic(*this, options_);
    } else if (POLICY_IS("InvocationOnce")) {
      policy_ = new InvocationOnce(*this, options_);
    } else {
      policy_ = new InvocationPeriodic(*this, options_);
    }
  }

  Status Serialize(const char* fname, unsigned char fname_len, char* data,
                   unsigned char data_len, unsigned char extra_data_len,
                   particle_mem_t& p);

  Status AttemptBuffer(particle_mem_t& p, bool& shuffle, bool& flush);

  /* Only for benchmarks, not for actual data runs */
  Status ForceRenegotiation();

  /* AdvanceEpoch is also called before the first epoch,
   * so it can't be assumed that the first epoch is e0
   */
  void AdvanceEpoch() {
    MutexLock ml(&mutex_);
    MainThreadState cur_state = mts_mgr_.GetState();
    assert(cur_state != MainThreadState::MT_BLOCK);
    policy_->AdvanceEpoch();
    epoch_++;
  }

  OobFlushIterator OobIterator() { return OobFlushIterator(oob_buffer_); }

  size_t OobSize() const { return oob_buffer_.Size(); }

  void UpdateState(MainThreadState new_state) {
    mutex_.AssertHeld();
    mts_mgr_.UpdateState(new_state);
  }

  MainThreadState GetCurState() {
    mutex_.AssertHeld();
    return mts_mgr_.GetState();
  }

  MainThreadState GetPrevState() {
    mutex_.AssertHeld();
    return mts_mgr_.GetPrevState();
  }

  Status HandleMessage(char* buf, unsigned int bufsz, int src) {
    return rtp_.HandleMessage(buf, bufsz, src);
  }

  int NumRounds() const { return rtp_.NumRounds(); }

 private:
  void AssignShuffleTarget(particle_mem_t& p) {
    int dest_rank = policy_->ComputeShuffleTarget(p);
    if (dest_rank >= 0 and dest_rank < options_.num_ranks) {
      p.shuffle_dest = dest_rank;
      rank_counts_[dest_rank]++;
      rank_counts_aggr_[dest_rank]++;
    } else {
      p.shuffle_dest = -1;
    }
  }

  void MarkFlushableBufferedItems() {
    std::vector<particle_mem_t>& oob_vec = oob_buffer_.buf_;
    for (size_t i = 0; i < oob_vec.size(); i++) {
      AssignShuffleTarget(oob_vec[i]);
    }
  }

  static float GetIndexedAttr(const char* data_buf, unsigned int data_len) {
    const float* prop = reinterpret_cast<const float*>(data_buf);
    return prop[0];
  }

  static float GetIndexedAttrAlt(const char* data_buf, unsigned int data_len) {
    assert(data_len >= 7 * sizeof(float));
    const float* p_ar = reinterpret_cast<const float*>(data_buf);
    const float ux = p_ar[4];
    const float uy = p_ar[5];
    const float uz = p_ar[6];

    return sqrt(ux * ux + uy * uy + uz * uz);
  }

  /* Not called directly - up to the invocation policy */
  bool Reset() {
    mutex_.AssertHeld();
    mts_mgr_.Reset();
    range_min_ = 0;
    range_max_ = 0;
    std::fill(rank_counts_.begin(), rank_counts_.end(), 0);
    std::fill(rank_counts_aggr_.begin(), rank_counts_aggr_.end(), 0);
    oob_buffer_.Reset();
  }

 private:
  const CarpOptions& options_;
  MainThreadStateMgr mts_mgr_;

  RTP rtp_;
  int epoch_;

 public:
  /* XXX: temporary, refactor RTP/perfstats as friend classes */
  port::Mutex mutex_;
  port::CondVar cv_;

  double range_min_;
  double range_max_;

  std::vector<float> rank_bins_;
  std::vector<float> rank_counts_;
  std::vector<uint64_t> rank_counts_aggr_;

  OobBuffer oob_buffer_;

  double my_pivots_[pdlfs::kMaxPivots];
  size_t my_pivot_count_;
  double my_pivot_width_;

 private:
  friend class InvocationPolicy;
  InvocationPolicy* policy_;

  friend class PivotUtils;
};
}  // namespace carp
}  // namespace pdlfs
