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
/* XXX: temporarily for range-utils, refactor ultimately */
#include "range_backend/mock_backend.h"

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
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
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
    policy_ = new InvocationPeriodic(*this, options_);
  }

  Status Serialize(const char* fname, unsigned char fname_len, char* data,
                   unsigned char data_len, unsigned char extra_data_len,
                   particle_mem_t& p);

  Status AttemptBuffer(rtp_ctx* rtp_ctx, particle_mem_t& p, bool& shuffle,
                       bool& flush);

  /* AdvanceEpoch is also called before the first epoch,
   * so it can't be assumed that the first epoch is e0
   */
  void AdvanceEpoch() {
    mutex_.Lock();
    MainThreadState cur_state = mts_mgr_.get_state();
    assert(cur_state != MainThreadState::MT_BLOCK);

    Reset();

    policy_->AdvanceEpoch();
    mutex_.Unlock();
  }

  OobFlushIterator OobIterator() { return OobFlushIterator(oob_buffer_); }

  size_t OobSize() { return oob_buffer_.Size(); }

  void UpdateState(MainThreadState new_state) {
    mutex_.AssertHeld();
    mts_mgr_.update_state(new_state);
  }

  MainThreadState GetCurState() {
    mutex_.AssertHeld();
    return mts_mgr_.get_state();
  }

  MainThreadState GetPrevState() {
    mutex_.AssertHeld();
    return mts_mgr_.get_prev_state();
  }

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

  float GetIndexedAttr(const char* data_buf, unsigned int data_len) {
    const float* prop = reinterpret_cast<const float*>(data_buf);
    return prop[0];
  }

  float GetIndexedAttrAlt(const char* data_buf, unsigned int data_len) {
    assert(data_len >= 7 * sizeof(float));
    const float* p_ar = reinterpret_cast<const float*>(data_buf);
    const float ux = p_ar[4];
    const float uy = p_ar[5];
    const float uz = p_ar[6];

    return sqrt(ux * ux + uy * uy + uz * uz);
  }

  bool Reset() {
    mutex_.AssertHeld();
    mts_mgr_.reset();
    range_min_ = 0;
    range_max_ = 0;
    std::fill(rank_counts_.begin(), rank_counts_.end(), 0);
    std::fill(rank_counts_aggr_.begin(), rank_counts_aggr_.end(), 0);
    oob_buffer_.Reset();
  }

 private:
  const CarpOptions& options_;
  MainThreadStateMgr mts_mgr_;

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
