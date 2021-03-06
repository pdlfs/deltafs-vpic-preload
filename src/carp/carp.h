//
// Created by Ankush J on 3/5/21.
//

#include <float.h>
#include <msgfmt.h>
#include <pdlfs-common/status.h>
#include <range_constants.h>
#include <range_utils.h>

#include "oob_buffer.h"
#include "pdlfs-common/mutexlock.h"
#include "policy.h"
/* XXX: temporarily for range-utils, refactor ultimately */
#include "range_backend/mock_backend.h"
#include "rtp.h"

#pragma once

namespace pdlfs {
namespace carp {

struct CarpOptions {
  uint32_t num_ranks;
  uint32_t oob_sz;
  uint64_t reneg_intvl;
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
        rank_bins_(options_.num_ranks + 1, 0),
        rank_counts_(options_.num_ranks, 0),
        rank_counts_aggr_(options_.num_ranks, 0),
        oob_buffer_(options.oob_sz),
        my_pivot_count_(0),
        my_pivot_width_(0),
        policy_(nullptr) {
    policy_ = new InvocationPeriodic(*this, options_.reneg_intvl);
  }

  Status Serialize(const char* fname, unsigned char fname_len, char* data,
                   unsigned char data_len, unsigned char extra_data_len,
                   particle_mem_t& p);

  Status AttemptBuffer(particle_mem_t& p, bool& shuffle) {
    Status s = Status::OK();
    mutex_.Lock();

    bool can_buf = oob_buffer_.OutOfBounds(p.indexed_prop);
    /* shuffle = true if we can't buffer */
    shuffle = !can_buf;

    if (can_buf) {
      /* XXX: check RV */
      oob_buffer_.Insert(p);
    }

    /* reneg = true iff
     * 1. Trigger returns true
     * 2. OOB is full
     * 3. Reneg is ongoing
     */

    bool reneg_ongoing = (mts_mgr_.get_state() != MainThreadState::MT_READY);
    bool reneg =
        policy_->TriggerReneg() || oob_buffer_.IsFull() || reneg_ongoing;

    if (reneg) {
      rtp_init_round(NULL);
      MarkFlushableBufferedItems();
    }

    if (shuffle) {
      AssignShuffleTarget(p);
      if (p.shuffle_dest == -1) {
        ABORT("Invalid shuffle target");
      }
    }

    mutex_.Unlock();
  }

  void AdvanceEpoch() { policy_->AdvanceEpoch(); }

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
    mutex_.Lock();
    range_.Reset();
    std::fill(rank_counts_.begin(), rank_counts_.end(), 0);
    std::fill(rank_counts_aggr_.begin(), rank_counts_aggr_.end(), 0);
    oob_buffer_.Reset();
    mutex_.Unlock();
  }

 private:
  const CarpOptions& options_;
  MainThreadStateMgr mts_mgr_;

  port::Mutex mutex_;

  Range range_;

  std::vector<float> rank_bins_;
  std::vector<float> rank_counts_;
  std::vector<uint64_t> rank_counts_aggr_;

  OobBuffer oob_buffer_;

  double my_pivots_[pdlfs::kMaxPivots];
  size_t my_pivot_count_;
  double my_pivot_width_;

  friend class InvocationPolicy;
  InvocationPolicy* policy_;
};
}  // namespace carp
}  // namespace pdlfs
