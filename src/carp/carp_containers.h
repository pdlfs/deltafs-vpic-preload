//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <float.h>
#include <math.h>

#include <algorithm>
#include <numeric>
#include <sstream>
#include <vector>

#include "common.h"
#include "range_common.h"

#include "ordered_bins.h"
#include "pivots.h"
#include "range.h"

namespace pdlfs {
namespace carp {

// fwd declaration for friendship
class PivotUtils;

/*
 * PivotCalcCtx: uses bins and other state, maintained by someone else
 * to compute the data needed for pivot calculation.
 * The resultant pivots etc. must be fed back into the respective containers
 * by the owner of this context. It is not meant to write to any other
 * container.
 *
 * Some util functions for pivotbench update the containers, and violate this
 * property.
 *
 * XXXAJ: Remove the AddData and CleanupOob functions
 * PivotBench should manipulate its OrderedBins directly, and not via this ctx
 * This should only take OrderedBins and act as a container for pvtcalc
 */
class PivotCalcCtx {
 public:
  PivotCalcCtx() : bins_(nullptr) {}

  PivotCalcCtx(OrderedBins* bins) : bins_(bins) {}

  bool FirstBlock() const { return (bins_ == nullptr) || (!bins_->IsSet()); }

  void SetBins(OrderedBins* bins) { bins_ = bins; }

  // only used for testing
  void SetOobInfo(std::vector<float>& left, std::vector<float>& right) {
    oob_left_ = left;
    oob_right_ = right;
  }

  Range GetBinRange() const {
    if (bins_ == nullptr)
      return Range();
    else
      return bins_->GetRange();
  }

  // Util func for pvtbench, not used by the real CARP code
  void AddData(const float* data, size_t data_sz) {
    if (FirstBlock()) {
      if (oob_left_.size() < data_sz) {
        oob_left_.resize(data_sz);
      }
      oob_left_.resize(data_sz);
      std::copy(data, data + data_sz, oob_left_.begin());
      CleanupOob();
      return;
    }

    Range r = bins_->GetRange();

    for (size_t idx = 0; idx < data_sz; idx++) {
      float val = data[idx];
      int vpos = r.GetRelPos(val);
      if (vpos < 0) {
        oob_left_.push_back(val);
      } else if (vpos > 0) {
        oob_right_.push_back(val);
      } else {
        // This can't fail, since we've established bin bounds
        bins_->AddVal(val, /* force */ false);
      }
    }

    CleanupOob();
    return;
  }

  //
  // Util func for pvtbench, not used by real CARP code
  // Can't flush oob_left_/oob_right_ technically, as these are
  // deduped, but should be accurate enough for simulated benchmarks
  //
  void FlushOob() {
    assert(bins_);
    /* We force=true while flushing because:
     * If OOBs are flushed, contents must have been considered
     * for a renegotiation, and a valid reneg will include all OOB
     * contents in there. This is to accomodate marginal differences
     * between pivots computed and the largest OOB item.
     *
     * XXX: On the other hand, the pivots computed will include the largest
     * OOB item as an explicit right-side pivot, so maybe the more appropriate
     * fix here is to make OrderedBins->Search inclusive.
     *
     * Once OrderedBins->Search has been made inclusive, both the force flags
     * should be reverted to false.
     */
    for(float f: oob_left_) {
      bins_->AddVal(f, /* force */ true);
    }

    for(float f: oob_right_) {
      bins_->AddVal(f, /* force */ true);
    }

    oob_left_.resize(0);
    oob_right_.resize(0);
  }

  void Clear() {
    oob_left_.resize(0);
    oob_right_.resize(0);
  }

 private:
  void CleanupOob() {
    if (oob_left_.size() > 1) {
      std::sort(oob_left_.begin(), oob_left_.end());
      size_t oldlsz = oob_left_.size();
      deduplicate_sorted_vector(oob_left_);
      size_t newlsz  = oob_left_.size();
      if (newlsz != oldlsz) {
        flog(LOG_INFO, "[OOBBuffer, Left] Duplicates removed (%zu to %zu)",
             oldlsz, newlsz);
      }
    }

    if (oob_right_.size() > 1) {
      std::sort(oob_right_.begin(), oob_right_.end());
      size_t oldrsz = oob_left_.size();
      deduplicate_sorted_vector(oob_right_);
      size_t newrsz = oob_left_.size();
      if (newrsz != oldrsz) {
        flog(LOG_INFO, "[OOBBuffer, Right] Duplicates removed (%zu to %zu)",
             oldrsz, newrsz);
      }
    }
  }

  std::vector<float> oob_left_;
  std::vector<float> oob_right_;
  OrderedBins* bins_;

  friend class PivotUtils;
  friend class Carp;  /// XXXAJ: tmp, ugly
};
}  // namespace carp
}  // namespace pdlfs
