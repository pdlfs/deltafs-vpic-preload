//
// Created by Ankush J on 10/28/22.
//

#pragma once

#include "pivot_common.h"
#include "trace_reader.h"

namespace pdlfs {
namespace carp {
class Rank {
 public:
  Rank(int rank, const pdlfs::PivotBenchOpts& opts, TraceReader& tr)
      : rank_(rank),
        opts_(opts),
        tr_(&tr),
        cur_epoch_read_(-1),
        cur_ep_offset_(0),
        cur_ep_size_(0),
        bins_(opts_.nranks),
        pivot_ctx_(&bins_) {}

  void GetOobPivots(PivotCalcCtx* pvt_ctx, int epoch, Pivots* oob_pivots,
                    int npivots) {
    // Compute OOB pivots from pivot_ctx

    ReadEpoch(epoch);
    cur_ep_offset_ = 0;

    bool eof = false;
    // Read oobsz items from epoch
    ReadItems(pvt_ctx, epoch, opts_.oobsz, eof);
    assert(eof == false);  // A rank trace should have a lot more than OOB items
    // Compute own pivots
    PivotUtils::CalculatePivots(pvt_ctx, oob_pivots, npivots);
  }

  void GetOobPivots(int epoch, Pivots* oob_pivots, int npivots) {
    PivotCalcCtx pivot_ctx;
    GetOobPivots(&pivot_ctx, epoch, oob_pivots, npivots);
  }

  void GetPerfectPivots(int epoch, Pivots* pivots, int npivots) {
    // Compute final pivots from pivot_ctx

    // 1. Start with empty pivot_ctx
    PivotCalcCtx pvt_ctx;
    Pivots oob_pivots;

    GetOobPivots(&pvt_ctx, epoch, &oob_pivots, npivots + 1);

    // Different from regular bins, which are = nranks
    OrderedBins bins_pp(npivots);
    pvt_ctx.SetBins(&bins_pp);
    bins_pp.UpdateFromPivots(oob_pivots);

    // Read rest of items from epoch
    bool eof = false;
    ReadItems(&pvt_ctx, epoch, SIZE_MAX, eof);
    assert(eof == true);

    // Calculate pivots. These should be a perfect fit for the epoch
    PivotUtils::CalculatePivots(&pvt_ctx, pivots, npivots);
  }

  void ReadEpochIntoBins(int epoch, OrderedBins* bins) {
    ReadEpoch(epoch);
    cur_ep_offset_ = 0;

    const float* items = reinterpret_cast<const float*>(data_.c_str());
    for (size_t item_idx = 0; item_idx < cur_ep_size_; item_idx++) {
      bins->AddVal(items[item_idx], /* force */ true);
    }
  }

 private:
  void ReadItems(PivotCalcCtx* pvt_ctx, int epoch, size_t itemcnt, bool& eof) {
    ReadEpoch(epoch);
    size_t items_rem = cur_ep_size_ - cur_ep_offset_;
    if (items_rem <= itemcnt) {
      itemcnt = items_rem;
      eof = true;
    }

    const float* items = reinterpret_cast<const float*>(data_.c_str());
    // Deprecated: check the OOB/bins condition here. Maintain your own OOB
    pvt_ctx->AddData(items + cur_ep_offset_, itemcnt);
    cur_ep_offset_ += itemcnt;
  }

  void ReadEpoch(int epoch) {
    if (cur_epoch_read_ == epoch) {
      return;
    }

    tr_->ReadEpoch(epoch, rank_, data_);
    cur_epoch_read_ = epoch;
    cur_ep_offset_ = 0;
    cur_ep_size_ = data_.size() / sizeof(float);
  }

  const pdlfs::PivotBenchOpts opts_;
  const int rank_;
  TraceReader* const tr_;
  std::string data_;
  int cur_epoch_read_;
  size_t cur_ep_offset_;
  size_t cur_ep_size_;

  carp::PivotCalcCtx pivot_ctx_;
  carp::OrderedBins bins_;
};
}  // namespace carp
}  // namespace pdlfs
