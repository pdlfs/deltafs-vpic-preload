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
      : opts_(opts),
        rank_(rank),
        tr_(&tr),
        cur_epoch_read_(-1),
        cur_ep_offset_(0),
        cur_ep_size_(0) {}

  void GetOobPivots(int epoch, Pivots* oob_pivots, int pivot_count) {
    // Compute OOB pivots
    ReadEpoch(epoch);
    cur_ep_offset_ = 0;
    bool eof = false;

    // Read oobsz items from epoch
    OobBuffer oob(opts_.oobsz);   /* tmp holding area */
    ReadItems(epoch, opts_.oobsz, NULL, &oob, eof);
    assert(eof == false);  // A rank trace should have a lot more than OOB items

    // Compute own pivots
    ComboConsumer<float,uint64_t> cco(NULL, &oob);
    oob_pivots->Resize(pivot_count);    // XXX caller should do this?
    oob_pivots->Calculate(cco);
  }

  void GetPerfectPivots(int epoch, Pivots* pivots, int npchunk) {
    Pivots oob_pivots(npchunk + 1);   /* convert to pivot count */
    GetOobPivots(epoch, &oob_pivots, npchunk + 1);

    // Different from regular bins, which are = nranks
    OrderedBins bins_pp(npchunk);
    oob_pivots.InstallInOrderedBins(&bins_pp);

    // Read rest of items from epoch
    bool eof = false;
    OobBuffer oob(cur_ep_size_);
    oob.SetInBoundsRange(bins_pp.GetRange());
    ReadItems(epoch, SIZE_MAX, &bins_pp, &oob, eof);
    assert(eof == true);

    ComboConsumer<float,uint64_t> cco(&bins_pp, &oob);
    pivots->Resize(npchunk);    // XXX caller should do this?
    pivots->Calculate(cco);
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
  void ReadItems(int epoch, size_t itemcnt, OrderedBins* bins,
                 OobBuffer* oob, bool& eof) {
    ReadEpoch(epoch);   /* noop if already loaded */
    size_t items_rem = cur_ep_size_ - cur_ep_offset_;
    if (items_rem <= itemcnt) {
      itemcnt = items_rem;
      eof = true;
    }

    const float* items = reinterpret_cast<const float*>(data_.c_str());
    for (size_t i = cur_ep_offset_ ; i < cur_ep_offset_ + itemcnt ; i++) {
      size_t bidx;
      if (bins && bins->SearchBins(items[i], bidx, false) == 0) {
        bins->IncrementBin(bidx);
      } else if (oob) {
        particle_mem_t ptmp;
        ptmp.indexed_prop = items[i];
        ptmp.buf_sz = 0;
        ptmp.shuffle_dest = -1;
        if (oob->Insert(ptmp) != 0) {
          fprintf(stderr, "oob insert failed?!\n");
          exit(1);
        }
      }
    }
    cur_ep_offset_ += itemcnt;
  }

  void ReadEpoch(int epoch) {
    if (cur_epoch_read_ == epoch) {
      return;
    }

    tr_->ReadEpoch(epoch, rank_, data_);  /* stores data in data_ */
    cur_epoch_read_ = epoch;
    cur_ep_offset_ = 0;
    cur_ep_size_ = data_.size() / sizeof(float);
  }

  const pdlfs::PivotBenchOpts opts_;     /* options (from ctor) */
  const int rank_;                       /* my rank (from ctor) */
  TraceReader* const tr_;                /* data source (from ctor) */
  std::string data_;                     /* current epoch data */
  int cur_epoch_read_;                   /* current epoch # */
  size_t cur_ep_offset_;                 /* float offset in data */
  size_t cur_ep_size_;                   /* #floats in data */
};

}  // namespace carp
}  // namespace pdlfs
