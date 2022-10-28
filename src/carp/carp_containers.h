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

namespace pdlfs {
namespace carp {
//
// range of floating point numbers.  the range is defined by a min and
// max value (inclusive and exclusive, respectively).  the default/reset
// range is an undefined one (where we set rmin=FLT_MAX, rmax=FLT_MIN).
// note that a range with rmin==rmax is a zero-width range (nothing can
// be inside it).  ranges are safe to copy.  owner must provide locking.
//

class Range {
 public:
  Range() : rmin_(DBL_MAX), rmax_(DBL_MIN) {}

  Range(double rmin, double rmax) : rmin_(rmin), rmax_(rmax) {}

  Range(const Range& other) = default;

  /* accessor functions */
  double rmin() const { return rmin_; }
  double rmax() const { return rmax_; }

  void Reset() { /* reset everything to clean state */
    rmin_ = DBL_MAX;
    rmax_ = DBL_MIN;
  }

  bool IsSet() const { /* is range set? */
    return (rmin_ != DBL_MAX) and (rmax_ != DBL_MIN);
  }

  void Set(double qr_min, double qr_max) { /* set a new range */
    if (qr_min > qr_max) return;           /* XXX: invalid, shouldn't happen */
    rmin_ = qr_min;
    rmax_ = qr_max;
  }

  /* is "f" inside our range? returns false for unset ranges */
  virtual bool Inside(double f) const { return (f >= rmin_ && f < rmax_); }

  //
  // Return the relative position of a point relative to the range
  // -1: to left. 0: inside. +1: to right.
  int GetRelPos(double f) {
    if (!IsSet())
      return -1;
    else if (f < rmin_)
      return -1;
    else if (f >= rmax_)
      return 1;
    else
      return 0;
  }

 protected:
  double rmin_; /* start of range (inclusive) */
  double rmax_; /* end of range (exclusive) */
};

class InclusiveRange : public Range {
 public:
  InclusiveRange() : Range() {}
  InclusiveRange(double rmin, double rmax) : Range(rmin, rmax) {}
  InclusiveRange(const InclusiveRange& other) = default;
  /* is "f" inside our range? returns false for unset ranges */
  bool Inside(double f) const override { return (f >= rmin_ && f <= rmax_); }
};

// fwd declaration for friendship
class OrderedBins;
class PivotUtils;

class Pivots {
 public:
  Pivots() : pivots_(0), width_(0), is_set_(false) {}

  Pivots(int npivots) : pivots_(npivots, 0), width_(0), is_set_(false) {}

  size_t Size() const { return pivots_.size(); }

  std::string ToString() const;

  void LoadPivots(std::vector<double>& pvtvec, double pvtwidth) {
    Resize(pvtvec.size());
    std::copy(pvtvec.begin(), pvtvec.end(), pivots_.begin());
    width_ = pvtwidth;
    is_set_ = true;
  }

  Range GetPivotBounds() {
    if (!is_set_) {
      ABORT("Range is not set!");
      return Range();
    }

    return Range(pivots_[0], pivots_[pivots_.size() - 1]);
  }

  void FillZeros() {
    std::fill(pivots_.begin(), pivots_.end(), 0);
    width_ = 0;
    is_set_ = true;
  }

  void GetPivotsArr(const double** pivots, int* pvtcnt) const {
    *pivots = pivots_.data();
    *pvtcnt = pivots_.size();
  }

  float GetPivot(int idx) const { return pivots_[idx]; }

  const double* GetPivotData() const { return pivots_.data(); }

  double GetPivotWidth() const { return width_; }

  void MakeUpEpsilonPivots() {
    int num_pivots = pivots_.size();

    float mass_per_pivot = 1.0f / (num_pivots - 1);
    width_ = mass_per_pivot;

    for (int pidx = 0; pidx < num_pivots; pidx++) {
      pivots_[pidx] = mass_per_pivot * pidx;
    }

    is_set_ = true;
  }

  void AssertMonotonicity() {
    if (!is_set_) {
      logf(LOG_WARN, "No pivots set for monotonicity check!");
      return;
    }

    int num_pivots = pivots_.size();
    for (int pidx = 0; pidx < num_pivots - 1; pidx++) {
      assert(pivots_[pidx] <= pivots_[pidx + 1]);
    }
  }

 private:
  //
  // To be called by friends only
  //
  void Resize(size_t npivots) { pivots_.resize(npivots); }

  //
  // Pivots are doubles because we need them to be (for pivotcalc)
  // Everything is a double except rank_bins
  // As we don't need them to be, and they scale linearly with ranks
  //
  std::vector<double> pivots_;
  double width_;
  bool is_set_;

  friend class Carp;
  friend class OrderedBins;
  friend class PivotUtils;
};

class OrderedBins {
 public:
  OrderedBins(int nbins)
      : bins_(nbins + 1, 0),
        counts_(nbins, 0),
        counts_aggr_(nbins, 0),
        is_set_(false) {}

  OrderedBins(const OrderedBins& other) = default;

  OrderedBins operator+(const OrderedBins& rhs) {
    assert (Size() == rhs.Size());
    OrderedBins tmp(Size());
    std::copy(bins_.begin(), bins_.end(), tmp.bins_.begin());
    std::copy(counts_.begin(), counts_.end(), tmp.counts_.begin());
    std::copy(counts_aggr_.begin(), counts_aggr_.end(), tmp.counts_aggr_.begin());

    for (size_t i = 0; i < Size(); i++) {
      tmp.counts_[i] += rhs.counts_[i];
      tmp.counts_aggr_[i] += rhs.counts_aggr_[i];
    }

    return tmp;
  }

  //
  // After a renegotiation is complete, our bins are updated from the pivots
  // aggr counts are not reset, everything else is
  //
  void UpdateFromPivots(Pivots& pivots);

  //
  // Both counts_ and aggr_counts_ are set to counts
  //
  void UpdateFromArrays(int nbins, const float* bins, const uint64_t* counts);

  // Various accessors

  bool IsSet() const { return is_set_; }

  Range GetBin(int bidx) const { return Range(bins_[bidx], bins_[bidx + 1]); }

  Range GetRange() const {
    if (is_set_)
      return Range(bins_[0], bins_[Size()]);
    else
      return Range();
  }

  uint64_t GetTotalMass() const;

  void GetBinsArr(const float** bins, int* binsz) const {
    *bins = bins_.data();
    *binsz = bins_.size();
  }

  void GetCountsArr(const uint64_t** counts, int* countsz) const {
    *counts = counts_.data();
    *countsz = counts_.size();
  }

  void GetAggrCountsArr(const uint64_t** counts, int* countsz) const {
    *counts = counts_aggr_.data();
    *countsz = counts_aggr_.size();
  }

  //
  // size of OrderedBins is nbins == sizeof counts
  // sizeof bins == nbins + 1
  //
  size_t Size() const { return counts_.size(); }

  //
  // Reset all structs incl aggr counts. Called at the end of epochs.
  //
  void Reset();

  void ZeroCounts() {
    std::fill(counts_.begin(), counts_.end(), 0);
  }

  //
  // semantics for SearchBins:
  // 1. each bin represents an exclusive range
  // 2. this assumes that the bins contain the val, semantics weird otherwise
  // 3. return -1 if first_bin > val. return last bin (n - 1) if val > last_bin.
  //
  int SearchBins(float val);

  void IncrementBin(int bidx) {
    assert(bidx < (int)Size() and bidx >= 0);
    counts_[bidx]++;
    counts_aggr_[bidx]++;
  }

  //
  // Searches for the bin corresponding to a value
  // Adds it there. Undefined behavior if val is out of bounds
  //
  void AddVal(float val, bool force) {
    assert(IsSet());
    if (!force) assert(Range().Inside(val));

    int bidx = SearchBins(val);

    if (bidx < 0) {
      if (force)
        bidx = 0;
      else
        ABORT("OrderedBins: bidx < 0");
    }

    if (bidx >= Size()) {
      if (force)
        bidx = Size() - 1;
      else
        ABORT("OrderedBins: bidx >= Size()");
    }

    IncrementBin(bidx);
  }

  std::string ToString() const {
    std::ostringstream binstr;
    binstr << "[BinMass] " << pretty_num(GetTotalMass());
    binstr << "\n[Bins] " << vec_to_str<float>(bins_);
    binstr << "\n[BinCounts] " << vec_to_str<uint64_t>(counts_);
    return binstr.str();
  }

  double PrintNormStd();

 private:
  std::vector<float> bins_;
  std::vector<uint64_t> counts_;
  std::vector<uint64_t> counts_aggr_;
  bool is_set_;

  friend class PivotUtils;
};

/*
 * PivotCalcCtx: uses bins and other state, maintained by someone else
 * to compute the data needed for pivot calculation.
 * The resultant pivots etc. must be fed back into the respective containers
 * by the owner of this context. It is not meant to write to any other container.
 *
 * Some util functions for pivotbench update the containers, and violate this
 * property, TODO: fix this.
 */
class PivotCalcCtx {
 public:
  PivotCalcCtx() : bins_(nullptr) {}

  PivotCalcCtx(OrderedBins* bins) : bins_(bins) {}

  bool FirstBlock() const { return (bins_ == nullptr) || (!bins_->IsSet()); }

  void SetBins(OrderedBins* bins) { bins_ = bins; }

  Range GetRange() const {
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
    for(float f: oob_left_) {
      bins_->AddVal(f, /* force */ false); // forcing shouldn't be necessary
    }

    for(float f: oob_right_) {
      bins_->AddVal(f, /* force */ false); // forcing shouldn't be necessary
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
      deduplicate_sorted_vector(oob_left_);
    }

    if (oob_right_.size() > 1) {
      std::sort(oob_right_.begin(), oob_right_.end());
      deduplicate_sorted_vector(oob_right_);
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
