//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <float.h>

#include <numeric>
#include <vector>

#include "common.h"

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

  bool IsValid() const { /* is range valid?  (i.e. reset or set) */
    return ((rmin_ == DBL_MAX && rmax_ == DBL_MIN) or (rmin_ < rmax_));
  }

  void Set(double qr_min, double qr_max) { /* set a new range */
    if (qr_min > qr_max) return;           /* XXX: invalid, shouldn't happen */
    rmin_ = qr_min;
    rmax_ = qr_max;
  }

  void Extend(double emin, double emax) { /* extend range if needed */
    rmin_ = std::min(rmin_, emin);
    rmax_ = std::max(rmax_, emax);
  }

  /* is "f" inside our range? returns false for unset ranges */
  virtual bool Inside(double f) const { return (f >= rmin_ && f < rmax_); }

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
  Pivots(int npivots) : pivots_(npivots, 0), width_(0), is_set_(false) {}

  size_t Size() const { return pivots_.size(); }

  float GetPivot(int idx) const { return pivots_[idx]; }

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

  Range GetBin(int bidx) const { return Range(bins_[bidx], bins_[bidx + 1]); }

  void IncrementBin(int bidx) {
    assert(bidx < (int)Size());
    counts_[bidx]++;
    counts_aggr_[bidx]++;
  }

  uint64_t GetTotalMass() const {
    return std::accumulate(counts_.begin(), counts_.end(), 0ull);
  }

  //
  // After a renegotiation is complete, our bins are updated from the pivots
  //
  void UpdateFromPivots(Pivots& pivots) {
    if (pivots.Size() != Size()) {
      logf(LOG_ERRO, "[OrderedBins] SetFromPivots: size mismatch (%zu vs %zu)",
           pivots.Size(), Size());
      ABORT("OrderedBins - size mismatch!!");
      return;
    }

    std::copy(pivots.pivots_.begin(), pivots.pivots_.end(), bins_.begin());
    std::fill(counts_.begin(), counts_.end(), 0);
    is_set_ = true;
  }

  void UpdateFromArrays(int nbins, const float* bins, const float* counts) {
    bins_.resize(nbins + 1);
    counts_.resize(nbins);

    std::copy(bins, bins + nbins + 1, bins_.begin());
    std::copy(counts, counts + nbins, counts_.begin());

    is_set_ = true;
  }

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
  // Reset all structs. Called at the end of epochs.
  //
  void Reset() {
    std::fill(bins_.begin(), bins_.end(), 0);
    std::fill(counts_.begin(), counts_.end(), 0);
    std::fill(counts_aggr_.begin(), counts_aggr_.end(), 0);
    is_set_ = false;
  }

  size_t Size() const { return bins_.size(); }

  //
  // semantics for SearchBins:
  // 1. each bin represents an exclusive range
  // 2. this assumes that the bins contain the val, semantics weird otherwise
  // 3. return -1 if first_bin > val. return last bin (n - 1) if val > last_bin.
  //
  int SearchBins(float val) {
    auto iter = std::lower_bound(bins_.begin(), bins_.end(), val);
    unsigned int idx = iter - bins_.begin();
    while (idx < bins_.size() && val == bins_[idx]) idx++;  // skip equal vals
    return idx - 1;
  }

 private:
  std::vector<float> bins_;
  std::vector<uint64_t> counts_;
  std::vector<uint64_t> counts_aggr_;
  bool is_set_;

  friend class PivotUtils;
};

struct PivotCalcCtx {
  bool first_block;
  Range range;
  std::vector<float> oob_left;
  std::vector<float> oob_right;
  OrderedBins* bins;
};
}  // namespace carp
}  // namespace pdlfs
