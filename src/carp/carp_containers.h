//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <float.h>
#include <math.h>

#include <numeric>
#include <sstream>
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
  Pivots() : pivots_(0), width_(0), is_set_(false) {}

  Pivots(int npivots) : pivots_(npivots, 0), width_(0), is_set_(false) {}

  size_t Size() const { return pivots_.size(); }

  std::string ToString() const {
    if (!is_set_) {
      return "[PivotWidth]: unset [Pivots]: unset";
    }

    std::ostringstream pvtstr;
    pvtstr.precision(3);

    pvtstr << "PivotWidth: " << width_;
    pvtstr << ", [PivotCount] " << pivots_.size();
    pvtstr << ", [Pivots]:";

    for (size_t pvt_idx = 0; pvt_idx < pivots_.size(); pvt_idx++) {
      if (pvt_idx % 16 == 0) {
        pvtstr << "\n\t";
      }

      double pivot = pivots_[pvt_idx];
      pvtstr << pivot << ", ";
    }

    return pvtstr.str();
  }

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

  //
  // After a renegotiation is complete, our bins are updated from the pivots
  //
  void UpdateFromPivots(Pivots& pivots) {
    if (pivots.Size() != Size() + 1) {
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

  // Various accessors

  Range GetBin(int bidx) const { return Range(bins_[bidx], bins_[bidx + 1]); }

  Range GetRange() const { return Range(bins_[0], bins_[Size()]); }

  uint64_t GetTotalMass() const {
    return std::accumulate(counts_.begin(), counts_.end(), 0ull);
  }

  std::string GetTotalMassPretty() const {
    uint64_t mass = GetTotalMass();
    std::stringstream mstr;
    mstr.precision(2);
    if (mass > 1e9)
      mstr << mass / 1e9 << "B";
    else if (mass > 1e6)
      mstr << mass / 1e6 << "M";
    else if (mass > 1e3)
      mstr << mass / 1e3 << "K";
    else
      mstr << mass;
    return mstr.str();
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
  // size of OrderedBins is nbins == sizeof counts
  // sizeof bins == nbins + 1
  //
  size_t Size() const { return counts_.size(); }

  //
  // Reset all structs. Called at the end of epochs.
  //
  void Reset() {
    std::fill(bins_.begin(), bins_.end(), 0);
    std::fill(counts_.begin(), counts_.end(), 0);
    std::fill(counts_aggr_.begin(), counts_aggr_.end(), 0);
    is_set_ = false;
  }

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

  void IncrementBin(int bidx) {
    assert(bidx < (int)Size() and bidx >= 0);
    counts_[bidx]++;
    counts_aggr_[bidx]++;
  }

  std::string ToString() const {
    std::ostringstream binstr;
    binstr << "[BinMass] " << GetTotalMassPretty();
    binstr << "\n[Bins] " << VecToString<float>(bins_);
    binstr << "\n[BinCounts] " << VecToString<uint64_t>(counts_);
    return binstr.str();
  }

  double PrintNormStd() {
    uint64_t total_sz = std::accumulate(counts_.begin(), counts_.end(), 0ull);
    double avg_binsz = total_sz * 1.0 / counts_.size();

    double normx_sum = 0;
    double normx2_sum = 0;

    for (uint64_t bincnt : counts_) {
      double normbincnt = bincnt / avg_binsz;
      double norm_x = normbincnt;
      double norm_x2 = normbincnt * normbincnt;
      normx_sum += norm_x;
      normx2_sum += norm_x2;
      logf(LOG_DBG2, "normbincnt: x: %lf, x2: %lf\n", normx_sum, normx2_sum);
    }

    normx_sum /= counts_.size();
    normx2_sum /= counts_.size();

    double normvar = normx2_sum - (normx_sum * normx_sum);
    double normstd = pow(normvar, 0.5);

    logf(LOG_INFO, "OrderedBins, Normalized Stddev: %.3lf\n", normstd);
    return normstd;
  }

 private:
  template <typename T>
  std::string VecToString(const std::vector<T>& vec) const {
    std::ostringstream vecstr;
    vecstr.precision(3);

    for (size_t vecidx = 0; vecidx < vec.size(); vecidx++) {
      if (vecidx % 10 == 0) {
        vecstr << "\n\t";
      }

      vecstr << vec[vecidx] << ", ";
    }

    return vecstr.str();
  }
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
