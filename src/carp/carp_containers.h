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

// fwd declaration for friendship
class OrderedBins;
class PivotUtils;

 /*
  * Pivots and OrderedBins define a sorted set of bins on the
  * floating point value being indexed.   Bin boundaries are
  * encoded in an array with the following property:
  *
  *   bin[r]   is the start of bin 'r' (inclusive)
  *   bin[r+1] is the end value of bin 'r' (exclusive)
  *
  * To define 'n' bins, the bin[] array contains 'n+1' entries.
  * points less than bin[0] or greater than or equal to bin[n]
  * are considered out of bounds (OOB).  A bin whose starting
  * and ending value are equal are 'zero-width bins' and
  * cannot contain data.  The Range object can be used with
  * individual bins or over an entire Pivots or OrderedBins.
  *
  * Pivots are constructed by CalculatePivots() during the
  * renegotiation process.  The pivot size is fixed by level
  * in the RTP fanout and set with the RANGE_Pvtcnt_s{1,2,3}
  * configuration variables.  The pivot count is defined as
  * the number of pivot point values used and maps directly
  * to the length of the Pivots' bin vector array (named
  * 'pivots_').  e.g. a Pivot with a count of 256 has 256
  * bin values that define 255 bins.   By definition, each
  * bin in a pivot created by CalculatePivots() should have
  * approximately the same weight/count of particles in it.
  * We store this value in weight_.
  *
  * OrderedBins are the result of the renegotiation process
  * and are used to map the indexed value of a particle to
  * the rank that managed the bin the indexed value fall in.
  * CARP bootstraps with an empty set of ordered bins (thus,
  * everything is out of bound until the first renegotiation is
  * complete and the initial set of OrderedBins has been sent
  * to all ranks).   The size of the OrderedBins is set to
  * the number of ranks in the job (each rank gets 1 bin).
  * OrderedBins also tracks the number of times a particle
  * is added to each bin using "count_" ... this histogram
  * info is used to calculate Pivots with ~equal weights
  * in each pivot bin.
  *
  * Note that the length of the bin array in Pivots is defined
  * as the pivot_count, while the length of the bin array in
  * OrderedBins is defined as the number of ranks plus one.
  */

class Pivots {
 public:
  // XXX: make private?  used by tools/carp_benchmarks/pivot_bench/rank.h
  Pivots() : pivots_(0), weight_(0), is_set_(false) {}

  Pivots(int npivots) : pivots_(npivots, 0), weight_(0), is_set_(false) {
    assert(npivots > 1);
  }

  /* set the pivot_count to a new value (defines npivots-1 bins) */
  void Resize(size_t npivots) {
    assert(npivots > 1);
    pivots_.resize(npivots);
  }

  size_t Size() const { return pivots_.size(); }

  double PivotWeight() const { return weight_; }

  std::string ToString() const;

  const double& operator[](std::size_t idx) const { return pivots_[idx]; }

  void LoadPivots(std::vector<double>& pvtvec, double pvtweight) {
    Resize(pvtvec.size());
    std::copy(pvtvec.begin(), pvtvec.end(), pivots_.begin());
    weight_ = pvtweight;
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
    weight_ = 0;
    is_set_ = true;
  }

  //
  // Used as implicit invalid pivots. XXXAJ: can be deprecated
  //
  void MakeUpEpsilonPivots() {
    int num_pivots = pivots_.size();

    float mass_per_pivot = 1.0f / (num_pivots - 1);
    weight_ = mass_per_pivot;

    for (int pidx = 0; pidx < num_pivots; pidx++) {
      pivots_[pidx] = mass_per_pivot * pidx;
    }

    is_set_ = true;
  }

  void AssertMonotonicity() {
    if (!is_set_) {
      flog(LOG_WARN, "No pivots set for monotonicity check!");
      return;
    }

    if (weight_ == CARP_BAD_PIVOTS) {
      flog(LOG_DBUG, "Pivots set to invalid. Not checking...");
      return;
    }

    int num_pivots = pivots_.size();
    for (int pidx = 0; pidx < num_pivots - 1; pidx++) {
      assert(pivots_[pidx] < pivots_[pidx + 1]);
    }
  }

 private:
  //
  // To be called by friends only
  //

  //
  // Pivots are doubles because we need them to be (for pivotcalc)
  // Everything is a double except rank_bins
  // As we don't need them to be, and they scale linearly with ranks
  //
  std::vector<double> pivots_;      /* define Pivots bin */
  double weight_;                   /* bin weight (~same for all bins) */
  bool is_set_;                     /* true if Pivots are setup */

  friend class PivotUtils;
};

class OrderedBins {
 public:
  OrderedBins(int nbins)            /* nbins is normally number of ranks */
      : bins_(nbins + 1, 0),
        counts_(nbins, 0),
        counts_aggr_(nbins, 0),
        is_set_(false) {
    assert(nbins >= 1);
  }

  OrderedBins(const OrderedBins& other) = default;

  OrderedBins operator+(const OrderedBins& rhs) {
    assert(Size() == rhs.Size());
    OrderedBins tmp(Size());
    std::copy(bins_.begin(), bins_.end(), tmp.bins_.begin());
    std::copy(counts_.begin(), counts_.end(), tmp.counts_.begin());
    std::copy(counts_aggr_.begin(), counts_aggr_.end(),
              tmp.counts_aggr_.begin());

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
  // Only used by RangeUtilsTest
  // Both counts_ and aggr_counts_ are set to counts
  //
  void UpdateFromArrays(int nbins, const float* bins, const uint64_t* counts);

  // Various accessors

  bool IsSet() const { return is_set_; }

  Range GetBin(size_t bidx) const {
    assert(bidx >= 0 && bidx < counts_.size());
    return Range(bins_[bidx], bins_[bidx + 1]);
  }

  Range GetRange() const {
    if (is_set_)
      return Range(bins_[0], bins_[Size()]);
    else
      return Range();
  }

  uint64_t GetTotalMass() const;

  //
  // Only used by logging functions
  //
  void GetCountsArr(const uint64_t** counts, int* countsz) const {
    *counts = counts_.data();
    *countsz = counts_.size();
  }

  //
  // Only used by logging functions
  //
  void GetAggrCountsArr(const uint64_t** counts, int* countsz) const {
    *counts = counts_aggr_.data();
    *countsz = counts_aggr_.size();
  }

  //
  // size of OrderedBins is nbins, aka counts_.size()
  // note that sizeof bins_ == nbins + 1
  //
  size_t Size() const { return counts_.size(); }

  //
  // Reset all structs incl aggr counts. Called at the end of epochs.
  //
  void Reset();

  //
  // Reset counts only. Called after a renegotiation round
  //
  void ZeroCounts() { std::fill(counts_.begin(), counts_.end(), 0); }

  //
  // Search bins.  if value is in bounds, return 0 and set rank.
  // if value is out of bounds and 'force' is true, return rank
  // on the end that is closest to value (either rank 0 or the last rank).
  // if value is out of bounds and 'force' is false, return -1
  // if value is out of bounds on the left, otherwise 1 (for oob right).
  //
  int SearchBins(float val, int& rank, bool force);

  void IncrementBin(size_t bidx) {
    assert(bidx >= 0 && bidx < counts_.size());
    counts_[bidx]++;
    counts_aggr_[bidx]++;
  }

  //
  // Searches for the bin corresponding to a value
  // Adds it there. Undefined behavior if val is out of bounds
  //
  void AddVal(float val, bool force) {
    int rv, rank;
    assert(IsSet());
    if (!force) assert(GetRange().Inside(val));

    rv = SearchBins(val, rank, force);
    if (rv < 0)
        ABORT("OrderedBins: bidx < 0");
    if (rv > 0)
        ABORT("OrderedBins: bidx >= Size()");

    IncrementBin(rank);
  }

  //
  // return number (weight) of particles in bin "idx" ... ret 0 if
  // out of range.
  //
  uint64_t Weight(size_t idx) {
    return (idx < counts_.size()) ? counts_[idx] : 0;
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
 /*
  * each time we assign a particle to rank "r" we increment both
  * counts_[r] and counts_aggr_[r].  counts_[] is reset
  * to zero by UpdatePivots(), counts_aggr_[] is not.
  *
  * bins_[] and counts_[] are used by the reneg protocol
  * to calculate a new set of pivots.  counts_aggr_[] is
  * only used for perflog reporting (not used in reneg
  * protocol).
  */
  std::vector<float> bins_;             /* current bins (sz=nranks+1) */
  std::vector<uint64_t> counts_;        /* bin histogram info (sz=nranks) */
  std::vector<uint64_t> counts_aggr_;
  bool is_set_;

  friend class PivotUtils;
};

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
