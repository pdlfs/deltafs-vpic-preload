//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <assert.h>
#include <unistd.h>

#include <sstream>
#include <string>
#include <vector>

#include "comboconsumer.h"
#include "ordered_bins.h"
#include "range.h"

namespace pdlfs {
namespace carp {

class PivotUtils;     // fwd declaration for friendship (XXX)

/*
 * Pivots define a sorted set of bins on the value being indexed.
 * Bin boundaries are encoded in an array with the following property:
 *
 *   bin[r]   is the start of bin 'r' (inclusive)
 *   bin[r+1] is the end value of bin 'r' (exclusive)
 *
 * To define 'n' bins, the bin[] array contains 'n+1' entries.
 * points less than bin[0] or greater than or equal to bin[n]
 * are considered out of bounds (OOB).  A bin whose starting
 * and ending value are equal are 'zero-width bins' and
 * cannot contain data.  The Range object can be used with
 * individual bins or over an entire Pivots.
 *
 * Pivots are constructed by CalculatePivots() during the
 * renegotiation process.  The pivot size (aka pivot count)
 * is fixed by level in the RTP fanout and set with the
 * RANGE_Pvtcnt_s{1,2,3} configuration variables.  The pivot
 * count is defined as the number of pivot point values used
 * and maps directly to the length of the Pivots' bin vector
 * array (named 'pivots_').  e.g. a Pivot with a count of 256
 * has 256 bin values that define 255 bins (aka pivot chunks).
 * By definition, each bin in a pivot created by CalculatePivots()
 * should have  approximately the same weight/count of particles
 * in it. We store this value in weight_.
 *
 * Note that the length of the bin array in Pivots is defined
 * as the pivot_count.  the number of pivot chunks is defined as
 * pivot_count - 1.
 */
class Pivots {
 public:
  // XXX: make private?  used by tools/carp_benchmarks/pivot_bench/rank.h
  Pivots() : pivots_(0), weight_(0), is_set_(false) {}

  Pivots(int pcount) : pivots_(pcount, 0), weight_(0), is_set_(false) {
    assert(pcount > 1);
  }

  /* accessor functions */
  size_t Size() const { return pivots_.size(); }    /* current pivot_count */
  double PivotWeight() const { return weight_; }    /* chunk weight */
  const double& operator[](std::size_t idx) const { /* access bin array */
    return pivots_[idx];
  }

  /* set the pivot_count to a new value (defines pcount-1 chunks/bins) */
  void Resize(size_t pcount) {
    assert(pcount > 1);
    pivots_.resize(pcount);
  }

  /* load pivots and weight from a vector and weight value */
  void LoadPivots(std::vector<double>& pvtvec, double pvtweight) {
    Resize(pvtvec.size());
    std::copy(pvtvec.begin(), pvtvec.end(), pivots_.begin());
    weight_ = pvtweight;
    is_set_ = true;
  }

  /* returns overall range of our pivots */
  Range GetPivotBounds() {
    if (!is_set_) {
      ABORT("Range is not set!");
      return Range();
    }

    return Range(pivots_[0], pivots_[pivots_.size() - 1]);
  }

  /* set all pivot values to 0 (zero-width) and 0 weight */
  void FillZeros() {
    std::fill(pivots_.begin(), pivots_.end(), 0);
    weight_ = 0;
    is_set_ = true;
  }

  /* install our pivots into an ordered_bins */
  void InstallInOrderedBins(OrderedBins *ob) {
    ob->UpdateFromPivVec(pivots_);
  }

  void Calculate(ComboConsumer<float,uint64_t>& cco) {
    this->FillZeros();
    if (this->Size() == 0)  /* no data, return leaving zeros in pivots */
      return;
    assert(this->Size() > 1);

    int npchunk = this->Size() - 1;
    for (size_t lcv = 0 ; lcv < this->Size() ; lcv++) {
      if (lcv)
        cco.ConsumeWeightTo(cco.TotalWeight() *
                            ((npchunk - lcv) / (double) npchunk) );
      pivots_[lcv] = cco.CurrentValue();
    }

    this->weight_ = cco.TotalWeight() / (double) npchunk;

    this->AssertMonotonicity();
  }

  /* methods in pivots.cc */

  // assert monotonicity in pivots_ array
  void AssertMonotonicity();

  // provide a string (for debug/diag info)
  std::string ToString() const;

 private:
  //
  // Pivots are doubles because we need them to be (for pivotcalc)
  // Everything is a double except rank_bins
  // As we don't need them to be, and they scale linearly with ranks
  //
  std::vector<double> pivots_;      /* define Pivots bin */
  double weight_;                   /* bin weight (~same for all chunks) */
  bool is_set_;                     /* true if Pivots are setup */

  friend class PivotUtils;  /* XXX */
};
}  // namespace carp
}  // namespace pdlfs
