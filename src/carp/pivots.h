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
  Pivots(size_t pivot_count) : weight_(0), pivot_count_(pivot_count) {
    assert(pivot_count > 1);
    pivots_.reserve(pivot_count);
  }

  /* accessor functions */
  size_t Size() const { return pivot_count_; }      /* pivot_count if set */
  double PivotWeight() const { return weight_; }    /* chunk weight */
  const double& operator[](std::size_t idx) const { /* access bin array */
    assert(idx < pivots_.size());
    return pivots_[idx];
  }

  /* load pivots and weight from an array and weight value */
  void LoadPivots(double *pvtarr, size_t pcount, double pvtweight) {
    assert(pcount == pivot_count_);
    pivots_.resize(0);
    pivots_.insert(pivots_.begin(), pvtarr, pvtarr + pcount);
    weight_ = pvtweight;
  }

  /* returns overall range of our pivots */
  Range GetPivotBounds() {
    if (pivots_.size() == 0) {
      ABORT("Range is not set!");
      return Range();
    }

    return Range(pivots_[0], pivots_[pivots_.size() - 1]);
  }

  /* set all pivot values to 0 (zero-width) and 0 weight */
  void FillZeros() {
    if (pivots_.size() == 0) {
      pivots_.resize(pivot_count_, 0);
    } else {
      std::fill(pivots_.begin(), pivots_.end(), 0);
    }
    weight_ = 0;
  }

  /* install our pivots into an ordered_bins */
  void InstallInOrderedBins(OrderedBins *ob) {
    ob->UpdateFromPivVec(pivots_);
  }

  void Calculate(ComboConsumer<float,uint64_t>& cco) {

    /* no data?  fill pivots with zeros and zero weight */
    if (cco.TotalWeight() == 0) {
      this->FillZeros();
      return;
    }

    pivots_.resize(0);
    int npchunk = pivot_count_ - 1;
    for (size_t lcv = 0 ; lcv < pivot_count_ ; lcv++) {
      if (lcv)
        cco.ConsumeWeightTo(cco.TotalWeight() *
                            ((npchunk - lcv) / (double) npchunk) );
      pivots_.push_back(cco.CurrentValue());
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
  Pivots() {};                    /* private to disallow */

  // note that Pivots are doubles, while OrderedBins are floats
  std::vector<double> pivots_;      /* define Pivots bin */
  double weight_;                   /* bin weight (~same for all chunks) */
  size_t pivot_count_;              /* pivot_'s pivot_count (once filled) */

  friend class PivotUtils;  /* XXX */
};
}  // namespace carp
}  // namespace pdlfs
