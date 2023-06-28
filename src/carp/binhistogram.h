//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <unistd.h>

#include <numeric>
#include <sstream>
#include <vector>

#include "pivots.h"
#include "range.h"
#include "range_common.h"   /* for vec_to_str */

namespace pdlfs {
namespace carp {

/*
 * bin histogram with bin type BT and weight type WT.  Bin boundaries
 * are encoded in an array with the following property:
 *
 *   bin[r]   is the start of bin 'r' (inclusive)
 *   bin[r+1] is the end value of bin 'r' (exclusive)
 *
 * the bins should sorted and contiguous.
 *
 * we can setup bins all at once (e.g. with an array) or with a starting
 * point that we extend.
 */
template <typename BT, typename WT> class BinHistogram {
 public:
  BinHistogram(size_t nbins_cap) { /* allows us to prealloc memory capacity */
    bins_.reserve(nbins_cap + 1);  /* +1 for bin encoding */
    weights_.reserve(nbins_cap);   /* # of active bins is weights_.size() */
    total_weight_ = 0;
  }

  /* accessors */

  /* are any bins defined yet? */
  bool IsSet() const { return (weights_.size() > 0); }

  /* number of bins currently allocated */
  size_t Size() const { return weights_.size(); }

  /* total weight/count we have */
  WT GetTotalWeight() const { return total_weight_; }

  /* weight of a given bin index */
  WT Weight(size_t bidx) const {
    return (bidx < weights_.size()) ? weights_[bidx] : 0;
  }

  /* range of entire object */
  Range GetRange() const {
    return (weights_.size()) ? Range(bins_.front(), bins_.back()) : Range();
  }

  /* range of a given bin index */
  Range GetBin(size_t bidx) const {
    assert(bidx >= 0 && bidx < weights_.size());
    return Range(bins_[bidx], bins_[bidx + 1]);
  }

  /* access weight array (XXX: only used by logging, layering issues) */
  void GetWeightArr(const WT** weights, int* weightsz) const {
    *weights = weights_.data();
    *weightsz = weights_.size();
  }

  /* non-accessors */

  /* Reset/resize to init state */
  void Reset() {
    bins_.resize(0);
    weights_.resize(0);
    total_weight_ = 0;
  }

  /* reset all weights to zero */
  void ZeroWeights() {
    std::fill(weights_.begin(), weights_.end(), 0);
     total_weight_ = 0;
  }

  /* add weight to a bin */
  void AddToBinWeight(size_t bidx, WT val) {
    assert(bidx >= 0 && bidx < weights_.size());
    weights_[bidx] += val;
    total_weight_ += val;
  }

  /*
   * search bins to determine the index of the bin a value falls in,
   * or if the value is out of bounds.  if in bounds set ret_bidx and
   * return 0.  return -1 if out of bounds before bins (oob left)
   * or +1 if past the end of our bins (oob right).
   * XXX: if force is true return first bin for oob left and last
   * bin for oob right (force in bounds). XXX not needed anymore?
   */
  int SearchBins(BT val, size_t& ret_bidx, bool force) {
    auto iter = std::lower_bound(bins_.begin(), bins_.end(), val);
    size_t idx = iter - bins_.begin();
    while (idx < bins_.size() && val == bins_[idx]) idx++;  // skip equal vals
    if (idx == 0) {
      if (!force)
        return(-1);     /* out of bounds on left side */
      idx = 1;          /* force: push up to bidx 0 */
    }
    if (idx == bins_.size()) {
      if (!force)
        return(1);      /* out of bounds on right side */
      idx--;            /* force: pull back to last bidx */
    }
    ret_bidx = idx - 1;
    return(0);        /* in bounds */
  }

  /* load new set of bins from pivots and zero weights */
  void UpdateFromPivots(Pivots& pivots) {
    assert(pivots.Size() >= 2);
    bins_.resize(pivots.Size());
    weights_.resize(pivots.Size() - 1);
    for (size_t idx = 0 ; idx < pivots.Size() ; idx++) {
      bins_[idx] = pivots[idx];
    }
    this->ZeroWeights();
  }

  /* load new set of bins from arrays and determine new total weight */
  void UpdateFromArrays(size_t nbins, const BT* bins, const WT* weights) {
    bins_.resize(nbins + 1);
    weights_.resize(nbins);
    std::copy(bins, bins + nbins + 1, bins_.begin());
    std::copy(weights, weights + nbins, weights_.begin());
    total_weight_ = std::accumulate(weights_.begin(), weights_.end(), 0ull);
  }

  /* setup for extending bins one entry at a time */
  void InitForExtend(BT start) {
    bins_.resize(1);
    bins_[0] = start;
    weights_.resize(0);
    total_weight_ = 0;
  }

  /* extend current bins by one entry */
  void Extend(BT new_end, WT weight) {
    assert(new_end >= bins_.back());
    bins_.push_back(new_end);
    weights_.push_back(weight);
    total_weight_ += weight;
  }

  std::string ToString() const {
    std::ostringstream binstr;
    binstr << "[BinWeight] " << pretty_num(total_weight_);
    binstr << "\n[Bins] " << vec_to_str<BT>(bins_);
    binstr << "\n[BinWeights] " << vec_to_str<WT>(weights_);
    return binstr.str();
  }

 private:
  std::vector<BT> bins_;           /* bins, size=nbins+1 (+1 for encoding) */
  std::vector<WT> weights_;        /* weights, size==nbins */
  WT total_weight_;                /* total weight in weights_ */
};
}  // namespace carp
}  // namespace pdlfs
