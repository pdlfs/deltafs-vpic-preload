//
// Created by Ankush J on 10/25/22.
//

#pragma once

#include <unistd.h>

#include "binhistogram.h"

namespace pdlfs {
namespace carp {

/*
 * OrderedBins is the histogram in the main carp structure that maps
 * ranges to mpi ranks (the rank# corresponds to the bin index).
 * OrderedBins is a BinHistogram that also tracks aggregate counts
 * (for diagnostic purposes, not used with rtp).
 *
 * OrderedBins are the result of the renegotiation process.
 * CARP bootstraps with an empty set of ordered bins (thus,
 * everything is out of bound until the first renegotiation is
 * complete and the initial set of OrderedBins has been sent
 * to all ranks).   The size of the OrderedBins is set to
 * the number of ranks in the job (each rank gets 1 bin).
 * OrderedBins tracks the number of times a particle is added
 * to a bin using this BinHistogram weight ... this histogram
 * info is used to calculate Pivots with ~equal weights
 * in each pivot bin.
 *
 * Note that the length of the bin array in OrderedBins is defined
 * as the number of ranks plus one.
 */
class OrderedBins : public BinHistogram<float,uint64_t> {
 public:
  OrderedBins(int nbins_cap) : BinHistogram<float,uint64_t>(nbins_cap) {
    counts_aggr_.reserve(nbins_cap);
  }

  /* aggr count of a given bin index */
  uint64_t AggrCount(size_t bidx) const {
    return (bidx < counts_aggr_.size()) ? counts_aggr_[bidx] : 0;
  }

  /* Reset/resize to init state, includes aggregate counts */
  void Reset() {
    this->BinHistogram<float,uint64_t>::Reset();

    /* additional work for aggregates */
    counts_aggr_.resize(0);
  }

  /* add 1 to bin weight and update aggregate counts as well */
  void IncrementBin(size_t bidx) {
    this->AddToBinWeight(bidx, 1);

    /* additional work for aggregates */
    counts_aggr_[bidx]++;
   }

  /* load bins from pivot vector and zero weights */
  void UpdateFromPivVec(std::vector<double>& pv) {
    this->BinHistogram<float,uint64_t>::UpdateFromPivVec(pv);

    /* additional work for aggregates */
    if (counts_aggr_.size() < this->Size())  /* grow aggr if needed */
      counts_aggr_.resize(this->Size());
  }

  /* load bins from arrays and determine new total weight (for debug/diag) */
  void UpdateFromArrays(size_t nbins, const float* bins,
                        const uint64_t* weights) {
    this->BinHistogram<float,uint64_t>::UpdateFromArrays(nbins, bins, weights);

    /* additional work for aggregates */
    if (counts_aggr_.size() < this->Size())  /* grow aggr if needed */
      counts_aggr_.resize(this->Size());
    /* this resets aggr to match the inbound counts array */
    std::copy(weights, weights + nbins, counts_aggr_.begin());
  }

  /* extend current bins by one entry */
  void Extend(float new_end, uint64_t weight) {
    this->BinHistogram<float,uint64_t>::Extend(new_end, weight);

    /* additional work for aggregates */
    if (counts_aggr_.size() < this->Size())  /* grow aggr if needed */
      counts_aggr_.resize(this->Size());
    counts_aggr_[this->Size() - 1] += weight;
  }

  /* methods in ordered_bins.cc */

  /* operator+ is only used by test/tools code */
  OrderedBins operator+(const OrderedBins& rhs);

  // search for bin and add (XXX: may not be needed anymore?)
  void AddVal(float val, bool force);

  // Only used by test/tools programs
  double PrintNormStd();

 private:
  std::vector<uint64_t> counts_aggr_;  /* only for perflog, not used by rtp */
};

}  // namespace carp
}  // namespace pdlfs
