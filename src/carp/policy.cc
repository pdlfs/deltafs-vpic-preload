//
// Created by Ankush J on 3/5/21.
//

#include "policy.h"

#include "carp.h"

namespace pdlfs {
namespace carp {
InvocationPolicy::InvocationPolicy(Carp& carp, const CarpOptions& options)
    : options_(options),
      carp_(carp),
      invoke_intvl_(options_.reneg_intvl),
      epoch_(0),
      num_writes_(0) {}

bool InvocationPolicy::BufferInOob(particle_mem_t& p) {
  // XXX: should ideally be the same as consulting
  // carp.range_min_ and carp.range_max_
  // TODO: remove redundancy
  return carp_.oob_buffer_.OutOfBounds(p.indexed_prop);
}

bool InvocationPolicy::FirstRenegCompleted() {
  return !carp_.mts_mgr_.FirstBlock();
}

void InvocationPolicy::Reset() { carp_.Reset(); }

bool InvocationPolicy::IsOobFull() { return carp_.oob_buffer_.IsFull(); }

InvocationIntraEpoch::InvocationIntraEpoch(Carp& carp,
                                           const CarpOptions& options)
    : InvocationPolicy(carp, options) {}

bool InvocationIntraEpoch::TriggerReneg() {
  num_writes_++;
  bool intvl_trigger =
      (options_.my_rank == 0) && (num_writes_ % invoke_intvl_ == 0);
  return intvl_trigger || IsOobFull();
}

int InvocationIntraEpoch::ComputeShuffleTarget(particle_mem_t& p, int& rank) {
    size_t bidx;
    int rv;
    rv = carp_.bins_.SearchBins(p.indexed_prop, bidx, false);
    rank = bidx;
    return(rv);
}

bool InvocationInterEpoch::TriggerReneg() {
  if ((options_.my_rank == 0) && !reneg_triggered_ &&
      InvocationPolicy::IsOobFull()) {
    reneg_triggered_ = true;
    return true;
  } else if (options_.my_rank != 0 && !reneg_triggered_) {
    reneg_triggered_ = FirstRenegCompleted();
  }

  return false;
}

int InvocationInterEpoch::ComputeShuffleTarget(particle_mem_t& p, int& rank) {
  bool force = reneg_triggered_;
  size_t bidx;
  int rv;
  rv = carp_.bins_.SearchBins(p.indexed_prop, bidx, force);
  rank = bidx;
  return(rv);
}

}  // namespace carp
}  // namespace pdlfs
