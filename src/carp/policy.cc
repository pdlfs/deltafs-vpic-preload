//
// Created by Ankush J on 3/5/21.
//

#include "policy.h"

#include "carp.h"

namespace pdlfs {
namespace carp {
InvocationPolicy::InvocationPolicy(Carp& carp, const CarpOptions& options)
    : epoch_(0), num_writes_(0), carp_(carp), options_(options) {}

int InvocationPolicy::ComputeShuffleTarget(particle_mem_t& p, int& rank,
                                           int& num_ranks) {
  auto rank_iter = std::lower_bound(carp_.rank_bins_.begin(),
                                    carp_.rank_bins_.end(), p.indexed_prop);

  rank = rank_iter - carp_.rank_bins_.begin() - 1;
  num_ranks = carp_.options_.num_ranks;
  return 0;
}

bool InvocationPolicy::IsOobFull() { return carp_.oob_buffer_.IsFull(); }

InvocationPeriodic::InvocationPeriodic(Carp& carp, const CarpOptions& options)
    : InvocationPolicy(carp, options), invoke_intvl_(options_.reneg_intvl) {}

bool InvocationPeriodic::TriggerReneg() {
  num_writes_++;
  bool trigger = (options_.my_rank == 0) && (num_writes_ % invoke_intvl_ == 0);
  return trigger;
}
}  // namespace carp
}  // namespace pdlfs
