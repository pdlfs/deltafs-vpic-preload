//
// Created by Ankush J on 3/5/21.
//

#include "policy.h"

#include "carp.h"

namespace pdlfs {
namespace carp {
int InvocationPolicy::ComputeShuffleTarget(particle_mem_t& p, int& rank,
                                           int& num_ranks) {
  auto rank_iter = std::lower_bound(carp_.rank_bins_.begin(),
                                    carp_.rank_bins_.end(), p.indexed_prop);

  rank = rank_iter - carp_.rank_bins_.begin() - 1;
  num_ranks = carp_.options_.num_ranks;
  return 0;
}

bool InvocationPolicy::IsOobFull() { return carp_.oob_buffer_.IsFull(); }

}  // namespace carp
}  // namespace pdlfs