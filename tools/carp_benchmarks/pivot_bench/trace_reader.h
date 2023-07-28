//
// Created by Ankush J on 10/24/22.
//

#pragma once

#include <algorithm>

#include "carp/ordered_bins.h"
#include "pdlfs-common/env.h"
#include "pivot_common.h"

namespace pdlfs {
class TraceReader {
 public:
  explicit TraceReader(const PivotBenchOpts& opts);

  // Copied from range_runner
  Status DiscoverEpochs(size_t& num_ep);

  Status ReadRankIntoBins(size_t ep_idx, int rank, carp::OrderedBins& bins);

  Status ReadAllRanksIntoBins(size_t ep_idx, carp::OrderedBins& bins);

  Status ReadEpoch(size_t ep_idx, int rank, std::string& data);

 private:
  Env* const env_;
  const std::string trace_root_;
  const int nranks_;
  std::vector<int> trace_epochs_;
};
}  // namespace pdlfs
