//
// Created by Ankush J on 10/24/22.
//

#pragma once

#include <pdlfs-common/status.h>
#include <stdlib.h>

#include <vector>

#include "carp/range_constants.h"
#include "common.h"
#include "pivot_aggr.h"
#include "trace_reader.h"

namespace pdlfs {
class PivotBench {
 public:
  PivotBench(PivotBenchOpts& opts) : opts_(opts) {}
  Status Run() {
    logf(LOG_INFO, "hello world!");
    TraceReader tr(opts_);
    tr.DiscoverEpochs();

    int nranks = 8;
    std::vector<carp::Pivots> pivots(nranks);
    for (int r = 0; r < nranks; r++) {
      tr.GetOobPivots(0, r, 512, &pivots[r], 32);
      logf(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
    }
    const int aggr_num_pivots[STAGES_MAX + 1] = {
        -1, 32, 32, 32,
    };

    carp::Pivots merged_pivots;

    carp::PivotAggregator aggr(aggr_num_pivots);
    aggr.AggregatePivotsRoot(pivots, merged_pivots, 1, 32);
    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());

    return Status::OK();

  }

 private:
  const PivotBenchOpts opts_;
};
}  // namespace pdlfs
