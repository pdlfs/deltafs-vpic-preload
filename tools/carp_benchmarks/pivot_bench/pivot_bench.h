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
  PivotBench(PivotBenchOpts& opts)
      : opts_(opts), pvtcnt_vec_(STAGES_MAX + 1, opts_.pvtcnt) {}

  Status Run() {
    TraceReader tr(opts_);
    tr.DiscoverEpochs();

    carp::Pivots oob_pivots;
    GetOOBPivots(tr, 0, oob_pivots);
    carp::OrderedBins bins(opts_.nranks);
    bins.UpdateFromPivots(oob_pivots);

//    tr.ReadRankIntoBins(0, 0, bins);
    tr.ReadAllRanksIntoBins(0, bins);
    logf(LOG_INFO, "%s", bins.ToString().c_str());
    bins.PrintNormStd();

    return Status::OK();
  }

 private:
  void GetOOBPivots(TraceReader& tr, int epoch, carp::Pivots& merged_pivots) {
    std::vector<carp::Pivots> pivots(opts_.nranks);

    for (int r = 0; r < opts_.nranks; r++) {
      tr.GetOobPivots(0, r, opts_.oobsz, &pivots[r], pvtcnt_vec_[1]);
      logf(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
    }

    merged_pivots.FillZeros();
    carp::PivotAggregator aggr(pvtcnt_vec_);
    //    aggr.AggregatePivotsRoot(pivots, merged_pivots, 1, opts_.nranks);
    aggr.AggregatePivots(pivots, merged_pivots);

    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }
  const PivotBenchOpts opts_;
  const std::vector<int> pvtcnt_vec_;
};
}  // namespace pdlfs
