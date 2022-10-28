//
// Created by Ankush J on 10/27/22.
//

#include "pivot_bench.h"

namespace pdlfs {
void PivotBench::GetOobPivotsParallel(TraceReader& tr, int epoch,
                                      carp::Pivots& merged_pivots) {
  ThreadPool* tp = ThreadPool::NewFixed(4, true, nullptr);

  std::vector<::OOBPivotTask> all_tasks(opts_.nranks);
  std::vector<carp::Pivots> pivots(opts_.nranks);

  for (int r = 0; r < opts_.nranks; r++) {
    all_tasks[r].tr = &tr;
    all_tasks[r].epoch = epoch;
    all_tasks[r].rank = r;
    all_tasks[r].oobsz = opts_.oobsz;
    all_tasks[r].pivots = &pivots[r];
    all_tasks[r].num_pivots = pvtcnt_vec_[1];

    tp->Schedule(::get_oob_pivots, &all_tasks[r]);
  }

  delete tp;
  tp = nullptr;

  merged_pivots.FillZeros();
  carp::PivotAggregator aggr(pvtcnt_vec_);
  //    aggr.AggregatePivotsRoot(pivots, merged_pivots, 1, opts_.nranks);
  aggr.AggregatePivots(pivots, merged_pivots);

  logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
}
}  // namespace pdlfs