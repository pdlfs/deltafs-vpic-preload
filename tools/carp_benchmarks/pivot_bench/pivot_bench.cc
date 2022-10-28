//
// Created by Ankush J on 10/27/22.
//

#include "pivot_bench.h"

namespace pdlfs {
void PivotBench::GetOobPivotsParallel(TraceReader& tr, int epoch,
                                      carp::Pivots& merged_pivots) {
  ThreadPool* tp = ThreadPool::NewFixed(16, true, nullptr);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  int rem_count = opts_.nranks;

  std::vector<::OOBPivotTask> all_tasks(opts_.nranks);
  std::vector<carp::Pivots> pivots(opts_.nranks);

  for (int r = 0; r < opts_.nranks; r++) {
    all_tasks[r].tr = &tr;
    all_tasks[r].epoch = epoch;
    all_tasks[r].rank = r;
    all_tasks[r].oobsz = opts_.oobsz;
    all_tasks[r].pivots = &pivots[r];
    all_tasks[r].num_pivots = pvtcnt_vec_[1];
    all_tasks[r].mutex = &mutex;
    all_tasks[r].cv = &cv;
    all_tasks[r].rem_count = &rem_count;

    tp->Schedule(::get_oob_pivots, &all_tasks[r]);
  }

  mutex.Lock();
  while (rem_count != 0) {
    cv.Wait();
  }
  mutex.Unlock();

  merged_pivots.FillZeros();
  carp::PivotAggregator aggr(pvtcnt_vec_);
  //    aggr.AggregatePivotsRoot(pivots, merged_pivots, 1, opts_.nranks);
  aggr.AggregatePivots(pivots, merged_pivots);

  logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());

  delete tp;
}

void PivotBench::AnalyzePivotsAgainstEpochParallel(pdlfs::TraceReader& tr,
                                                   carp::Pivots& oob_pivots,
                                                   int epoch) {
  ThreadPool* tp = ThreadPool::NewFixed(16, true, nullptr);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  int rem_count = opts_.nranks;

  std::vector<::OOBPivotTask> all_tasks(opts_.nranks);
  std::vector<carp::OrderedBins> bins(opts_.nranks, opts_.nranks);

  for (int r = 0; r < opts_.nranks; r++) {
    bins[r].UpdateFromPivots(oob_pivots);

    all_tasks[r].tr = &tr;
    all_tasks[r].epoch = epoch;
    all_tasks[r].rank = r;
    all_tasks[r].oobsz = opts_.oobsz;
    all_tasks[r].bins = &bins[r];
    all_tasks[r].num_pivots = pvtcnt_vec_[1];
    all_tasks[r].mutex = &mutex;
    all_tasks[r].cv = &cv;
    all_tasks[r].rem_count = &rem_count;

    tp->Schedule(::read_rank_into_bins, &all_tasks[r]);
  }

  mutex.Lock();
  while (rem_count != 0) {
    cv.Wait();
  }
  mutex.Unlock();

  carp::OrderedBins merged_bins(opts_.nranks);
  for (int r = 0; r < opts_.nranks; r++) {
    merged_bins = merged_bins + bins[r];
  }

  logf(LOG_INFO, "%s", merged_bins.ToString().c_str());
  double load_std = merged_bins.PrintNormStd();
  printf("--------------\n");
  logger_.LogData(opts_.nranks, opts_.pvtcnt, epoch, load_std);

  delete tp;
}
}  // namespace pdlfs
