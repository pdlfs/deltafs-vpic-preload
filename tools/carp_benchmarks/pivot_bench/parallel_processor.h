//
// Created by Ankush J on 10/28/22.
//

#pragma once

#include <pdlfs-common/env.h>
#include <pdlfs-common/port_posix.h>

#include "carp/carp_containers.h"
#include "pivot_common.h"
#include "trace_reader.h"

static void read_rank_into_oob_pivots(void* args);
static void read_rank_into_bins(void* args);

namespace {
struct PivotTask {
  pdlfs::port::Mutex* mutex;
  pdlfs::port::CondVar* cv;
  int* jobs_rem;

  pdlfs::TraceReader* tr;
  int epoch;
  int rank;
  int oobsz;
  int num_pivots;
  pdlfs::carp::Pivots* pivots;
  pdlfs::carp::OrderedBins* bins;
};
}  // namespace

namespace pdlfs {
class ParallelProcessor {
 public:
  ParallelProcessor(const PivotBenchOpts& opts)
      : opts_(opts), cv_(&mutex_), jobs_rem_(0) {
    tp_ = ThreadPool::NewFixed(opts_.nthreads, true, nullptr);
  }

  ~ParallelProcessor() {
    delete tp_;
    tp_ = nullptr;
  }

  void ComputeOobPivotsParallel(TraceReader& tr, int epoch,
                                std::vector<carp::Pivots>& pivots) {
    assert(pivots.size() == opts_.nranks);
    std::vector<PivotTask> all_tasks(opts_.nranks);

    jobs_rem_ = opts_.nranks;

    for (int r = 0; r < opts_.nranks; r++) {
      InitThreadTask(all_tasks[r]);
      all_tasks[r].tr = &tr;
      all_tasks[r].epoch = epoch;
      all_tasks[r].rank = r;
      all_tasks[r].pivots = &pivots[r];
      all_tasks[r].bins = nullptr;

      tp_->Schedule(read_rank_into_oob_pivots, &all_tasks[r]);
    }

    WaitUntilDone();
  }

  void ComputeBinsParallel(TraceReader& tr, int epoch,
                           std::vector<carp::OrderedBins>& bins) {
    assert(bins.size() == opts_.nranks);
    std::vector<PivotTask> all_tasks(opts_.nranks);

    jobs_rem_ = opts_.nranks;

    for (int r = 0; r < opts_.nranks; r++) {
      InitThreadTask(all_tasks[r]);
      all_tasks[r].tr = &tr;
      all_tasks[r].epoch = epoch;
      all_tasks[r].rank = r;
      all_tasks[r].pivots = nullptr;
      all_tasks[r].bins = &bins[r];

      tp_->Schedule(read_rank_into_bins, &all_tasks[r]);
    }

    WaitUntilDone();
  }

 private:
  void InitThreadTask(PivotTask& task) {
    task.oobsz = opts_.oobsz;
    task.num_pivots = opts_.pvtcnt;
    task.mutex = &mutex_;
    task.cv = &cv_;
    task.jobs_rem = &jobs_rem_;
  }

  void WaitUntilDone() {
    mutex_.Lock();
    while (jobs_rem_ != 0) {
      cv_.Wait();
    }
    mutex_.Unlock();
  }

  const PivotBenchOpts opts_;
  ThreadPool* tp_;
  port::Mutex mutex_;
  port::CondVar cv_;
  int jobs_rem_;
};
}  // namespace pdlfs

namespace {}