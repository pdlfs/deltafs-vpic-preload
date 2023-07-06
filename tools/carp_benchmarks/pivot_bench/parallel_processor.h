//
// Created by Ankush J on 10/28/22.
//

#pragma once

#include <pdlfs-common/env.h>
#include <pdlfs-common/port_posix.h>

#include "pivot_common.h"
#include "rank.h"
#include "trace_reader.h"

void rank_task_dual(void* args);

namespace {
struct RankTask {
  pdlfs::port::Mutex* mutex;
  pdlfs::port::CondVar* cv;
  int* jobs_rem;

  int epoch;
  pdlfs::carp::Rank* rank;
  pdlfs::carp::Pivots* pivots;
  int num_pivots;
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

  void GetPerfectPivotsParallel(int epoch, std::vector<carp::Rank*>& ranks,
                                std::vector<carp::Pivots>& pivots,
                                int num_pivots) {
    assert(ranks.size() == opts_.nranks);
    assert(pivots.size() == opts_.nranks);
    std::vector<RankTask> rank_tasks(ranks.size());
    jobs_rem_ = ranks.size();

    for (size_t ridx = 0; ridx < ranks.size(); ridx++) {
      InitTaskStruct(rank_tasks[ridx]);
      rank_tasks[ridx].epoch = epoch;
      rank_tasks[ridx].rank = ranks[ridx];
      rank_tasks[ridx].pivots = &pivots[ridx];
      rank_tasks[ridx].num_pivots = num_pivots;
      rank_tasks[ridx].bins = nullptr;

      tp_->Schedule(rank_task_dual, &rank_tasks[ridx]);
    }

    WaitUntilDone();
  }

  void ReadEpochIntoBinsParallel(int epoch, std::vector<carp::Rank*>& ranks,
                                  std::vector<carp::OrderedBins>& bins) {
    assert(ranks.size() == opts_.nranks);
    assert(bins.size() == opts_.nranks);
    std::vector<RankTask> rank_tasks(ranks.size());
    jobs_rem_ = ranks.size();

    for (size_t ridx = 0; ridx < ranks.size(); ridx++) {
      InitTaskStruct(rank_tasks[ridx]);
      rank_tasks[ridx].epoch = epoch;
      rank_tasks[ridx].rank = ranks[ridx];
      rank_tasks[ridx].pivots = nullptr;
      rank_tasks[ridx].num_pivots = -1;
      rank_tasks[ridx].bins = &bins[ridx];

      tp_->Schedule(rank_task_dual, &rank_tasks[ridx]);
    }

    WaitUntilDone();
  }

 private:
  void InitTaskStruct(RankTask& task) {
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
