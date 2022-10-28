//
// Created by Ankush J on 10/24/22.
//

#pragma once

#include <pdlfs-common/status.h>
#include <stdlib.h>

#include <vector>

#include "carp/range_constants.h"
#include "common.h"
#include "logger.h"
#include "parallel_processor.h"
#include "pivot_aggr.h"
#include "rank.h"
#include "trace_reader.h"

namespace pdlfs {
class PivotBench {
 public:
  PivotBench(PivotBenchOpts& opts)
      : opts_(opts),
        pvtcnt_vec_(STAGES_MAX + 1, opts_.pvtcnt),
        logger_(opts.log_file),
        parallel_processor_(opts_),
        tr_(opts_),
        num_ep_(0) {
    tr_.DiscoverEpochs(num_ep_);
    InitAllRanks();
  }

  ~PivotBench() { DestroyAllRanks(); }

  Status Run() {
    Status s = Status::OK();
    TraceReader tr(opts_);

    size_t num_ep;
    tr.DiscoverEpochs(num_ep);

    s = RunSuite1(0);
    if (!s.ok()) return s;

    return s;
  }

  Status RunSuite1(int epoch) {
    Status s = Status::OK();

    runtype = "tmp";
    carp::Pivots oob_pivots;
    GetOobPivots(epoch, oob_pivots, pvtcnt_vec_[1], false);

    carp::OrderedBins bins(opts_.nranks);
    bins.UpdateFromPivots(oob_pivots);
    bins.ZeroCounts();
    ReadEpochIntoBins(epoch, bins, false);

    double load_std = bins.PrintNormStd();
    printf("--------------\n");
    logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, epoch, load_std);

    return s;
  }

 private:
  void GetOobPivots(int epoch, carp::Pivots& merged_pivots, int num_pivots,
                    bool parallel) {
    std::vector<carp::Pivots> pivots(opts_.nranks);

    if (parallel) {
      logf(LOG_ERRO, "Not implemented");
      ABORT("NOT IMPLEMENTED!!");
    } else {
      for (int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->GetOobPivots(epoch, &pivots[r], num_pivots);
        logf(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
      }
    }

    merged_pivots.FillZeros();
    carp::PivotAggregator aggr(pvtcnt_vec_);
    aggr.AggregatePivots(pivots, merged_pivots);

    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void GetPerfectPivots(int epoch, carp::Pivots& merged_pivots, int num_pivots,
                        bool parallel) {
    std::vector<carp::Pivots> pivots(opts_.nranks);
    if (parallel) {
      parallel_processor_.GetPerfectPivotsParallel(epoch, ranks_, pivots,
                                                   num_pivots);
    } else {
      for (int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->GetOobPivots(epoch, &pivots[r], num_pivots);
        logf(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
      }
    }

    merged_pivots.FillZeros();
    carp::PivotAggregator aggr(pvtcnt_vec_);
    aggr.AggregatePivots(pivots, merged_pivots);

    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void ReadEpochIntoBins(int epoch, carp::OrderedBins& merged_bins,
                         bool parallel) {
    std::vector<carp::OrderedBins> bins(merged_bins.Size(), merged_bins);
    if (parallel) {
      parallel_processor_.ReadEpochIntoBinsParallel(epoch, ranks_, bins);
    } else {
      for (int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->ReadEpochIntoBins(epoch, &bins[r]);
        logf(LOG_INFO, "%s\n", bins[r].ToString().c_str());
      }
    }

    merged_bins.Reset();
    for (auto bin : bins) {
      merged_bins = merged_bins + bin;
    }

    logf(LOG_INFO, "%s\n", merged_bins.ToString().c_str());
  }

  void InitAllRanks() {
    assert(ranks_.size() == 0);
    for (int r = 0; r < opts_.nranks; r++) {
      ranks_.push_back(new carp::Rank(r, opts_, tr_));
    }
  }

  void DestroyAllRanks() {
    assert(ranks_.size() == opts_.nranks);
    for (int r = 0; r < opts_.nranks; r++) {
      delete ranks_[r];
      ranks_[r] = nullptr;
    }
  }

  std::string runtype;
  const PivotBenchOpts opts_;
  const std::vector<int> pvtcnt_vec_;
  PivotLogger logger_;
  ParallelProcessor parallel_processor_;
  TraceReader tr_;
  std::vector<carp::Rank*> ranks_;
  size_t num_ep_;
};
}  // namespace pdlfs