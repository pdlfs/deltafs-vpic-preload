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

    size_t num_eps;
    tr.DiscoverEpochs(num_eps);

    bool parallel = true;

    RunSuiteEp0PP(num_eps, parallel);
    RunSuiteEpXSub1PP(num_eps, parallel);
    RunSuiteEpXPP(num_eps, parallel);

    return s;
  }

  void RunSuiteExp(int epoch) {
    bool parallel = false;

    runtype = "tmp";
    carp::Pivots oob_pivots(opts_.nranks+1);
    GetPerfectPivots(epoch, oob_pivots, pvtcnt_vec_[1], parallel);

    carp::OrderedBins bins(opts_.nranks);
    oob_pivots.InstallInOrderedBins(&bins);
    bins.ZeroWeights();
    ReadEpochIntoBins(epoch, bins, parallel);

    double load_std = bins.PrintNormStd();
    printf("--------------\n");
    logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, epoch, load_std);
  }

  void RunSuiteEp0PP(const int num_eps, bool parallel) {
    runtype = "ep0pp";

    carp::Pivots pp_ep0(opts_.nranks+1);
    GetPerfectPivots(0, pp_ep0,  pvtcnt_vec_[1], parallel);

    for (int ep = 0; ep < num_eps; ep++) {
      carp::OrderedBins bins(opts_.nranks);
      pp_ep0.InstallInOrderedBins(&bins);
      bins.ZeroWeights();
      ReadEpochIntoBins(ep, bins, parallel);

      double load_std = bins.PrintNormStd();
      printf("--------------\n");
      logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, ep, load_std);
    }
  }

  void RunSuiteEpXSub1PP(const int num_eps, bool parallel) {
    runtype = "epxsub1pp";

    for (int ep = 1; ep < num_eps; ep++) {
      carp::Pivots pp_epXsub1(opts_.nranks+1);
      GetPerfectPivots(ep - 1, pp_epXsub1,  pvtcnt_vec_[1], parallel);

      carp::OrderedBins bins(opts_.nranks);
      pp_epXsub1.InstallInOrderedBins(&bins);
      bins.ZeroWeights();
      ReadEpochIntoBins(ep, bins, parallel);

      double load_std = bins.PrintNormStd();
      printf("--------------\n");
      logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, ep, load_std);
    }
  }

  void RunSuiteEpXPP(const int num_eps, bool parallel) {
    runtype = "epxpp";

    for (int ep = 0; ep < num_eps; ep++) {
      carp::Pivots pp_epX(opts_.nranks+1);
      GetPerfectPivots(ep, pp_epX,  pvtcnt_vec_[1], parallel);

      carp::OrderedBins bins(opts_.nranks);
      pp_epX.InstallInOrderedBins(&bins);
      bins.ZeroWeights();
      ReadEpochIntoBins(ep, bins, parallel);

      double load_std = bins.PrintNormStd();
      printf("--------------\n");
      logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, ep, load_std);
    }
  }

 private:
  void GetOobPivots(int epoch, carp::Pivots& merged_pivots, int num_pivots,
                    bool parallel) {
    std::vector<carp::Pivots> pivots(opts_.nranks, carp::Pivots(num_pivots));

    if (parallel) {
      flog(LOG_ERRO, "Not implemented");
      ABORT("NOT IMPLEMENTED!!");
    } else {
      for (unsigned int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->GetOobPivots(epoch, &pivots[r]);
        flog(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
      }
    }

    carp::PivotAggregator aggr(pvtcnt_vec_);
    aggr.AggregatePivots(pivots, merged_pivots);

    flog(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void GetPerfectPivots(int epoch, carp::Pivots& merged_pivots,
                        size_t in_pivot_count, bool parallel) {
    std::vector<carp::Pivots> pivots(opts_.nranks,
                                     carp::Pivots(in_pivot_count));
    if (parallel) {
      parallel_processor_.GetPerfectPivotsParallel(epoch, ranks_, pivots);
    } else {
      for (unsigned int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->GetPerfectPivots(epoch, &pivots[r]);
        flog(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
      }
    }

    carp::PivotAggregator aggr(pvtcnt_vec_);
    aggr.AggregatePivots(pivots, merged_pivots);

    flog(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void ReadEpochIntoBins(int epoch, carp::OrderedBins& merged_bins,
                         bool parallel) {
    std::vector<carp::OrderedBins> bins(merged_bins.Size(), merged_bins);
    if (parallel) {
      parallel_processor_.ReadEpochIntoBinsParallel(epoch, ranks_, bins);
    } else {
      for (unsigned int r = 0; r < opts_.nranks; r++) {
        ranks_[r]->ReadEpochIntoBins(epoch, &bins[r]);
        flog(LOG_INFO, "%s\n", bins[r].ToString().c_str());
      }
    }

    merged_bins.Reset();
    for (auto bin : bins) {
      merged_bins = merged_bins + bin;
    }

    flog(LOG_INFO, "%s\n", merged_bins.ToString().c_str());
  }

  void InitAllRanks() {
    assert(ranks_.size() == 0);
    for (unsigned int r = 0; r < opts_.nranks; r++) {
      ranks_.push_back(new carp::Rank(r, opts_, tr_));
    }
  }

  void DestroyAllRanks() {
    assert(ranks_.size() == opts_.nranks);
    for (unsigned int r = 0; r < opts_.nranks; r++) {
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
