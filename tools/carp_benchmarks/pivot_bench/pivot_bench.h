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
#include "trace_reader.h"

namespace pdlfs {
class PivotBench {
 public:
  PivotBench(PivotBenchOpts& opts)
      : opts_(opts),
        pvtcnt_vec_(STAGES_MAX + 1, opts_.pvtcnt),
        logger_(opts.log_file),
        parallel_processor_(opts_) {}

  Status Run() {
    Status s = Status::OK();
    TraceReader tr(opts_);

    size_t num_ep;
    tr.DiscoverEpochs(num_ep);

    s = RunWithInitialPivots(tr, num_ep);
    if (!s.ok()) return s;
    s = RunWithRespectivePivots(tr, num_ep);
    if (!s.ok()) return s;

    return s;
  }

  Status RunWithInitialPivots(TraceReader& tr, size_t num_ep) {
    runtype = "initpvt";
    Status s = Status::OK();
    carp::Pivots oob_pivots;
    //    GetOobPivots(tr, 0, oob_pivots);
    GetOobPivotsParallel(tr, 0, oob_pivots);

    for (size_t ep_idx = 0; ep_idx < num_ep; ep_idx++) {
      //      AnalyzePivotsAgainstEpoch(tr, oob_pivots, ep_idx);
      AnalyzePivotsAgainstEpochParallel(tr, oob_pivots, ep_idx);
    }

    return s;
  }

  Status RunWithRespectivePivots(TraceReader& tr, size_t num_ep) {
    runtype = "ownpvt";
    Status s = Status::OK();
    for (size_t ep_idx = 0; ep_idx < num_ep; ep_idx++) {
      carp::Pivots oob_pivots;
      GetOobPivotsParallel(tr, ep_idx, oob_pivots);
      AnalyzePivotsAgainstEpochParallel(tr, oob_pivots, ep_idx);
    }
    return s;
  }

 private:
  void AnalyzePivotsAgainstEpoch(TraceReader& tr, carp::Pivots& oob_pivots,
                                 int epoch) {
    carp::OrderedBins bins(opts_.nranks);
    bins.UpdateFromPivots(oob_pivots);
    tr.ReadAllRanksIntoBins(epoch, bins);
    logf(LOG_INFO, "%s", bins.ToString().c_str());
    double load_std = bins.PrintNormStd();
    printf("--------------\n");
    logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, epoch, load_std);
  }

  void GetOobPivots(TraceReader& tr, int epoch, carp::Pivots& merged_pivots) {
    std::vector<carp::Pivots> pivots(opts_.nranks);

    for (int r = 0; r < opts_.nranks; r++) {
      carp::PivotCalcCtx pvt_ctx;
      tr.ReadRankIntoPivotCtx(epoch, r, &pvt_ctx, opts_.oobsz);
      carp::PivotUtils::CalculatePivots(&pvt_ctx, &pivots[r],
                                        /* num_pivots */ pvtcnt_vec_[1]);
      logf(LOG_INFO, "%s\n", pivots[r].ToString().c_str());
    }

    merged_pivots.FillZeros();
    carp::PivotAggregator aggr(pvtcnt_vec_);
    //    aggr.AggregatePivotsRoot(pivots, merged_pivots, 1, opts_.nranks);
    aggr.AggregatePivots(pivots, merged_pivots);

    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void GetOobPivotsParallel(TraceReader& tr, int epoch,
                            carp::Pivots& merged_pivots) {
    std::vector<carp::Pivots> pivots(opts_.nranks);
    parallel_processor_.ComputeOobPivotsParallel(tr, epoch, pivots);

    merged_pivots.FillZeros();
    carp::PivotAggregator aggr(pvtcnt_vec_);
    aggr.AggregatePivots(pivots, merged_pivots);
    logf(LOG_INFO, "%s\n", merged_pivots.ToString().c_str());
  }

  void AnalyzePivotsAgainstEpochParallel(TraceReader& tr,
                                         carp::Pivots& oob_pivots, int epoch) {
    std::vector<carp::OrderedBins> bins(opts_.nranks, opts_.nranks);

    for (int r = 0; r < opts_.nranks; r++) {
      bins[r].UpdateFromPivots(oob_pivots);
    }

    parallel_processor_.ComputeBinsParallel(tr, epoch, bins);
    carp::OrderedBins merged_bins(opts_.nranks);
    for (int r = 0; r < opts_.nranks; r++) {
      merged_bins = merged_bins + bins[r];
    }

    logf(LOG_INFO, "%s", merged_bins.ToString().c_str());
    double load_std = merged_bins.PrintNormStd();
    printf("--------------\n");
    logger_.LogData(runtype, opts_.nranks, opts_.pvtcnt, epoch, load_std);
    printf("--------------\n");
  }

  std::string runtype;
  const PivotBenchOpts opts_;
  const std::vector<int> pvtcnt_vec_;
  PivotLogger logger_;
  ParallelProcessor parallel_processor_;
};
}  // namespace pdlfs