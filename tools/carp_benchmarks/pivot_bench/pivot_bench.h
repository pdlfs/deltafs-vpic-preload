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
#include "pivot_aggr.h"
#include "trace_reader.h"

namespace pdlfs {
class PivotBench {
 public:
  PivotBench(PivotBenchOpts& opts)
      : opts_(opts),
        pvtcnt_vec_(STAGES_MAX + 1, opts_.pvtcnt),
        logger_(opts.log_file) {}

  Status Run() {
    Status s = Status::OK();
    TraceReader tr(opts_);

    size_t num_ep;
    tr.DiscoverEpochs(num_ep);

    carp::Pivots oob_pivots;
//    GetOobPivots(tr, 0, oob_pivots);
    GetOobPivotsParallel(tr, 0, oob_pivots);

    for (size_t ep_idx = 0; ep_idx < num_ep; ep_idx++) {
      AnalyzePivotsAgainstEpoch(tr, oob_pivots, ep_idx);
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
    logger_.LogData(opts_.nranks, opts_.pvtcnt, epoch, load_std);
  }

  // TODO - doesn't work for some reason
  // Isn't accurate either (need to merge sets of pivots)
  void GetPerfectPivots(TraceReader& tr, int epoch,
                        carp::Pivots& perfect_pivots) {
    carp::Pivots oob_pivots;
    GetOobPivots(tr, epoch, oob_pivots);
    // Analyze OOB pivots against its own epoch
    AnalyzePivotsAgainstEpoch(tr, oob_pivots, epoch);

    carp::OrderedBins bins(opts_.nranks);
    bins.UpdateFromPivots(oob_pivots);
    carp::PivotCalcCtx pvt_ctx;
    pvt_ctx.SetBins(&bins);

    for (int r = 0; r < opts_.nranks; r++) {
      tr.ReadRankIntoPivotCtx(epoch, r, &pvt_ctx, -1);
    }

    perfect_pivots.FillZeros();
    carp::PivotUtils::CalculatePivots(&pvt_ctx, &perfect_pivots,
                                      opts_.nranks + 1);
    logf(LOG_INFO, "%s\n", perfect_pivots.ToString().c_str());
    //
    // Analyze perfect pivots against its own epoch
    AnalyzePivotsAgainstEpoch(tr, perfect_pivots, epoch);
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
                            carp::Pivots& merged_pivots);

  const PivotBenchOpts opts_;
  const std::vector<int> pvtcnt_vec_;
  PivotLogger logger_;
};
}  // namespace pdlfs

namespace {
struct OOBPivotTask {
  pdlfs::TraceReader* tr;
  int epoch;
  int rank;
  int oobsz;
  int num_pivots;
  pdlfs::carp::Pivots* pivots;
};

static void get_oob_pivots(void* args) {
  OOBPivotTask* task = (OOBPivotTask*)args;
  pdlfs::carp::PivotCalcCtx pvt_ctx;
  task->tr->ReadRankIntoPivotCtx(task->epoch, task->rank, &pvt_ctx,
                                 task->oobsz);
  pdlfs::carp::PivotUtils::CalculatePivots(&pvt_ctx, task->pivots,
                                           task->num_pivots);
}
}  // namespace