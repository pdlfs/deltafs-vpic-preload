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

namespace {
void SortAndCopyWithoutDuplicates(std::vector<float>& in,
                                  std::vector<float>& out) {
  if (in.size() == 0) return;

  std::sort(in.begin(), in.end());

  out.push_back(in[0]);
  float last_copied = in[0];

  for (size_t idx = 1; idx < in.size(); idx++) {
    float cur = in[idx];
    float prev = in[idx - 1];

    assert(cur >= prev);
    assert(cur >= last_copied);

    if (cur - last_copied > 1e-7) {
      // arbitrary comparison threshold
      out.push_back(cur);
      last_copied = cur;
    }
  }

  size_t in_sz = in.size(), out_sz = out.size();
  if (in_sz != out_sz) {
    logf(LOG_WARN,
         "[OOB::RemoveDuplicates] Some elements dropped (orig: %zu, "
         "dupl: %zu)",
         in_sz, out_sz);
  }
}
}  // namespace

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
    GetOobPivots(tr, 0, oob_pivots);

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
    pvt_ctx.first_block = false;
    pvt_ctx.bins = &bins;
    pvt_ctx.range = bins.GetRange();

    std::vector<float> oobl_tmp;
    std::vector<float> oobr_tmp;

    for (int r = 0; r < opts_.nranks; r++) {
      tr.UpdateCtxWithRank(epoch, r, &pvt_ctx, oobl_tmp, oobr_tmp);
    }

    ::SortAndCopyWithoutDuplicates(oobl_tmp, pvt_ctx.oob_left);
    ::SortAndCopyWithoutDuplicates(oobr_tmp, pvt_ctx.oob_right);

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
  PivotLogger logger_;
};
}  // namespace pdlfs