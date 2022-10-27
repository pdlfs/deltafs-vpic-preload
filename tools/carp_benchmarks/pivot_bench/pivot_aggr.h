//
// Created by Ankush J on 10/24/22.
//

#pragma once

#include "carp/data_buffer.h"
#include "carp/rtp.h"
#include "carp/rtp_internal.h"
#include "pivot_common.h"

namespace pdlfs {
namespace carp {
class PivotAggregator {
  typedef std::vector<Pivots> PvtVec;

 public:
  PivotAggregator(const int num_pivots[STAGES_MAX + 1]) {
    for (int sidx = 1; sidx <= STAGES_MAX; sidx++) {
      this->num_pivots_[sidx] = num_pivots[sidx];
    }
  }

  void AggregatePivots(std::vector<Pivots>& pivots, Pivots& merged_pivots) {
    const int pvtcnt = 256;

    int nranks = pivots.size();
    int fanout_arr[STAGES_MAX + 1];
    carp::RTPUtil::ComputeTreeFanout(nranks, fanout_arr);

    std::vector<PvtVec> chunked_pvts_s1, chunked_pvts_s2, chunked_pvts_s3;
    PvtVec merged_pvts_s1, merged_pvts_s2, merged_pvts_s3;

    ChunkPivotsStage(pivots, chunked_pvts_s1, fanout_arr[1]);
    AggregatePivotsStage(chunked_pvts_s1, merged_pvts_s1, 1, pvtcnt);
    ChunkPivotsStage(merged_pvts_s1, chunked_pvts_s2, fanout_arr[2]);
    AggregatePivotsStage(chunked_pvts_s2, merged_pvts_s2, 2, pvtcnt);
    assert(merged_pvts_s2.size() == fanout_arr[3]);
    AggregatePivotsRoot(merged_pvts_s2, merged_pivots, 3, nranks + 1);
  }

// private:
  void ChunkPivotsStage(std::vector<Pivots>& pivots,
                        std::vector<PvtVec>& chunked_pivots, int nchunks) {
    int pvtsz = pivots.size();
    if (pvtsz % nchunks) {
      logf(LOG_ERRO, "[ChunkPivots] pvtsz must be a multiple of nchunks (%d/%d",
           pvtsz, nchunks);
    }
    int chunksz = pvtsz / nchunks;

    PvtVec::const_iterator it = pivots.cbegin();
    while (it != pivots.cend()) {
      // copy chunksz elements starting from pidx to chunked_pivots
      chunked_pivots.push_back({it, it + chunksz});
      it += chunksz;
    }
  }

  void AggregatePivotsStage(std::vector<PvtVec>& all_pivots,
                            std::vector<Pivots>& all_merged_pivots, int stage,
                            int num_out) {
    size_t allpvtsz = all_pivots.size();
    all_merged_pivots.resize(allpvtsz);

//    for (size_t pidx = 0; pidx < allpvtsz; pidx++) {
//      AggregatePivotsRoot(all_pivots[pidx], all_merged_pivots[pidx], stage,
//                          num_out);
//    }
//    Pivots tmp;
//    AggregatePivotsRoot(all_pivots[0], tmp, 1, 256);
  }

  void AggregatePivotsRoot(std::vector<Pivots>& pivots, Pivots& merged_pivots,
                           int stage, int num_out) {
    return;
    DataBuffer dbuf(num_pivots_);

    for (size_t pidx = 0; pidx < pivots.size(); pidx++) {
      BufferPivots(dbuf, stage, pivots[pidx]);
    }

    std::vector<rb_item_t> rbvec;
    std::vector<double> unified_bins;
    std::vector<float> unified_bin_counts;
    std::vector<double> pivot_widths;

    dbuf.LoadIntoRbvec(stage, rbvec);
    dbuf.GetPivotWidths(stage, pivot_widths);
    pivot_union(rbvec, unified_bins, unified_bin_counts, pivot_widths,
                pivots.size());

    std::vector<double> pvt_tmp(num_out, 0);
    double pvtwidth_tmp;

    resample_bins_irregular(unified_bins, unified_bin_counts, pvt_tmp,
                            pvtwidth_tmp, num_out);

    merged_pivots.LoadPivots(pvt_tmp, pvtwidth_tmp);
  }

  void BufferPivots(DataBuffer& dbuf, int stage, Pivots& p) {
    const double* pvt_data = p.GetPivotData();
    double pvt_width = p.GetPivotWidth();
    int pvt_count = p.Size();

    dbuf.StoreData(stage, pvt_data, pvt_count, pvt_width, /* isnext */ false);
  }

  int num_pivots_[STAGES_MAX + 1];
};
}  // namespace carp
}  // namespace pdlfs
