//
// Created by Ankush J on 10/26/22.
//

#include "pivot_aggr.h"

namespace pdlfs {
namespace carp {
void PivotAggregator::AggregatePivots(std::vector<Pivots>& pivots,
                                      Pivots& merged_pivots) {
  int nranks = pivots.size();
  int fanout_arr[STAGES_MAX + 1];
  carp::RTPUtil::ComputeTreeFanout(nranks, fanout_arr);

  std::vector<PvtVec> chunked_pvts_s1, chunked_pvts_s2, chunked_pvts_s3;
  PvtVec merged_pvts_s1, merged_pvts_s2, merged_pvts_s3;

  /* Example: nranks = 256. Fanout = 8/8/4
   * merge 256 -> 32 -> 4 -> 1
   * nchunks_s1 = 8*4
   * nchunks_s2 = 4
   * nchunks_s3 = 1 always
   */

  int nchunks_s1 = fanout_arr[2] * fanout_arr[3];
  int nchunks_s2 = fanout_arr[3];
  /* int nchunks_s3 = 1; we do a single merge manually */

  // pivots=256, chunked_pvts=32, nchunks=32
  ChunkPivotsStage(pivots, chunked_pvts_s1, nchunks_s1);
  AggregatePivotsStage(chunked_pvts_s1, merged_pvts_s1, 1, pvtcnt_vec_[2]);

  // pivots=32, chunked_pvts=4, nchunks=4
  ChunkPivotsStage(merged_pvts_s1, chunked_pvts_s2, nchunks_s2);
  AggregatePivotsStage(chunked_pvts_s2, merged_pvts_s2, 2, pvtcnt_vec_[3]);

  assert(merged_pvts_s2.size() == (size_t)fanout_arr[3]);
  AggregatePivotsRoot(merged_pvts_s2, merged_pivots, 3, nranks + 1);
}

void PivotAggregator::ChunkPivotsStage(std::vector<Pivots>& pivots,
                                       std::vector<PvtVec>& chunked_pivots,
                                       int nchunks) {
  int pvtsz = pivots.size();
  if (pvtsz % nchunks) {
    flog(LOG_ERRO, "[ChunkPivots] pvtsz must be a multiple of nchunks (%d/%d",
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

void PivotAggregator::AggregatePivotsStage(
    std::vector<PvtVec>& all_pivots, std::vector<Pivots>& all_merged_pivots,
    int stage, int num_out) {
  size_t allpvtsz = all_pivots.size();
  all_merged_pivots.resize(allpvtsz);

  flog(LOG_INFO, "[AggregatePivots] Stage: %d, Merging %zu pivot sets\n", stage,
       allpvtsz);

  for (size_t pidx = 0; pidx < allpvtsz; pidx++) {
    flog(LOG_INFO,
         "[AggregatePivots] Stage: %d, Merging %zu pivot sets to %d pivots \n",
         stage, all_pivots[pidx].size(), num_out);

    AggregatePivotsRoot(all_pivots[pidx], all_merged_pivots[pidx], stage,
                        num_out);
  }
}

void PivotAggregator::AggregatePivotsRoot(std::vector<Pivots>& pivots,
                                          Pivots& merged_pivots, int stage,
                                          int num_out) {
  DataBuffer dbuf(pvtcnt_vec_.data());
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
  flog(LOG_INFO, "[AggregatePivots] Stage: %d, %s\n", stage,
       merged_pivots.ToString().c_str());
}

void PivotAggregator::BufferPivots(DataBuffer& dbuf, int stage, Pivots& p) {
  double pvt_width = p.PivotWidth();
  size_t pvt_count = p.Size();
  double pvt_data[pvt_count];

  for (size_t idx = 0; idx < pvt_count; idx++) {
    pvt_data[idx] = p[idx];
  }

  dbuf.StoreData(stage, pvt_data, pvt_count, pvt_width, /* isnext */ false);
}
}  // namespace carp
}  // namespace pdlfs
