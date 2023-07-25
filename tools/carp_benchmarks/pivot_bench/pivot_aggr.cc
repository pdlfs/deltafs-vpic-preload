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
                                          size_t num_out) {
  PivotBuffer dbuf(pvtcnt_vec_.data());
  for (size_t pidx = 0; pidx < pivots.size(); pidx++) {
    BufferPivots(dbuf, stage, pivots[pidx]);
  }

  std::vector<bounds_t> boundsv;
  std::vector<double> unified_bins;
  std::vector<float> unified_bin_counts;
  std::vector<double> pivot_weights;

  dbuf.LoadBounds(stage, boundsv);
  dbuf.GetPivotWeights(stage, pivot_weights);
  std::vector<double> pvt_tmp(num_out, 0);
  double pvtweight_tmp;

  int npivotmsgs = pivots.size();
  int npchunk = dbuf.PivotCount(stage) - 1;
  size_t maxbins = (npchunk * npivotmsgs) + (npivotmsgs - 1);
  BinHistogram<double,float> mergedhist(maxbins);

  pivot_union(boundsv, pivot_weights, npivotmsgs, mergedhist);
  assert(mergedhist.Size() <= maxbins);  /* verify maxbins was big enough */

  int output_npchunk = num_out - 1;
  BinHistConsumer<double,float> cons(&mergedhist);
  for (size_t lcv = 0 ; lcv < num_out ; lcv++) {
    if (lcv)
      cons.ConsumeWeightTo(cons.TotalWeight() *
                           ((output_npchunk - lcv) / (double)output_npchunk) );
    pvt_tmp[lcv] = cons.CurrentValue();
  }

  pvtweight_tmp =  mergedhist.GetTotalWeight() / (double) output_npchunk;

  merged_pivots.LoadPivots(pvt_tmp.data(), pvt_tmp.size(), pvtweight_tmp);
  flog(LOG_INFO, "[AggregatePivots] Stage: %d, %s\n", stage,
       merged_pivots.ToString().c_str());
}

void PivotAggregator::BufferPivots(PivotBuffer& dbuf, int stage, Pivots& p) {
  double pvt_weight = p.PivotWeight();
  size_t pvt_count = p.Size();
  double pvt_data[pvt_count];

  for (size_t idx = 0; idx < pvt_count; idx++) {
    pvt_data[idx] = p[idx];
  }

  dbuf.StoreData(stage, pvt_data, pvt_count, pvt_weight, /* isnext */ false);
}
}  // namespace carp
}  // namespace pdlfs
