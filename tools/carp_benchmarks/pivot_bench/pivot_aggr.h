//
// Created by Ankush J on 10/24/22.
//

#pragma once

#include "carp/carp_containers.h"
#include "carp/data_buffer.h"
#include "carp/rtp.h"
#include "carp/rtp_internal.h"
#include "pivot_common.h"

namespace pdlfs {
namespace carp {
class PivotAggregator {
  typedef std::vector<Pivots> PvtVec;

 public:
  PivotAggregator(const std::vector<int>& pvtcnt_vec)
      : pvtcnt_vec_(pvtcnt_vec) {}

  void AggregatePivots(std::vector<Pivots>& pivots, Pivots& merged_pivots);

  void AggregatePivotsRoot(std::vector<Pivots>& pivots, Pivots& merged_pivots,
                           int stage, int num_out);

 private:
  void ChunkPivotsStage(std::vector<Pivots>& pivots,
                        std::vector<PvtVec>& chunked_pivots, int nchunks);

  void AggregatePivotsStage(std::vector<PvtVec>& all_pivots,
                            std::vector<Pivots>& all_merged_pivots, int stage,
                            int num_out);

  static void BufferPivots(DataBuffer& dbuf, int stage, Pivots& p);

  const std::vector<int> pvtcnt_vec_;
};
}  // namespace carp
}  // namespace pdlfs
