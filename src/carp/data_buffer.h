#pragma once

#include "carp/rtp_internal.h"
#include "range_common.h"

namespace pdlfs {
class DataBuffer {
 private:
  /* This simple storage format has 2*512KB of theoretical
   * footprint. (2* 4 * 128 * 256 * 4B). But no overhead will
   * be incurred for ranks that aren't actually using those
   * stages. (Virtual Memory ftw)
   */
  double data_store_[2][STAGES_MAX + 1][FANOUT_MAX][CARP_MAXPIVOTS];
  double data_weights_[2][STAGES_MAX + 1][FANOUT_MAX];
  int data_len_[2][STAGES_MAX + 1];

  int num_pivots_[STAGES_MAX + 1];
  int cur_store_idx_;

 public:
  //
  // DataBuffer constructor
  // num_pivots contains the expected pivot count for each stage
  //
  DataBuffer(const int num_pivots[STAGES_MAX + 1]);

  //
  // StoreData: Store pivots for the current round
  // isnext is true if data is for the next round, false o/w
  //
  int StoreData(int stage, const double* pivot_data, int dlen,
                double pivot_weight, bool isnext);

  //
  // Get number of pivot sets for each stage. This count will include invalid
  // pivots, if any, and can be used to check if the expected number of pivots
  // have been received.
  //
  // set isnext to true if request is for the next round, false o/w
  //
  int GetNumItems(int stage, bool isnext);

  //
  // Clear all data for current round, set next round data as cur. Returns err/0
  //
  int AdvanceRound();

  //
  // Get pivot weight for current stage. This will filter out invalid pivots
  // therefore it may be a smaller number than the total pivot set for the stage
  //
  int GetPivotWeights(int stage, std::vector<double>& weights);

  //
  // Get pivots for stage as rbvec items. This will filter out invalid pivots
  //
  int LoadIntoRbvec(int stage, std::vector<rb_item_t>& rbvec);

  //
  // Clear ALL data (both current round and next). Use with caution.
  //
  int ClearAllData();
};
}  // namespace pdlfs
