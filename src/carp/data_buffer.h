#pragma once

#include "range_common.h"
#include "carp/rtp_internal.h"

namespace pdlfs {
class DataBuffer {
 private:
  /* This simple storage format has 2*512KB of theoretical
   * footprint. (2* 4 * 128 * 256 * 4B). But no overhead will
   * be incurred for ranks that aren't actually using those
   * stages. (Virtual Memory ftw)
   */
  double data_store_[2][STAGES_MAX + 1][FANOUT_MAX][kMaxPivots];
  double data_widths_[2][STAGES_MAX + 1][FANOUT_MAX];
  int data_len_[2][STAGES_MAX + 1];

  int num_pivots_[STAGES_MAX + 1];
  int cur_store_idx_;

 public:
  /**
   * @brief Constructor
   *
   * @param num_pivots Expected pivot_count for each stage
   */
  DataBuffer(const int num_pivots[STAGES_MAX + 1]);

  /**
   * @brief Store pivots for the current round
   *
   * @param stage
   * @param data
   * @param dlen
   * @param pivot_width
   * @param isnext true if data is for the next round, false o/w
   *
   * @return errno if < 0, else num_items in store for the stage
   */
  int StoreData(int stage, const double* pivot_data, int dlen, double pivot_width,
                 bool isnext);

  /**
   * @brief
   *
   * @param stage
   * @param isnext true if data is for the next round, false o/w
   *
   * @return
   */
  int GetNumItems(int stage, bool isnext);

  /**
   * @brief Clear all data for current round, set next round data as cur
   *
   * @return errno or 0
   */
  int AdvanceRound();

  /**
   * @brief A somewhat hacky way to get pivot width arrays withouy copying
   *
   * @param stage
   *
   * @return
   */
  int GetPivotWidths(int stage, std::vector<double>& widths);

  /**
   * @brief
   *
   * @param stage
   * @param rbvec
   *
   * @return
   */
  int LoadIntoRbvec(int stage, std::vector<rb_item_t>& rbvec);

  /**
   * @brief Clear ALL data (both current round and next). Use with caution.
   *
   * @return
   */
  int ClearAllData();
};
}  // namespace pdlfs
