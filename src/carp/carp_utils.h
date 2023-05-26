#pragma once

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "carp/oob_buffer.h"
#include "msgfmt.h"
#include "range_constants.h"

namespace pdlfs {
namespace carp {

/* forward declaration */
class Carp;

class PivotUtils {
 public:
  /**
   * @brief Calculate pivots from the current pivot_ctx state.
   * This also modifies OOB buffers (sorts them), but their order shouldn't
   * be relied upon anyway.
   *
   * SAFE version computes "token pivots" in case no mass is there to
   * actually compute pivots. This ensures that merging calculations
   * do not fail.
   *
   * XXX: a more semantically appropriate fix would be to define addition
   * and resampling for zero-pivots
   *
   * @param carp pivot context
   *
   * @return
   */
  static int CalculatePivots(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                             size_t num_pivots);

  /**
   * @brief Update pivots after renegotiation. This *does not* manipulate the
   * state manager. State manager needs to be directly controlled by the
   * renegotiation provider because of synchronization implications
   *
   * @param carp
   * @param pivots
   * @return
   */
  static int UpdatePivots(Carp* carp, Pivots* pivots);

  static int EncodePivots(void* buf, int buf_sz, int round_num, int stage_num,
                          int sender_id, Pivots* pivots, bool bcast) {
    double* pivots_arr = pivots->pivots_.data();
    double pivot_weight = pivots->weight_;
    int num_pivots = pivots->pivots_.size();

    return msgfmt_encode_rtp_pivots(buf, buf_sz, round_num, stage_num,
                                    sender_id, pivots_arr, pivot_weight,
                                    num_pivots, bcast);
  }

  static void DecodePivots(void* buf, int buf_sz, int* round_num,
                           int* stage_num, int* sender_id, Pivots* pivots,
                           bool bcast) {
    int num_pivots_from_buf;
    double* pvts_from_buf;
    msgfmt_decode_rtp_pivots(buf, buf_sz, round_num, stage_num, sender_id,
                             &pvts_from_buf, &pivots->weight_,
                             &num_pivots_from_buf, bcast);
    int pvtvecsz = pivots->Size();
    assert(pvtvecsz == num_pivots_from_buf);
    std::copy(pvts_from_buf, pvts_from_buf + num_pivots_from_buf,
              pivots->pivots_.begin());
  }

 private:
  static int CalculatePivotsFromOob(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                                    size_t num_pivots);

  static int CalculatePivotsFromAll(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                                    size_t num_pivots);

  static int GetRangeBounds(PivotCalcCtx* pvt_ctx, float& range_start,
                            float& range_end);

  static double WeightedAverage(double a, double b, double frac);

  friend class RangeUtilsTest;
};
}  // namespace carp
}  // namespace pdlfs
