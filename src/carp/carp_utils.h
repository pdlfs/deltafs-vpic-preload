#pragma once

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#include <string>
#include <vector>

#include "msgfmt.h"
#include "oob_buffer.h"
#include "pivots.h"
#include "range_constants.h"

namespace pdlfs {
namespace carp {

/* forward declaration */
class Carp;

class PivotUtils {
 public:
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
    pivots->is_set_ = true;   // XXX
  }

 private:
  static double WeightedAverage(double a, double b, double frac);

  friend class RangeUtilsTest;
};
}  // namespace carp
}  // namespace pdlfs
