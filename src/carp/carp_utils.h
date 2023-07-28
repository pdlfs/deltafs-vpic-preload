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
  static int EncodePivots(void* buf, int buf_sz, int round_num, int stage_num,
                          int sender_id, Pivots* pivots, bool bcast) {
    double* pivots_arr = pivots->pivots_.data();
    double pivot_weight = pivots->weight_;
    int num_pivots = pivots->pivots_.size();

    return msgfmt_encode_rtp_pivots(buf, buf_sz, round_num, stage_num,
                                    sender_id, pivots_arr, pivot_weight,
                                    num_pivots, bcast);
  }

 private:

  friend class RangeUtilsTest;
};
}  // namespace carp
}  // namespace pdlfs
