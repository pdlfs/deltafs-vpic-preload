#pragma once

#include <vector>

#include "binhistogram.h"
#include "pivot_buffer.h"   /* for bounds_t */

namespace pdlfs {
namespace carp {

/*
 * pivot_union: takes a set of pivots loaded into a bounds vector
 * (e.g. from the pivot_buffer) and unions them together into a
 * merged binhistogram.  RTP uses this to generate a new set of
 * pivots based on the unioned/merged data.
 */
void pivot_union(std::vector<bounds_t>& bounds,
                 std::vector<double>& weights,
                 size_t ninputs,
                 pdlfs::carp::BinHistogram<double,float>& mergedhist);

}  // namespace carp
}  // namespace pdlfs
