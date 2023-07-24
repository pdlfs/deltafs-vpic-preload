#pragma once

#include <vector>

#include "binhistogram.h"
#include "pivot_buffer.h"   /* for bounds_t */

void pivot_union(std::vector<bounds_t> bounds,
                 std::vector<double>& weights,
                 size_t ninputs,
                 pdlfs::carp::BinHistogram<double,float>& mergedhist);
