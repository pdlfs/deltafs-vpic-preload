#pragma once

#include <numeric>
#include <vector>

#include "range_common.h"
#include "binhistogram.h"

void pivot_union(std::vector<bounds_t> bounds,
                 std::vector<double>& weights,
                 size_t ninputs,
                 pdlfs::carp::BinHistogram<double,float>& mergedhist);
