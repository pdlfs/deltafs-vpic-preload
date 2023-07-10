#pragma once

#include <numeric>
#include <vector>

#include "range_common.h"
#include "binhistogram.h"

#define RANGE_PARANOID_CHECKS 1
#define RANGE_EPSILON 1e-5

void pivot_union(std::vector<bounds_t> bounds,
                 std::vector<double>& weights,
                 size_t ninputs,
                 pdlfs::carp::BinHistogram<double,float>& mergedhist);

int get_particle_count(int total_ranks, int total_bins, int par_per_bin);
