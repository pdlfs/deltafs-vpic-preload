#pragma once

#include <numeric>
#include <vector>

#include "range_common.h"

#define RANGE_PARANOID_CHECKS 1
#define RANGE_EPSILON 1e-5

void pivot_union(std::vector<bounds_t> bounds,
                 std::vector<double>& unified_bins,
                 std::vector<float>& unified_bin_counts,
                 std::vector<double>& rank_bin_weights, int num_ranks);

int get_particle_count(int total_ranks, int total_bins, int par_per_bin);

int resample_bins_irregular(const std::vector<double>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<double>& samples, double& sample_weight,
                            int nsamples);
