#pragma once

#include <numeric>
#include <vector>

#include "range_common.h"

#define RANGE_PARANOID_CHECKS 1
#define RANGE_EPSILON 1e-5

void load_bins_into_rbvec(std::vector<double>& bins,
                          std::vector<rb_item_t>& rbvec, int num_bins,
                          int num_ranks, int bins_per_rank);

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<double>& unified_bins,
                 std::vector<float>& unified_bin_counts,
                 std::vector<double>& rank_bin_widths, int num_ranks);

int get_particle_count(int total_ranks, int total_bins, int par_per_bin);

int resample_bins_irregular(const std::vector<double>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<double>& samples, double& sample_width,
                            int nsamples);

void repartition_bin_counts(std::vector<double>& old_bins,
                            std::vector<float>& old_bin_counts,
                            std::vector<double>& new_bins,
                            std::vector<float>& new_bin_counts);

void assert_monotically_increasing(double* array, int array_sz);
void assert_monotically_decreasing(double* array, int array_sz);
