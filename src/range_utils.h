#pragma once

#include <numeric>
#include <vector>

#define RANGE_PARANOID_CHECKS 1
#define RANGE_EPSILON 1e-5

typedef struct rb_item {
  int rank;
  float bin_val;
  float bin_other;
  bool is_start;
} rb_item_t;  // rank-bin item

void load_bins_into_rbvec(std::vector<float>& bins,
                          std::vector<rb_item_t>& rbvec, int num_bins,
                          int num_ranks, int bins_per_rank);

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<float>& unified_bins,
                 std::vector<float>& unified_bin_counts,
                 std::vector<float>& rank_bin_widths, int num_ranks);

int get_particle_count(int total_ranks, int total_bins, int par_per_bin);

int resample_bins_irregular(const std::vector<float>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<float>& samples, int nsamples);

void repartition_bin_counts(std::vector<float>& old_bins,
                            std::vector<float>& old_bin_counts,
                            std::vector<float>& new_bins,
                            std::vector<float>& new_bin_counts);
