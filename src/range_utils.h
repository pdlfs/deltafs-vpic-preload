#pragma once

#include <numeric>
#include <vector>

#define RANGE_PARANOID_CHECKS 1
#define RANGE_DEBUG 1
#define RANGE_EPSILON 1e-5
#define fprintf \
  if (RANGE_DEBUG) fprintf
#define _fprintf fprintf

typedef struct rb_item {
  int rank;
  float bin_val;
  float bin_other;
  bool is_start;
} rb_item_t;  // rank-bin item

void pivot_union(std::vector<rb_item_t> rb_items,
                 std::vector<float> unified_bins,
                 std::vector<float> unified_bin_counts, int num_ranks,
                 int part_per_rank);

int get_particle_count(int total_ranks, int total_bins, int par_per_bin);

int resample_bins_irregular(const std::vector<float>& bins,
                            const std::vector<float>& bin_counts,
                            std::vector<float>& samples, int nsamples,
                            int nparticles);
