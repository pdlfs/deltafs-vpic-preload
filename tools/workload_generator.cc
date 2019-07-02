#include "workload_generator.h"

namespace rangeutils {

WorkloadGenerator::WorkloadGenerator(float bins[], int num_bins,
                                     float range_start, float range_end,
                                     int num_queries, WorkloadPattern wp,
                                     int my_rank, int num_ranks)
    : num_bins(num_bins),
      range_start(range_start),
      range_end(range_end),
      queries_total(num_queries),
      queries_left(num_queries),
      wp(wp),
      my_rank(my_rank),
      num_ranks(num_ranks) {
  assert(num_bins < MAX_BINS);

  srand(time(NULL));

  _seq_cur_bin = 0;

  bin_width = (range_end - range_start) / num_bins;
  float bin_total = 0;

  for (int bidx = 0; bidx < num_bins; bidx++) {
    bin_weights[bidx] = bins[bidx];
    bin_starts[bidx] = range_start + bidx * bin_width;
    bin_total += bins[bidx];
  }

  for (int bidx = 0; bidx < num_bins; bidx++) {
    bin_emits_left[bidx] = roundf(bin_weights[bidx] / bin_total * num_queries);

    bin_total -= bin_weights[bidx];
    num_queries -= bin_emits_left[bidx];
  }
}

int WorkloadGenerator::next(float &value) {
  if (queries_left == 0) return -1;
  switch (wp) {
    case WorkloadPattern::WP_SEQUENTIAL:
      return next_sequential(value);
    case WorkloadPattern::WP_RANDOM:
      return next_random(value);
  }
}

int WorkloadGenerator::next_sequential(float &value) {
  while (bin_emits_left[_seq_cur_bin] == 0) {
    _seq_cur_bin++;
  }

  bin_emits_left[_seq_cur_bin]--;

  value = static_cast<float>(rand()) / static_cast<float>(RAND_MAX) * bin_width;
  value += bin_starts[_seq_cur_bin];
  queries_left--;

  return 0;
}

int WorkloadGenerator::next_random(float &value) {
  int bin = rand() % num_bins;

  do {
    bin = rand() % num_bins;
  } while (bin_emits_left[bin] == 0);

  bin_emits_left[bin]--;

  value = static_cast<float>(rand()) / static_cast<float>(RAND_MAX) * bin_width;
  value += bin_starts[bin];

  queries_left--;
  return 0;
}
}  // namespace rangeutils
