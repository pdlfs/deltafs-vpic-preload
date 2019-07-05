#include <sys/time.h>
#include "workload_generator.h"

namespace rangeutils {

static void rand_seed() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  srand(tv.tv_usec);
}

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

  rand_seed();

  _seq_cur_bin = 0;

  bin_width = (range_end - range_start) / num_bins;
  bin_total = 0;

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

  if (num_ranks < 2) return;

  assert(queries_total % num_ranks == 0);

  queries_left = queries_total / num_ranks;
  adjust_queries();
}

void WorkloadGenerator::adjust_queries() {
  switch (wp) {
    case WorkloadPattern::WP_SEQUENTIAL:
      adjust_queries_sequential();
      break;
    case WorkloadPattern::WP_RANDOM:
      adjust_queries_random();
      break;
    case WorkloadPattern::WP_RANK_SEQUENTIAL:
      throw std::invalid_argument("WP_RANK_SEQUENTIAL is not implemented");
      break;
  }

  return;
}

void WorkloadGenerator::adjust_queries_sequential() {
  _debug_print_bins("Sequential Before: ");
  int queries_per_rank = queries_total / num_ranks;

  bin_total = 0;

  for (int bidx = 0; bidx < num_bins; bidx++) {
    bin_total += bin_weights[bidx];
  }

  for (int bidx = 0; bidx < num_bins; bidx++) {
    bin_emits_left[bidx] =
        roundf(bin_weights[bidx] / bin_total * queries_per_rank);

    bin_total -= bin_weights[bidx];
    queries_per_rank -= bin_emits_left[bidx];
  }

  assert(queries_per_rank == 0);
  _debug_print_bins("Sequential After: ");
}

void WorkloadGenerator::adjust_queries_random() {
  _debug_print_bins("Random Before: ");
  int queries_to_adjust = queries_total - queries_left;

  int bidx = 0;

  while (queries_to_adjust) {
    bidx = rand() % num_bins;
    if (bin_emits_left[bidx] > 0) {
      bin_emits_left[bidx]--;
      queries_to_adjust--;
    }

    assert(bidx < num_bins);
    assert(bidx > 0);
  }
  _debug_print_bins("Random After: ");
}

void WorkloadGenerator::adjust_queries_rank_sequential() {
  int queries_to_adjust = queries_total - queries_left;

  int queries_before = my_rank * queries_total / num_ranks;
  int queries_to_preserve = queries_total / num_ranks;
  int queries_after = (num_ranks - my_rank - 1) * queries_total / num_ranks;

  assert(queries_before + queries_after == queries_to_adjust);

  int pos_flag = -1;  // -1 is before, 0 is in, 1 is after

  for (int bidx = 0; bidx < num_bins; bidx++) {
    if (pos_flag < 0) {
      assert(queries_before >= 0);

      int cur_reduction = min(queries_before, bin_emits_left[bidx]);
      bin_emits_left[bidx] -= cur_reduction;
      assert(bin_emits_left[bidx] >= 0);
      queries_before -= cur_reduction;
      queries_to_adjust -= cur_reduction;

      if (queries_before == 0) pos_flag = 0;
    }

    if (pos_flag == 0) {
      assert(queries_before == 0);
      assert(queries_after == queries_to_adjust);

      int cur_preservation = min(queries_to_preserve, bin_emits_left[bidx]);
      queries_to_preserve -= cur_preservation;
      if (queries_to_preserve == 0) {
        if (bin_emits_left[bidx] > cur_preservation) {
          queries_after -= (bin_emits_left[bidx] - cur_preservation);
          queries_to_adjust -= (bin_emits_left[bidx] - cur_preservation);
          continue;
        }
        pos_flag = 1;
      }
    }

    if (pos_flag > 0) {
      assert(queries_before == 0);
      assert(queries_to_preserve == 0);
      assert(queries_after >= 0);

      queries_after -= bin_emits_left[bidx];
      queries_to_adjust -= bin_emits_left[bidx];

      bin_emits_left[bidx] = 0;
    }
  }

  assert(queries_before == 0);
  assert(queries_to_preserve == 0);
  assert(queries_after == 0);
  assert(queries_to_adjust == 0);

  int new_total = 0;

  for (int bidx = 0; bidx < num_bins; bidx++) {
    new_total += bin_emits_left[bidx];
  }

  assert(new_total == queries_left);
}

int WorkloadGenerator::next(float &value) {
  if (queries_left == 0) return -1;
  switch (wp) {
    case WorkloadPattern::WP_SEQUENTIAL:
      return next_sequential(value);
    case WorkloadPattern::WP_RANDOM:
      return next_random(value);
    case WorkloadPattern::WP_RANK_SEQUENTIAL:
      throw std::invalid_argument("WP_RANK_SEQUENTIAL is not implemented");
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

void WorkloadGenerator::_debug_print_bins(const char *leadstr) {
  fprintf(stderr, "%s", leadstr);
  for (int i = 0; i < num_bins; i++) {
    fprintf(stderr, "%d ", bin_emits_left[i]);
  }
  fprintf(stderr, "\n");
}
}  // namespace rangeutils
