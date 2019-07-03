#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <stdexcept>

namespace rangeutils {

static const int MAX_BINS = 100;

static int min(int a, int b) {
  return a < b ? a : b;
}

enum class WorkloadPattern {
  /* All ranks produce all particles from the entire range sequentially
   */
  WP_SEQUENTIAL,
  /* All ranks produce particles from the entire range at random
   * The resulting aggregate distribution will not follow the
   * given distribution exactly, but will follow it approximately
   * assuming number of particles/rank is sufficiently large
   */
  WP_RANDOM,
  /* All ranks produce particles from a non-overlapping partition of the
   * range sequentially
   * TODO: IMPLEMENT THIS
   */
  WP_RANK_SEQUENTIAL
};

class WorkloadGenerator {
 public:
  WorkloadGenerator(float bins[], int num_bins, float range_start,
                    float range_end, int num_queries, WorkloadPattern wp,
                    int my_rank, int num_ranks);

  int next(float &value);

 private:

  /* Apply per-rank adjustment to produced data such that
   * each rank produces a portion of the total global distribution
   */
  void adjust_queries();
  void adjust_queries_sequential();
  void adjust_queries_random();
  // TODO: test rank_sequential and complete next
  void adjust_queries_rank_sequential();

  int next_sequential(float &value);

  int _seq_cur_bin;

  int next_random(float &value);

  int my_rank;
  int num_ranks;

  float range_start;
  float range_end;

  float bin_weights[MAX_BINS];
  float bin_starts[MAX_BINS];
  float bin_total;

  int bin_emits_left[MAX_BINS];

  int queries_total;
  int queries_left;

  int num_bins;
  float bin_width;

  WorkloadPattern wp;
};
}  // namespace rangeutils
