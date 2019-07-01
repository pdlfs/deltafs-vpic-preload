#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <ctime>

namespace rangeutils {

static const int MAX_BINS = 100;

enum class WorkloadPattern { WP_SEQUENTIAL, WP_RANDOM };

class WorkloadGenerator {
 public:
  WorkloadGenerator(float bins[], int num_bins, float range_start,
                    float range_end, int num_queries, WorkloadPattern wp);

  int next(float &value);

 private:
  int next_sequential(float &value);

  int _seq_cur_bin;

  int next_random(float &value);

  int my_rank;
  int num_ranks;

  float range_start;
  float range_end;

  float bin_weights[MAX_BINS];
  float bin_starts[MAX_BINS];

  int bin_emits_left[MAX_BINS];

  int queries_total;
  int queries_left;

  int num_bins;
  float bin_width;

  WorkloadPattern wp;
};
}  // namespace rangeutils
