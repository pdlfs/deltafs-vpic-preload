#include "range_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "range_common.h"

namespace {
/* TODO, struct and loop through all */
typedef struct pivot_state {
  const int num_ranks;
  const int oob_data_sz;
  const float oob_data[1000];
  const float range_min;
  const float range_max;
  const int num_pivots;
  const float rank_bin_counts[2000];
  const float rank_bins[2000];
};
namespace data_pc4 {
#include "pivot_calc_5_data.cc"  // NOLINT(bugprone-suspicious-include)
}

namespace data_pc5 {
#include "pivot_calc_5_data.cc"  // NOLINT(bugprone-suspicious-include)
}

namespace data_pc6 {
#include "pivot_calc_6_data.cc"  // NOLINT(bugprone-suspicious-include)
}

template <typename T>
bool assert_monotonic(T& seq, size_t seq_sz, bool verbose = false) {
  for (size_t i = 1; i < seq_sz; i++) {
    auto a = seq[i];
    auto b = seq[i - 1];
    if (verbose) {
      std::cout << a << " " << b << "\n";
    }
    ASSERT_GT(a, b);
  }
}
}  // namespace

namespace pdlfs {

class RangeUtilsTest {};

TEST(RangeUtilsTest, ParticleCount) {
  ASSERT_EQ(get_particle_count(3, 5, 2), 4);
  ASSERT_EQ(get_particle_count(2, 5, 2), 6);
  ASSERT_EQ(get_particle_count(3, 3, 2), 0);
}

TEST(RangeUtilsTest, PivotCalc) {
  srand(time(NULL));
  pivot_ctx_t pctx;

  int oob_count = pdlfs::kMaxOobSize;

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    float rand_val = rand() * 1.0f / RAND_MAX;
    particle_mem_t p;
    p.indexed_prop = rand_val;
    pctx.oob_buffer.Insert(p);
  }

  int num_pivots = 8;
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pivot_calculate_safe(&pctx, num_pivots);

  size_t buf_sz = 2048;
  char buf[buf_sz];
  print_vector(buf, buf_sz, pctx.my_pivots, num_pivots, false);

  for (int pvt_idx = 1; pvt_idx < num_pivots; pvt_idx++) {
    float a = pctx.my_pivots[pvt_idx];
    float b = pctx.my_pivots[pvt_idx - 1];
    printf("%d: %f %f %s\n", pvt_idx, a, b, a > b ? "ge" : "le");
    ASSERT_GT(a, b);
  }
}

TEST(RangeUtilsTest, PivotCalc2) {
  pivot_ctx_t pctx;
  int oob_count = 12;

  const float data[] = {
      0.183005, 0.261744, 0.379052, 0.130448, 0.400778, 0.327600,
      0.831964, 0.363970, 1.327184, 0.193020, 2.427586, 0.213298,
  };

  const float pivots_ref[] = {0.130447999, 0.189264387, 0.19808951,
                              0.319368005, 0.345785022, 0.365855247,
                              0.724167525, 2.42758608};

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    particle_mem_t p;
    p.indexed_prop = data[oob_idx];
    pctx.oob_buffer.Insert(p);
  }

  int num_pivots = 8;
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pivot_calculate_safe(&pctx, num_pivots);

  for (int pvt_idx = 0; pvt_idx < num_pivots; pvt_idx++) {
    float pvt = pctx.my_pivots[pvt_idx];
    float ref = pivots_ref[pvt_idx];

    ASSERT_TRUE(float_eq(pvt, ref));
  }
}

TEST(RangeUtilsTest, PivotCalc3) {
  const float oob_data[] = {
      0.530524611, 2.07151246,  0.129153624, 0.317573667, 0.179045826,
      1.58116162,  3.8822875,   0.311510593, 0.475103348, 0.10761077,
      0.264501095, 0.594769895, 1.46151435,  0.779551029, 2.9388082,
      1.78281081,  4.48976421,  0.371605545, 0.250535101, 1.02625966,
      0.250535101, 0.433013767, 3.02271819,  0.179088727, 0.348295808};

  const float rank_bins[] = {0.011929879, 0.203471959, 0.338690162,
                             0.500766993, 0.735446155, 1.01613414,
                             1.4439038,   2.18780971,  4.48976707};

  const float rank_bin_counts[] = {11, 10, 10, 6, 1, 3, 2, 1};

  const float range_min = 0.011929879, range_max = 4.48976707;

  pivot_ctx_t pctx;

  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pctx.mts_mgr.update_state(MainThreadState::MT_READY);
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);

  pctx.range_min = range_min;
  pctx.range_max = range_max;

  for (int i = 0; i < 25; i++) {
    particle_mem_t p;
    p.indexed_prop = oob_data[i];
    pctx.oob_buffer.Insert(p);
  }

  pctx.oob_buffer.SetRange(range_min, range_max);

  pctx.rank_bins.resize(9);
  pctx.rank_bin_count.resize(8);
  std::copy(rank_bins, rank_bins + 9, pctx.rank_bins.begin());
  std::copy(rank_bin_counts, rank_bin_counts + 8, pctx.rank_bin_count.begin());

  float my_pivots[8];
  ::pivot_calculate_safe(&pctx, 8);
  ::assert_monotonic(pctx.my_pivots, 8);
}

TEST(RangeUtilsTest, PivotCalc6) {
  pivot_ctx_t pctx;

  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pctx.mts_mgr.update_state(MainThreadState::MT_READY);
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);

  pctx.range_min = ::data_pc6::range_min;
  pctx.range_max = ::data_pc6::range_max;

  for (int i = 0; i < ::data_pc6::oob_data_sz; i++) {
    particle_mem_t p;
    p.indexed_prop = ::data_pc6::oob_data[i];
    pctx.oob_buffer.Insert(p);
  }

  pctx.oob_buffer.SetRange(::data_pc6::range_min, ::data_pc6::range_max);

  int num_ranks = ::data_pc6::num_ranks;
  int num_pivots = ::data_pc6::num_pivots;

  pctx.rank_bins.resize(num_ranks + 1);
  pctx.rank_bin_count.resize(num_ranks);
  std::copy(::data_pc6::rank_bins, ::data_pc6::rank_bins + num_ranks + 1,
            pctx.rank_bins.begin());
  std::copy(::data_pc6::rank_bin_counts,
            ::data_pc6::rank_bin_counts + num_ranks,
            pctx.rank_bin_count.begin());

  float my_pivots[num_pivots];
  ::pivot_calculate_safe(&pctx, num_pivots);

  ::assert_monotonic(pctx.my_pivots, num_pivots);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}