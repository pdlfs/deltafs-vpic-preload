#include "range_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>

#include "carp/carp.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "perfstats/manifest_analytics.h"
#include "range_common.h"

namespace {
template <typename T>
void assert_monotonic(T& seq, size_t seq_sz, bool verbose = false) {
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

class RangeUtilsTest {
 public:
  carp::Carp* carp;
  carp::CarpOptions ro;

  RangeUtilsTest() {
    ro.rtp_pvtcnt[1] = DEFAULT_PVTCNT;
    ro.rtp_pvtcnt[2] = DEFAULT_PVTCNT;
    ro.rtp_pvtcnt[3] = DEFAULT_PVTCNT;
    ro.oob_sz = DEFAULT_OOBSZ;

    carp = new carp::Carp(ro);

    carp->UpdateState(MainThreadState::MT_READY);
    carp->UpdateState(MainThreadState::MT_BLOCK);
  }

  void AdvancePastInit() {
    carp->UpdateState(MainThreadState::MT_READY);
    carp->UpdateState(MainThreadState::MT_BLOCK);
  }

  void LoadData(const float* oob_data, const int oob_data_sz) {
    carp->oob_buffer_.Reset();

    for (int i = 0; i < oob_data_sz; i++) {
      carp::particle_mem_t p;
      p.indexed_prop = oob_data[i];
      carp->oob_buffer_.Insert(p);
    }
  }

  void LoadData(const int num_ranks, const float range_min,
                const float range_max, const float* rank_bin_counts,
                const float* rank_bins) {
    carp->range_min_ = range_min;
    carp->range_max_ = range_max;

    carp->oob_buffer_.SetRange(range_min, range_max);

    carp->rank_bins_.resize(num_ranks + 1);
    carp->rank_counts_.resize(num_ranks);
    std::copy(rank_bins, rank_bins + num_ranks + 1, carp->rank_bins_.begin());
    std::copy(rank_bin_counts, rank_bin_counts + num_ranks,
              carp->rank_counts_.begin());
  }

  ~RangeUtilsTest() { delete carp; }
};

TEST(RangeUtilsTest, ParticleCount) {
  ASSERT_EQ(get_particle_count(3, 5, 2), 4);
  ASSERT_EQ(get_particle_count(2, 5, 2), 6);
  ASSERT_EQ(get_particle_count(3, 3, 2), 0);
}

TEST(RangeUtilsTest, PivotCalc) {
  srand(time(NULL));

  int oob_count = DEFAULT_OOBSZ;

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    float rand_val = rand() * 1.0f / RAND_MAX;
    carp::particle_mem_t p;
    p.indexed_prop = rand_val;
    carp->oob_buffer_.Insert(p);
  }

  int num_pivots = 8;
  carp::PivotUtils::CalculatePivots(carp, num_pivots);

  for (int pvt_idx = 1; pvt_idx < num_pivots; pvt_idx++) {
    float a = carp->my_pivots_[pvt_idx];
    float b = carp->my_pivots_[pvt_idx - 1];
    ASSERT_GT(a, b);
  }
}

TEST(RangeUtilsTest, PivotCalc2) {
  int oob_count = 12;

  const float data[] = {
      0.183005, 0.261744, 0.379052, 0.130448, 0.400778, 0.327600,
      0.831964, 0.363970, 1.327184, 0.193020, 2.427586, 0.213298,
  };

  const float pivots_ref[] = {0.130447999, 0.186760634, 0.208228499,
                              0.26997599,  0.345785022, 0.377166778,
                              0.508574486, 2.42758608};

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    carp::particle_mem_t p;
    p.indexed_prop = data[oob_idx];
    carp->oob_buffer_.Insert(p);
  }

  int num_pivots = 8;
  carp::PivotUtils::CalculatePivots(carp, num_pivots);

  for (int pvt_idx = 0; pvt_idx < num_pivots; pvt_idx++) {
    float pvt = carp->my_pivots_[pvt_idx];
    float ref = pivots_ref[pvt_idx];

    ASSERT_TRUE(float_eq(pvt, ref));
  }
}

TEST(RangeUtilsTest, PivotCalc3) {
  AdvancePastInit();

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
  const int oob_data_sz = 25;
  const int num_ranks = 8;
  const int num_pivots = 8;

  LoadData(oob_data, oob_data_sz);
  LoadData(num_ranks, range_min, range_max, rank_bin_counts, rank_bins);
  carp::PivotUtils::CalculatePivots(carp, 8);
  ::assert_monotonic(carp->my_pivots_, 8);
}

TEST(RangeUtilsTest, PivotCalc4) {
#include "pivot_calc_4_data.cc"  // NOLINT(bugprone-suspicious-include)
  AdvancePastInit();
  LoadData(oob_data, oob_data_sz);
  LoadData(num_ranks, range_min, range_max, rank_bin_counts, rank_bins);
  carp::PivotUtils::CalculatePivots(carp, num_pivots);
  ::assert_monotonic(carp->my_pivots_, num_pivots);
}

TEST(RangeUtilsTest, PivotCalc5) {
#include "pivot_calc_5_data.cc"  // NOLINT(bugprone-suspicious-include)
  AdvancePastInit();
  LoadData(oob_data, oob_data_sz);
  LoadData(num_ranks, range_min, range_max, rank_bin_counts, rank_bins);
  carp::PivotUtils::CalculatePivots(carp, num_pivots);
  ::assert_monotonic(carp->my_pivots_, num_pivots);
}

TEST(RangeUtilsTest, PivotCalc6) {
#include "pivot_calc_6_data.cc"  // NOLINT(bugprone-suspicious-include)
  AdvancePastInit();
  LoadData(oob_data, oob_data_sz);
  LoadData(num_ranks, range_min, range_max, rank_bin_counts, rank_bins);
  carp::PivotUtils::CalculatePivots(carp, num_pivots);
  ::assert_monotonic(carp->my_pivots_, num_pivots);
}

TEST(RangeUtilsTest, PivotCalc7) {
#include "pivot_calc_7_data.cc"  // NOLINT(bugprone-suspicious-include)
  LoadData(oob_data, oob_data_sz);
  carp::PivotUtils::CalculatePivots(carp, num_pivots);
  ::assert_monotonic(carp->my_pivots_, num_pivots);
}

TEST(RangeUtilsTest, PivotCalc8) {
#include "pivot_calc_8_data.cc"  // NOLINT(bugprone-suspicious-include)
  AdvancePastInit();
  LoadData(oob_data, oob_data_sz);
  LoadData(num_ranks, range_min, range_max, rank_bin_counts, rank_bins);
  carp::PivotUtils::CalculatePivots(carp, num_pivots);
  ::assert_monotonic(carp->my_pivots_, num_pivots);
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}