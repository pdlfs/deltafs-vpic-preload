#include "range_utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"
#include "range_common.h"

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
  pivot_calculate(&pctx, num_pivots);

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

  const float pivots_ref[] = {
      0.130448, 0.193020, 0.213298, 0.327600,
      0.379052, 0.831964, 1.327184, 2.427586,
  };

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    particle_mem_t p;
    p.indexed_prop = data[oob_idx];
    pctx.oob_buffer.Insert(p);
  }

  int num_pivots = 8;
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pivot_calculate(&pctx, num_pivots);

  for (int pvt_idx = 0; pvt_idx < num_pivots; pvt_idx++) {
    float pvt = pctx.my_pivots[pvt_idx];
    float ref = pivots_ref[pvt_idx];

    ASSERT_TRUE(float_eq(pvt, ref));
  }
}

}  // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}