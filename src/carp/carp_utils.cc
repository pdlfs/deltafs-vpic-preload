
#include <math.h>

#include <algorithm>
#include <numeric>

#include "carp_utils.h"
#include "common.h"
#include "preload_internal.h"
#include "range_common.h"

namespace {
void assert_monotonicity(pdlfs::carp::Range& rold, pdlfs::carp::Range& rnew) {
  if (!rold.IsSet()) {
    assert(rnew.IsSet());
    return;
  }

  assert(float_lte(rnew.rmin(), rold.rmin()));
  assert(float_gte(rnew.rmax(), rold.rmax()));
}
}  // namespace

namespace pdlfs {
namespace carp {

int PivotUtils::UpdatePivots(Carp* carp, Pivots* pivots) {
  carp->mutex_.AssertHeld();
  int num_pivots = pivots->Size();
  assert(num_pivots == pctx.comm_sz + 1);
  double* pivots_arr = pivots->pivots_.data();

  // since Pivots are protected, the class needs to be unwrapped here
  carp->LogMyPivots(pivots_arr, num_pivots, "RENEG_AGGR_PIVOTS");

  Range carp_range = carp->GetInBoundsRange();
  Range pivot_bounds = pivots->GetPivotBounds();
  assert_monotonicity(carp_range, pivot_bounds);
  carp->UpdateInBoundsRange(pivot_bounds);
  pivots->InstallInOrderedBins(&carp->bins_);

#ifdef DELTAFS_PLFSDIR_RANGEDB
  // make safe to invoke CARP-RTP without a properly initiialized
  // storage backend, such as for benchmarks
  if (pctx.plfshdl != NULL) {
    Range our_bin = carp->bins_.GetBin(pctx.my_rank);
    deltafs_plfsdir_range_update(pctx.plfshdl, our_bin.rmin(), our_bin.rmax());
  }
#else
  ABORT("linked deltafs does not support rangedb");
#endif

  return 0;
}

double PivotUtils::WeightedAverage(double a, double b, double frac) {
  /* Weighted avg of a and b was updated because of test case 11 (pvtcalc)
   * When a and b are very close, the following weighted avg is not monotonic.
   * pvt = (1 - frac_a) * val_a + (frac_a) * val_b;
   *
   * weighted avg of (0.473880023 and 0.473880142) should be <= for
   * frac_a =  0.412109375, vs frac_a = 0.536865234.
   *
   * The approach below seems more robust.
   * XXX: If it still persists, use double instead of float for pivot
   * computation
   */
  assert(frac >= 0);
  assert(a <= b);

  double pvt;
  double delta = b - a;
  double frac_delta = frac * delta;
  pvt = a + frac_delta;

  assert(delta >= 0);
  assert(frac_delta <= delta);
  assert(pvt <= b);

  return pvt;
}
}  // namespace carp
}  // namespace pdlfs
