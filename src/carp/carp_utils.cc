
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

  // since Pivots are protected, the class needs to be unwrapped here
  carp->LogMyPivots(pivots, "RENEG_AGGR_PIVOTS");

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

}  // namespace carp
}  // namespace pdlfs
