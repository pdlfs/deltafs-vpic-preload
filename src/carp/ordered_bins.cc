//
// Created by Ankush J on 10/27/22.
//

#include "../common.h"         /* for flog */

#include "ordered_bins.h"
#include "range.h"

namespace pdlfs {
namespace carp {

//
// Searches for the bin corresponding to a value
// Adds it there. Undefined behavior if val is out of bounds
// XXX: forced still needed?
// XXX: if not, maybe call SearchBins and IncrementBin directly?
void OrderedBins::AddVal(float val, bool force) {
  int rv;
  size_t bidx;
  assert(this->IsSet());
  if (!force) assert(this->GetRange().Inside(val));

  rv = this->SearchBins(val, bidx, force);
  if (rv < 0)
      ABORT("OrderedBins: bidx < 0");
  if (rv > 0)
      ABORT("OrderedBins: bidx >= Size()");

  this->IncrementBin(bidx);
}

double OrderedBins::PrintNormStd() {
  uint64_t total_sz = this->GetTotalWeight();
  assert(this->Size());
  double avg_binsz = total_sz * 1.0 / this->Size();

  double normx_sum = 0;
  double normx2_sum = 0;

  for (size_t bidx = 0 ; bidx < this->Size() ; bidx++) {
    uint64_t bincnt = this->Weight(bidx);
    double normbincnt = bincnt / avg_binsz;
    double norm_x = normbincnt;
    double norm_x2 = normbincnt * normbincnt;
    normx_sum += norm_x;
    normx2_sum += norm_x2;
    flog(LOG_DBG2, "normbincnt: x: %lf, x2: %lf\n", normx_sum, normx2_sum);
  }

  normx_sum /= this->Size();
  normx2_sum /= this->Size();

  double normvar = normx2_sum - (normx_sum * normx_sum);
  double normstd = pow(normvar, 0.5);

  flog(LOG_INFO, "OrderedBins, Normalized Stddev: %.3lf\n", normstd);
  return normstd;
}

}  // namespace carp
}  // namespace pdlfs
