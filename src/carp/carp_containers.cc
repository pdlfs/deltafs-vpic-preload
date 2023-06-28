//
// Created by Ankush J on 10/27/22.
//

#include "carp_containers.h"

namespace pdlfs {
namespace carp {
std::string Pivots::ToString() const {
  if (!is_set_) {
    return "[PivotWeight]: unset [Pivots]: unset";
  }

  std::ostringstream pvtstr;
  pvtstr.precision(3);

  pvtstr << "PivotWeight: " << weight_;
  pvtstr << ", [PivotCount] " << pivots_.size();
  pvtstr << ", [Pivots]:";

  for (size_t pvt_idx = 0; pvt_idx < pivots_.size(); pvt_idx++) {
    if (pvt_idx % 16 == 0) {
      pvtstr << "\n\t";
    }

    double pivot = pivots_[pvt_idx];
    pvtstr << pivot << ", ";
  }

  return pvtstr.str();
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
