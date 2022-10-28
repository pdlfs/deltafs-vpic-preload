//
// Created by Ankush J on 10/27/22.
//

#include "carp_containers.h"

namespace pdlfs {
namespace carp {
std::string Pivots::ToString() const {
  if (!is_set_) {
    return "[PivotWidth]: unset [Pivots]: unset";
  }

  std::ostringstream pvtstr;
  pvtstr.precision(3);

  pvtstr << "PivotWidth: " << width_;
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

void OrderedBins::UpdateFromPivots(Pivots& pivots) {
  if (pivots.Size() != Size() + 1) {
    logf(LOG_ERRO, "[OrderedBins] SetFromPivots: size mismatch (%zu vs %zu)",
         pivots.Size(), Size());
    ABORT("OrderedBins - size mismatch!!");
    return;
  }

  std::copy(pivots.pivots_.begin(), pivots.pivots_.end(), bins_.begin());
  std::fill(counts_.begin(), counts_.end(), 0);
  is_set_ = true;
}

void OrderedBins::UpdateFromArrays(int nbins, const float* bins,
                                   const uint64_t* counts) {
  bins_.resize(nbins + 1);
  counts_.resize(nbins);

  std::copy(bins, bins + nbins + 1, bins_.begin());
  std::copy(counts, counts + nbins, counts_.begin());
  std::copy(counts, counts + nbins, counts_aggr_.begin());

  is_set_ = true;
}

uint64_t OrderedBins::GetTotalMass() const {
  return std::accumulate(counts_.begin(), counts_.end(), 0ull);
}

void OrderedBins::Reset() {
  std::fill(bins_.begin(), bins_.end(), 0);
  std::fill(counts_.begin(), counts_.end(), 0);
  std::fill(counts_aggr_.begin(), counts_aggr_.end(), 0);
  is_set_ = false;
}

int OrderedBins::SearchBins(float val) {
  auto iter = std::lower_bound(bins_.begin(), bins_.end(), val);
  unsigned int idx = iter - bins_.begin();
  while (idx < bins_.size() && val == bins_[idx]) idx++;  // skip equal vals
  return idx - 1;
}

double OrderedBins::PrintNormStd() {
  uint64_t total_sz = std::accumulate(counts_.begin(), counts_.end(), 0ull);
  double avg_binsz = total_sz * 1.0 / counts_.size();

  double normx_sum = 0;
  double normx2_sum = 0;

  for (uint64_t bincnt : counts_) {
    double normbincnt = bincnt / avg_binsz;
    double norm_x = normbincnt;
    double norm_x2 = normbincnt * normbincnt;
    normx_sum += norm_x;
    normx2_sum += norm_x2;
    logf(LOG_DBG2, "normbincnt: x: %lf, x2: %lf\n", normx_sum, normx2_sum);
  }

  normx_sum /= counts_.size();
  normx2_sum /= counts_.size();

  double normvar = normx2_sum - (normx_sum * normx_sum);
  double normstd = pow(normvar, 0.5);

  logf(LOG_INFO, "OrderedBins, Normalized Stddev: %.3lf\n", normstd);
  return normstd;
}
}  // namespace carp
}  // namespace pdlfs