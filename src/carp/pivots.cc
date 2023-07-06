//
// Created by Ankush J on 10/27/22.
//

#include "../common.h"         /* for flog */
#include "range_constants.h"   /* for CARP_BAD_PIVOTS */

#include "pivots.h"

namespace pdlfs {
namespace carp {

//
// Used as implicit invalid pivots. XXXAJ: can be deprecated
//
void Pivots::MakeUpEpsilonPivots() {
  int num_pivots = pivots_.size();

  float mass_per_pivot = 1.0f / (num_pivots - 1);
  weight_ = mass_per_pivot;

  for (int pidx = 0; pidx < num_pivots; pidx++) {
    pivots_[pidx] = mass_per_pivot * pidx;
  }

  is_set_ = true;
}

void Pivots::AssertMonotonicity() {
  if (!is_set_) {
    flog(LOG_WARN, "No pivots set for monotonicity check!");
    return;
  }

  if (weight_ == CARP_BAD_PIVOTS) {
    flog(LOG_DBUG, "Pivots set to invalid. Not checking...");
    return;
  }

  int num_pivots = pivots_.size();
  for (int pidx = 0; pidx < num_pivots - 1; pidx++) {
    assert(pivots_[pidx] < pivots_[pidx + 1]);
  }
}

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
}  // namespace carp
}  // namespace pdlfs
