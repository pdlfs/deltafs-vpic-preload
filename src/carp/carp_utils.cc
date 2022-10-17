#include "carp_utils.h"

#include <math.h>

#include <algorithm>
#include <numeric>

#include "common.h"
#include "preload_internal.h"
#include "range_common.h"

namespace pdlfs {
namespace carp {

int PivotUtils::CalculatePivots(Carp* carp, const size_t num_pivots) {
  carp->mutex_.AssertHeld();

  assert(num_pivots <= pdlfs::kMaxPivots);

  int rv = 0;

  const MainThreadState cur_state = carp->mts_mgr_.GetState();

  carp->my_pivot_width_ = 0;

  assert(cur_state == MainThreadState::MT_BLOCK);

  if (carp->mts_mgr_.FirstBlock()) {
    rv = CalculatePivotsFromOob(carp, num_pivots);
  } else {
    rv = CalculatePivotsFromAll(carp, num_pivots);
  }

  logf(LOG_DBG2, "pvt_calc_local @ R%d, pvt width: %.2f", pctx.my_rank,
       carp->my_pivot_width_);

  if (carp->my_pivot_width_ < 1e-3) {
    float mass_per_pivot = 1.0f / (num_pivots - 1);
    carp->my_pivot_width_ = mass_per_pivot;

    for (int pidx = 0; pidx < num_pivots; pidx++) {
      carp->my_pivots_[pidx] = mass_per_pivot * pidx;
    }
  }

  for (int pidx = 0; pidx < num_pivots - 1; pidx++) {
    assert(carp->my_pivots_[pidx] < carp->my_pivots_[pidx + 1]);
  }

  return rv;
}

int PivotUtils::CalculatePivotsFromOob(Carp* carp, int num_pivots) {
  carp->mutex_.AssertHeld();

  int rv = 0;

  std::vector<float> oobl, oobr;

  pdlfs::carp::OobBuffer* oob = &(carp->oob_buffer_);
  carp->oob_buffer_.GetPartitionedProps(oobl, oobr);

  assert(oobr.size() == 0);
  const int oobl_sz = oobl.size();

  if (oobl_sz < 2) return 0;

  const float range_min = oobl[0];
  const float range_max = oobl[oobl_sz - 1];

  carp->my_pivots_[0] = range_min;
  carp->my_pivots_[num_pivots - 1] = range_max;

  carp->my_pivot_width_ = oobl_sz * 1.0 / num_pivots;

  /* for computation purposes, we need to reserve one, so as to always have
   * two points of interpolation */

  float part_per_pivot = (oobl_sz - 1) * 1.0 / num_pivots;

  for (int pvt_idx = 1; pvt_idx < num_pivots - 1; pvt_idx++) {
    float oob_idx = part_per_pivot * pvt_idx;
    int oob_idx_trunc = (int)oob_idx;

    assert(oob_idx_trunc + 1 < oobl_sz);

    float val_a = oobl[oob_idx_trunc];
    float val_b = oobl[oob_idx_trunc + 1];

    float frac_a = oob_idx - (float)oob_idx_trunc;
    carp->my_pivots_[pvt_idx] = (1 - frac_a) * val_a + (frac_a)*val_b;
  }

  return rv;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int PivotUtils::CalculatePivotsFromAll(Carp* carp, int num_pivots) {
  carp->mutex_.AssertHeld();

  assert(num_pivots <= pdlfs::kMaxPivots);

  carp->my_pivot_count_ = num_pivots;

  const float prev_range_begin = carp->range_min_;
  const float prev_range_end = carp->range_max_;

  float range_start, range_end;
  std::vector<float> oobl, oobr;

  pdlfs::carp::OobBuffer* oob = &(carp->oob_buffer_);
  oob->GetPartitionedProps(oobl, oobr);

  GetRangeBounds(carp, oobl, oobr, range_start, range_end);
  assert(range_end >= range_start);

  int oobl_sz = oobl.size(), oobr_sz = oobr.size();

  float particle_count = std::accumulate(carp->rank_counts_.begin(),
                                         carp->rank_counts_.end(), 0.f);

  int my_rank = pctx.my_rank;

  carp->my_pivots_[0] = range_start;
  carp->my_pivots_[num_pivots - 1] = range_end;

  particle_count += (oobl_sz + oobr_sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (num_pivots - 1);

  if (part_per_pivot < 1e-5) {
    std::fill(carp->my_pivots_, carp->my_pivots_ + num_pivots, 0);
    carp->my_pivot_width_ = 0;
    return 0;
  }

  float accumulated_ppp = 0;
  float particles_carried_over = 0;
  /* Pivot calculation for OOBL and OOBR is similar, except OOBL will not
   * consume any "particles_carried_over"
   */

  float oob_idx = 0;
  while (1) {
    float part_left = oobl_sz - oob_idx;
    if (part_per_pivot < 1e-5 || part_left < part_per_pivot) {
      particles_carried_over += part_left;
      break;
    }

    accumulated_ppp += part_per_pivot;

    float part_idx = accumulated_ppp;
    int part_idx_trunc = (int)accumulated_ppp;

    if (part_idx_trunc >= oobl_sz) break;

    float frac_a = part_idx - (float)part_idx_trunc;
    float val_a = oobl[part_idx_trunc];
    float val_b = (part_idx_trunc + 1 < oobl_sz) ? oobl[part_idx_trunc + 1]
                                                 : prev_range_begin;
    assert(val_b > val_a);

    carp->my_pivots_[cur_pivot] = (1 - frac_a) * val_a + (frac_a)*val_b;
    cur_pivot++;

    oob_idx = accumulated_ppp;
  }

  int bin_idx = 0;

  for (int bidx = 0; bidx < carp->rank_bins_.size() - 1; bidx++) {
    const double cur_bin_total = carp->rank_counts_[bidx];
    double cur_bin_left = carp->rank_counts_[bidx];

    double bin_start = carp->rank_bins_[bidx];
    double bin_end = carp->rank_bins_[bidx + 1];
    const double bin_width_orig = bin_end - bin_start;

    while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
      double take_from_bin = part_per_pivot - particles_carried_over;

      /* advance bin_start st take_from_bin is removed */
      double bin_width_left = bin_end - bin_start;
      double width_to_remove = take_from_bin / cur_bin_total * bin_width_orig;

      bin_start += width_to_remove;
      carp->my_pivots_[cur_pivot] = bin_start;

      cur_pivot++;

      cur_bin_left -= take_from_bin;
      particles_carried_over = 0;
    }

    // XXX: Arbitrarily chosen threshold, may cause troubles at
    // large scales
    assert(cur_bin_left >= -1e-3);

    particles_carried_over += cur_bin_left;
  }

  fprintf(stderr, "cur_pivot: %d, pco: %0.3f\n", cur_pivot,
          particles_carried_over);

  oob_idx = 0;

  while (1) {
    float part_left = oobr_sz - oob_idx;
    if (part_per_pivot < 1e-5 ||
        part_left + particles_carried_over < part_per_pivot + 1e-5) {
      particles_carried_over += part_left;
      break;
    }

    float next_idx = oob_idx + part_per_pivot - particles_carried_over;
    int next_idx_trunc = (int)next_idx;
    particles_carried_over = 0;

    if (next_idx_trunc >= oobr_sz) break;

    /* Current pivot is computed from fractional index weighted average,
     * we interpolate between current index and next, if next index is out of
     * bounds, we just interpolate */

    float frac_b = next_idx - next_idx_trunc;
    assert(frac_b >= 0);

    float val_b = oobr[next_idx_trunc];
    float val_a;

    if (next_idx_trunc > 0) {
      val_a = oobr[next_idx_trunc - 1];
    } else if (next_idx_trunc == 0) {
      val_a = prev_range_end;
    } else {
      assert(false);
    }

    float next_pivot = (1 - frac_b) * val_a + frac_b * val_b;
    carp->my_pivots_[cur_pivot++] = next_pivot;
    oob_idx = next_idx;
  }

#define ERRLIM 1e0

  if (cur_pivot == num_pivots) {
    assert(fabs(particles_carried_over) < ERRLIM);
  } else if (cur_pivot == num_pivots - 1) {
    assert(fabs(particles_carried_over) < ERRLIM or
           fabs(particles_carried_over - part_per_pivot) < ERRLIM);
  } else {
    /* shouldn't happen */
    assert(false);
  }

  carp->my_pivots_[num_pivots - 1] = range_end;
  carp->my_pivot_width_ = part_per_pivot;

  return 0;
}

int PivotUtils::UpdatePivots(Carp* carp, double* pivots, int num_pivots) {
  carp->mutex_.AssertHeld();
  assert(num_pivots == pctx.comm_sz + 1);

  carp->LogMyPivots(pivots, num_pivots, "RENEG_AGGR_PIVOTS");

  double& pvtbeg = carp->range_min_;
  double& pvtend = carp->range_max_;

  double updbeg = pivots[0];
  double updend = pivots[num_pivots - 1];

  // carp->range_min = pivots[0];
  // carp->range_max = pivots[num_pivots_ - 1];
  if (!carp->mts_mgr_.FirstBlock()) {
    assert(float_lte(updbeg, pvtbeg));
    assert(float_gte(updend, pvtend));
  }

  pvtbeg = updbeg;
  pvtend = updend;

  std::copy(pivots, pivots + num_pivots, carp->rank_bins_.begin());
  std::fill(carp->rank_counts_.begin(), carp->rank_counts_.end(), 0);

  carp->oob_buffer_.SetRange(carp->range_min_, carp->range_max_);

  double our_bin_start = pivots[pctx.my_rank];
  double our_bin_end = pivots[pctx.my_rank + 1];
#ifdef DELTAFS_PLFSDIR_RANGEDB
  deltafs_plfsdir_range_update(pctx.plfshdl, our_bin_start, our_bin_end);
#else
  ABORT("linked deltafs does not support rangedb");
#endif

  return 0;
}

int PivotUtils::GetRangeBounds(Carp* carp, std::vector<float>& oobl,
                               std::vector<float>& oobr, float& range_start,
                               float& range_end) {
  int rv = 0;

  size_t oobl_sz = oobl.size();
  size_t oobr_sz = oobr.size();

  double oobl_min = oobl_sz ? oobl[0] : 0;
  double oobr_min = oobr_sz ? oobr[0] : 0;
  /* If both OOBs are filled, their minimum, otherwise, the non-zero val */
  double oob_min =
      (oobl_sz && oobr_sz) ? std::min(oobl_min, oobr_min) : oobl_min + oobr_min;

  double oobl_max = oobl_sz ? oobl[oobl_sz - 1] : 0;
  double oobr_max = oobr_sz ? oobr[oobr_sz - 1] : 0;
  double oob_max = std::max(oobl_max, oobr_max);

  assert(oobl_min <= oobl_max);
  assert(oobr_min <= oobr_max);

  if (oobl_sz and oobr_sz) {
    assert(oobl_min <= oobr_min);
    assert(oobl_max <= oobr_max);
  }

  /* Since our default value is zero, min needs to obtained
   * complex-ly, while max is just max
   * i.e., if range_min is 0.63, and OOBs are empty, then
   * oob_min (= 0) needs to be ignored
   */
  if (carp->mts_mgr_.FirstBlock()) {
    range_start = oob_min;
  } else if (oobl_sz) {
    range_start = std::min(oob_min, carp->range_min_);
  } else {
    range_start = carp->range_min_;
  }

  range_end = std::max(oob_max, carp->range_max_);

  return rv;
}
}  // namespace carp
}  // namespace pdlfs
