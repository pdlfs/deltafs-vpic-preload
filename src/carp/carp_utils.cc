#include "carp_utils.h"

#include <math.h>

#include <algorithm>
#include <numeric>

#include "carp_containers.h"
#include "common.h"
#include "preload_internal.h"
#include "range_common.h"

/**** FOR DEBUGGING ****/
/* local functions */

/* return true if a is smaller - we prioritize smaller bin_val
 * and for same bin_val, we prioritize ending items (is_start == false)
 * first */
bool rb_item_lt(const rb_item_t& a, const rb_item_t& b) {
  return (a.bin_val < b.bin_val) ||
         ((a.bin_val == b.bin_val) && (!a.is_start && b.is_start));
}

namespace {
bool pmt_comp(const pdlfs::carp::particle_mem_t& a,
              const pdlfs::carp::particle_mem_t& b) {
  return a.indexed_prop < b.indexed_prop;
}

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
template <typename T>
std::string PivotUtils::SerializeVector(std::vector<T>& v) {
  std::string s;
  for (size_t i = 0; i < v.size(); i++) {
    T elem = v[i];
    if (i == 0) {
      s += std::to_string(elem);
    } else {
      s += "," + std::to_string(elem);
    }
  }

  return s;
}

template <typename T>
std::string PivotUtils::SerializeVector(T* v, size_t vsz) {
  std::string s;
  for (size_t i = 0; i < vsz; i++) {
    T elem = v[i];
    if (i == 0) {
      s += std::to_string(elem);
    } else {
      s += "," + std::to_string(elem);
    }
  }

  return s;
}

template std::string PivotUtils::SerializeVector<float>(std::vector<float>& v);
template std::string PivotUtils::SerializeVector<double>(
    std::vector<double>& v);
template std::string PivotUtils::SerializeVector<double>(double* v, size_t vsz);

int PivotUtils::CalculatePivots(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                                size_t num_pivots) {
  int rv = 0;
  pivots->Resize(num_pivots);

  if (pvt_ctx->FirstBlock()) {
    rv = CalculatePivotsFromOob(pvt_ctx, pivots, num_pivots);
  } else {
    rv = CalculatePivotsFromAll(pvt_ctx, pivots, num_pivots);
  }

  pivots->is_set_ = true;

  logf(LOG_DBG2, "pvt_calc_local @ R%d, pvt width: %.2f", pctx.my_rank,
       pivots->width_);

  return rv;
}

int PivotUtils::CalculatePivotsFromOob(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                                       size_t num_pivots) {
  int rv = 0;

  assert(pvt_ctx->oob_right_.size() == 0);
  const int oob_left__sz = pvt_ctx->oob_left_.size();

  if (oob_left__sz < 2) return 0;

  const float range_min = pvt_ctx->oob_left_[0];
  const float range_max = pvt_ctx->oob_left_[oob_left__sz - 1];

  pivots->pivots_[0] = range_min;
  pivots->pivots_[num_pivots - 1] = range_max;

  pivots->width_ = oob_left__sz * 1.0 / num_pivots;

  /* for computation purposes, we need to reserve one, so as to always have
   * two points of interpolation */

  float part_per_pivot = (oob_left__sz - 1) * 1.0 / num_pivots;

  for (int pvt_idx = 1; pvt_idx < num_pivots - 1; pvt_idx++) {
    float oob_idx = part_per_pivot * pvt_idx;
    int oob_idx_trunc = (int)oob_idx;

    assert(oob_idx_trunc + 1 < oob_left__sz);

    float val_a = pvt_ctx->oob_left_[oob_idx_trunc];
    float val_b = pvt_ctx->oob_left_[oob_idx_trunc + 1];

    float frac_a = oob_idx - (float)oob_idx_trunc;
    float pvt = WeightedAverage(val_a, val_b, frac_a);
    pivots->pivots_[pvt_idx] = pvt;
  }

  return rv;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int PivotUtils::CalculatePivotsFromAll(PivotCalcCtx* pvt_ctx, Pivots* pivots,
                                       size_t num_pivots) {
  OrderedBins* bins = pvt_ctx->bins_;
  assert(num_pivots <= pdlfs::kMaxPivots);

  const float prev_range_begin = pvt_ctx->GetRange().rmin();
  const float prev_range_end = pvt_ctx->GetRange().rmax();

  float range_start, range_end;

  GetRangeBounds(pvt_ctx, range_start, range_end);
  assert(range_end >= range_start);

  int oob_left__sz = pvt_ctx->oob_left_.size(),
      oob_right__sz = pvt_ctx->oob_right_.size();

  float particle_count = bins->GetTotalMass();

  pivots->pivots_[0] = range_start;
  pivots->pivots_[num_pivots - 1] = range_end;

  particle_count += (oob_left__sz + oob_right__sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (num_pivots - 1);

  if (part_per_pivot < 1e-5) {
    pivots->FillZeros();
    return 0;
  }

#if 0
  /**********************/
  std::vector<float>& ff = carp->rank_counts_;
  std::vector<float>& gg = carp->rank_bins_;
  // fprintf(stderr,
  // "rank%d get_local_pivots state_dump "
  // "oob_count_left: %d, oob_count_right: %d\n"
  // "pivot range: (%.1f %.1f), particle_cnt: %.1f\n"
  // "rbc: %s (%zu)\n"
  // "bin: %s (%zu)\n",
  // pctx.my_rank, pvt_ctx->oob_left__sz, pvt_ctx->oob_right__sz, range_start, range_end,
  // particle_count, SerializeVector(ff).c_str(), ff.size(),
  // SerializeVector(gg).c_str(), gg.size());
  /**********************/
#endif

  float accumulated_ppp = 0;
  float particles_carried_over = 0;
  /* Pivot calculation for OOBL and OOBR is similar, except OOBL will not
   * consume any "particles_carried_over"
   */

  float oob_idx = 0;
  while (1) {
    float part_left = oob_left__sz - oob_idx;
    if (part_per_pivot < 1e-5 || part_left < part_per_pivot) {
      particles_carried_over += part_left;
      break;
    }

    accumulated_ppp += part_per_pivot;

    float part_idx = accumulated_ppp;
    int part_idx_trunc = (int)accumulated_ppp;

    if (part_idx_trunc >= oob_left__sz) break;

    float frac_a = part_idx - (float)part_idx_trunc;
    float val_a = pvt_ctx->oob_left_[part_idx_trunc];
    float val_b = (part_idx_trunc + 1 < oob_left__sz)
                      ? pvt_ctx->oob_left_[part_idx_trunc + 1]
                      : prev_range_begin;
    assert(val_b > val_a);

    pivots->pivots_[cur_pivot] = (1 - frac_a) * val_a + (frac_a)*val_b;
    cur_pivot++;

    oob_idx = accumulated_ppp;
  }

  int bin_idx = 0;

  for (int bidx = 0; bidx < bins->Size(); bidx++) {
    const double cur_bin_total = bins->counts_[bidx];
    double cur_bin_left = bins->counts_[bidx];

    double bin_start = bins->bins_[bidx];
    double bin_end = bins->bins_[bidx + 1];
    const double bin_width_orig = bin_end - bin_start;

    while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
      double take_from_bin = part_per_pivot - particles_carried_over;

      /* advance bin_start st take_from_bin is removed */
      double bin_width_left = bin_end - bin_start;
      double width_to_remove = take_from_bin / cur_bin_total * bin_width_orig;

      bin_start += width_to_remove;
      pivots->pivots_[cur_pivot] = bin_start;

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
    float part_left = oob_right__sz - oob_idx;
    if (part_per_pivot < 1e-5 ||
        part_left + particles_carried_over < part_per_pivot + 1e-5) {
      particles_carried_over += part_left;
      break;
    }

    float next_idx = oob_idx + part_per_pivot - particles_carried_over;
    int next_idx_trunc = (int)next_idx;
    particles_carried_over = 0;

    if (next_idx_trunc >= oob_right__sz) break;

    /* Current pivot is computed from fractional index weighted average,
     * we interpolate between current index and next, if next index is out of
     * bounds, we just interpolate */

    float frac_b = next_idx - next_idx_trunc;
    assert(frac_b >= 0);

    float val_b = pvt_ctx->oob_right_[next_idx_trunc];
    float val_a;

    if (next_idx_trunc > 0) {
      val_a = pvt_ctx->oob_right_[next_idx_trunc - 1];
    } else if (next_idx_trunc == 0) {
      val_a = prev_range_end;
    } else {
      assert(false);
    }

    float next_pivot = (1 - frac_b) * val_a + frac_b * val_b;
    pivots->pivots_[cur_pivot++] = next_pivot;
    oob_idx = next_idx;
  }

#define ERRLIM 1e-3

  float norm_unalloc_mass = (particles_carried_over / part_per_pivot);

  if (cur_pivot == num_pivots) {
    // all pivots allocated, unallocated mass should be ~0
    assert(fabs(norm_unalloc_mass) <= ERRLIM);
  } else if (cur_pivot == num_pivots - 1) {
    // one pivot unallocated, unallocated mass should be ~1
    assert(fabs(norm_unalloc_mass - 1.0f) < ERRLIM);
  } else {
    /* shouldn't happen */
    assert(false);
  }

  pivots->pivots_[num_pivots - 1] = range_end;
  pivots->width_ = part_per_pivot;

  return 0;
}

int PivotUtils::UpdatePivots(Carp* carp, Pivots* pivots) {
  carp->mutex_.AssertHeld();
  int num_pivots = pivots->Size();
  assert(num_pivots == pctx.comm_sz + 1);
  double* pivots_arr = pivots->pivots_.data();

  // since Pivots are protected, the class needs to be unwrapped here
  carp->LogMyPivots(pivots_arr, num_pivots, "RENEG_AGGR_PIVOTS");

  // casting inclusive to exclusive, but ok for the purpose used
  Range carp_range = carp->GetRange();
  Range pivot_bounds = pivots->GetPivotBounds();
  assert_monotonicity(carp_range, pivot_bounds);
  carp->UpdateRange(pivot_bounds);
  carp->bins_.UpdateFromPivots(*pivots);

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

void PivotUtils::LogPivots(Carp* carp, int pvtcnt) {
  if (!carp->PerflogOn()) return;

  char label[64];
  snprintf(label, 64, "RENEG_PIVOTS_E%d", carp->epoch_);
  carp->LogMyPivots(carp->pivots_.pivots_.data(), pvtcnt, label);
  snprintf(label, 64, "RENEG_BINCNT_E%d", carp->epoch_);
  carp->LogVec(carp->bins_.counts_aggr_, label);
}

int PivotUtils::GetRangeBounds(PivotCalcCtx* pvt_ctx, float& range_start,
                               float& range_end) {
  int rv = 0;

  size_t oob_left__sz = pvt_ctx->oob_left_.size();
  size_t oob_right__sz = pvt_ctx->oob_right_.size();

  double oob_left__min = oob_left__sz ? pvt_ctx->oob_left_[0] : 0;
  double oob_right__min = oob_right__sz ? pvt_ctx->oob_right_[0] : 0;
  /* If both OOBs are filled, their minimum, otherwise, the non-zero val */
  double oob_min = (oob_left__sz && oob_right__sz)
                       ? std::min(oob_left__min, oob_right__min)
                       : oob_left__min + oob_right__min;

  double oob_left__max = oob_left__sz ? pvt_ctx->oob_left_[oob_left__sz - 1] : 0;
  double oob_right__max =
      oob_right__sz ? pvt_ctx->oob_right_[oob_right__sz - 1] : 0;
  double oob_max = std::max(oob_left__max, oob_right__max);

  assert(oob_left__min <= oob_left__max);
  assert(oob_right__min <= oob_right__max);

  if (oob_left__sz and oob_right__sz) {
    assert(oob_left__min <= oob_right__min);
    assert(oob_left__max <= oob_right__max);
  }

  /* Since our default value is zero, min needs to obtained
   * complex-ly, while max is just max
   * i.e., if range_min is 0.63, and OOBs are empty, then
   * oob_min (= 0) needs to be ignored
   */
  // if (prev_state_ == MainThreadState::MT_INIT) {
  if (pvt_ctx->FirstBlock()) {
    range_start = oob_min;
  } else if (oob_left__sz) {
    range_start = std::min(oob_min, pvt_ctx->GetRange().rmin());
  } else {
    range_start = pvt_ctx->GetRange().rmin();
  }

  range_end = std::max(oob_max, pvt_ctx->GetRange().rmax());

  return rv;
}

float PivotUtils::WeightedAverage(float a, float b, float frac) {
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

  float pvt;
  float delta = b - a;
  float frac_delta = frac * delta;
  pvt = a + frac_delta;

  assert(delta >= 0);
  assert(frac_delta <= delta);
  assert(pvt <= b);

  return pvt;
}
}  // namespace carp
}  // namespace pdlfs
