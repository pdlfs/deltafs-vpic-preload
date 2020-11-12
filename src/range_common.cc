#include "range_common.h"

#include <math.h>

#include <algorithm>
#include <numeric>

#include "common.h"
#include "preload_internal.h"

/**** FOR DEBUGGING ****/
#define PRINTBUF_LEN 16384

static char rs_pb_buf[16384];
static char rs_pb_buf2[16384];
static char rs_pbin_buf[16384];

static char* print_vec(char* buf, int buf_len, float* v, int vlen) {
  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.1f ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

static char* print_vec(char* buf, int buf_len, std::vector<float>& v,
                       int vlen) {
  assert(v.size() >= vlen);

  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.2f, ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

/* local functions */

static int pivot_calculate_from_oobl(pivot_ctx_t* pvt_ctx, int num_pivots);

static int pivot_calculate_from_all(pivot_ctx_t* pvt_ctx,
                                    const size_t num_pivots);

/* return true if a is smaller - we prioritize smaller bin_val
 * and for same bin_val, we prioritize ending items (is_start == false)
 * first */
bool rb_item_lt(const rb_item_t& a, const rb_item_t& b) {
  return (a.bin_val < b.bin_val) ||
         ((a.bin_val == b.bin_val) && (!a.is_start && b.is_start));
}

namespace {
bool pmt_comp(const pdlfs::particle_mem_t& a, const pdlfs::particle_mem_t& b) {
  return a.indexed_prop < b.indexed_prop;
}

int get_range_bounds(pivot_ctx_t* pvt_ctx, std::vector<float>& oobl,
                     std::vector<float>& oobr, float& range_start,
                     float& range_end) {
  int rv = 0;

  size_t oobl_sz = oobl.size();
  size_t oobr_sz = oobr.size();

  float oobl_min = oobl_sz ? oobl[0] : 0;
  float oobr_min = oobr_sz ? oobr[0] : 0;
  /* If both OOBs are filled, their minimum, otherwise, the non-zero val */
  float oob_min =
      (oobl_sz && oobr_sz) ? std::min(oobl_min, oobr_min) : oobl_min + oobr_min;

  float oobl_max = oobl_sz ? oobl[oobl_sz - 1] : 0;
  float oobr_max = oobr_sz ? oobr[oobr_sz - 1] : 0;
  float oob_max = std::max(oobl_max, oobr_max);

  assert(oobl_min <= oobl_max);
  assert(oobr_min <= oobr_max);

  if (oobl_sz and oobr_sz) {
    assert(oobl_min <= oobr_min);
    assert(oobl_max <= oobr_max);
  }

  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();

  /* Since our default value is zero, min needs to obtained
   * complex-ly, while max is just max
   * i.e., if range_min is 0.63, and OOBs are empty, then
   * oob_min (= 0) needs to be ignored
   */
  if (prev_state == MainThreadState::MT_INIT) {
    range_start = oob_min;
  } else if (oobl_sz) {
    range_start = std::min(oob_min, pvt_ctx->range_min);
  } else {
    range_start = pvt_ctx->range_min;
  }

  range_end = std::max(oob_max, pvt_ctx->range_max);

  return rv;
}

}  // namespace

MainThreadStateMgr::MainThreadStateMgr()
    : current_state{MT_INIT}, prev_state{MT_INIT} {};

MainThreadState MainThreadStateMgr::get_state() { return this->current_state; }

MainThreadState MainThreadStateMgr::get_prev_state() {
  return this->prev_state;
}

MainThreadState MainThreadStateMgr::update_state(MainThreadState new_state) {
  MainThreadState cur_state = this->current_state;

  if (cur_state == MainThreadState::MT_INIT &&
      new_state == MainThreadState::MT_BLOCK) {
    // pass
  } else if (cur_state == MainThreadState::MT_READY &&
             new_state == MainThreadState::MT_BLOCK) {
    // pass
  } else if (cur_state == MainThreadState::MT_BLOCK &&
             new_state == MainThreadState::MT_READY) {
    // pass
  } else {
    logf(LOG_ERRO, "update_state @ R%d: %d to %d\n", pctx.my_rank, cur_state,
         new_state);
    ABORT("MainThreadStateMgr::update_state: unexpected transition");
  }

  this->prev_state = this->current_state;
  this->current_state = new_state;

  return this->prev_state;
}

int pivot_ctx_init(pivot_ctx_t** pvt_ctx, reneg_opts* ro) {
  (*pvt_ctx) = new pivot_ctx_t();
  pivot_ctx_t* pvt_ctx_dref = *pvt_ctx;

  pthread_mutex_lock(&(pvt_ctx_dref->pivot_access_m));

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "pivot_ctx_init: oob_buf_sz: %d\n", ro->oob_buf_sz);
  }

  pvt_ctx_dref->oob_buffer = new pdlfs::OobBuffer(ro->oob_buf_sz);

  pvt_ctx_dref->rank_bins.resize(pctx.comm_sz + 1);
  pvt_ctx_dref->rank_bin_count.resize(pctx.comm_sz);
  pvt_ctx_dref->rank_bin_count_aggr.resize(pctx.comm_sz);

  pthread_mutex_unlock(&(pvt_ctx_dref->pivot_access_m));
  return 0;
}

int pivot_ctx_reset(pivot_ctx_t* pvt_ctx) {
  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));

  MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();
  assert(cur_state != MainThreadState::MT_BLOCK);

  pvt_ctx->range_min = 0;
  pvt_ctx->range_max = 0;

  std::fill(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(), 0);
  std::fill(pvt_ctx->rank_bin_count_aggr.begin(),
            pvt_ctx->rank_bin_count_aggr.end(), 0);

  pvt_ctx->oob_buffer->Reset();

  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
  return 0;
}

int pivot_ctx_destroy(pivot_ctx_t** pvt_ctx) {
  int rv = 0;

  assert(*pvt_ctx != nullptr);
  assert((*pvt_ctx)->oob_buffer != nullptr);

  delete ((*pvt_ctx)->oob_buffer);
  delete *pvt_ctx;

  *pvt_ctx = nullptr;

  return rv;
}

int pivot_calculate_safe(pivot_ctx_t* pvt_ctx, const size_t num_pivots) {
  assert(num_pivots <= pdlfs::kMaxPivots);

  int rv = 0;

  const MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();
  const MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();

  pvt_ctx->pivot_width = 0;

  assert(cur_state == MainThreadState::MT_BLOCK);

  if (prev_state == MainThreadState::MT_INIT) {
    rv = pivot_calculate_from_oobl(pvt_ctx, num_pivots);
  } else {
    rv = pivot_calculate_from_all(pvt_ctx, num_pivots);
  }

  logf(LOG_DBG2, "pvt_calc_local @ R%d, pvt width: %.2f\n", pctx.my_rank,
       pvt_ctx->pivot_width);

  if (pvt_ctx->pivot_width < 1e-3) {
    float mass_per_pivot = 1.0f / (num_pivots - 1);
    pvt_ctx->pivot_width = mass_per_pivot;

    for (int pidx = 0; pidx < num_pivots; pidx++) {
      pvt_ctx->my_pivots[pidx] = mass_per_pivot * pidx;
    }
  }

  for (int pidx = 0; pidx < num_pivots - 1; pidx++) {
    assert(pvt_ctx->my_pivots[pidx] < pvt_ctx->my_pivots[pidx + 1]);
  }

  return rv;
}

int pivot_calculate_from_oobl(pivot_ctx_t* pvt_ctx, int num_pivots) {
  int rv = 0;

  std::vector<float> oobl, oobr;

  pdlfs::OobBuffer* oob = pvt_ctx->oob_buffer;
  pvt_ctx->oob_buffer->GetPartitionedProps(oobl, oobr);

  assert(oobr.size() == 0);
  const int oobl_sz = oobl.size();

  if (oobl_sz < 2) return 0;

  const float range_min = oobl[0];
  const float range_max = oobl[oobl_sz - 1];

  pvt_ctx->my_pivots[0] = range_min;
  pvt_ctx->my_pivots[num_pivots - 1] = range_max;

  pvt_ctx->pivot_width = oobl_sz * 1.0 / num_pivots;

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
    pvt_ctx->my_pivots[pvt_idx] = (1 - frac_a) * val_a + (frac_a)*val_b;
  }

  return rv;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int pivot_calculate_from_all(pivot_ctx_t* pvt_ctx, const size_t num_pivots) {
  // XXX: assert(pivot_access_m) held

  assert(num_pivots <= pdlfs::kMaxPivots);

  pvt_ctx->my_pivot_count = num_pivots;

  const float prev_range_begin = pvt_ctx->range_min;
  const float prev_range_end = pvt_ctx->range_max;

  float range_start, range_end;
  std::vector<float> oobl, oobr;

  pdlfs::OobBuffer* oob = pvt_ctx->oob_buffer;
  pvt_ctx->oob_buffer->GetPartitionedProps(oobl, oobr);

  get_range_bounds(pvt_ctx, oobl, oobr, range_start, range_end);
  assert(range_end >= range_start);

  int oobl_sz = oobl.size(), oobr_sz = oobr.size();

  float particle_count = std::accumulate(pvt_ctx->rank_bin_count.begin(),
                                         pvt_ctx->rank_bin_count.end(), 0.f);

  int my_rank = pctx.my_rank;

  pvt_ctx->my_pivots[0] = range_start;
  pvt_ctx->my_pivots[num_pivots - 1] = range_end;

  particle_count += (oobl_sz + oobr_sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (num_pivots - 1);

  if (part_per_pivot < 1e-5) {
    std::fill(pvt_ctx->my_pivots, pvt_ctx->my_pivots + num_pivots, 0);
    pvt_ctx->pivot_width = 0;
    return 0;
  }

  /**********************/
  std::vector<float>& ff = pvt_ctx->rank_bin_count;
  std::vector<float>& gg = pvt_ctx->rank_bins;
  fprintf(stderr,
          "rank%d get_local_pivots state_dump "
          "oob_count_left: %d, oob_count_right: %d\n"
          "pivot range: (%.1f %.1f), particle_cnt: %.1f\n"
          "rbc: %s (%zu)\n"
          "bin: %s (%zu)\n",
          pctx.my_rank, oobl_sz, oobr_sz, range_start, range_end,
          particle_count, print_vec(rs_pb_buf, PRINTBUF_LEN, ff, ff.size()),
          ff.size(), print_vec(rs_pb_buf2, PRINTBUF_LEN, gg, gg.size()),
          gg.size());
  /**********************/

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

    pvt_ctx->my_pivots[cur_pivot] = (1 - frac_a) * val_a + (frac_a)*val_b;
    cur_pivot++;

    oob_idx = accumulated_ppp;
  }

  int bin_idx = 0;

  for (int bidx = 0; bidx < pvt_ctx->rank_bins.size() - 1; bidx++) {
    const double cur_bin_total = pvt_ctx->rank_bin_count[bidx];
    double cur_bin_left = pvt_ctx->rank_bin_count[bidx];

    double bin_start = pvt_ctx->rank_bins[bidx];
    double bin_end = pvt_ctx->rank_bins[bidx + 1];
    const double bin_width_orig = bin_end - bin_start;

    while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
      double take_from_bin = part_per_pivot - particles_carried_over;

      /* advance bin_start st take_from_bin is removed */
      double bin_width_left = bin_end - bin_start;
      double width_to_remove = take_from_bin / cur_bin_total * bin_width_orig;

      bin_start += width_to_remove;
      pvt_ctx->my_pivots[cur_pivot] = bin_start;

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
    pvt_ctx->my_pivots[cur_pivot++] = next_pivot;
    oob_idx = next_idx;
  }

  if (cur_pivot == num_pivots) {
    assert(fabs(particles_carried_over) < 1e-1);
  } else if (cur_pivot == num_pivots - 1) {
    assert(fabs(particles_carried_over) < 1e-1 or
           fabs(particles_carried_over - part_per_pivot) < 1e-1);
  } else {
    /* shouldn't happen */
    assert(false);
  }

  pvt_ctx->my_pivots[num_pivots - 1] = range_end;
  pvt_ctx->pivot_width = part_per_pivot;

  return 0;
}

int pivot_update_pivots(pivot_ctx_t* pvt_ctx, float* pivots, int num_pivots) {
  /* Assert LockHeld */
  assert(num_pivots == pctx.comm_sz + 1);

  perfstats_log_mypivots(&(pctx.perf_ctx), pivots, num_pivots,
                         "RENEG_AGGR_PIVOTS");

  float& pvtbeg = pvt_ctx->range_min;
  float& pvtend = pvt_ctx->range_max;

  float updbeg = pivots[0];
  float updend = pivots[num_pivots - 1];

  // pvt_ctx->range_min = pivots[0];
  // pvt_ctx->range_max = pivots[num_pivots - 1];
  assert(float_lte(updbeg, pvtbeg));
  assert(float_gte(updend, pvtend));

  pvtbeg = updbeg;
  pvtend = updend;

  pvt_ctx->last_reneg_counter = 0;

  std::copy(pivots, pivots + num_pivots, pvt_ctx->rank_bins.begin());
  std::fill(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(), 0);

  pvt_ctx->oob_buffer->SetRange(pvt_ctx->range_min, pvt_ctx->range_max);
  deltafs_plfsdir_range_update(pctx.plfshdl, pvt_ctx->range_min,
                               pvt_ctx->range_max);

  float our_bin_start = pivots[pctx.my_rank];
  float our_bin_end = pivots[pctx.my_rank + 1];
  pctx.range_backend->UpdateBounds(our_bin_start, our_bin_end);

  return 0;
}
