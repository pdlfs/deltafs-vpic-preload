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

  range_end = std::max(oob_max, range_end);

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

int pivot_ctx_init(pivot_ctx_t* pvt_ctx) {
  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));
  pthread_mutex_lock(&(pvt_ctx->snapshot_access_m));

  pvt_ctx->rank_bins.resize(pctx.comm_sz + 1);
  pvt_ctx->rank_bin_count.resize(pctx.comm_sz);
  pvt_ctx->rank_bin_count_aggr.resize(pctx.comm_sz);

  pvt_ctx->snapshot.rank_bins.resize(pctx.comm_sz + 1);
  pvt_ctx->snapshot.rank_bin_count.resize(pctx.comm_sz);

  pthread_mutex_unlock(&(pvt_ctx->snapshot_access_m));
  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
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

  pvt_ctx->oob_buffer.Reset();

  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
  return 0;
}

int pivot_calculate_safe(pivot_ctx_t* pvt_ctx, const int num_pivots) {
  int rv = 0;

  pivot_calculate(pvt_ctx, num_pivots);

  logf(LOG_DBG2, "pvt_calc_local @ R%d, pvt width: %.2f\n", pctx.my_rank,
       pvt_ctx->pivot_width);

  if (pvt_ctx->pivot_width > 1e-3) return rv;

  float mass_per_pivot = 1.0f / num_pivots;
  pvt_ctx->pivot_width = mass_per_pivot;

  for (int pidx = 0; pidx <= num_pivots; pidx++) {
    pvt_ctx->my_pivots[pidx] = mass_per_pivot * pidx;
  }

  return rv;
}
/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int pivot_calculate(pivot_ctx_t* pvt_ctx, const size_t num_pivots) {
  // XXX: assert(pivot_access_m) held

  assert(num_pivots <= pdlfs::kMaxPivots);

  pvt_ctx->my_pivot_count = num_pivots;

  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();
  MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();

  float range_start, range_end;
  std::vector<float> oobl, oobr;

  pdlfs::OobBuffer& oob = pvt_ctx->oob_buffer;
  pvt_ctx->oob_buffer.GetPartitionedProps(oobl, oobr);

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

  if (part_per_pivot < 1) {
    logf(LOG_WARN,
         "pivot_calculate: ppp < 1, calculation may not proceed correctly\n");
  }

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
          "bin: %s (%zu)\n"
          "prevIsInit: %s\n",
          pctx.my_rank, oobl_sz, oobr_sz, range_start, range_end,
          particle_count, print_vec(rs_pb_buf, PRINTBUF_LEN, ff, ff.size()),
          ff.size(), print_vec(rs_pb_buf2, PRINTBUF_LEN, gg, gg.size()),
          gg.size(), (prev_state == MT_INIT) ? "true" : "false");
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

    int cur_part_idx = round(accumulated_ppp);
    if (cur_part_idx >= oobl_sz) break;

    pvt_ctx->my_pivots[cur_pivot] = oobl[cur_part_idx];
    cur_pivot++;

    oob_idx = accumulated_ppp;
  }

  int bin_idx = 0;
  assert(cur_state == MainThreadState::MT_BLOCK);

  if (prev_state != MT_INIT) {
    for (int bidx = 0; bidx < pvt_ctx->rank_bins.size() - 1; bidx++) {
      float cur_bin_left = pvt_ctx->rank_bin_count[bidx];
      float bin_start = pvt_ctx->rank_bins[bidx];
      float bin_end = pvt_ctx->rank_bins[bidx + 1];

      while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
        float take_from_bin = part_per_pivot - particles_carried_over;

        /* advance bin_start st take_from_bin is removed */
        float bin_width = bin_end - bin_start;
        float width_to_remove = take_from_bin / cur_bin_left * bin_width;

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
  }

  fprintf(stderr, "cur_pivot: %d, pco: %0.3f\n", cur_pivot,
          particles_carried_over);

  oob_idx = 0;
  /* XXX: There is a minor bug here, part_left should be computed using
   * the un-rounded accumulated_ppp, not oob_index
   */

  while (1) {
    float part_left = oobr_sz - oob_idx;
    if (part_per_pivot < 1e-5 ||
        part_left + particles_carried_over < part_per_pivot + 1e-5) {
      particles_carried_over += part_left;
      break;
    }

    float next_idx = oob_idx + part_per_pivot - particles_carried_over;

    particles_carried_over = 0;

    int cur_part_idx = round(next_idx);
    if (cur_part_idx >= oobr_sz) break;

    pvt_ctx->my_pivots[cur_pivot] = oobr[cur_part_idx];
    cur_pivot++;
    oob_idx = next_idx;
  }

  if (float_gt(particles_carried_over, 0)) {
    assert(float_eq(particles_carried_over, part_per_pivot));
    pvt_ctx->my_pivots[cur_pivot++] = range_end;
  } else {
    assert(float_eq(particles_carried_over, 0));
  }

  for (; cur_pivot < num_pivots - 1; cur_pivot++) {
    pvt_ctx->my_pivots[cur_pivot] = pvt_ctx->my_pivots[num_pivots - 1];
  }

  pvt_ctx->pivot_width = part_per_pivot;

  return 0;
}

int pivot_state_snapshot(pivot_ctx* pvt_ctx) {
  // XXX: assert(pivot_access_m) held
  // XXX: assert(snapshot_access_m) held

  snapshot_state& snap = pvt_ctx->snapshot;
  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();

  int num_ranks = pvt_ctx->rank_bins.size();
  std::copy(pvt_ctx->rank_bins.begin(), pvt_ctx->rank_bins.end(),
            snap.rank_bins.begin());
  std::copy(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(),
            snap.rank_bin_count.begin());

  snap.range_min = pvt_ctx->range_min;
  snap.range_max = pvt_ctx->range_max;

  std::vector<float>& snap_oob_left = snap.oob_buffer_left;
  std::vector<float>& snap_oob_right = snap.oob_buffer_right;

  snap_oob_left.resize(0);
  snap_oob_right.resize(0);

  assert(snap_oob_left.empty());
  assert(snap_oob_right.empty());

  float bins_min = snap.rank_bins[0];
  float bins_max = snap.rank_bins[pctx.comm_sz];

  assert(snap.range_min == bins_min);
  assert(snap.range_max == bins_max);

  logf(LOG_DBUG, "At Rank %d, Repartitioning OOBs using bin range %.1f-%.1f\n",
       pctx.my_rank, bins_min, bins_max);

  pvt_ctx->oob_buffer.GetPartitionedProps(snap_oob_left, snap_oob_right);

  return 0;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int pivot_calculate_from_snapshot(pivot_ctx_t* pvt_ctx,
                                  const size_t num_pivots) {
  // XXX: assert(pivot_access_m) held
  // XXX: assert(snapshot_access_m) held

  assert(num_pivots <= pdlfs::kMaxPivots);

  snapshot_state& snap = pvt_ctx->snapshot;

  float range_start = snap.range_min;
  float range_end = snap.range_max;

  std::vector<float>& oobl = snap.oob_buffer_left;
  std::vector<float>& oobr = snap.oob_buffer_right;

  const int oobl_sz = oobl.size();
  const int oobr_sz = oobr.size();

  std::sort(oobl.begin(), oobl.begin() + oobl_sz);
  std::sort(oobr.begin(), oobr.begin() + oobr_sz);

  float particle_count = std::accumulate(snap.rank_bin_count.begin(),
                                         snap.rank_bin_count.end(), 0.f);

  int my_rank = pctx.my_rank;

  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();
  MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();

  if (prev_state == MainThreadState::MT_INIT) {
    range_start = oobl_sz ? oobl[0] : 0;
    range_end = oobl_sz ? oobl[oobl_sz - 1] : 0;
  } else if (particle_count > 1e-5) {
    range_start = snap.range_min;
    range_end = snap.range_max;
  } else {
    /* all pivots need to be zero but algorithm below handles the rest */
    range_start = 0;
    range_end = 0;
  }

  /* update the left boundary of the new range */
  if (oobl_sz > 0) {
    range_start = oobl[0];
    if (particle_count < 1e-5 && oobr_sz == 0) {
      range_end = oobl[oobl_sz - 1];
    }
  }
  /* update the right boundary of the new range */
  if (oobr_sz > 0) {
    range_end = oobr[oobr_sz - 1];
    if (particle_count < 1e-5 && oobl_sz == 0) {
      range_start = oobr[0];
    }
  }

  assert(range_end >= range_start);

  pvt_ctx->my_pivots[0] = range_start;
  pvt_ctx->my_pivots[num_pivots - 1] = range_end;

  particle_count += (oobl_sz + oobr_sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (num_pivots - 1);

  if (part_per_pivot < 1e-5) {
    std::fill(pvt_ctx->my_pivots, pvt_ctx->my_pivots + num_pivots, 0);
    return 0;
  }

  /**********************/
  std::vector<float>& ff = snap.rank_bin_count;
  std::vector<float>& gg = snap.rank_bins;
  fprintf(stderr,
          "rank%d get_local_pivots state_dump "
          "oob_count_left: %d, oob_count_right: %d\n"
          "pivot range: (%.1f %.1f), particle_cnt: %.1f\n"
          "rbc: %s (%zu)\n"
          "bin: %s (%zu)\n"
          "prevIsInit: %s\n",
          pctx.my_rank, oobl_sz, oobr_sz, range_start, range_end,
          particle_count, print_vec(rs_pb_buf, PRINTBUF_LEN, ff, ff.size()),
          ff.size(), print_vec(rs_pb_buf2, PRINTBUF_LEN, gg, gg.size()),
          gg.size(), (prev_state == MT_INIT) ? "true" : "false");
  /**********************/

  float accumulated_ppp = 0;
  float particles_carried_over = 0;

  int oob_index = 0;
  while (1) {
    int part_left = oobl_sz - oob_index;
    if (part_per_pivot < 1e-5 || part_left < part_per_pivot) {
      particles_carried_over += part_left;
      break;
    }

    accumulated_ppp += part_per_pivot;
    int cur_part_idx = round(accumulated_ppp);
    pvt_ctx->my_pivots[cur_pivot] = oobl[cur_part_idx];
    cur_pivot++;

    oob_index = cur_part_idx + 1;
  }

  int bin_idx = 0;
  assert(cur_state == MainThreadState::MT_BLOCK);

  if (prev_state != MT_INIT) {
    for (int bidx = 0; bidx < snap.rank_bins.size() - 1; bidx++) {
      float cur_bin_left = snap.rank_bin_count[bidx];
      float bin_start = pvt_ctx->rank_bins[bidx];
      float bin_end = pvt_ctx->rank_bins[bidx + 1];

      while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
        float take_from_bin = part_per_pivot - particles_carried_over;

        /* advance bin_start st take_from_bin is removed */
        float bin_width = bin_end - bin_start;
        float width_to_remove = take_from_bin / cur_bin_left * bin_width;

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
  }

  oob_index = 0;
  /* XXX: There is a minor bug here, part_left should be computed using
   * the un-rounded accumulated_ppp, not oob_index
   */

  while (1) {
    int part_left = oobr_sz - oob_index;
    if (part_per_pivot < 1e-5 ||
        part_left + particles_carried_over < part_per_pivot - 1e-5) {
      particles_carried_over += part_left;
      break;
    }

    float next_idx = oob_index + part_per_pivot - particles_carried_over;

    particles_carried_over = 0;

    int cur_part_idx = round(next_idx);
    if (cur_part_idx >= oobr_sz) cur_part_idx = oobr_sz - 1;

    pvt_ctx->my_pivots[cur_pivot] = oobr[cur_part_idx];
    cur_pivot++;
    oob_index = cur_part_idx + 1;
  }

  for (; cur_pivot < num_pivots - 1; cur_pivot++) {
    pvt_ctx->my_pivots[cur_pivot] = pvt_ctx->my_pivots[num_pivots - 1];
  }

  pvt_ctx->pivot_width = part_per_pivot;

  return 0;
}

int pivot_update_pivots(pivot_ctx_t* pvt_ctx, float* pivots, int num_pivots) {
  /* Assert LockHeld */
  assert(num_pivots == pctx.comm_sz + 1);

  pvt_ctx->range_min = pivots[0];
  pvt_ctx->range_max = pivots[num_pivots - 1];

  pvt_ctx->last_reneg_counter = 0;

  std::copy(pivots, pivots + num_pivots, pvt_ctx->rank_bins.begin());
  std::fill(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(), 0);

  pvt_ctx->oob_buffer.SetRange(pvt_ctx->range_min, pvt_ctx->range_max);

  return 0;
}
