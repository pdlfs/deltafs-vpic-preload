#include <math.h>
#include <algorithm>
#include <numeric>

#include "common.h"
#include "preload_internal.h"
#include "range_common.h"

/**** FOR DEBUGGING ****/
#define PRINTBUF_LEN 16384

static char rs_pb_buf[16384];
static char rs_pb_buf2[16384];
static char rs_pbin_buf[16384];

static char *print_vec(char *buf, int buf_len, float *v, int vlen) {
  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.1f ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

static char *print_vec(char *buf, int buf_len, std::vector<float> &v,
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

namespace {
bool pmt_comp(const particle_mem_t &a, const particle_mem_t &b) {
  return a.indexed_prop < b.indexed_prop;
}

int get_oob_min_max(pivot_ctx_t *pvt_ctx, int &oob_min, int &oob_max) {
  int rv = 0;

  std::vector<particle_mem_t> &oobl = pvt_ctx->oob_buffer_left;
  std::vector<particle_mem_t> &oobr = pvt_ctx->oob_buffer_right;

  const int oobl_sz = pvt_ctx->oob_count_left;
  const int oobr_sz = pvt_ctx->oob_count_right;

  if (oobl_sz + oobr_sz == 0) {
    oob_min = 0;
    oob_max = 0;
    return rv;
  }

  return rv;
}

int sanitize_oob(std::vector<particle_mem_t> &oob, int oob_sz,
                 std::vector<float> &san_oobl, int &san_oobl_sz,
                 std::vector<float> &san_oobr, int &san_oobr_sz, int range_min,
                 int range_max) {
  int rv = 0;

  for (int oidx = 0; oidx < oob_sz; oidx++) {
    float pval = oob[oidx].indexed_prop;
    if (pval < range_min) {
      san_oobl.push_back(pval);
      san_oobl_sz++;
    } else if (pval > range_max) {
      san_oobr.push_back(pval);
      san_oobr_sz++;
    }
  }

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
}

int pivot_ctx_init(pivot_ctx_t *pvt_ctx) {
  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));
  pthread_mutex_lock(&(pvt_ctx->snapshot_access_m));

  pvt_ctx->rank_bins.resize(pctx.comm_sz + 1);
  pvt_ctx->rank_bin_count.resize(pctx.comm_sz);

  pvt_ctx->snapshot.rank_bins.resize(pctx.comm_sz + 1);
  pvt_ctx->snapshot.rank_bin_count.resize(pctx.comm_sz);

  pvt_ctx->oob_buffer_left.resize(RANGE_MAX_OOB_SZ);
  pvt_ctx->oob_buffer_right.resize(RANGE_MAX_OOB_SZ);

  pthread_mutex_unlock(&(pvt_ctx->snapshot_access_m));
  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
  return 0;
}

int pivot_ctx_reset(pivot_ctx_t *pvt_ctx) {
  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));

  MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();
  assert(cur_state != MainThreadState::MT_BLOCK);

  pctx.rctx.range_min = 0;
  pctx.rctx.range_max = 0;

  std::fill(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(), 0);

  pctx.rctx.oob_count_left = 0;
  pctx.rctx.oob_count_right = 0;

  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
  return 0;
}

int pivot_calculate_safe(pivot_ctx_t *pvt_ctx, const int num_pivots) {
  int rv = 0;

  pivot_calculate(pvt_ctx, num_pivots);

  logf(LOG_DBG2, "pvt_calc_local @ R%d, pvt width: %.2f\n", 
      pctx.my_rank, pvt_ctx->pivot_width);

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
int pivot_calculate(pivot_ctx_t *pvt_ctx, const int num_pivots) {
  // XXX: assert(pivot_access_m) held

  assert(num_pivots <= RANGE_MAX_PIVOTS);

  float range_start = pvt_ctx->range_min;
  // [> update the left boundary of the new range <]
  // if (oobl_sz > 0) {
  // range_start = oobl[0].indexed_prop;
  // if (particle_count < 1e-5 && oobr_sz == 0) {
  // range_end = oobl[oobl_sz - 1].indexed_prop;
  // }
  // }
  // [> update the right boundary of the new range <]
  // if (oobr_sz > 0) {
  // range_end = oobr[oobr_sz - 1].indexed_prop;
  // if (particle_count < 1e-5 && oobl_sz == 0) {
  // range_start = oobr[0].indexed_prop;
  // }
  // }
  float range_end = pvt_ctx->range_max;

  std::vector<float> oobl, oobr;

  int oobl_sz = 0, oobr_sz = 0;

  ::sanitize_oob(pvt_ctx->oob_buffer_left, pvt_ctx->oob_count_left, oobl,
                 oobl_sz, oobr, oobr_sz, range_start, range_end);
  ::sanitize_oob(pvt_ctx->oob_buffer_right, pvt_ctx->oob_count_right, oobl,
                 oobl_sz, oobr, oobr_sz, range_start, range_end);

  /* We are modifying the original state as well, but sorting does not
   * affect the correctness of pivot_ctx_t
   */
  std::sort(oobl.begin(), oobl.begin() + oobl_sz);
  std::sort(oobr.begin(), oobr.begin() + oobr_sz);

  float particle_count = std::accumulate(pvt_ctx->rank_bin_count.begin(),
                                         pvt_ctx->rank_bin_count.end(), 0.f);

  float oobl_min = oobl_sz ? oobl[0] : 0;
  float oobl_max = oobl_sz ? oobl[oobl_sz - 1] : 0;
  float oobr_min = oobr_sz ? oobr[0] : 0;
  float oobr_max = oobr_sz ? oobr[oobr_sz - 1] : 0;
  float oob_min = oobl_min < oobr_min ? oobl_min : oobl_min;
  float oob_max = oobl_max > oobr_max ? oobl_max : oobr_max;

  int my_rank = pctx.my_rank;

  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();
  MainThreadState cur_state = pvt_ctx->mts_mgr.get_state();

  if (prev_state == MainThreadState::MT_INIT) {
    range_start = oobl_sz ? oobl[0] : 0;
    range_end = oobl_sz ? oobl[oobl_sz - 1] : 0;
  } else if (particle_count > 1e-5) {
    range_start = pvt_ctx->range_min;
    range_end = pvt_ctx->range_max;
  } else {
    /* all pivots need to be zero but algorithm below handles the rest */
    range_start = 0;
    range_end = 0;
  }

  if (range_start != 0 && oobl_min != 0) {
    range_start = (range_start < oob_min) ? range_start : oob_min;
  } else {
    /* ignore whichever is zero */
    range_start = range_start + oob_min;
  }

  range_end = (range_end > oob_max) ? range_end : oob_max;

  assert(range_end >= range_start);

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
  std::vector<float> &ff = pvt_ctx->rank_bin_count;
  std::vector<float> &gg = pvt_ctx->rank_bins;
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

int pivot_state_snapshot(pivot_ctx *pvt_ctx) {
  // XXX: assert(pivot_access_m) held
  // XXX: assert(snapshot_access_m) held

  snapshot_state &snap = pvt_ctx->snapshot;
  MainThreadState prev_state = pvt_ctx->mts_mgr.get_prev_state();

  int num_ranks = pvt_ctx->rank_bins.size();
  std::copy(pvt_ctx->rank_bins.begin(), pvt_ctx->rank_bins.end(),
            snap.rank_bins.begin());
  std::copy(pvt_ctx->rank_bin_count.begin(), pvt_ctx->rank_bin_count.end(),
            snap.rank_bin_count.begin());

  snap.range_min = pvt_ctx->range_min;
  snap.range_max = pvt_ctx->range_max;

  std::vector<particle_mem_t> &oob_left = pvt_ctx->oob_buffer_left;
  std::vector<particle_mem_t> &oob_right = pvt_ctx->oob_buffer_right;

  int oob_count_left = pvt_ctx->oob_count_left;
  int oob_count_right = pvt_ctx->oob_count_right;

  std::vector<float> &snap_oob_left = snap.oob_buffer_left;
  std::vector<float> &snap_oob_right = snap.oob_buffer_right;

  snap_oob_left.resize(0);
  snap_oob_right.resize(0);

  assert(snap_oob_left.size() == 0);
  assert(snap_oob_right.size() == 0);

  float bins_min = snap.rank_bins[0];
  float bins_max = snap.rank_bins[pctx.comm_sz];

  logf(LOG_DBUG, "At Rank %d, Repartitioning OOBs using bin range %.1f-%.1f\n",
       pctx.my_rank, bins_min, bins_max);

  for (int oob_idx = 0; oob_idx < oob_count_left; oob_idx++) {
    float prop = oob_left[oob_idx].indexed_prop;

    if (prop < bins_min) {
      snap_oob_left.push_back(prop);
      // } else if (pvt_ctx->range_state_prev == range_state_t::RS_INIT) {
    } else if (prev_state == MT_INIT) {
      snap_oob_left.push_back(prop);
    } else if (prop > bins_max) {
      snap_oob_right.push_back(prop);
    } else {
      logf(LOG_DBUG,
           "Dropping OOBL item %.1f for Rank %d from pivot calc (%.1f-%.1f) "
           "(%zu %zu)\n",
           prop, pctx.my_rank, bins_min, bins_max, snap_oob_left.size(),
           snap_oob_right.size());
    }
  }

  for (int oob_idx = 0; oob_idx < oob_count_right; oob_idx++) {
    float prop = oob_right[oob_idx].indexed_prop;

    if (prop < bins_min) {
      snap_oob_left.push_back(prop);
      // } else if (pvt_ctx->range_state_prev == range_state_t::RS_INIT) {
    } else if (prev_state == MT_INIT) {
      snap_oob_left.push_back(prop);
    } else if (oob_right[oob_idx].indexed_prop > bins_max) {
      snap_oob_right.push_back(prop);
    } else {
      logf(LOG_DBUG,
           "Dropping OOBR item %.1f for Rank %d from pivot calc (%.1f-%.1f) "
           "(%zu %zu)\n",
           prop, pctx.my_rank, bins_min, bins_max, snap_oob_left.size(),
           snap_oob_right.size());
    }
  }

  return 0;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
int pivot_calculate_from_snapshot(pivot_ctx_t *pvt_ctx, const int num_pivots) {
  // XXX: assert(pivot_access_m) held
  // XXX: assert(snapshot_access_m) held

  assert(num_pivots <= RANGE_MAX_PIVOTS);

  snapshot_state &snap = pvt_ctx->snapshot;

  float range_start = snap.range_min;
  float range_end = snap.range_max;

  std::vector<float> &oobl = snap.oob_buffer_left;
  std::vector<float> &oobr = snap.oob_buffer_right;

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
  std::vector<float> &ff = snap.rank_bin_count;
  std::vector<float> &gg = snap.rank_bins;
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
