#include <algorithm>
#include <numeric>

#include "math.h"
#include "msgfmt.h"
#include "preload_internal.h"
#include "preload_range.h"
#include "preload_shuffle.h"
#include "range_utils.h"
#include "xn_shuffler.h"

extern const int num_eps;

/* Forward declarations */
void take_snapshot(range_ctx_t *rctx);
void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz, bool send_to_self = false);
void range_collect_and_send_pivots(range_ctx_t *rctx, shuffle_ctx_t *sctx);
void recalculate_local_bins();

/* XXX: Make sure concurrent runs with delivery thread
 * are correct - acquire some shared lock in the beginning
 * This should ideally happen in its own thread so that all
 * renego attempts are serialized. After we acquire a lock
 * for our_rank directed reneg, we should make sure that the
 * requirement for it still exists by checking the oob_buf
 * sizes. */

void range_init_negotiation(preload_ctx_t *pctx) {
  logf(LOG_INFO, "Rank %d is initiating a negotiation\n", pctx->my_rank);

  range_ctx_t *rctx = &(pctx->rctx);
  shuffle_ctx_t *sctx = &(pctx->sctx);

  if (RANGE_IS_RENEGO(rctx)) {
    /* we are already renegotiating */
    logf(LOG_INFO,
         "Rank %d is already negotiating. Aborting"
         " self-initiated negotiation.\n",
         pctx->my_rank);

    return;
  }

  /* We initiate our own renego; note that there might be
   * a renego initiated by someone else, we just don't
   * know of it */

  std::lock_guard<std::mutex> balg(pctx->rctx.bin_access_m);

  // if (!(rctx->range_state == range_state_t::RS_INIT) &&
  // !(rctx->range_state == range_state_t::RS_READY)) {
  // return;
  // }
  if (RANGE_IS_RENEGO(rctx)) {
    /* we are already renegotiating */
    logf(LOG_INFO,
         "Rank %d is already negotiating. Aborting"
         " self-initiated negotiation.\n",
         pctx->my_rank);
    return;
  }

  rctx->range_state_prev = rctx->range_state;
  rctx->range_state = range_state_t::RS_RENEGO;
  rctx->ranks_responded = 0;

  // Broadcast range state to everyone
  char msg[12];
  uint32_t msg_sz =
      msgfmt_encode_reneg_begin(msg, 12, rctx->neg_round_num, pctx->my_rank);

  if (sctx->type == SHUFFLE_XN) {
    /* send to all NOT including self */
    send_all_to_all(sctx, msg, msg_sz, pctx->my_rank, pctx->comm_sz);
  } else {
    ABORT("Only 3-hop shuffler supported by range-indexer");
  }

  std::lock_guard<std::mutex> salg(pctx->rctx.snapshot_access_m);
  range_collect_and_send_pivots(rctx, sctx);
  /* After our own receiving thread receives all pivots, it will
   * wake up the main thread */
  return;
}

/* XXX: Must acquire both range_access and snapshot_access locls before calling
 */
void range_collect_and_send_pivots(range_ctx_t *rctx, shuffle_ctx_t *sctx) {
  take_snapshot(rctx);

  get_local_pivots(rctx);

  uint32_t msg_sz = msgfmt_nbytes_reneg_pivots(RANGE_NUM_PIVOTS);

  char pivot_msg[msg_sz];
  msgfmt_encode_reneg_pivots(pivot_msg, msg_sz, rctx->neg_round_num,
                             rctx->my_pivots, rctx->pivot_width,
                             RANGE_NUM_PIVOTS);

  /* send to all including self */
  send_all_to_all(sctx, pivot_msg, msg_sz, pctx.my_rank, pctx.comm_sz, true);
}

void range_handle_reneg_begin(char *buf, unsigned int buf_sz) {
  char msg_type = msgfmt_get_msgtype(buf);
  assert(msg_type == MSGFMT_RENEG_BEGIN);
  int round_num, reneg_rank;

  msgfmt_parse_reneg_begin(buf, buf_sz, &round_num, &reneg_rank);

  logf(LOG_INFO,
       "At %d, Received RENEG initiation from Rank %d (theirs %d, ours %d)\n",
       pctx.my_rank, reneg_rank, round_num, pctx.rctx.neg_round_num);

  /* Cheap check without acquiring the lock */
  if (range_state_t::RS_RENEGO == pctx.rctx.range_state) {
    logf(LOG_INFO,
         "Rank %d received multiple RENEGO reqs. DROPPING"
         " (theirs: %d, ours: %d)\n",
         pctx.my_rank, round_num, pctx.rctx.neg_round_num);
    return;
  }

  std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

  if (range_state_t::RS_RENEGO == pctx.rctx.range_state) {
    logf(LOG_INFO,
         "Rank %d - looks like we started our our RENEGO concurrently. "
         "DROPPING received RENEG request (theirs: %d, ours:  %d)\n",
         pctx.my_rank, round_num, pctx.rctx.neg_round_num);
    return;
  }

  pctx.rctx.range_state = range_state_t::RS_RENEGO;

  pctx.rctx.ranks_responded = 0;

  std::lock_guard<std::mutex> salg(pctx.rctx.snapshot_access_m);
  range_collect_and_send_pivots(&(pctx.rctx), &(pctx.sctx));
}

void range_handle_reneg_pivots(char *buf, unsigned int buf_sz, int src_rank) {
  logf(LOG_INFO, "Rank %d received some pivots from %d!!\n", pctx.my_rank,
       src_rank);

  char msg_type = msgfmt_get_msgtype(buf);
  assert(msg_type == MSGFMT_RENEG_PIVOTS);

  int round_num;
  float *pivots;
  int num_pivots;
  float pivot_width;
  msgfmt_parse_reneg_pivots(buf, buf_sz, &round_num, &pivots, &pivot_width,
                            &num_pivots);

  if (range_state_t::RS_RENEGO != pctx.rctx.range_state) {
    /* panic - this should not happen. did messages arrive
     * out of order or something? */
    logf(LOG_ERRO,
         "Rank %d received pivots when we were not in RENEGO phase "
         "(theirs: %d, ours: %d)\n",
         pctx.my_rank, round_num, pctx.rctx.neg_round_num);
    ABORT("Inconsistency panic");
  }

  logf(LOG_INFO,
       "Rank %d received %d pivots: %.1f to %.1f \
      (theirs: %d, ours: %d)\n",
       pctx.my_rank, num_pivots, pivots[0], pivots[num_pivots - 1], round_num,
       pctx.rctx.neg_round_num);

  logf(LOG_INFO, "Pivots: %.1f, %.1f, %.1f, %.1f\n", pivots[0], pivots[1],
       pivots[2], pivots[3]);

  int our_offset = src_rank * RANGE_NUM_PIVOTS;
  std::copy(pivots, pivots + num_pivots,
            pctx.rctx.all_pivots.begin() + our_offset);
  pctx.rctx.all_pivot_widths[src_rank] = pivot_width;

  for (int i = 0; i < num_pivots; i++) {
    fprintf(stderr, "Range pivot: %.1f\n",
            pctx.rctx.all_pivots[src_rank * RANGE_NUM_PIVOTS + i]);
  }
  pctx.rctx.ranks_responded++;
  logf(LOG_INFO, "Rank %d received %d pivot sets thus far\n", pctx.my_rank,
       pctx.rctx.ranks_responded.load());
  if (pctx.rctx.ranks_responded == pctx.comm_sz) {
    logf(LOG_INFO, "Rank %d ready to update its bins\n", pctx.my_rank);
    recalculate_local_bins();
  }
}

/***** Private functions - not in header file ******/
/* XXX: caller must not hold any locks */
void recalculate_local_bins() {
  for (int i = 0; i < pctx.rctx.all_pivots.size(); i++) {
    fprintf(stderr, "Rank %d/%d - %.1f\n", pctx.my_rank, i,
            pctx.rctx.all_pivots[i]);
  }

  std::vector<rb_item_t> rbvec;
  std::vector<float> unified_bins;
  std::vector<float> unified_bin_counts;
  std::vector<float> samples;
  std::vector<float> sample_counts;

  load_bins_into_rbvec(pctx.rctx.all_pivots, rbvec, pctx.rctx.all_pivots.size(),
                       pctx.comm_sz, RANGE_NUM_PIVOTS);
  pivot_union(rbvec, unified_bins, unified_bin_counts,
              pctx.rctx.all_pivot_widths, pctx.comm_sz);
  resample_bins_irregular(unified_bins, unified_bin_counts, samples,
                          pctx.comm_sz + 1);

  for (int i = 0; i < samples.size(); i++) {
    fprintf(stderr, "RankSample %d/%d - %.1f\n", pctx.my_rank, i, samples[i]);
  }

  { /* lock guard scope begin */
    std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

    if (range_state_t::RS_RENEGO != pctx.rctx.range_state_prev) {
      repartition_bin_counts(pctx.rctx.rank_bins, pctx.rctx.rank_bin_count,
                             samples, sample_counts);

      fprintf(stderr, "===>%d  SAMPLE CNTS1: %.1f %.1f %.1f\n", pctx.my_rank,
              pctx.rctx.rank_bins[0], pctx.rctx.rank_bins[1],
              pctx.rctx.rank_bins[2]);
      fprintf(stderr, "===>%d  SAMPLE CNTS2: %.1f %.1f\n", pctx.my_rank,
              pctx.rctx.rank_bin_count[0], pctx.rctx.rank_bin_count[1]);
      fprintf(stderr, "===>%d  SAMPLE CNTS3: %.1f %.1f %.1f\n", pctx.my_rank,
              samples[0], samples[1], samples[2]);
      fprintf(stderr, "===>%d  SAMPLE CNTS4: %.1f %.1f\n", pctx.my_rank,
              sample_counts[0], sample_counts[1]);

      std::copy(sample_counts.begin(), sample_counts.end(),
                pctx.rctx.rank_bin_count.begin());
    } else {
      std::fill(pctx.rctx.rank_bin_count.begin(),
                pctx.rctx.rank_bin_count.end(), 0);
    }

    std::copy(samples.begin(), samples.end(), pctx.rctx.rank_bins.begin());
    // std::fill(pctx.rctx.rank_bin_count.begin(),
    // pctx.rctx.rank_bin_count.end(), 0);

    // fprintf(stderr, "Bin counts r%d: %.1f %.1f\n", pctx.my_rank,
    // sample_counts[0], sample_counts[1]);
    pctx.rctx.range_min = rbvec[0].bin_val;
    pctx.rctx.range_max = rbvec[rbvec.size() - 1u].bin_val;
    fprintf(stderr, "Min: %.1f, Max: %.1f\n", pctx.rctx.range_min,
            pctx.rctx.range_max);
    std::vector<float> &f = pctx.rctx.rank_bins;
    // fprintf(stderr, "RankSample%d, %.1f %.1f %.1f\n", pctx.my_rank, f[0],
    // f[1], f[2]);
    pctx.rctx.neg_round_num++;
    pctx.rctx.range_state_prev = pctx.rctx.range_state;
    pctx.rctx.range_state = range_state_t::RS_READY;
  } /* lock guard scope end */

  // shouldn't be more than one waiter anyway
  pctx.rctx.block_writes_cv.notify_all();
}

bool comp_particle(const particle_mem_t &a, const particle_mem_t &b) {
  return a.indexed_prop < b.indexed_prop;
}

/* This function is supposed to produce all zeroes if there are no
 * particles with the current rank (either pre-shuffled or OOB'ed)
 * and is supposed to produce valid pivots in every single other
 * case even if there's only one pivot. XXX: We're not sure whether
 * that's currently the case
 * */
void get_local_pivots(range_ctx_t *rctx) {
  // std::sort(rctx->oob_buffer_left.begin(), rctx->oob_buffer_left.end(),
  // comp_particle);
  // std::sort(rctx->oob_buffer_right.begin(), rctx->oob_buffer_right.end(),
  // comp_particle);

  float range_start = rctx->range_min_ss;
  float range_end = rctx->range_max_ss;

  std::vector<particle_mem_t> &oobl = rctx->oob_buffer_left;
  std::vector<particle_mem_t> &oobr = rctx->oob_buffer_right;

  const int oobl_sz = rctx->oob_count_left;
  const int oobr_sz = rctx->oob_count_right;

  std::sort(oobl.begin(), oobl.begin() + oobl_sz, comp_particle);
  std::sort(oobr.begin(), oobr.begin() + oobr_sz, comp_particle);

  // Compute total particles
  float particle_count = std::accumulate(rctx->rank_bin_count_ss.begin(),
                                         rctx->rank_bin_count_ss.end(), 0.f);

  int my_rank = pctx.my_rank;

  if (rctx->range_state_prev == range_state_t::RS_INIT) {
    range_start = oobl_sz ? oobl[0].indexed_prop : 0;
    range_end = oobl_sz ? oobl[oobl_sz - 1].indexed_prop : 0;
  } else if (particle_count > 1e-5) {
    range_start = rctx->range_min_ss;
    range_end = rctx->range_max_ss;
    // range_start = rctx->rank_bins_ss[my_rank];
    // range_end = (my_rank + 1 == pctx.comm_sz) ? rctx->range_max_ss
                                              // : rctx->rank_bins_ss[my_rank + 1];
  } else {
    /* all pivots need to be zero but algorithm below handles the rest */
    range_start = 0;
    range_end = 0;
  }

  /* update the left boundary of the new range */
  if (oobl_sz > 0) range_start = oobl[0].indexed_prop;
  /* update the right boundary of the new range */
  if (oobr_sz > 0) range_end = oobr[oobr_sz - 1].indexed_prop;

  rctx->my_pivots[0] = range_start;
  rctx->my_pivots[RANGE_NUM_PIVOTS - 1] = range_end;

  logf(LOG_INFO, "Rank %d: Pivot range  (%.1f, %.1f)\n", pctx.my_rank,
       range_start, range_end);

  fprintf(stderr, "r%d ptclcnt: %f\n", pctx.my_rank, particle_count);
  particle_count += (oobl_sz + oobr_sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (RANGE_NUM_PIVOTS - 1);
  fprintf(stderr, "ppp: %f\n", part_per_pivot);

  if (part_per_pivot < 1e-5) {
    std::fill(rctx->my_pivots, rctx->my_pivots + RANGE_NUM_PIVOTS, 0);
    fprintf(stderr, "rank %d, filling zeroes\n", pctx.my_rank);
    return;
  }

  for (int i = 0; i < rctx->oob_count_right; i++) {
    fprintf(stderr, "rank%d ptcl %.1f\n", pctx.my_rank,
            rctx->oob_buffer_right[i].indexed_prop);
  }

  /**********************/
  std::vector<float> &ff = rctx->rank_bin_count_ss;
  std::vector<float> &gg = rctx->rank_bins_ss;
  fprintf(
      stderr,
      "rank%d get_local_pivots state_dump "
      "oob_count_left: %d, oob_count_right: %d\n"
      "pivot range: (%.1f %.1f), particle_cnt: %.1f\n"
      "rbc: %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f (%zu)\n"
      "bin: %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f %.1f (%zu)\n"
      "prevIsInit: %s\n",
      pctx.my_rank, oobl_sz, oobr_sz, range_start, range_end, particle_count,
      ff[0], ff[1], ff[2], ff[3], ff[4], ff[5], ff[6], ff[7], ff[8], ff.size(),
      gg[0], gg[1], gg[2], gg[3], gg[4], gg[5], gg[6], gg[7], gg[8], gg[9],
      gg.size(),
      (rctx->range_state_prev == range_state_t::RS_INIT) ? "true" : "false");
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
    rctx->my_pivots[cur_pivot] = oobl[cur_part_idx].indexed_prop;
    cur_pivot++;
    fprintf(stderr, "r%d +> curpivotA %d\n", pctx.my_rank, cur_pivot);
    oob_index = cur_part_idx + 1;
  }

  int bin_idx = 0;
  assert(rctx->range_state == range_state_t::RS_RENEGO);

  /* If we're initializing, no bins currently negotiated */
  // if (rctx->range_state_prev != range_state_t::RS_INIT)
  // for (int bidx = 0; bidx < rctx->rank_bins_ss.size() - 1; bidx++) {
  // if (particles_carried_over + rctx->rank_bin_count_ss[bidx] >=
  // part_per_pivot) {
  // float count_cur = part_per_pivot - particles_carried_over;
  // float count_next = rctx->rank_bin_count_ss[bidx] - count_cur;

  // float bin_start = rctx->rank_bins[bidx];
  // float bin_end = rctx->rank_bins[bidx + 1];
  // float bin_width = (bin_end - bin_start);
  // float bin_count_total = rctx->rank_bin_count_ss[bidx];

  // rctx->my_pivots[cur_pivot] =
  // bin_start + (bin_width * count_cur / bin_count_total);
  // fprintf(stderr,
  // "r%d +> curpivotB %d - %.1f %.1f %.1f %.1f (%.1f, %.1f)\n",
  // pctx.my_rank, cur_pivot, count_cur, count_next, bin_start,
  // bin_end, particles_carried_over, rctx->rank_bin_count_ss[bidx]);
  // cur_pivot++;

  // particles_carried_over = count_next;
  // accumulated_ppp += part_per_pivot;
  // } else {
  // particles_carried_over += rctx->rank_bin_count_ss[bidx];
  // }
  // }
  fprintf(stderr, "r%d ptcls carried over: %.1f\n", pctx.my_rank,
          particles_carried_over);

  if (rctx->range_state_prev != range_state_t::RS_INIT)
    for (int bidx = 0; bidx < rctx->rank_bins_ss.size() - 1; bidx++) {
      float cur_bin_left = rctx->rank_bin_count_ss[bidx];
      float bin_start = rctx->rank_bins[bidx];
      float bin_end = rctx->rank_bins[bidx + 1];

      fprintf(stderr, "rank%d bin (%.1f-%.1f): left %.1f\n", pctx.my_rank,
              bin_start, bin_end, cur_bin_left);

      while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
        float take_from_bin = part_per_pivot - particles_carried_over;

        /* advance bin_start st take_from_bin is removed */
        float bin_width = bin_end - bin_start;
        float width_to_remove = take_from_bin / cur_bin_left * bin_width;

        bin_start += width_to_remove;
        rctx->my_pivots[cur_pivot] = bin_start;

        fprintf(stderr, "rank %d pivotFoundB: %.1f %.1f %.1f\n", pctx.my_rank, bin_width, cur_bin_left, take_from_bin);
        fprintf(stderr, "rank %d pivotFoundBB: %.1f %.1f %.1f\n", pctx.my_rank, cur_bin_left, particles_carried_over, part_per_pivot);
        fprintf(stderr, "rank%d pivotFoundB[%d]: %.1f\n", pctx.my_rank,
                cur_pivot, bin_start);

        cur_pivot++;

        cur_bin_left -= take_from_bin;
        particles_carried_over = 0;
      }

      assert(cur_bin_left >= -1e-5);
      particles_carried_over += cur_bin_left;
      fprintf(stderr, "r%d ptcls carried over: %.1f\n", pctx.my_rank,
              particles_carried_over);
    }

  oob_index = 0;
  /* XXX: There is a minor bug here, part_left should be computed using
   * the un-rounded accumulated_ppp, not oob_index
   */

  fprintf(stderr, "r%d ptcls carried over: %.1f\n", pctx.my_rank,
          particles_carried_over);
  while (1) {
    int part_left = oobr_sz - oob_index;
    if (part_per_pivot < 1e-5 ||
        part_left + particles_carried_over < part_per_pivot - 1e-5) {
      particles_carried_over += part_left;
      break;
    }

    float next_idx = oob_index + part_per_pivot - particles_carried_over;

    particles_carried_over = 0;

    // accumulated_ppp += part_per_pivot;
    // int cur_part_idx = round(accumulated_ppp);
    int cur_part_idx = round(next_idx);
    if (cur_part_idx >= oobr_sz) cur_part_idx = oobr_sz - 1;

    rctx->my_pivots[cur_pivot] = oobr[cur_part_idx].indexed_prop;
    fprintf(stderr, "rank%d pivotFoundC[%d], oobr_idx: %d\n", pctx.my_rank,
            cur_pivot, cur_part_idx);
    cur_pivot++;
    oob_index = cur_part_idx + 1;
  }

  rctx->pivot_width = part_per_pivot;

  for (int pvt = 0; pvt < RANGE_NUM_PIVOTS; pvt++) {
    fprintf(stderr, "r%d pivot %d: %.2f\n", pctx.my_rank, pvt,
            rctx->my_pivots[pvt]);
  }

  fprintf(stderr, "r%d bin1: %.1f\n", pctx.my_rank, rctx->rank_bin_count_ss[0]);
  fprintf(stderr, "r%d bin2: %.1f\n", pctx.my_rank, rctx->rank_bin_count_ss[1]);
  fprintf(stderr, "\n");
}

/* Move to shuffler? */
void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz, bool send_to_self) {
  for (int drank = 0; drank < comm_sz; drank++) {
    if (!send_to_self && drank == my_rank) continue;
    fprintf(stderr, "All to all from %d to %d\n", my_rank, drank);
    xn_shuffler_priority_send(static_cast<xn_ctx_t *>(ctx->rep), buf, buf_sz,
                              num_eps - 1, drank, my_rank);
  }

  return;
}

/* Needs to be executed under adequate locking */
void take_snapshot(range_ctx_t *rctx) {
  int num_ranks = rctx->rank_bins.size();
  std::copy(rctx->rank_bins.begin(), rctx->rank_bins.end(),
            rctx->rank_bins_ss.begin());
  std::copy(rctx->rank_bin_count.begin(), rctx->rank_bin_count.end(),
            rctx->rank_bin_count_ss.begin());

  rctx->range_min_ss = rctx->range_min;
  rctx->range_max_ss = rctx->range_max;
}

