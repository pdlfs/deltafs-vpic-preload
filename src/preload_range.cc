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
  range_ctx_t *rctx = &(pctx->rctx);
  shuffle_ctx_t *sctx = &(pctx->sctx);

  if (!(rctx->range_state == range_state_t::RS_INIT) &&
      !(rctx->range_state == range_state_t::RS_READY)) {
    return;
  }

  std::lock_guard<std::mutex> balg(pctx->rctx.bin_access_m);

  if (!(rctx->range_state == range_state_t::RS_INIT) &&
      !(rctx->range_state == range_state_t::RS_READY)) {
    return;
  }

  fprintf(stderr, "==Check 333==\n");

  rctx->range_state_prev = rctx->range_state;
  rctx->range_state = range_state_t::RS_RENEGO;
  rctx->ranks_responded = 0;

  // Broadcast range state to everyone
  char msg[8];
  uint32_t msg_sz = msgfmt_encode_reneg_begin(msg, 8, pctx->my_rank);

  fprintf(stderr, "==Check 4444==\n");
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

  fprintf(stderr, "==Check 55555 %u==\n", msg_sz);

  char pivot_msg[msg_sz];
  msgfmt_encode_reneg_pivots(pivot_msg, msg_sz, rctx->my_pivots,
                             rctx->pivot_width, RANGE_NUM_PIVOTS);

  fprintf(stderr, "==Check 666666 %u==\n", msg_sz);
  /* send to all including self */
  send_all_to_all(sctx, pivot_msg, msg_sz, pctx.my_rank, pctx.comm_sz, true);
}

void range_handle_reneg_begin(char *buf, unsigned int buf_sz) {
  char msg_type = msgfmt_get_msgtype(buf);
  assert(msg_type == MSGFMT_RENEG_BEGIN);
  int reneg_rank = msgfmt_parse_reneg_begin(buf, buf_sz);

  logf(LOG_INFO, "At %d, Received RENEG initiation from Rank: %d\n",
       pctx.my_rank, reneg_rank);

  /* Cheap check without acquiring the lock */
  if (range_state_t::RS_RENEGO == pctx.rctx.range_state) {
    logf(LOG_INFO, "Rank %d received multiple RENEGO reqs. DROPPING\n",
         pctx.my_rank);
    return;
  }

  std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

  if (range_state_t::RS_RENEGO == pctx.rctx.range_state) {
    logf(LOG_INFO,
         "Rank %d - looks like we started our our RENEGO concurrently. "
         "DROPPING received RENEG request\n",
         pctx.my_rank);
    return;
  }

  pctx.rctx.range_state = range_state_t::RS_RENEGO;

  pctx.rctx.ranks_responded = 0;

  std::lock_guard<std::mutex> salg(pctx.rctx.snapshot_access_m);
  range_collect_and_send_pivots(&(pctx.rctx), &(pctx.sctx));
}

void range_handle_reneg_pivots(char *buf, unsigned int buf_sz, int src_rank) {
  logf(LOG_INFO, "Rank %d received some pivots!!\n", pctx.my_rank);

  char msg_type = msgfmt_get_msgtype(buf);
  assert(msg_type == MGFMT_RENEG_PIVOTS);

  if (range_state_t::RS_RENEGO != pctx.rctx.range_state) {
    /* panic - this should not happen. did messages arrive
     * out of order or something? */
    logf(LOG_ERRO, "Rank %d received pivots when we were not in RENEGO phase\n",
         pctx.my_rank);
    ABORT("Inconsistency panic");
  }

  float *pivots;
  int num_pivots;
  float pivot_width;
  msgfmt_parse_reneg_pivots(buf, buf_sz, &pivots, &pivot_width, &num_pivots);

  logf(LOG_INFO, "Rank %d received %d pivots: %.1f to %.1f\n", pctx.my_rank,
       num_pivots, pivots[0], pivots[num_pivots - 1]);

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

  load_bins_into_rbvec(pctx.rctx.all_pivots, rbvec, pctx.rctx.all_pivots.size(),
                       pctx.comm_sz, RANGE_NUM_PIVOTS);
  pivot_union(rbvec, unified_bins, unified_bin_counts,
              pctx.rctx.all_pivot_widths, pctx.comm_sz);
  resample_bins_irregular(unified_bins, unified_bin_counts, samples,
                          pctx.comm_sz + 1);
  for (int i = 0; i < samples.size(); i++) {
    fprintf(stderr, "RankSample %d/%d - %.1f\n", pctx.my_rank, i, samples[i]);
  }

  {
    std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);
    std::copy(samples.begin(), samples.end(), pctx.rctx.rank_bins.begin());
    std::fill(pctx.rctx.rank_bin_count.begin(), pctx.rctx.rank_bin_count.end(),
              0);
    pctx.rctx.range_min = rbvec[0].bin_val;
    pctx.rctx.range_max = rbvec[rbvec.size() - 1u].bin_val;
    fprintf(stderr, "Min: %.1f, Max: %.1f\n", pctx.rctx.range_min,
            pctx.rctx.range_max);
    pctx.rctx.range_state = range_state_t::RS_READY;
  }
  // shouldn't be more than one waiter anyway
  pctx.rctx.block_writes_cv.notify_all();
}

bool comp_particle(const particle_mem_t &a, const particle_mem_t &b) {
  return a.indexed_prop < b.indexed_prop;
}

/* Total pivots written is RANGE_NUM_PIVOTS + 2, the start and the end of the
 * range are always the first and the last pivots */
void get_local_pivots(range_ctx_t *rctx) {
  /* hopefully free of cost if already the desired size */
  std::sort(rctx->oob_buffer_left.begin(), rctx->oob_buffer_left.end(),
            comp_particle);
  std::sort(rctx->oob_buffer_right.begin(), rctx->oob_buffer_right.end(),
            comp_particle);

  if (rctx->range_state_prev == range_state_t::RS_INIT) {
    rctx->my_pivots[0] = rctx->oob_buffer_left[0].indexed_prop;
    rctx->my_pivots[RANGE_NUM_PIVOTS - 1] =
        rctx->oob_buffer_left[rctx->oob_count_left - 1].indexed_prop;
    fprintf(stderr, "range: %f %f\n", rctx->my_pivots[0],
            rctx->my_pivots[RANGE_NUM_PIVOTS - 1]);
  } else {
    rctx->my_pivots[0] = rctx->range_min_ss;
    rctx->my_pivots[RANGE_NUM_PIVOTS - 1] = rctx->range_max_ss;
  }

  // Compute total particles
  int particle_count = std::accumulate(rctx->rank_bin_count_ss.begin(),
                                       rctx->rank_bin_count_ss.end(), 0);

  particle_count += rctx->oob_count_left + rctx->oob_count_right;

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (RANGE_NUM_PIVOTS - 1);
  fprintf(stderr, "ppp: %f\n", part_per_pivot);

  float accumulated_ppp = 0;
  float particles_carried_over = 0;

  int oob_index = 0;
  while (1) {
    int part_left = rctx->oob_count_left - oob_index;
    if (part_left < part_per_pivot) {
      particles_carried_over += part_left;
      break;
    }

    accumulated_ppp += part_per_pivot;
    int cur_part_idx = round(accumulated_ppp);
    rctx->my_pivots[cur_pivot] =
        rctx->oob_buffer_left[cur_part_idx].indexed_prop;
    cur_pivot++;
    oob_index = cur_part_idx + 1;
  }

  int bin_idx = 0;
  assert(rctx->range_state == range_state_t::RS_RENEGO);

  /* If we're initializing, no bins currently negotiated */
  if (rctx->range_state_prev != range_state_t::RS_INIT)
    for (int bidx = 0; bidx < rctx->rank_bins_ss.size() - 1; bidx++) {
      if (particles_carried_over + rctx->rank_bin_count_ss[bidx] >=
          part_per_pivot) {
        int count_cur = part_per_pivot - particles_carried_over;
        int count_next = rctx->rank_bin_count_ss[bidx] - count_cur;

        double bin_start = rctx->rank_bins[bidx];
        double bin_end = rctx->rank_bins[bidx + 1];
        double bin_width = (bin_end - bin_start);
        double bin_count_total = rctx->rank_bin_count_ss[bidx];

        rctx->my_pivots[cur_pivot++] =
            bin_start + (bin_width * count_cur / bin_count_total);

        particles_carried_over = count_next;
        accumulated_ppp += part_per_pivot;
      } else {
        particles_carried_over += rctx->rank_bin_count_ss[bidx];
      }
    }

  oob_index = 0;
  /* XXX: There is a minor bug here, part_left should be computed using
   * the un-rounded accumulated_ppp, not oob_index
   */
  while (1) {
    int part_left = rctx->oob_count_right - oob_index;
    if (part_left < part_per_pivot) {
      particles_carried_over += part_left;
      break;
    }

    accumulated_ppp += part_per_pivot;
    int cur_part_idx = round(accumulated_ppp);
    rctx->my_pivots[cur_pivot] =
        rctx->oob_buffer_right[cur_part_idx].indexed_prop;
    cur_pivot++;
    oob_index = cur_part_idx + 1;
  }

  rctx->pivot_width = part_per_pivot;

  for (int pvt = 0; pvt < RANGE_NUM_PIVOTS; pvt++) {
    fprintf(stderr, "pivot %d: %.2f\n", pvt, rctx->my_pivots[pvt]);
  }
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

