#include <algorithm>
#include <numeric>

#include "math.h"
#include "msgfmt.h"
#include "preload_internal.h"
#include "preload_range.h"
#include "preload_shuffle.h"
#include "xn_shuffler.h"

extern const int num_eps;

/* Forward declarations */
void take_snapshot(range_ctx_t *rctx);
void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz);

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

  fprintf(stderr, "==Check 1==\n");

  assert((rctx->range_state == range_state_t::RS_INIT) ||
         (rctx->range_state == range_state_t::RS_READY));

  fprintf(stderr, "==Check 22==\n");

  take_snapshot(rctx);

  fprintf(stderr, "==Check 333==\n");

  rctx->range_state_prev = rctx->range_state;
  rctx->range_state = range_state_t::RS_RENEGO;

  // Broadcast range state to everyone
  char msg[8];
  uint32_t msg_sz = msgfmt_begin_reneg(msg, 8, pctx->my_rank);

  fprintf(stderr, "==Check 4444==\n");
  if (sctx->type == SHUFFLE_XN) {
    send_all_to_all(sctx, msg, msg_sz, pctx->my_rank, pctx->comm_sz);
  } else {
    ABORT("Only 3-hop shuffler supported by range-indexer");
  }

  // Broadcast bins to everyone
  get_local_pivots(rctx);
  msg_sz = msgfmt_pivot_bytes(RANGE_NUM_PIVOTS);

  fprintf(stderr, "==Check 55555 %u==\n", msg_sz);

  char pivot_msg[msg_sz];
  msgfmt_encode_pivots(pivot_msg, msg_sz, rctx->my_pivots);

  fprintf(stderr, "==Check 666666 %u==\n", msg_sz);
  // send_all_to_all(sctx, pivot_msg, msg_sz, pctx->my_rank, pctx->comm_sz);
  // Collect bins from everyone
  std::unique_lock<std::mutex> ulock(rctx->block_writes_m);
  rctx->block_writes_cv.notify_one();
  return;
}

/***** Private functions - not in header file ******/
bool comp_particle(const particle_mem_t &a, const particle_mem_t &b) {
  return a.indexed_prop < b.indexed_prop;
}

/* Total pivots written is RANGE_NUM_PIVOTS + 2, the start and the end of the
 * range are always the first and the last pivots */
void get_local_pivots(range_ctx_t *rctx) {
  /* hopefully free of cost if already the desired size */
  rctx->my_pivots.resize(RANGE_NUM_PIVOTS);

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

  for (int pvt = 0; pvt < RANGE_NUM_PIVOTS; pvt++) {
    fprintf(stderr, "pivot %d: %.2f\n", pvt, rctx->my_pivots[pvt]);
  }
  fprintf(stderr, "\n");
}

/* Move to shuffler? */
void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz) {
  for (int drank = 0; drank < comm_sz; drank++) {
    if (drank == my_rank) continue;
    xn_shuffler_priority_send(static_cast<xn_ctx_t *>(ctx->rep), buf, buf_sz,
                              num_eps - 1, drank, my_rank);
  }

  return;
}

void take_snapshot(range_ctx_t *rctx) {
  assert(rctx->snapshot_in_progress == false);

  rctx->snapshot_in_progress = true;

  asm volatile("mfence" ::: "memory");

  int num_ranks = rctx->rank_bins.size();
  std::copy(rctx->rank_bins.begin(), rctx->rank_bins.end(),
            rctx->rank_bins_ss.begin());
  std::copy(rctx->rank_bin_count.begin(), rctx->rank_bin_count.end(),
            rctx->rank_bin_count_ss.begin());

  rctx->range_min_ss = rctx->range_min;
  rctx->range_max_ss = rctx->range_max;

  asm volatile("mfence" ::: "memory");

  rctx->snapshot_in_progress = false;
}

