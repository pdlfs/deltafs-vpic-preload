#include <algorithm>
#include <numeric>

#include "math.h"
#include "msgfmt.h"
#include "preload_internal.h"
#include "preload_range.h"
#include "preload_shuffle.h"
#include "range_utils.h"
#include "xn_shuffle.h"

#define RANGE_DEBUG_T 1
#define RANGE_PARANOID_CHECKS 1

extern const int num_eps;

/* Forward declarations */
void take_snapshot(range_ctx_t *rctx);

void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz, bool send_to_self, int label);

void send_all_acks();

void range_collect_and_send_pivots(range_ctx_t *rctx, shuffle_ctx_t *sctx);

void recalculate_local_bins();

#define PRINTBUF_LEN 16384

char rs_pb_buf[16384];
char rs_pb_buf2[16384];
char rs_pbin_buf[16384];

char *print_state(range_state_t state) {
  switch (state) {
    case range_state_t::RS_INIT:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_INIT");
      break;
    case range_state_t::RS_READY:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_READY");
      break;
    case range_state_t::RS_RENEGO:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_RENEGO");
      break;
    case range_state_t::RS_BLOCKED:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_BLOCKED");
      break;
    case range_state_t::RS_ACK:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_ACK");
      break;
    default:
      snprintf(rs_pb_buf, PRINTBUF_LEN, "RS_UNKNOWN");
      break;
  }

  return rs_pb_buf;
}

char *print_pivots(preload_ctx_t *pctx, range_ctx_t *rctx) {
  char *start = rs_pbin_buf;
  int start_ptr = 0;

  start_ptr += snprintf(&(start[start_ptr]), PRINTBUF_LEN - start_ptr,
                        "RENEG_PIVOTS Rank %d/%d: ", pctx->my_rank,
                        rctx->pvt_round_num.load());

  int bin_vec_sz = rctx->rank_bins.size();

  for (int i = 0; i < bin_vec_sz; i++) {
    start_ptr += snprintf(&(start[start_ptr]), PRINTBUF_LEN - start_ptr, "%f ",
                          rctx->rank_bins[i]);
    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  start_ptr += snprintf(&(start[start_ptr]), PRINTBUF_LEN - start_ptr, "\n");

  return rs_pbin_buf;
}

char *print_vec(char *buf, int buf_len, float *v, int vlen) {
  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.1f ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

char *print_vec(char *buf, int buf_len, std::vector<float> &v, int vlen) {
  assert(v.size() >= vlen);

  int start_ptr = 0;

  for (int item = 0; item < vlen; item++) {
    start_ptr +=
        snprintf(&buf[start_ptr], buf_len - start_ptr, "%.2f, ", v[item]);

    if (PRINTBUF_LEN - start_ptr < 20) break;
  }

  return buf;
}

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

  std::lock_guard<std::mutex> balg(pctx->rctx.bin_access_m);

  logf(LOG_INFO, "Rank %d initiating a negotiation - past lg\n", pctx->my_rank);

  /* can proceed if READY or blocked */
  if (!(RANGE_IS_INIT(rctx) || RANGE_IS_READY(rctx))) {
    /* we are already renegotiating */
    logf(LOG_INFO,
         "Rank %d is already negotiating. Aborting"
         " self-initiated negotiation - 1.\n",
         pctx->my_rank);

    return;
  }

  /* We initiate our own renego; note that there might be
   * a renego initiated by someone else somewhere, we just don't
   * know of it but we'll handle it gracefully */

  rctx->range_state_prev = rctx->range_state;
  rctx->range_state = range_state_t::RS_RENEGO;
  rctx->ranks_responded = 0;

  /* Broadcast range state to everyone */
  std::lock_guard<std::mutex> salg(pctx->rctx.snapshot_access_m);
  range_collect_and_send_pivots(rctx, sctx);
  return;
}

/* XXX: Must acquire both range_access and snapshot_access locls before calling
 */
void range_collect_and_send_pivots(range_ctx_t *rctx, shuffle_ctx_t *sctx) {
  take_snapshot(rctx);

  get_local_pivots(rctx);

#ifdef RANGE_PARANOID_CHECKS
  assert_monotically_increasing(rctx->my_pivots, RANGE_NUM_PIVOTS);
#endif

  uint32_t msg_sz = msgfmt_nbytes_reneg_pivots(RANGE_NUM_PIVOTS);

  char pivot_msg[msg_sz];
  msgfmt_encode_reneg_pivots(pivot_msg, msg_sz, rctx->pvt_round_num.load(),
                             rctx->my_pivots, rctx->pivot_width,
                             RANGE_NUM_PIVOTS);

  /* send to all including self */
  send_all_to_all(sctx, pivot_msg, msg_sz, pctx.my_rank, pctx.comm_sz, true, 1);
}

void range_handle_reneg_pivots(char *buf, unsigned int buf_sz, int src_rank) {
  // logf(LOG_INFO, "Rank %d received RENEG PIVOTS from %d!!\n", pctx.my_rank,
  // src_rank);

  char msg_type = msgfmt_get_msgtype(buf);
  assert(msg_type == MSGFMT_RENEG_PIVOTS);

  int round_num;
  float *pivots;
  int num_pivots;
  float pivot_width;
  msgfmt_parse_reneg_pivots(buf, buf_sz, &round_num, &pivots, &pivot_width,
                            &num_pivots);

#ifdef RANGE_PARANOID_CHECKS

  for (int pvt_i = 1; pvt_i < num_pivots; pvt_i++) {
    assert(pivots[pvt_i] >= pivots[pvt_i - 1]);
  }

#endif

  int comm_sz;
  int ranks_responded;
  range_state_t state;

  /* Goal of ACKs is to guarantee that once you see a RANGE_PIVOT_R+1 message,
   * you'll never see another RANGE_PIVOT_R message. _ALL_ other message
   * reorderings are possible
   */

  {
    std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

    // if (range_state_t::RS_RENEGO != pctx.rctx.range_state) {
    range_ctx_t *rptr = &(pctx.rctx);
    if ((RANGE_IS_READY(rptr)) || (RANGE_IS_INIT(rptr)) ||
        (RANGE_IS_ACK(rptr))) {
      /* first pivot of new round */

      /* We can't enter here on BLOCKED.  */

      logf(LOG_INFO,
           "Rank %d received pivots when we were not in RENEGO phase "
           "(theirs: %d, ours: %d). Buffering (PS: %s)\n",
           // pctx.my_rank, round_num, pctx.rctx.neg_round_num.load());
           pctx.my_rank, round_num, pctx.rctx.pvt_round_num.load(),
           print_state(pctx.rctx.range_state));

      /* XXX: should be exactly equal, but we are testing for
       * "can't be less than" for now
       */
      assert(round_num >= pctx.rctx.pvt_round_num.load());

      /* Implicit reneg begin */
      std::lock_guard<std::mutex> salg(pctx.rctx.snapshot_access_m);
      pctx.rctx.range_state_prev = pctx.rctx.range_state;
      pctx.rctx.range_state = range_state_t::RS_RENEGO;
      pctx.rctx.ranks_responded = 0;
      range_collect_and_send_pivots(&(pctx.rctx), &(pctx.sctx));
    } else {
      assert(RANGE_IS_RENEGO((&(pctx.rctx))));
    }

#ifdef RANGE_DEBUG
    logf(LOG_INFO,
         "Rank %d received %d pivots: %.1f to %.1f \
      (theirs: %d, ours: %d)\n",
         pctx.my_rank, num_pivots, pivots[0], pivots[num_pivots - 1], round_num,
         pctx.rctx.pvt_round_num.load());
#endif

    /* can't ever receive pivots of next round as they
     * are separated by acks */
    if (round_num != pctx.rctx.pvt_round_num.load()) {
      ABORT("Invalid pivot order!");
    }

#ifdef RANGE_DEBUG_T
    logf(LOG_INFO,
         "RENEG_PVT Rank %d from %d: "
         "Pivots: %.1f, %.1f, %.1f, %.1f\n",
         pctx.my_rank, src_rank, pivots[0], pivots[1], pivots[2], pivots[3]);
#endif

    int our_offset = src_rank * RANGE_NUM_PIVOTS;
    std::copy(pivots, pivots + num_pivots,
              pctx.rctx.all_pivots.begin() + our_offset);
    pctx.rctx.all_pivot_widths[src_rank] = pivot_width;

#ifdef RANGE_DEBUG
    for (int i = 0; i < num_pivots; i++) {
      fprintf(stderr, "Range pivot: %.1f\n",
              pctx.rctx.all_pivots[src_rank * RANGE_NUM_PIVOTS + i]);
    }
#endif

    pctx.rctx.ranks_responded++;

    assert(pctx.rctx.ranks_responded.load() <= pctx.comm_sz);

    comm_sz = pctx.comm_sz;
    state = pctx.rctx.range_state;
    ranks_responded = pctx.rctx.ranks_responded.load();
  }

  // XXX: here be dragons. We assume that all RPC handlers are serialized
  // by the delivery thread. handlers ARE NOT thread safe w/ each other
  // they're only thread safe w/ non-RPC threads
  //
  // 3hop does not pre-empt handlers to invoke another. but if it did, we'd
  // have a problem

#ifdef RANGE_DEBUG
  logf(LOG_INFO, "Rank %d received %d pivot sets thus far\n", pctx.my_rank,
       pctx.rctx.ranks_responded.load());
#endif

  if (ranks_responded == comm_sz) {
    assert(range_state_t::RS_RENEGO == state);

#ifdef RANGE_DEBUG
    logf(LOG_INFO, "Rank %d ready to update its bins\n", pctx.my_rank);
#endif

    recalculate_local_bins();
  }
}

/***** Private functions - not in header file ******/
/* XXX: caller must not hold any locks */
void recalculate_local_bins() {
#ifdef RANGE_DEBUG
  for (int i = 0; i < pctx.rctx.all_pivots.size(); i++) {
    fprintf(stderr, "Rank %d/%d - %.1f\n", pctx.my_rank, i,
            pctx.rctx.all_pivots[i]);
  }
#endif

  std::vector<rb_item_t> rbvec;
  std::vector<float> unified_bins;
  std::vector<float> unified_bin_counts;
  std::vector<float> samples;
  std::vector<float> sample_counts;

  load_bins_into_rbvec(pctx.rctx.all_pivots, rbvec, pctx.rctx.all_pivots.size(),
                       pctx.comm_sz, RANGE_NUM_PIVOTS);
  pivot_union(rbvec, unified_bins, unified_bin_counts,
              pctx.rctx.all_pivot_widths, pctx.comm_sz);

  if (pctx.my_rank == 0) {
    fprintf(stderr, "All pvt R0: %s\n",
            print_vec(rs_pbin_buf, PRINTBUF_LEN, pctx.rctx.all_pivots,
                      pctx.rctx.all_pivots.size()));
    fprintf(stderr, "All pvw R0: %s\n",
            print_vec(rs_pbin_buf, PRINTBUF_LEN, pctx.rctx.all_pivot_widths,
                      pctx.rctx.all_pivot_widths.size()));

    fprintf(stderr, "Unified pvt R0: %s\n",
            print_vec(rs_pbin_buf, PRINTBUF_LEN, unified_bins,
                      unified_bins.size()));
    fprintf(stderr, "Unified pvc R0: %s\n",
            print_vec(rs_pbin_buf, PRINTBUF_LEN, unified_bin_counts,
                      unified_bin_counts.size()));
  }

  resample_bins_irregular(unified_bins, unified_bin_counts, samples,
                          pctx.comm_sz + 1);

  if (pctx.my_rank == 0) {
    fprintf(stderr, "Unified samples R0: %s\n",
            print_vec(rs_pbin_buf, PRINTBUF_LEN, samples, samples.size()));
  }

  { /* lock guard scope begin */
    std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

    if (range_state_t::RS_RENEGO != pctx.rctx.range_state_prev) {
      // XXX: bin counts are globally distributed in resample_bins now.
      repartition_bin_counts(pctx.rctx.rank_bins, pctx.rctx.rank_bin_count,
                             samples, sample_counts);

      std::copy(sample_counts.begin(), sample_counts.end(),
                pctx.rctx.rank_bin_count.begin());
    } else {
      std::fill(pctx.rctx.rank_bin_count.begin(),
                pctx.rctx.rank_bin_count.end(), 0);
    }

    std::copy(samples.begin(), samples.end(), pctx.rctx.rank_bins.begin());
    pctx.rctx.range_min = rbvec[0].bin_val;
    pctx.rctx.range_max = rbvec[rbvec.size() - 1u].bin_val;

    printf("%s\n", print_pivots(&pctx, &(pctx.rctx)));

    pctx.rctx.pvt_round_num++;
    pctx.rctx.range_state_prev = pctx.rctx.range_state;
    pctx.rctx.range_state = range_state_t::RS_ACK;
  } /* lock guard scope end */

  logf(LOG_INFO, "Rank %d updated its bins. Sending acks now\n", pctx.my_rank);
  assert(pctx.rctx.range_state != range_state_t::RS_READY);
  send_all_acks();
}

/* XXX: This function does not grab locks for most of its operation,
 * so it needs to be called extremely carefully. It is being designed to
 * be called from one place only - after recalculate_local_bins
 * It is basedd on the assumption that no other reneg round will be allowed if
 * current status is RS_RENEGO, and the main thread also remains blocked
 */
void send_all_acks() {
  assert(range_state_t::RS_ACK == pctx.rctx.range_state);

  shuffle_ctx_t *sctx = &(pctx.sctx);

  char buf[255];
  msgfmt_encode_ack(buf, 255, pctx.my_rank, pctx.rctx.ack_round_num);

  /* if send_to_self is false, the all_acks_rcvd condition might be
   * satisfied in this function itself. Setting it to true ensures it
   * will only become true in a handle_ack call
   */
  if (sctx->type == SHUFFLE_XN) {
    /* send to all INCLUDING self */
    // logf(LOG_INFO, "Rank %d sending RENEG ACKs to-all", pctx.my_rank);
    send_all_to_all(sctx, buf, 255, pctx.my_rank, pctx.comm_sz, true, 2);
  } else {
    ABORT("Only 3-hop shuffler supported by range-indexer");
  }

  return;
}

/* Receiving the first ACK means you've sent all your pivots, and returned
 * from range_collect_and_send_pivots. However, you might not have processed
 * even  a single pivot. It is possible that by the time you call send_all_acks
 * you might have received every other ack
 */

#define IS_NEGOTIATING(x) (range_state_t::RS_RENEGO == x.range_state)
#define IS_NOT_NEGOTIATING(x) (range_state_t::RS_RENEGO != x.range_state)

#define IS_ACK_RNUM_VALID(theirs, ours, rctx) \
  ((theirs == ours) || (theirs == ours + 1))

void range_handle_reneg_acks(char *buf, unsigned int buf_sz) {
  int srank;
  int sround_num;

  msgfmt_parse_ack(buf, buf_sz, &srank, &sround_num);

  std::lock_guard<std::mutex> balg(pctx.rctx.bin_access_m);

  logf(LOG_DBUG, "Rank %d rcvd RENEG ACK from %d for R%d/%d\n", pctx.my_rank,
       srank, sround_num, pctx.rctx.ack_round_num.load());

  int round_num = pctx.rctx.ack_round_num.load();
  int pvt_round_num = pctx.rctx.pvt_round_num.load();

  int round_delta = pvt_round_num - round_num;
  assert(round_delta >= 0 && round_delta <= 1);

  if (!IS_ACK_RNUM_VALID(sround_num, round_num, pctx.rctx)) {
    logf(LOG_ERRO,
         "Rank %d rcvd R+1 RENEG ACK from %d for R%d (ours: %d, %s)\n",
         pctx.my_rank, srank, sround_num, pctx.rctx.ack_round_num.load(),
         pctx.rctx.range_state == range_state_t::RS_RENEGO ? "RENEGO"
                                                           : "NOT RENEGO");

    ABORT("Received RENEG ACK for R+1!");
  }

  if (srank < 0 || srank >= pctx.comm_sz) {
    logf(LOG_ERRO, "Rank %d rcvd RENEG ACK from invalid rank %d.", pctx.my_rank,
         srank);

    ABORT("Invalid sender rank for ACK");
  }

  /* We may receive an ACK for Round R+1 if ACK's for R are still in flight
   * and some node has moved on to, and negotiated R+1  */
  if (sround_num == round_num + 1) {

    if (pctx.rctx.ranks_acked_next[srank]) {
      logf(LOG_ERRO, "Rank %d rcvd duplicate RENEG ACK from rank %d.",
           pctx.my_rank, srank);

      ABORT("Duplicate ACK");
    }
    pctx.rctx.ranks_acked_next[srank] = true;
    pctx.rctx.ranks_acked_count_next++;

    /* If we're still in ACK barrier R, ACK barrier R+1 should
     * never be complete, as it will always be waiting for our own
     * ACK */
    assert(pctx.rctx.ranks_acked_count_next < pctx.comm_sz);

    logf(LOG_DBUG,
         "At Rank %d, Ack UNEXPECTED_1 (%d/%d from %d)"
         " Count: %d!!\n",
         pctx.my_rank, sround_num, round_num + 1, srank,
         pctx.rctx.ranks_acked_count_next.load());

  } else if (sround_num == round_num) {

    if (pctx.rctx.ranks_acked[srank]) {
      logf(LOG_ERRO, "Rank %d rcvd duplicate RENEG ACK from rank %d.",
           pctx.my_rank, srank);

      ABORT("Duplicate ACK");
    }

    logf(LOG_DBUG, "At Rank %d, Ack Count: %d, sround_num: %d/%d from %d\n",
         pctx.my_rank, pctx.rctx.ranks_acked_count.load(), sround_num,
         round_num, srank);

    pctx.rctx.ranks_acked[srank] = true;
    pctx.rctx.ranks_acked_count++;

  } else {

    logf(LOG_ERRO, "Unexpected ACK round received at Rank %d\n", pctx.my_rank);
    ABORT("Unexpected ACK");  // paranoid check

  }

  /* All ACKs have been received */
  if (pctx.rctx.ranks_acked_count == pctx.comm_sz) {

    /* Transfer pre-ACKs for R+1 to cur-ACK array */
    std::copy(pctx.rctx.ranks_acked_next.begin(),
              pctx.rctx.ranks_acked_next.end(), pctx.rctx.ranks_acked.begin());

    /*  Reset pre-ACK array */
    std::fill(pctx.rctx.ranks_acked_next.begin(),
              pctx.rctx.ranks_acked_next.end(), false);

    pctx.rctx.ranks_acked_count = pctx.rctx.ranks_acked_count_next.load();
    pctx.rctx.ranks_acked_count_next = 0;

    pctx.rctx.ack_round_num++;

    /* Ack Round is over - whom to transfer back control to?
     * Case 1. No more renegs were started
     * > To shuffle thread (main thread)
     * Case 2. Someone already forced us into next reneg round
     * > don't do anything. next round's ACK handler will handle control xfer
     */

    if (pctx.rctx.range_state == range_state_t::RS_ACK) {
      /* Everything was blocked on us */
      pctx.rctx.range_state_prev = pctx.rctx.range_state;
      pctx.rctx.range_state = range_state_t::RS_READY;
      pctx.rctx.block_writes_cv.notify_all();
      /* Note that from here on, either a new RENEG_PIVOT or MT can "win"
       * both of which should be, (and hopefully are) okay
       */
    } else if (pctx.rctx.range_state == range_state_t::RS_RENEGO) {
      /* someone forced us into R+1. Last ACK_R handler should do nothing? */
    } else {
      ABORT("Last ACK handler saw unexpected state");
    }
  }

  return;
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
  float range_start = rctx->range_min_ss;
  float range_end = rctx->range_max_ss;

  std::vector<float> &oobl = rctx->oob_buffer_left_ss;
  std::vector<float> &oobr = rctx->oob_buffer_right_ss;

  const int oobl_sz = oobl.size();
  const int oobr_sz = oobr.size();

  std::sort(oobl.begin(), oobl.begin() + oobl_sz);
  std::sort(oobr.begin(), oobr.begin() + oobr_sz);

  float particle_count = std::accumulate(rctx->rank_bin_count_ss.begin(),
                                         rctx->rank_bin_count_ss.end(), 0.f);

  int my_rank = pctx.my_rank;

  if (rctx->range_state_prev == range_state_t::RS_INIT) {
    range_start = oobl_sz ? oobl[0] : 0;
    range_end = oobl_sz ? oobl[oobl_sz - 1] : 0;
  } else if (particle_count > 1e-5) {
    range_start = rctx->range_min_ss;
    range_end = rctx->range_max_ss;
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

  rctx->my_pivots[0] = range_start;
  rctx->my_pivots[RANGE_NUM_PIVOTS - 1] = range_end;

  particle_count += (oobl_sz + oobr_sz);

  int cur_pivot = 1;
  float part_per_pivot = particle_count * 1.0 / (RANGE_NUM_PIVOTS - 1);

  if (part_per_pivot < 1e-5) {
    std::fill(rctx->my_pivots, rctx->my_pivots + RANGE_NUM_PIVOTS, 0);
    return;
  }

  /**********************/
  std::vector<float> &ff = rctx->rank_bin_count_ss;
  std::vector<float> &gg = rctx->rank_bins_ss;
  fprintf(
      stderr,
      "rank%d get_local_pivots state_dump "
      "oob_count_left: %d, oob_count_right: %d\n"
      "pivot range: (%.1f %.1f), particle_cnt: %.1f\n"
      "rbc: %s (%zu)\n"
      "bin: %s (%zu)\n"
      "prevIsInit: %s\n",
      pctx.my_rank, oobl_sz, oobr_sz, range_start, range_end, particle_count,
      print_vec(rs_pb_buf, PRINTBUF_LEN, ff, ff.size()), ff.size(),
      print_vec(rs_pb_buf2, PRINTBUF_LEN, gg, gg.size()), gg.size(),
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
    rctx->my_pivots[cur_pivot] = oobl[cur_part_idx];
    cur_pivot++;

    oob_index = cur_part_idx + 1;
  }

  int bin_idx = 0;
  assert(rctx->range_state == range_state_t::RS_RENEGO);

  if (rctx->range_state_prev != range_state_t::RS_INIT)
    for (int bidx = 0; bidx < rctx->rank_bins_ss.size() - 1; bidx++) {
      float cur_bin_left = rctx->rank_bin_count_ss[bidx];
      float bin_start = rctx->rank_bins[bidx];
      float bin_end = rctx->rank_bins[bidx + 1];

      while (particles_carried_over + cur_bin_left >= part_per_pivot - 1e-05) {
        float take_from_bin = part_per_pivot - particles_carried_over;

        /* advance bin_start st take_from_bin is removed */
        float bin_width = bin_end - bin_start;
        float width_to_remove = take_from_bin / cur_bin_left * bin_width;

        bin_start += width_to_remove;
        rctx->my_pivots[cur_pivot] = bin_start;

        cur_pivot++;

        cur_bin_left -= take_from_bin;
        particles_carried_over = 0;
      }

      // XXX: Arbitrarily chosen threshold, may cause troubles at
      // large scales
      assert(cur_bin_left >= -1e-3);

      particles_carried_over += cur_bin_left;
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

    rctx->my_pivots[cur_pivot] = oobr[cur_part_idx];
    cur_pivot++;
    oob_index = cur_part_idx + 1;
  }

  for (; cur_pivot < RANGE_NUM_PIVOTS - 1; cur_pivot++) {
    rctx->my_pivots[cur_pivot] = rctx->my_pivots[RANGE_NUM_PIVOTS - 1];
  }

  rctx->pivot_width = part_per_pivot;
}

/* Move to shuffler? */
void send_all_to_all(shuffle_ctx_t *ctx, char *buf, uint32_t buf_sz,
                     int my_rank, int comm_sz, bool send_to_self, int label) {
  for (int drank = 0; drank < comm_sz; drank++) {
    if (!send_to_self && drank == my_rank) continue;

    xn_shuffle_priority_send(static_cast<xn_ctx_t *>(ctx->rep), buf, buf_sz,
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

  std::vector<particle_mem_t> &oob_left = rctx->oob_buffer_left;
  std::vector<particle_mem_t> &oob_right = rctx->oob_buffer_right;

  int oob_count_left = rctx->oob_count_left;
  int oob_count_right = rctx->oob_count_right;

  std::vector<float> &oob_left_ss = rctx->oob_buffer_left_ss;
  std::vector<float> &oob_right_ss = rctx->oob_buffer_right_ss;

  oob_left_ss.resize(0);
  oob_right_ss.resize(0);

  assert(oob_left_ss.size() == 0);
  assert(oob_right_ss.size() == 0);

  float bins_min = rctx->rank_bins_ss[0];
  float bins_max = rctx->rank_bins_ss[pctx.comm_sz];

  logf(LOG_DBUG, "At Rank %d, Repartitioning OOBs using bin range %.1f-%.1f\n",
          pctx.my_rank, bins_min, bins_max);

  for (int oob_idx = 0; oob_idx < oob_count_left; oob_idx++) {
    float prop = oob_left[oob_idx].indexed_prop;

    if (prop < bins_min) {
      oob_left_ss.push_back(prop);
    } else if (rctx->range_state_prev == range_state_t::RS_INIT) {
      oob_left_ss.push_back(prop);
    } else if (prop > bins_max) {
      oob_right_ss.push_back(prop);
    } else {
      logf(LOG_DBUG,
              "Dropping OOBL item %.1f for Rank %d from pivot calc (%.1f-%.1f) "
              "(%zu %zu)\n",
              prop, pctx.my_rank, bins_min, bins_max, oob_left_ss.size(),
              oob_right_ss.size());
    }
  }

  for (int oob_idx = 0; oob_idx < oob_count_right; oob_idx++) {
    float prop = oob_right[oob_idx].indexed_prop;

    if (prop < bins_min) {
      oob_left_ss.push_back(prop);
    } else if (rctx->range_state_prev == range_state_t::RS_INIT) {
      oob_left_ss.push_back(prop);
    } else if (oob_right[oob_idx].indexed_prop > bins_max) {
      oob_right_ss.push_back(prop);
    } else {
      logf(LOG_DBUG,
              "Dropping OOBR item %.1f for Rank %d from pivot calc (%.1f-%.1f) "
              "(%zu %zu)\n",
              prop, pctx.my_rank, bins_min, bins_max, oob_left_ss.size(),
              oob_right_ss.size());
    }
  }
}

