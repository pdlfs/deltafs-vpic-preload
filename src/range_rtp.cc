#include "range_rtp.h"
#include "msgfmt.h"

/* BEGIN internal declarations */

/**
 * @brief Initialize our peers_s1/s2/s3 (if applicable), and root_sx's
 * Only nodes that are a part of stage 2 have a root_s2, same for s1
 * This consults with Nexus to get our rank.
 * XXX: TODO: Ensure that Stage 1 is all local/shared memory nodes.
 *
 * @param rctx The RTP context
 *
 * @return 0 or errno
 */
int reneg_topology_init(reneg_ctx_t rctx);

int mock_pivots_init(reneg_ctx_t rctx);

void send_to_rank(reneg_ctx_t rctx, char *buf, int buf_sz, int drank);

void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz, int my_rank = -1);
#define send_to_all_s1(...) \
  send_to_all(rctx->peers_s1, rctx->num_peers_s1, __VA_ARGS__)
#define send_to_all_s2(...) \
  send_to_all(rctx->peers_s2, rctx->num_peers_s2, __VA_ARGS__)
#define send_to_all_s3(...) \
  send_to_all(rctx->peers_s3, rctx->num_peers_s3, __VA_ARGS__)

/**
 * @brief Directly send rtp_begin to tree root for dissemination
 *
 * @param rctx
 */
void broadcast_rtp_begin(reneg_ctx_t rctx);

void replay_rtp_begin(reneg_ctx_t rctx);

int reneg_handle_rtp_begin(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                           int src);

int reneg_handle_rtp_pivot(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                           int src);

int reneg_handle_pivot_bcast(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                             int src);
bool expected_items_for_stage(reneg_ctx_t rctx, int stage, int items);
/* END internal declarations */

int reneg_init(reneg_ctx_t rctx, shuffle_ctx_t *sctx, float *data,
               int *data_len, int data_max, pthread_mutex_t *data_mutex,
               struct reneg_opts ro) {
  xn_ctx_t *xn_sctx = NULL;
  int rv = 0;

  pthread_mutex_lock(&(rctx->reneg_mutex));

  if (rctx->state_mgr.get_state() != RenegState::INIT) {
    logf(LOG_DBUG, "reneg_init: can initialize only in init stage\n");
    rv = -1;
    goto cleanup;
  }

  if (sctx->type != SHUFFLE_XN) {
    logf(LOG_DBUG, "Only 3-hop is supported by the RTP protocol\n");
    rv = -1;
    goto cleanup;
  }

  if (data_max < RANGE_NUM_PIVOTS) {
    logf(LOG_DBUG,
         "reneg_init: data array should be at least RANGE_NUM_PIVOTS\n");
    rv = -1;
    goto cleanup;
  }

  if (ro.fanout_s1 > FANOUT_MAX) {
    logf(LOG_DBUG, "fanout_s1 exceeds FANOUT_MAX\n");
    rv = -1;
    goto cleanup;
  }

  if (ro.fanout_s2 > FANOUT_MAX) {
    logf(LOG_DBUG, "fanout_s2 exceeds FANOUT_MAX\n");
    rv = -1;
    goto cleanup;
  }

  if (ro.fanout_s3 > FANOUT_MAX) {
    logf(LOG_DBUG, "fanout_s3 exceeds FANOUT_MAX\n");
    rv = -1;
    goto cleanup;
  }

  xn_sctx = static_cast<xn_ctx_t *>(sctx->rep);

  rctx->xn_sctx = xn_sctx;
  rctx->nxp = xn_sctx->nx;

  rctx->data = data;
  rctx->data_len = data_len;
  rctx->data_mutex = data_mutex;

  rctx->round_num = 0;

  rctx->fanout_s1 = ro.fanout_s1;
  rctx->fanout_s2 = ro.fanout_s2;
  rctx->fanout_s3 = ro.fanout_s3;

  mock_pivots_init(rctx);
  reneg_topology_init(rctx);

  rctx->state_mgr.update_state(RenegState::READY);

cleanup:
  pthread_mutex_unlock(&(rctx->reneg_mutex));
  return rv;
}

int reneg_topology_init(reneg_ctx_t rctx) {
  if (rctx->state_mgr.get_state() != RenegState::INIT) {
    logf(LOG_DBUG, "reneg_topology_init: can initialize only in init stage\n");
    return -1;
  }

  int grank, gsz;
  grank = nexus_global_rank(rctx->nxp);
  gsz = nexus_global_size(rctx->nxp);

  int s1mask = ~(rctx->fanout_s1 - 1);
  int s2mask = ~(rctx->fanout_s1 * rctx->fanout_s2 - 1);
  int s3mask = ~((rctx->fanout_s1 * rctx->fanout_s2 * rctx->fanout_s3) - 1);

  int s1root = grank & s1mask;
  rctx->root_s1 = s1root;
  rctx->num_peers_s1 = 0;

  for (int pidx = 0; pidx < rctx->fanout_s1; pidx++) {
    int jump = 1;
    int peer = s1root + pidx * jump;

    if (peer >= gsz) break;

    rctx->peers_s1[pidx] = peer;
    rctx->num_peers_s1++;
  }

  int s2root = grank & s2mask;
  rctx->root_s2 = s2root;
  rctx->num_peers_s2 = 0;

  for (int pidx = 0; pidx < rctx->fanout_s2; pidx++) {
    int jump = rctx->fanout_s1;
    int peer = s2root + pidx * jump;

    if (peer >= gsz) break;

    rctx->peers_s2[pidx] = peer;
    rctx->num_peers_s2++;
  }

  int s3root = grank & s3mask;
  rctx->root_s3 = s3root;
  rctx->num_peers_s3 = 0;

  for (int pidx = 0; pidx < rctx->fanout_s3; pidx++) {
    int jump = rctx->fanout_s1 * rctx->fanout_s2;
    int peer = s3root + pidx * jump;

    if (peer >= gsz) break;

    rctx->peers_s3[pidx] = peer;
    rctx->num_peers_s3++;
  }

  rctx->my_rank = grank;
  rctx->num_ranks = gsz;

  return 0;
}

int reneg_init_round(reneg_ctx_t rctx) {
  pthread_mutex_lock(&(rctx->reneg_mutex));

  if (rctx->state_mgr.get_state() == RenegState::READY) {
    logf(LOG_DBUG, "reneg_init_round: broacasting... \n");
    broadcast_rtp_begin(rctx);
    rctx->state_mgr.update_state(RenegState::READYBLOCK);
  }

  while (rctx->state_mgr.get_state() != RenegState::READY) {
    pthread_cond_wait(&(rctx->reneg_cv), &(rctx->reneg_mutex));
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));
  return 0;
}

int reneg_handle_msg(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                     int src) {
  if (rctx == NULL) {
    logf(LOG_DBUG, "reneg_handle_msg: rctx is null!\n");
    ABORT("panic");
  }

  // logf(LOG_DBUG, "reneg_handle_msg: recvd an RTP msg at %d from %d\n",
  // rctx->my_rank, src);

  int rv = 0;
  char msg_type = msgfmt_get_rtp_msgtype(buf);

  switch (msg_type) {
    case MSGFMT_RTP_BEGIN:
      rv = reneg_handle_rtp_begin(rctx, buf, buf_sz, src);
      break;
    case MSGFMT_RTP_PIVOT:
      rv = reneg_handle_rtp_pivot(rctx, buf, buf_sz, src);
      break;
    case MSGFMT_RTP_PVT_BCAST:
      rv = reneg_handle_pivot_bcast(rctx, buf, buf_sz, src);
      break;
    default:
      ABORT("reneg_handle_msg: unknown msg_type");
      break;
  }

  return rv;
}

/**
 * @brief Handle an RTP Begin message. When we receive this message, we can
 * be:
 * 1. READY - regular case. Their round_num == our round_num
 * 2. Not READY - In the middle of a renegotiation.
 * 2a. If their_round_num == our_round_num, ignore
 * 2b. If their round_num == our_round_num + 1, buffer and replay later
 *
 * @param rctx
 * @param buf
 * @param buf_sz
 * @param src
 *
 * @return
 */
int reneg_handle_rtp_begin(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                           int src) {
  logf(LOG_DBUG, "Received RTP_BEGIN at Rank %d from %d\n", rctx->my_rank, src);

  int srank, round_num;
  msgfmt_decode_rtp_begin(buf, buf_sz, &srank, &round_num);

  bool activated_now = false;

  pthread_mutex_lock(&(rctx->reneg_mutex));

  if (rctx->state_mgr.get_state() == RenegState::READY ||
      rctx->state_mgr.get_state() == RenegState::READYBLOCK) {
    assert(round_num == rctx->round_num);
    // logf(LOG_DBUG, "reneg_handle_rtp_begin: rank %d activated\n",
    // rctx->my_rank);
    rctx->state_mgr.update_state(RenegState::PVTSND);
    rctx->reneg_bench.rec_active();
    activated_now = true;
  } else if (round_num == rctx->round_num + 1) {
    rctx->state_mgr.mark_next_round_start(round_num);
  } else if (round_num != rctx->round_num) {
    /* If round_nums are equal, msg is duplicate and DROP */
    ABORT("reneg_handle_rtp_begin: unexpected round_num recvd");
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  if (activated_now) {
    /* Can reuse the same RTP_BEGIN buf. src_rank is anyway sent separately
     * If we're an S3 root, prioritize Stage 3 sending first, so as to
     * trigger other leaf broadcasts in parallel
     * */
    if (rctx->my_rank == rctx->root_s3) {
      send_to_all_s3(rctx, buf, buf_sz);
    }
    if (rctx->my_rank == rctx->root_s2) {
      send_to_all_s2(rctx, buf, buf_sz);
    }
    if (rctx->my_rank == rctx->root_s1) {
      send_to_all_s1(rctx, buf, buf_sz);
    }

    /* send pivots to s1root now */
    char pvt_buf[1024];

    pthread_mutex_lock(rctx->data_mutex);
    int pvt_buf_len = msgfmt_encode_rtp_pivots(pvt_buf, 1024, rctx->round_num,
                                               1, rctx->my_rank, rctx->data, 1,
                                               *(rctx->data_len));
    pthread_mutex_unlock(rctx->data_mutex);

    send_to_rank(rctx, pvt_buf, pvt_buf_len, rctx->root_s1);
  }

  return 0;
}

/**
 * @brief Handle pivots from lower stages
 *
 * Will only be invoked by intermediate nodes. Needn't worry about
 * duplicates, as they'll be resolved at leaf level. Leaves will never send
 * duplicate pivots to intermediate stages.
 *
 * Can't think of any race conditions that need to be handled here. We'll
 * always have seen RTP_BEGIN by the time we receive pivots. Since we're
 * the root for those leaves, they can't receive RTP_BEGIN without us sending
 * it.
 *
 * @param rctx
 * @param buf
 * @param buf_sz
 * @param src
 *
 * @return
 */
int reneg_handle_rtp_pivot(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                           int src) {
  // logf(LOG_DBUG, "reneg_handle_rtp_pivot: msg at %d from %d\n",
  // rctx->my_rank, src);

  int round_num, stage_num, sender_id, num_pivots;
  float pivot_width;
  float *pivots;

  msgfmt_decode_rtp_pivots(buf, buf_sz, &round_num, &stage_num, &sender_id,
                           &pivots, &pivot_width, &num_pivots);

  assert(num_pivots == PIVOTS_MAX);

  // logf(LOG_DBUG, "reneg_handle_rtp_pivot: %.1f %.1f %.1f %.1f\n", pivots[0],
  // pivots[1], pivots[2], pivots[3]);

  pthread_mutex_lock(&(rctx->reneg_mutex));

  int stage_pivot_count;

  if (round_num != rctx->round_num) {
    stage_pivot_count =
        rctx->data_buffer.store_data(stage_num, pivots, num_pivots, true);

    /* If we're receiving a pivot for a future round, we can never have
     * received all pivots for a future round, as we also expect one pivot
     * from ourselves, which can't be received out of turn
     *
     * This assumption is important, as it enables us to not have to store
     * and replay the handler logic for when all items of a future round/stage
     * have been received
     */
    assert(!expected_items_for_stage(rctx, stage_num, stage_pivot_count));
  } else {
    stage_pivot_count =
        rctx->data_buffer.store_data(stage_num, pivots, num_pivots, false);
  }
  // int num_items = rctx->data_buffer.get_num_items(stage_num);

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  logf(LOG_INFO,
       "reneg_handle_rtp_pivot: S%d at Rank %d, item from %d. Total: %d\n",
       stage_num, rctx->my_rank, src, stage_pivot_count);

  if (expected_items_for_stage(rctx, stage_num, stage_pivot_count)) {
    // Aggregate pivots
    // XXX: pretend new pivots are also in pivots
    char next_buf[1024];
    logf(LOG_INFO, "reneg_handle_rtp_pivot: S%d at Rank %d, collected\n",
         stage_num, rctx->my_rank);

    if (stage_num < STAGES_MAX) {
      logf(LOG_DBUG, "reneg_handle_rtp_pivot: choice 1\n");

      int next_buf_len = msgfmt_encode_rtp_pivots(
          next_buf, 1024, round_num, stage_num + 1, rctx->my_rank, pivots,
          pivot_width, num_pivots);

      int new_dest = stage_num == 1 ? rctx->root_s2 : rctx->root_s3;

      send_to_rank(rctx, next_buf, next_buf_len, new_dest);
    } else {
      /* New pivots need to be broadcast from S3. We send them back to
       * ourself (s3root) so that the broadcast code can be cleanly
       * contained in reneg_handle_pivot_bcast
       */
      logf(LOG_DBUG, "reneg_handle_rtp_pivot: choice 2 @ %d\n", rctx->root_s3);

      assert(rctx->my_rank == rctx->root_s3);

      rctx->reneg_bench.rec_pvt_bcast();

      int next_buf_len = msgfmt_encode_rtp_pivots(
          next_buf, 1024, round_num, stage_num + 1, rctx->my_rank, pivots,
          pivot_width, num_pivots, true);

      send_to_rank(rctx, next_buf, next_buf_len, rctx->root_s3);
    }
  }  // if

  return 0;
}

int reneg_handle_pivot_bcast(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                             int src) {
  /* You only expect to receive this message once per-round;
   * TODO: parse the round-num for a FATAL error verification
   * but no intermediate state change necessary here. Process
   * the update and then move to READY
   */

  pthread_mutex_lock(&(rctx->reneg_mutex));

  RenegState rstate = rctx->state_mgr.get_state();

  if (rstate != RenegState::PVTSND) {
    ABORT("reneg_handle_pivot_bcast: unexpected pivot bcast");
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  /* send_to_all here excludes self; sinne we're passing my_rank to exclude */
  if (rctx->my_rank == rctx->root_s3) {
    send_to_all_s3(rctx, buf, buf_sz, rctx->my_rank);
  }
  if (rctx->my_rank == rctx->root_s2) {
    send_to_all_s2(rctx, buf, buf_sz, rctx->my_rank);
  }
  if (rctx->my_rank == rctx->root_s1) {
    send_to_all_s1(rctx, buf, buf_sz, rctx->my_rank);
  }

  logf(LOG_DBUG, "reneg_handle_pivot_bcast: received pivots at %d from %d\n",
       rctx->my_rank, src);

  if (rctx->my_rank == 0) {
    logf(LOG_INFO, "rtp round %d completed at rank 0\n", rctx->round_num);
  }

  /* Install pivots, reset state, and signal back to the main thread */
  bool replay_rtp_begin_flag = false;

  pthread_mutex_lock(&(rctx->reneg_mutex));

  rctx->data_buffer.advance_round();
  rctx->round_num++;

  if (rctx->state_mgr.get_next_round_start()) {
    /* Next round has started, keep main thread sleeping and participate */
    replay_rtp_begin_flag = true;
    rctx->state_mgr.update_state(RenegState::READYBLOCK);
  } else {
    /* Next round has not started yet, okay to wake up main thread */
    rctx->state_mgr.update_state(RenegState::READY);
    pthread_cond_signal(&(rctx->reneg_cv));
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  if (replay_rtp_begin_flag) {
    /* Send rtp_begin to self, to resume pivot send etc */
    replay_rtp_begin(rctx);
  }

  return 0;
}

void broadcast_rtp_begin(reneg_ctx_t rctx) {
  /* XXX: ASSERT reneg_mutex is held */
  logf(LOG_DBUG, "broadcast_rtp_begin: at rank %d, to %d\n", rctx->my_rank,
       rctx->root_s1);

  char buf[256];
  int buflen =
      msgfmt_encode_rtp_begin(buf, 256, rctx->my_rank, rctx->round_num);
  send_to_rank(rctx, buf, buflen, rctx->root_s3);
}

void replay_rtp_begin(reneg_ctx_t rctx) {
  /* XXX: ASSERT reneg_mutex is held */
  logf(LOG_DBUG, "replay_rtp_begin: at rank %d\n", rctx->my_rank);

  char buf[256];

  // TODO: round_num is correct?
  int buflen =
      msgfmt_encode_rtp_begin(buf, 256, rctx->my_rank, rctx->round_num);
  send_to_rank(rctx, buf, buflen, rctx->my_rank);
}

void send_to_rank(reneg_ctx_t rctx, char *buf, int buf_sz, int drank) {
  xn_shuffle_priority_send(rctx->xn_sctx, buf, buf_sz, 0, drank, rctx->my_rank);
}

void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz, int my_rank) {
  // logf(LOG_DBUG, "send_to_all: %d bytes to %d peers at rank %d\n", buf_sz,
  // num_peers, rctx->my_rank);

  for (int rank_idx = 0; rank_idx < num_peers; rank_idx++) {
    int drank = peers[rank_idx];

    /*  Not enabled by default; my_rank is set to -1 unless overridden */
    if (drank == my_rank) continue;

    logf(LOG_DBUG, "send_to_all: sending from %d to %d\n", rctx->my_rank,
         drank);

    xn_shuffle_priority_send(rctx->xn_sctx, buf, buf_sz, 0, drank,
                             rctx->my_rank);
  }

  return;
}

int reneg_destroy(reneg_ctx_t rctx) {
  pthread_mutex_destroy(&(rctx->reneg_mutex));
  return 0;
}

bool expected_items_for_stage(reneg_ctx_t rctx, int stage, int items) {
  if (stage == 1 && rctx->num_peers_s1 == items) return true;
  if (stage == 2 && rctx->num_peers_s2 == items) return true;
  if (stage == 3 && rctx->num_peers_s3 == items) return true;

  return false;
}

/*************** Temporary Functions ********************/
int mock_pivots_init(reneg_ctx_t rctx) {
  if (rctx->state_mgr.get_state() != RenegState::INIT) {
    logf(LOG_DBUG, "mock_pivots_init: can initialize only in init stage\n");
    return -1;
  }

  // logf(LOG_DBUG, "mock_pivots_init: mutex %p\n", rctx->data_mutex);

  assert(rctx->data_mutex != NULL);
  assert(rctx->data != NULL);
  assert(rctx->data_len != NULL);

  pthread_mutex_lock(rctx->data_mutex);

  for (int pidx = 0; pidx < RANGE_NUM_PIVOTS; pidx++) {
    rctx->data[pidx] = (pidx + 1) * (pidx + 2);
  }

  *(rctx->data_len) = RANGE_NUM_PIVOTS;

  pthread_mutex_unlock(rctx->data_mutex);

  return 0;
}
