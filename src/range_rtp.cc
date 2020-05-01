#include "range_rtp.h"
#include "msgfmt.h"

/* BEGIN utility classes */
RenegStateMgr::RenegStateMgr() {
  current_state = RENEG_INIT;
  prev_state = RENEG_INIT;
}

RenegState RenegStateMgr::get_state() { return this->current_state; }

RenegState RenegStateMgr::update_state(RenegState new_state) {
  this->prev_state = this->current_state;
  this->current_state = new_state;
}

DataBuffer::DataBuffer() {
  memset(data_len, 0, sizeof(data_len));
  // XXX: revisit
  this->num_pivots = PIVOTS_MAX;
}

int DataBuffer::store_data(int stage, float *data, int dlen) {
  if (stage < 1 || stage > 3) {
    return -1;
  }

  if (data_len[stage] >= FANOUT_MAX) {
    return -2;
  }

  if (dlen != num_pivots) {
    return -3;
  }

  int idx = data_len[stage];
  memcpy(data_store[stage][idx], data, dlen * sizeof(float));
  data_len[stage]++;

  return 0;
}

int DataBuffer::get_num_items(int stage) {
  if (stage < 1 || stage > STAGES_MAX) {
    return -1;
  }

  return data_len[stage];
}

int DataBuffer::clear_all_data() {
  memset(data_len, 0, sizeof(data_len));
  return 0;
}
/* END utility classes */

/* BEGIN internal declarations */
int reneg_topology_init(reneg_ctx_t rctx);

int mock_pivots_init(reneg_ctx_t rctx);

void send_to_rank(reneg_ctx_t rctx, char *buf, int buf_sz, int drank);

void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz);
#define send_to_all_s1(...) \
  send_to_all(rctx->peers_s1, rctx->num_peers_s1, __VA_ARGS__)
#define send_to_all_s2(...) \
  send_to_all(rctx->peers_s2, rctx->num_peers_s2, __VA_ARGS__)
#define send_to_all_s3(...) \
  send_to_all(rctx->peers_s3, rctx->num_peers_s3, __VA_ARGS__)

void broadcast_rtp_begin(reneg_ctx_t rctx);

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
  // (*rctx_ptr) = (struct reneg_ctx *) malloc(sizeof(struct reneg_ctx));
  // reneg_ctx_t rctx = *rctx_ptr;

  if (rctx->state_mgr.get_state() != RenegState::RENEG_INIT) {
    fprintf(stderr, "reneg_init: can initialize only in init stage\n");
    return -1;
  }

  if (sctx->type != SHUFFLE_XN) {
    fprintf(stderr, "Only 3-hop is supported by the RTP protocol\n");
    return -1;
  }

  if (data_max < RANGE_NUM_PIVOTS) {
    fprintf(stderr,
            "reneg_init: data array should be at least RANGE_NUM_PIVOTS\n");
    return -1;
  }

  if (ro.fanout_s1 > FANOUT_MAX) {
    fprintf(stderr, "fanout_s1 exceeds FANOUT_MAX\n");
    return -1;
  }

  if (ro.fanout_s2 > FANOUT_MAX) {
    fprintf(stderr, "fanout_s1 exceeds FANOUT_MAX\n");
    return -1;
  }

  if (ro.fanout_s3 > FANOUT_MAX) {
    fprintf(stderr, "fanout_s1 exceeds FANOUT_MAX\n");
    return -1;
  }

  xn_ctx_t *xn_sctx = static_cast<xn_ctx_t *>(sctx->rep);

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

  rctx->state_mgr.update_state(RenegState::RENEG_READY);

  return 0;
}

int reneg_topology_init(reneg_ctx_t rctx) {
  if (rctx->state_mgr.get_state() != RenegState::RENEG_INIT) {
    fprintf(stderr, "reneg_topology_init: can initialize only in init stage\n");
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

  if (rctx->state_mgr.get_state() != RenegState::RENEG_READY) {
    while (rctx->state_mgr.get_state() != RenegState::RENEG_READY) {
      pthread_cond_wait(&(rctx->reneg_cv), &(rctx->reneg_mutex));
    }
  } else {
    // broadcast reneg_begin
    broadcast_rtp_begin(rctx);
    // lock data
    // snapshot data
    // unlock data
    // compute pivots
    pthread_mutex_lock(rctx->data_mutex);
    // Construct msgfmt from pivots
    pthread_mutex_unlock(rctx->data_mutex);
    // send s1 message to all s1 peers
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));
  return 0;
}

int reneg_handle_msg(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                     int src) {
  if (rctx == NULL) {
    fprintf(stderr, "reneg_handle_msg: rctx is null!\n");
    ABORT("panic");
  }

  fprintf(stderr, "reneg_handle_msg: recvd an RTP msg at %d from %d\n",
          rctx->my_rank, src);

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
 * 1. RENEG_READY - regular case. Their round_num == our round_num
 * 2. Not RENEG_READY - In the middle of a renegotiation.
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
  int srank, round_num;
  msgfmt_decode_rtp_begin(buf, buf_sz, &srank, &round_num);

  bool activated_now = false;

  pthread_mutex_lock(&(rctx->reneg_mutex));

  if (rctx->state_mgr.get_state() == RenegState::RENEG_READY) {
    fprintf(stderr, "reneg_handle_rtp_begin: rank %d activated\n",
            rctx->my_rank);
    rctx->state_mgr.update_state(RenegState::RENEG_R1SND);
    activated_now = true;
  } else if (round_num == rctx->round_num + 1) {
    // TODO: buffer and replay later
  } else if (round_num != rctx->round_num) {
    ABORT("reneg_handle_rtp_begin: unexpected round_num recvd");
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  if (activated_now) {
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

int reneg_handle_rtp_pivot(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                           int src) {
  fprintf(stderr, "reneg_handle_rtp_pivot: msg at %d from %d\n", rctx->my_rank,
          src);

  int round_num, stage_num, sender_id, num_pivots;
  float pivot_width;
  float *pivots;

  msgfmt_decode_rtp_pivots(buf, buf_sz, &round_num, &stage_num, &sender_id,
                           &pivots, &pivot_width, &num_pivots);

  assert(num_pivots == PIVOTS_MAX);

  fprintf(stderr, "reneg_handle_rtp_pivot: %.1f %.1f %.1f %.1f\n", pivots[0],
          pivots[1], pivots[2], pivots[3]);

  pthread_mutex_lock(&(rctx->reneg_mutex));

  rctx->data_buffer.store_data(stage_num, pivots, num_pivots);
  int num_items = rctx->data_buffer.get_num_items(stage_num);

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  fprintf(stderr, "reneg_handle_rtp_pivot: (S%d) %d items at %d\n", stage_num,
          num_items, rctx->my_rank);
  if (expected_items_for_stage(rctx, stage_num, num_items)) {
    // Aggregate pivots
    // XXX: pretend new pivots are also in pivots
    char next_buf[1024];
    fprintf(stderr, "reneg_handle_rtp_pivot: S%d @ %d collected\n", stage_num,
            rctx->my_rank);

    if (stage_num < STAGES_MAX) {
      int next_buf_len = msgfmt_encode_rtp_pivots(
          next_buf, 1024, round_num, stage_num + 1, rctx->my_rank, pivots,
          pivot_width, num_pivots);

      int new_dest = stage_num == 1 ? rctx->root_s2 : rctx->root_s3;

      send_to_rank(rctx, next_buf, next_buf_len, new_dest);
    } else {
      assert(rctx->my_rank == rctx->root_s3);

      int next_buf_len = msgfmt_encode_rtp_pivots(
          next_buf, 1024, round_num, stage_num + 1, rctx->my_rank, pivots,
          pivot_width, num_pivots, true);

      send_to_rank(rctx, next_buf, next_buf_len, rctx->root_s3);
    }
  }

  return 0;
}

int reneg_handle_pivot_bcast(reneg_ctx_t rctx, char *buf, unsigned int buf_sz,
                             int src) {
  bool should_broadcast = false;

  pthread_mutex_lock(&(rctx->reneg_mutex));

  if (rctx->state_mgr.get_state() != RenegState::RENEG_RECVWAIT) {
    rctx->state_mgr.update_state(RenegState::RENEG_RECVWAIT);
    should_broadcast = true;
  }

  pthread_mutex_unlock(&(rctx->reneg_mutex));

  if (should_broadcast) {
    if (rctx->my_rank == rctx->root_s3) {
      send_to_all_s3(rctx, buf, buf_sz);
    }
    if (rctx->my_rank == rctx->root_s2) {
      send_to_all_s2(rctx, buf, buf_sz);
    }
    if (rctx->my_rank == rctx->root_s1) {
      send_to_all_s1(rctx, buf, buf_sz);
    }
  }

  fprintf(stderr, "reneg_handle_pivot_bcast: received pivots at %d from %d\n",
          rctx->my_rank, src);

  return 0;
}

void broadcast_rtp_begin(reneg_ctx_t rctx) {
  /* XXX: ASSERT reneg_mutex is held */
  fprintf(stderr, "broadcast_rtp_begin: at rank %d\n", rctx->my_rank);
  char buf[256];
  int buflen =
      msgfmt_encode_rtp_begin(buf, 256, rctx->my_rank, rctx->round_num);
  send_to_rank(rctx, buf, buflen, rctx->root_s1);
}

void send_to_rank(reneg_ctx_t rctx, char *buf, int buf_sz, int drank) {
  xn_shuffle_priority_send(rctx->xn_sctx, buf, buf_sz, 0, drank, rctx->my_rank);
}

void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz) {
  fprintf(stderr, "send_to_all: %d bytes to %d peers at rank %d\n", buf_sz,
          num_peers, rctx->my_rank);

  for (int rank_idx = 0; rank_idx < num_peers; rank_idx++) {
    int drank = peers[rank_idx];

    fprintf(stderr, "send_to_all: sending from %d to %d\n", rctx->my_rank,
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
  fprintf(stderr, "expected_items_for_stage: %d %d -> %d %d %d\n", stage, items,
          rctx->num_peers_s1, rctx->num_peers_s2, rctx->num_peers_s3);
  if (stage == 1 && rctx->num_peers_s1 == items) return true;
  if (stage == 2 && rctx->num_peers_s2 == items) return true;
  if (stage == 3 && rctx->num_peers_s3 == items) return true;

  return false;
}

/*************** Temporary Functions ********************/
int mock_pivots_init(reneg_ctx_t rctx) {
  if (rctx->state_mgr.get_state() != RenegState::RENEG_INIT) {
    fprintf(stderr, "mock_pivots_init: can initialize only in init stage\n");
    return -1;
  }

  fprintf(stderr, "mock_pivots_init: mutex %p\n", rctx->data_mutex);

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
