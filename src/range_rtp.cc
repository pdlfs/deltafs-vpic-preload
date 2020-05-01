#include "msgfmt.h"
#include "range_rtp.h"

/* BEGIN internal declarations */
int reneg_topology_init(reneg_ctx_t rctx);
int mock_pivots_init(reneg_ctx_t rctx);
void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz);
#define send_to_all_s1(...) \
  send_to_all(rctx->peers_s1, rctx->num_peers_s1, __VA_ARGS__)
#define send_to_all_s2(...) \
  send_to_all(rctx->peers_s2, rctx->num_peers_s2, __VA_ARGS__)
#define send_to_all_s3(...) \
  send_to_all(rctx->peers_s3, rctx->num_peers_s3, __VA_ARGS__)
/* END internal declarations */

int reneg_init(reneg_ctx_t rctx, shuffle_ctx_t *sctx, float *data,
               int *data_len, int data_max, pthread_mutex_t *data_mutex,
               struct reneg_opts ro) {
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

  mock_pivots_init(rctx);

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
  rctx->num_peers_s1 = 0;

  for (int pidx = 0; pidx < rctx->fanout_s2; pidx++) {
    int jump = rctx->fanout_s1;
    int peer = s2root + pidx * jump;

    if (peer >= gsz) break;

    rctx->peers_s2[pidx] = peer;
    rctx->num_peers_s2++;
  }

  int s3root = grank & s3mask;
  rctx->root_s3 = s3root;
  rctx->num_peers_s1 = 0;

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

int reneg_handle_msg(reneg_ctx_t rctx, char *buf, unsigned int buf_sz, int src) {
  fprintf(stderr, "Received a reneg msg at %d from %d\n", rctx->my_rank, src);
  return 0;
}

void broadcast_rtp_begin(reneg_ctx_t rctx) {
  /* XXX: ASSERT reneg_mutex is held */
  char buf[256];
  int buflen =
      msgfmt_encode_rtp_begin(buf, 256, rctx->my_rank, rctx->round_num);
}

void send_to_all(int *peers, int num_peers, reneg_ctx_t rctx, char *buf,
                 int buf_sz) {
  for (int rank_idx = 0; rank_idx < num_peers; rank_idx++) {
    int drank = peers[rank_idx];
    xn_shuffle_priority_send(rctx->xn_sctx, buf, buf_sz, 0, drank, rctx->my_rank);
  }
}

int reneg_destroy(reneg_ctx_t rctx) {
  // pthread_mutex_destroy(&reneg_mutex);
  return 0;
}

/*************** Temporary Functions ********************/
int mock_pivots_init(reneg_ctx_t rctx) {
  if (rctx->state_mgr.get_state() != RenegState::RENEG_INIT) {
    fprintf(stderr, "mock_pivots_init: can initialize only in init stage\n");
    return -1;
  }

  pthread_mutex_lock(rctx->data_mutex);

  for (int pidx = 0; pidx < RANGE_NUM_PIVOTS; pidx++) {
    rctx->data[pidx] = (pidx + 1) * (pidx + 2);
  }

  *(rctx->data_len) = RANGE_NUM_PIVOTS;

  pthread_mutex_unlock(rctx->data_mutex);

  return 0;
}
