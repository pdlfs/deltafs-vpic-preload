#include "shuffle_write_providers.h"

void mock_reneg();
float get_indexable_property(const char* data_buf, unsigned int dbuf_sz);

void mock_reneg() {
  range_ctx_t* rctx = &pctx.rctx;

  range_init_negotiation(&pctx);

  std::unique_lock<std::mutex> bin_access_ul(rctx->bin_access_m);

  rctx->block_writes_cv.wait(bin_access_ul, [] {
    /* having a condition ensures we ignore spurious wakes */
    return (pctx.rctx.range_state == range_state_t::RS_READY);
  });
}

namespace {
float get_indexable_property(const char* data_buf, unsigned int dbuf_sz) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}
}  // namespace

int shuffle_write_mock(shuffle_ctx_t* ctx, const char* fname,
                       unsigned char fname_len, char* data,
                       unsigned char data_len, int epoch) {
#ifndef RANGE_MOCK_RENEG

  logf(LOG_ERRO, "RANGE_MOCK_RENEG flag is not enabled");
  return 0;

#endif
  mock_reneg();
  return 0;
}

int shuffle_write_nohash(shuffle_ctx_t* ctx, const char* fname,
                         unsigned char fname_len, char* data,
                         unsigned char data_len, int epoch) {
#define SHUFFLE_BUF_LEN 255

  char buf[SHUFFLE_BUF_LEN];

  float prop = ::get_indexable_property(data, data_len);
  int peer_rank = int(prop);
  int rank = shuffle_rank(ctx);
  int rv;
  // fprintf(stderr, "shuffle_write_nohash: %f %d\n", prop, dest_rank);

  range_ctx_t* rctx = &pctx.rctx;

  // fprintf(stderr, "fname comp: %d %d\n", ctx->fname_len, fname_len);
  // fprintf(stderr, "data comp: %d %d\n", ctx->data_len, data_len);

  assert(ctx == &pctx.sctx);
  assert(ctx->extra_data_len + ctx->data_len < 255 - ctx->fname_len - 1);
  if (ctx->fname_len != fname_len) ABORT("bad filename len");
  if (ctx->data_len != data_len) ABORT("bad data len");

  unsigned char base_sz = 1 + fname_len + data_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;

  buf_sz = msgfmt_write_data(buf, SHUFFLE_BUF_LEN, fname, fname_len, data,
                             data_len, ctx->extra_data_len);

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(fname, fname_len, data, data_len, epoch);
    return rv;
  }

  if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
    return 0;
  }

  rctx->ts_writes_shuffled++;

  // fprintf(stderr, "At SRC: %d, SEND to %d, P: %02x%02x%02x\n", rank,
  // peer_rank, buf[0], buf[1], buf[2]);

  if (ctx->type == SHUFFLE_XN) {
    /* TODO: Also handle flushed OOB packets */
    int padded_sz = buf_sz;
    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
                       peer_rank, rank);
  } else {
    nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  }
  return 0;
}

int shuffle_write_treeneg(shuffle_ctx_t* ctx, const char* fname,
                       unsigned char fname_len, char* data,
                       unsigned char data_len, int epoch) {
  struct reneg_ctx rctx;

  struct reneg_opts ro;
  ro.fanout_s1 = 4;
  ro.fanout_s2 = 4;
  ro.fanout_s3 = 4;

  float dt[100];
  int dlen;

  pthread_mutex_t dm;
  pthread_mutex_init(&dm, NULL);

  reneg_init(&rctx, ctx, dt, &dlen, 20, &dm, ro);
  // reneg_init_round(&rctx);
  // reneg_destroy(&rctx);

  pthread_mutex_destroy(&dm);
  return 0;
}
