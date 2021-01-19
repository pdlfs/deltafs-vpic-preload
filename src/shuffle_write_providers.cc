#include "shuffle_write_providers.h"

#include <pdlfs-common/xxhash.h>
#ifdef PRELOAD_HAS_CH_PLACEMENT
#include <ch-placement.h>
#endif

#include <time.h>

void mock_reneg();

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
buf_type_t compute_oob_buf(pivot_ctx_t* pvt_ctx, float indexed_prop) {
  /* Assert pvt_ctx->pvt_access_m.lockheld() */
  MainThreadState state = pvt_ctx->mts_mgr.get_state();
  pdlfs::OobBuffer* oob_buffer = pvt_ctx->oob_buffer;
  buf_type_t buf_type;

  if (oob_buffer->OutOfBounds(indexed_prop)) {
    buf_type = buf_type_t::RB_BUF_OOB;
  } else {
    buf_type = buf_type_t::RB_NO_BUF;
  }

  return buf_type;
}

float get_indexable_property(const char* data_buf, unsigned int dbuf_sz) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
  // assert(dbuf_sz >= 7 * sizeof(float));
  // const float* p_ar = reinterpret_cast<const float*>(data_buf);
  // const float ux = p_ar[4];
  // const float uy = p_ar[5];
  // const float uz = p_ar[6];

  // return sqrt(ux*ux + uy*uy + uz*uz);
}

int shuffle_data_target(const float& indexed_prop) {
  auto rank_iter =
      std::lower_bound(pctx.pvt_ctx->rank_bins.begin(),
                       pctx.pvt_ctx->rank_bins.end(), indexed_prop);

  int rank = rank_iter - pctx.pvt_ctx->rank_bins.begin() - 1;

  char print_buf[1024];
  print_vector(print_buf, 1024, pctx.pvt_ctx->rank_bins);
  logf(LOG_DBG2,
       "shuffle_data_target, new, destrank: %d (%.1f)\n"
       "%s\n",
       rank, indexed_prop, print_buf);

  return rank;
}

int shuffle_flush_oob(shuffle_ctx_t* sctx, pivot_ctx_t* pvt_ctx, int epoch) {
  int rv = 0;

  pdlfs::OobBuffer* oob_buffer = pvt_ctx->oob_buffer;
  pdlfs::OobFlushIterator fi(*oob_buffer);
  int rank = shuffle_rank(sctx);

  while (fi != oob_buffer->Size()) {
    pdlfs::particle_mem_t& p = *fi;
    if (oob_buffer->OutOfBounds(p.indexed_prop)) {
      fi.PreserveCurrent();
      fi++;
      continue;
    }

    int peer_rank = shuffle_data_target(p.indexed_prop);
    if (peer_rank < -1 || peer_rank >= pctx.comm_sz) {
      ABORT("shuffle_flush_oob: invalid peer_rank");
    }

    pvt_ctx->rank_bin_count[peer_rank]++;
    pvt_ctx->rank_bin_count_aggr[peer_rank]++;

    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(sctx->rep), p.buf, p.buf_sz,
                       epoch, peer_rank, rank);

    fi++;
  }

  return rv;
}

void shuffle_write_debug(shuffle_ctx_t* ctx, char* buf, unsigned char buf_sz,
                         int epoch, int src, int dst) {
  const int h = pdlfs::xxhash32(buf, buf_sz, 0);

  if (src != dst || ctx->force_rpc) {
    fprintf(pctx.trace, "[SH] %u bytes (ep=%d) r%d >> r%d (xx=%08x)\n", buf_sz,
            epoch, src, dst, h);
  } else {
    fprintf(pctx.trace,
            "[LO] %u bytes (ep=%d) "
            "(xx=%08x)\n",
            buf_sz, epoch, h);
  }
}

void shuffle_write_range_debug(shuffle_ctx_t* ctx, char* buf,
                               unsigned char buf_sz, int epoch, int src,
                               int dst) {
  const float h = ::get_indexable_property(buf, buf_sz);

  if (src != dst || ctx->force_rpc) {
    fprintf(pctx.trace, "[SH] %u bytes (ep=%d) r%d >> r%d (fl=%f)\n", buf_sz,
            epoch, src, dst, h);
  } else {
    fprintf(pctx.trace,
            "[LO] %u bytes (ep=%d) "
            "(fl=%f)\n",
            buf_sz, epoch, h);
  }
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
  // fprintf(stderr, "shuffle_write_nohash: %f %d\n", prop, peer_rank);

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

timespec diff(timespec start, timespec end) {
  timespec temp;
  if ((end.tv_nsec - start.tv_nsec) < 0) {
    temp.tv_sec = end.tv_sec - start.tv_sec - 1;
    temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec - start.tv_sec;
    temp.tv_nsec = end.tv_nsec - start.tv_nsec;
  }
  return temp;
}

int shuffle_write_treeneg(shuffle_ctx_t* ctx, const char* fname,
                          unsigned char fname_len, char* data,
                          unsigned char data_len, int epoch) {
  pivot_ctx* pvt_ctx = pctx.pvt_ctx;
  pdlfs::rtp_ctx_t rctx = &(pctx.rtp_ctx);

  for (int i = 0; i < 1000; i++) {
    float rand_value = (rand() % 16738) / 167.38;
    pdlfs::particle_mem_t p;
    p.indexed_prop = rand_value;
    p.buf_sz = 0;
    pvt_ctx->oob_buffer->Insert(p);
  }

  // sleep(5);
  pvt_ctx->mts_mgr.update_state(MT_BLOCK);

  if (pctx.my_rank == 3) {
    // rctx->reneg_bench.rec_start();
    rtp_init_round(rctx);
    // rctx->reneg_bench.rec_finished();
    // fprintf(stderr, "=========== MAIN THREAD AWAKE ==========\n");
    // rctx->reneg_bench.print_stderr();
    // rtp_init_round(rctx);
  }

  sleep(10);

  return 0;

  // for (int i = 0; i < 100; i++) {
  // int sleep_time = rand() % 10000;
  // usleep(sleep_time);
  // rtp_init_round(rctx);
  // }

  // if (pctx.my_rank == 0) {
  // rctx->reneg_bench.print_stderr();
  // }

  // sleep(20);
  /* Enable the snippet below to do some writing after RTP
   * Right now, run.sh has some issues with plfsdir compaction in this
   * function handler if BYPASS_Write is disabled, i.e. writes are enabled
   */

  // assert(ctx == &pctx.sctx);
  // assert(ctx->extra_data_len + ctx->data_len < 255 - ctx->fname_len - 1);
  // if (ctx->fname_len != fname_len) ABORT("bad filename len");
  // if (ctx->data_len != data_len) ABORT("bad data len");

  // unsigned char base_sz = 1 + fname_len + data_len;
  // unsigned char buf_sz = base_sz + ctx->extra_data_len;
  // int rank = shuffle_rank(ctx);
  // int peer_rank = -1;
  // char buf[255];

  // buf_sz = msgfmt_write_data(buf, SHUFFLE_BUF_LEN, fname, fname_len, data,
  // data_len, ctx->extra_data_len);

  // peer_rank = shuffle_target(ctx, buf, buf_sz);

  // if (ctx->type == SHUFFLE_XN) {
  // int padded_sz = buf_sz;
  // xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
  // peer_rank, rank);
  // } else {
  // nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  // }

  return 0;
}

int shuffle_write_range(shuffle_ctx_t* ctx, const char* fname,
                        unsigned char fname_len, char* data,
                        unsigned char data_len, int epoch) {
  pivot_ctx_t* pvt_ctx = pctx.pvt_ctx;
  pdlfs::rtp_ctx_t rctx = &(pctx.rtp_ctx);

  int peer_rank = -1;
  int rank;
  int rv = 0;

  assert(ctx == &pctx.sctx);
  assert(ctx->extra_data_len + ctx->data_len <
         pdlfs::kMaxPartSize - ctx->fname_len - 1);
  // if (ctx->fname_len != fname_len) ABORT("bad filename len");
  // if (ctx->data_len != data_len) ABORT("bad data len");

  rank = shuffle_rank(ctx);

  unsigned char base_sz = 1 + fname_len + data_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;

  /* Decide whether to buffer or send */
  float indexed_prop = get_indexable_property(data, data_len);

  /* Serialize data */
  pdlfs::particle_mem_t p;
  // p.buf_sz = msgfmt_write_data(p.buf, 255, fname, fname_len, data, data_len,
  // ctx->extra_data_len);

  char data_reorg[255];
  memcpy(data_reorg, fname, fname_len);
  memcpy(data_reorg + fname_len, data, data_len);

  p.buf_sz = msgfmt_write_data(
      p.buf, 255, reinterpret_cast<char*>(&indexed_prop), sizeof(float),
      data_reorg, fname_len + data_len, ctx->extra_data_len);

  logf(LOG_DBG2, "shuffle_write, bufsz: %d\n", p.buf_sz);

  p.indexed_prop = indexed_prop;

  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));

  pvt_ctx->last_reneg_counter++;

  buf_type_t dest_buf = compute_oob_buf(pvt_ctx, indexed_prop);

  bool shuffle_now = (dest_buf == buf_type_t::RB_NO_BUF);

  // [> XXX <] shuffle_now = true;

  if (!shuffle_now) {
    rv = pvt_ctx->oob_buffer->Insert(p);
  }

  /* At this point, we can (one of the following must hold):
   * 1. Return, since items have been buffered
   * 2. Init renegotiation and block, if OOBs full
   * 3. Block, if OOBs full and reneg ongoing
   * 4. Shuffle, if shuffle_now is true
   */

#define RENEG_ONGOING(pvt_ctx) \
  ((pvt_ctx)->mts_mgr.get_state() != MainThreadState::MT_READY)

  bool reneg_and_block =
      (!shuffle_now && pvt_ctx->oob_buffer->IsFull()) || RENEG_ONGOING(pvt_ctx);

  if (pvt_ctx->last_reneg_counter == pctx.carp_reneg_intvl) {
    reneg_and_block = true;
  }

  // [> XXX <] reneg_and_block = false;

  if (reneg_and_block) {
    /* Conditions 2 and 3:
     * This will init a renegotiation and block until complete
     * If active already, this will block anyway */
    rtp_init_round(rctx);
    ::shuffle_flush_oob(ctx, pvt_ctx, epoch);
  }

  if (!shuffle_now) {
    logf(LOG_DBG2, "Rank %d: buffering item %f\n", pctx.my_rank, indexed_prop);

    /* Condition 1. Buffered in OOB, flushed if necessary, nothing to do */
    rv = 0;
    goto cleanup;
  }

  logf(LOG_DBG2, "Rank %d: shuffling item %f\n", pctx.my_rank, indexed_prop);

  peer_rank = ::shuffle_data_target(indexed_prop);

  // [> XXX <] peer_rank = (*reinterpret_cast<int *>(&indexed_prop) ^ 0xc3) %
  // pctx.comm_sz;

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(fname, fname_len, data, data_len, epoch);
    goto cleanup;
  }

  if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
    rv = 0;
    goto cleanup;
  }

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_range_debug(ctx, p.buf, p.buf_sz, epoch, rank, peer_rank);

  pvt_ctx->rank_bin_count[peer_rank]++;
  pvt_ctx->rank_bin_count_aggr[peer_rank]++;

cleanup:
  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));

  /* Release lock before shuffling, to avoid deadlocks from backpressure */
  if (!shuffle_now) {
    return rv;
  }

  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), p.buf, p.buf_sz, epoch,
                       peer_rank, rank);
  } else {
    nn_shuffler_enqueue(p.buf, p.buf_sz, epoch, peer_rank, rank);
  }

  return rv;
}
