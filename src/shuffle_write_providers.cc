#include "shuffle_write_providers.h"
#include <pdlfs-common/xxhash.h>
#ifdef PRELOAD_HAS_CH_PLACEMENT
#include <ch-placement.h>
#endif

#include <time.h>

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
template <class T>
bool in_bounds(T val, T lower, T upper) {
  return (val >= lower && val <= upper);
}

buf_type_t compute_oob_buf(pivot_ctx_t* pvt_ctx, float indexed_prop) {
  /* Assert pvt_ctx->pvt_access_m.lockheld() */
  MainThreadState state = pvt_ctx->mts_mgr.get_state();
  buf_type_t buf_type;

  float range_min = pvt_ctx->range_min;
  float range_max = pvt_ctx->range_max;

  if (state == MainThreadState::MT_INIT) {
    buf_type = buf_type_t::RB_BUF_LEFT;
  } else if (in_bounds(indexed_prop, range_min, range_max)) {
    buf_type = buf_type_t::RB_NO_BUF;
  } else if (indexed_prop < range_min) {
    buf_type = buf_type_t::RB_BUF_LEFT;
  } else {
    assert(indexed_prop > range_max);
    buf_type = buf_type_t::RB_BUF_RIGHT;
  }

  return buf_type;
}

int store_item_in_oob(pivot_ctx_t* pvt_ctx, buf_type_t& buf_type,
                      float indexed_prop, const char* fname, int fname_len,
                      char* data, int data_len, int extra_data_len) {
  /* Assert lockheld */
  if (buf_type == buf_type_t::RB_BUF_LEFT) {
    particle_mem_t& p = pvt_ctx->oob_buffer_left[pvt_ctx->oob_count_left];

    p.indexed_prop = indexed_prop;
    int buf_sz = msgfmt_write_data(p.buf, RANGE_MAX_PSZ, fname, fname_len, data,
                                   data_len, extra_data_len);
    p.buf_sz = buf_sz;
    pvt_ctx->oob_count_left++;
  } else if (buf_type == buf_type_t::RB_BUF_RIGHT) {
    particle_mem_t& p = pvt_ctx->oob_buffer_right[pvt_ctx->oob_count_right];
    p.indexed_prop = indexed_prop;

    int buf_sz = msgfmt_write_data(p.buf, RANGE_MAX_PSZ, fname, fname_len, data,
                                   data_len, extra_data_len);
    p.buf_sz = buf_sz;
    pvt_ctx->oob_count_right++;
  } else {
    ABORT("unknown buf_type");
  }

  return 0;
}

float get_indexable_property(const char* data_buf, unsigned int dbuf_sz) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}

int shuffle_data_target(const float& indexed_prop) {
  auto rank_iter = std::lower_bound(pctx.pvt_ctx.rank_bins.begin(),
                                    pctx.pvt_ctx.rank_bins.end(), indexed_prop);

  int rank = rank_iter - pctx.pvt_ctx.rank_bins.begin() - 1;

  char print_buf[1024];
  print_vector(print_buf, 1024, pctx.pvt_ctx.rank_bins);
  logf(LOG_DBG2,
       "shuffle_data_target, new, destrank: %d (%.1f)\n"
       "%s\n",
       rank, indexed_prop, print_buf);

  return rank;
}

int shuffle_flush_oob(shuffle_ctx_t* sctx, pivot_ctx_t* pvt_ctx,
                      std::vector<particle_mem_t>& oob, int& oob_sz,
                      int epoch) {
  /* assert lockheld */
  int rv = 0;
  int repl_idx = 0;
  int rank = shuffle_rank(sctx);

  for (int oidx = 0; oidx < oob_sz; oidx++) {
    particle_mem_t& p = oob[oidx];

    if (p.indexed_prop < pvt_ctx->range_min ||
        p.indexed_prop > pvt_ctx->range_max) {
      oob[repl_idx++] = p;
      continue;
    }

    int peer_rank = shuffle_data_target(p.indexed_prop);
    if (peer_rank < -1 || peer_rank >= pctx.comm_sz) {
      ABORT("shuffle_flush_oob: invalid peer_rank");
    }

    pvt_ctx->rank_bin_count[peer_rank]++;

    // xn_shuffle_enqueue(static_cast<xn_ctx_t*>(sctx->rep), p.buf, p.buf_sz,
    // epoch, peer_rank, rank);
  }

  oob_sz = repl_idx;
  return rv;
}

int shuffle_rebalance_oobs(pivot_ctx_t* pvt_ctx,
                           std::vector<particle_mem_t>& oobl, int& oobl_sz,
                           std::vector<particle_mem_t>& oobr, int& oobr_sz) {
  /* assert lockheld */
  int rv = 0;
  std::vector<particle_mem_t> oob_pool;

  oob_pool.insert(oob_pool.end(), oobl.begin(), oobl.begin() + oobl_sz);
  oob_pool.insert(oob_pool.end(), oobr.begin(), oobr.begin() + oobr_sz);

  int oobp_sz = oob_pool.size();
  int oob_size_orig = oobl_sz + oobr_sz;

  oobl_sz = 0;
  oobr_sz = 0;

  for (int oidx = 0; oidx < oobp_sz; oidx++) {
    particle_mem_t& p = oob_pool[oidx];

    if (p.indexed_prop < pvt_ctx->range_min) {
      oobl[oobl_sz++] = p;
    } else if (p.indexed_prop > pvt_ctx->range_max) {
      oobr[oobr_sz++] = p;
    } else {
      ABORT("rebalance_oobs: unexpected condition");
    }
  }

  assert(oob_size_orig == oobl_sz + oobr_sz);
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
  pivot_ctx* pvt_ctx = &(pctx.pvt_ctx);
  pdlfs::reneg_ctx_t rctx = &(pctx.rtp_ctx);

  for (int i = 0; i < 1000; i++) {
    float rand_value = (rand() % 16738) / 167.38;
    particle_mem_t p;
    p.indexed_prop = rand_value;
    p.buf_sz = 0;
    pvt_ctx->oob_buffer_left[pvt_ctx->oob_count_left] = p;
    pvt_ctx->oob_count_left++;
  }

  // sleep(5);
  pvt_ctx->mts_mgr.update_state(MT_BLOCK);

  if (pctx.my_rank == 3) {
    // rctx->reneg_bench.rec_start();
    reneg_init_round(rctx);
    // rctx->reneg_bench.rec_finished();
    // fprintf(stderr, "=========== MAIN THREAD AWAKE ==========\n");
    // rctx->reneg_bench.print_stderr();
    // reneg_init_round(rctx);
  }

  sleep(10);

  return 0;

  // for (int i = 0; i < 100; i++) {
  // int sleep_time = rand() % 10000;
  // usleep(sleep_time);
  // reneg_init_round(rctx);
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
  pivot_ctx_t* pvt_ctx = &(pctx.pvt_ctx);
  pdlfs::reneg_ctx_t rctx = &(pctx.rtp_ctx);

  char buf[255];
  int peer_rank = -1;
  int rank;
  int rv = 0;

  assert(ctx == &pctx.sctx);
  assert(ctx->extra_data_len + ctx->data_len < 255 - ctx->fname_len - 1);
  if (ctx->fname_len != fname_len) ABORT("bad filename len");
  if (ctx->data_len != data_len) ABORT("bad data len");

  rank = shuffle_rank(ctx);

  unsigned char base_sz = 1 + fname_len + data_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_debug(ctx, buf, buf_sz, epoch, rank, peer_rank);

  /* Decide whether to buffer or send */
  float indexed_prop = ::get_indexable_property(data, data_len);

  pthread_mutex_lock(&(pvt_ctx->pivot_access_m));

  pvt_ctx->last_reneg_counter++;

  buf_type_t dest_buf = compute_oob_buf(pvt_ctx, indexed_prop);

  bool shuffle_now = (dest_buf == buf_type_t::RB_NO_BUF);
  /* msgfmt-ize the data */
  if (shuffle_now) {
    buf_sz = msgfmt_write_data(buf, 255, fname, fname_len, data, data_len,
                               ctx->extra_data_len);
  } else {
    store_item_in_oob(pvt_ctx, dest_buf, indexed_prop, fname, fname_len, data,
                      data_len, ctx->extra_data_len);
  }

  /* At this point, we can (one of the following must hold):
   * 1. Return, since items have been buffered
   * 2. Init renegotiation and block, if OOBs full
   * 3. Block, if OOBs full and reneg ongoing
   * 4. Shuffle, if shuffle_now is true
   */

#define OOB_LEFT_FULL(pvt_ctx) (pvt_ctx->oob_count_left == RANGE_MAX_OOB_SZ)
#define OOB_RIGHT_FULL(pvt_ctx) (pvt_ctx->oob_count_right == RANGE_MAX_OOB_SZ)
#define OOB_EITHER_FULL(pvt_ctx) \
  (OOB_LEFT_FULL(pvt_ctx) || OOB_RIGHT_FULL(pvt_ctx))

#define RENEG_ONGOING(pvt_ctx) \
  (pvt_ctx->mts_mgr.get_state() == MainThreadState::MT_BLOCK)

  bool reneg_and_block =
      (!shuffle_now && OOB_EITHER_FULL(pvt_ctx)) || RENEG_ONGOING(pvt_ctx);

  if (pvt_ctx->last_reneg_counter == 15000) reneg_and_block = true;

  if (reneg_and_block) {
    /* Conditions 2 and 3:
     * This will init a renegotiation and block until complete
     * If active already, this will block anyway */
    // TODO: make sure no gotchas here
    reneg_init_round(rctx);
    ::shuffle_flush_oob(ctx, pvt_ctx, pvt_ctx->oob_buffer_left,
                        pvt_ctx->oob_count_left, epoch);
    ::shuffle_flush_oob(ctx, pvt_ctx, pvt_ctx->oob_buffer_right,
                        pvt_ctx->oob_count_right, epoch);
    ::shuffle_rebalance_oobs(pvt_ctx, pvt_ctx->oob_buffer_left,
                             pvt_ctx->oob_count_left, pvt_ctx->oob_buffer_right,
                             pvt_ctx->oob_count_right);
  }

  if (!shuffle_now) {
    logf(LOG_DBG2, "Rank %d: buffering item %f\n", pctx.my_rank, indexed_prop);

    /* Condition 1. Buffered in OOB, flushed if necessary, nothing to do */
    rv = 0;
    goto cleanup;
  }

  logf(LOG_DBG2, "Rank %d: shuffling item %f\n", pctx.my_rank, indexed_prop);

  peer_rank = ::shuffle_data_target(indexed_prop);

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(fname, fname_len, data, data_len, epoch);
    goto cleanup;
  }

  if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
    rv = 0;
    goto cleanup;
  }

  pvt_ctx->rank_bin_count[peer_rank]++;

  // if (ctx->type == SHUFFLE_XN) {
  // xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
  // peer_rank, rank);
  // } else {
  // nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  // }

cleanup:
  pthread_mutex_unlock(&(pvt_ctx->pivot_access_m));
  return rv;
}
