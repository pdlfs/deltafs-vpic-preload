#include "shuffle_write_providers.h"

#include <pdlfs-common/xxhash.h>
#ifdef PRELOAD_HAS_CH_PLACEMENT
#include <ch-placement.h>
#endif

#include <time.h>

namespace {
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

int shuffle_flush_oob(int epoch) {
  int rv = 0;

  shuffle_ctx_t* sctx = &(pctx.sctx);

  pdlfs::carp::OobFlushIterator fi = pctx.carp->OobIterator();
  int rank = shuffle_rank(sctx);

  while (fi != pctx.carp->OobSize()) {
    pdlfs::carp::particle_mem_t& p = *fi;
    if (p.shuffle_dest == -1) {
      fi.PreserveCurrent();
      fi++;
      continue;
    }

    if (p.shuffle_dest < -1 || p.shuffle_dest >= pctx.comm_sz) {
      ABORT("shuffle_flush_oob: invalid peer_rank");
    }

    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(sctx->rep), p.buf, p.buf_sz,
                       epoch, p.shuffle_dest, rank);

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
  const float h = ::get_indexable_property(buf, buf_sz); //XXX

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

// the shuffle key/value should match the preload inkey/invalue
int shuffle_write(shuffle_ctx_t* ctx, const char* skey,
                  unsigned char skey_len, char* svalue,
                  unsigned char svalue_len, int epoch) {
  char buf[255];
  int peer_rank;
  int rank;
  int rv;

  assert(ctx == &pctx.sctx);
  assert(ctx->skey_len + ctx->svalue_len + ctx->extra_data_len <= sizeof(buf));
  if (ctx->skey_len != skey_len) ABORT("bad shuffle key len");
  if (ctx->svalue_len != svalue_len) ABORT("bad shuffle value len");

  unsigned char base_sz = skey_len + svalue_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;
  memcpy(buf, skey, skey_len);
  memcpy(buf + skey_len, svalue, svalue_len);
  if (buf_sz != base_sz) memset(buf + base_sz, 0, buf_sz - base_sz);

  peer_rank = shuffle_target(ctx, buf, buf_sz);
  rank = shuffle_rank(ctx);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_debug(ctx, buf, buf_sz, epoch, rank, peer_rank);

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(skey, skey_len, svalue, svalue_len, epoch);
    return rv;
  }

  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
                       peer_rank, rank);
  } else {
    nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  }

  return 0;
}

// the shuffle key/value should match the preload inkey/invalue
int shuffle_write_range(shuffle_ctx_t* ctx, const char* skey,
                        unsigned char skey_len, char* svalue,
                        unsigned char svalue_len, int epoch) {
  int peer_rank = -1;
  int rank;
  int rv = 0;
  pdlfs::carp::particle_mem_t p;

  assert(ctx == &pctx.sctx);
  assert(ctx->skey_len + ctx->svalue_len +
                         ctx->extra_data_len < pdlfs::kMaxPartSize);
  if (ctx->skey_len != skey_len) ABORT("bad shuffle key len");
  if (ctx->svalue_len != svalue_len) ABORT("bad shuffle value len");

  rank = shuffle_rank(ctx);   /* my rank */

  /* Serialize data into p->buf */
  /* XXXCDC: shouldn't need to do this if we end up calling native_write() */
  pctx.carp->Serialize(skey, skey_len, svalue, svalue_len,
                       ctx->extra_data_len, p);

  bool flush_oob;
  bool shuffle_now;
  /* AttemptBuffer will renegotiate internally if required */
  pctx.carp->AttemptBuffer(p, shuffle_now, flush_oob);
  peer_rank = p.shuffle_dest;

  if (flush_oob and pctx.carp->OobSize()) {
    shuffle_flush_oob(epoch);
  }

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    /* native write takes skey/svalue (aka preload inkey/invalue) */
    rv = native_write(skey, skey_len, svalue, svalue_len, epoch);
    shuffle_now = false;
  }

  if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
    rv = 0;
    shuffle_now = false;
  }

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_range_debug(ctx, p.buf, p.buf_sz, epoch, rank, peer_rank);

  if (!shuffle_now) {
    return rv;
  }

  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), p.buf, p.buf_sz, epoch,
                       peer_rank, rank);
  } else {
    /* but carp cannot use NN, so this case currently cannot happen ... */
    nn_shuffler_enqueue(p.buf, p.buf_sz, epoch, peer_rank, rank);
  }

  return rv;
}
