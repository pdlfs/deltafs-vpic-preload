/*
 * shuffle_write_range.cc  shuffle_write for carp range store
 */

/* XXXCDC: comment function usages */

#include "shuffle_write_range.h"
#include "../nn_shuffler.h"

static void shuffle_write_range_debug(shuffle_ctx_t* ctx, const char* skey,
                                      unsigned char skey_len,
                                      unsigned char buf_sz, int epoch,
                                      int src, int dst) {
  float h;
  assert(skey_len == sizeof(h));
  memcpy(&h, skey, sizeof(h));

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

static int shuffle_flush_oob(int epoch) {
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

// carp/range: the shuffle key/value should match the preload inkey/invalue
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
    shuffle_write_range_debug(ctx, skey, skey_len, p.buf_sz,
                              epoch, rank, peer_rank);

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
