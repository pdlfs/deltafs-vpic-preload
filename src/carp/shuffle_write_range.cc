/*
 * shuffle_write_range.cc  shuffle_write for carp range store
 */

/* XXXCDC: comment function usages */

#include "shuffle_write_range.h"

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

// carp/range: the shuffle key/value should match the preload inkey/invalue
int shuffle_write_range(shuffle_ctx_t* ctx, const char* skey,
                        unsigned char skey_len, char* svalue,
                        unsigned char svalue_len, int epoch) {
  int my_rank;
  pdlfs::carp::particle_mem_t p;
  bool is_oob, need_oob_flush;
  int rv = 0;

  assert(ctx == &pctx.sctx);
  assert(ctx->skey_len + ctx->svalue_len +
                         ctx->extra_data_len < CARP_MAXPARTSZ);
  if (ctx->skey_len != skey_len) ABORT("bad shuffle key len");
  if (ctx->svalue_len != svalue_len) ABORT("bad shuffle value len");

  my_rank = shuffle_rank(ctx);

  /* Serialize data into p->buf (sets p.shuffle_dest==-1, unknown) */
  /* XXX: could skip this if we knew we were going to call native_write() */
  /*      below.  but the current api needs a complete "p" in order to */
  /*      determine if we'll short circult to native_write(). */
  pctx.carp->Serialize(skey, skey_len, svalue, svalue_len,
                       ctx->extra_data_len, p);

  /*
   * AttemptBuffer will copy "p" to the OOB buffer if it is out of bounds.
   * In that case the buffer will be processed later when OOB is flushed
   * (so we no longer need to directly process "p" here, we'll let flush
   * handle it).  If the OOB buffer is full, AttemptBuffer will trigger
   * a new RTP round (if RTP isn't already running).
   *
   * If RTP is running (due to our OOB "p" just triggering it or it was
   * already running due to some other trigger) then AttemptBuffer
   * will block until the RTP completes.   In this case, AttemptBuffer
   * will ask us to flush oob (since the just completed RTP has changed
   * the range assignments and will likely allow us to clear buffered
   * items).
   *
   * If "p" was in bounds (i.e. !is_oob) then AttemptBuffer assigns a
   * shuffle_dest rank and we must send it now.
   */
  pctx.carp->AttemptBuffer(p, is_oob, need_oob_flush);

  /* write trace if we are in testing mode, shuffle_dest is -1 if oob */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_range_debug(ctx, skey, skey_len, p.buf_sz,
                              epoch, my_rank, p.shuffle_dest);

  if (need_oob_flush) {  /* true if an RTP just completed */
    pctx.carp->FlushOOB(false, epoch);  /* apply a non-purge flush */
  }

  /* we must send "p" now if it wasn't added to OOB buffer */
  if (!is_oob) {

    if (p.shuffle_dest == my_rank && !ctx->force_rpc) {

      /* bypass RPC and take native write short cut if allowed */
      rv = native_write(skey, skey_len, svalue, svalue_len, epoch);

    } else {

      /* send p through the shuffle */
      assert(p.shuffle_dest >= 0 && p.shuffle_dest < pctx.comm_sz);
      /* RTP ctor already ensured ctx->type == SHUFFLE_XN, NN not supported */
      xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), p.buf, p.buf_sz,
                         epoch, p.shuffle_dest, my_rank);

    }
  }    /* !is_oob */

  return rv;
}
