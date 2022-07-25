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

int shuffle_write_range(shuffle_ctx_t* ctx, const char* fname,
                        unsigned char fname_len, char* data,
                        unsigned char data_len, int epoch) {
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
  //  unsigned char buf_sz = base_sz + ctx->extra_data_len;

  /* Serialize data */
  pdlfs::carp::particle_mem_t p;
  pctx.carp->Serialize(fname, fname_len, data, data_len, ctx->extra_data_len,
                       p);

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
    rv = native_write(reinterpret_cast<char*>(&p.indexed_prop), sizeof(float),
                      p.data_ptr, p.data_sz, epoch);
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
    nn_shuffler_enqueue(p.buf, p.buf_sz, epoch, peer_rank, rank);
  }

  return rv;
}
