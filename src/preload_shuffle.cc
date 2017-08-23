/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>

#include "preload_internal.h"
#include "preload_mon.h"
#include "preload_shuffle.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"

#include <deltafs-nexus/deltafs-nexus_api.h>
#include <mercury_config.h>
#include <pdlfs-common/xxhash.h>

#include "common.h"

/* shuffle context for the 3-hop shuffler. */
typedef struct _3h_ctx {
  nexus_ctx_t nx;
  void* sh;
} _3h_ctx_t;

void shuffle_epoch_start(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
  } else {
    nn_shuffler_bgwait();
  }
}

void shuffle_epoch_end(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
  } else {
    nn_shuffler_flush_rpcq();
    if (!nnctx.force_sync) {
      /* wait for rpc replies */
      nn_shuffler_wait();
    }
  }
}

static int _3h_shuffle_write(_3h_ctx_t* ctx, const char* fn, char* d, size_t n,
                             int epoch) {
  char buf[200];
  hg_return_t hret;
  unsigned long target;
  unsigned char fname_len;
  const char* fname;
  uint16_t e;
  int dst;
  int rpc_sz;
  int sz;

  /* sanity checks */
  assert(pctx.len_plfsdir != 0);
  assert(pctx.plfsdir != NULL);
  assert(strncmp(fn, pctx.plfsdir, pctx.len_plfsdir) == 0);
  assert(fn != NULL);

  fname = fn + pctx.len_plfsdir + 1; /* remove parent path */
  assert(strlen(fname) < 256);
  fname_len = static_cast<unsigned char>(strlen(fname));
  assert(n < 256);

  dst = 0;  // FIXME

  rpc_sz = 0;
  /* get an estimated size of the rpc */
  rpc_sz += 1 + fname_len + 1; /* vpic fname */
  rpc_sz += 1 + n;             /* vpic data */
  rpc_sz += 2;                 /* epoch */
  assert(rpc_sz <= sizeof(buf));
  sz = 0;

  /* vpic fname */
  buf[sz] = static_cast<char>(fname_len);
  sz += 1;
  memcpy(buf + sz, fname, fname_len);
  sz += fname_len;
  buf[sz] = 0;
  sz += 1;
  /* vpic data */
  buf[sz] = static_cast<char>(n);
  sz += 1;
  memcpy(buf + sz, d, n);
  sz += n;
  /* epoch */
  e = htons(epoch);
  memcpy(buf + sz, &e, 2);
  sz += 2;

  assert(sz == rpc_sz);

  // shuffler_send(ctx->sh, dst, 0, buf, sz);

  if (hret != HG_SUCCESS) {
    rpc_abort("shsend", hret);
  }

  return 0;
}

int shuffle_write(shuffle_ctx_t* ctx, const char* fn, char* d, size_t n,
                  int epoch) {
  if (ctx->type == SHUFFLE_3HOP) {
    return _3h_shuffle_write(static_cast<_3h_ctx_t*>(ctx->rep), fn, d, n,
                             epoch);
  } else {
    return nn_shuffler_write(fn, d, n, epoch);
  }
}

void shuffle_finalize(shuffle_ctx_t* ctx) {
  char msg[200];
  unsigned long long accqsz;
  unsigned long long nps;
  int min_maxqsz;
  int max_maxqsz;
  int min_minqsz;
  int max_minqsz;
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
  } else {
    nn_shuffler_destroy();
    MPI_Reduce(&nnctx.accqsz, &accqsz, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.nps, &nps, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.maxqsz, &min_maxqsz, 1, MPI_INT, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.maxqsz, &max_maxqsz, 1, MPI_INT, MPI_MAX, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.minqsz, &min_minqsz, 1, MPI_INT, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.minqsz, &max_minqsz, 1, MPI_INT, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0 && nps != 0) {
      snprintf(msg, sizeof(msg),
               "[rpc] incoming queue depth: %.3f per rank\n"
               ">>> max: %d - %d, min: %d - %d",
               double(accqsz) / nps, min_maxqsz, max_maxqsz, min_minqsz,
               max_minqsz);
      info(msg);
    }
  }
}

static void _3h_shuffler_init(_3h_ctx_t* ctx) {
  const char* subnet;
  const char* proto;
  char msg[100];

  subnet = maybe_getenv("SHUFFLE_Subnet");
  if (subnet == NULL) {
    subnet = DEFAULT_SUBNET;
  }

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "using subnet %s*", subnet);
    if (strcmp(subnet, "127.0.0.1") == 0) {
      warn(msg);
    } else {
      info(msg);
    }
  }

  proto = maybe_getenv("SHUFFLE_Mercury_proto");
  if (proto == NULL) {
    proto = DEFAULT_HG_PROTO;
  }
  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "using %s", proto);
    if (strstr(proto, "tcp") != NULL) {
      warn(msg);
    } else {
      info(msg);
    }
  }

  ctx->nx =
      nexus_bootstrap(const_cast<char*>(subnet), const_cast<char*>(proto));
  if (ctx->nx == NULL) {
    msg_abort("nexus_bootstrap");
  }
}

void shuffle_init(shuffle_ctx_t* ctx) {
  char msg[200];
  int n;
  if (is_envset("SHUFFLE_Use_3hop")) {
    ctx->type = SHUFFLE_3HOP;
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg), "using the scalable 3-hop shuffler");
      info(msg);
    }
  } else {
    ctx->type = SHUFFLE_NN;
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg),
               "using the default NN shuffler: code might not scale well\n>>> "
               "switch to the 3-hop shuffler for better scalability");
      warn(msg);
    }
  }
  if (ctx->type == SHUFFLE_3HOP) {
    _3h_ctx_t* rep = new _3h_ctx_t;
    _3h_shuffler_init(rep);
    ctx->rep = rep;
  } else {
    nn_shuffler_init();
  }
  if (pctx.my_rank == 0) {
    n = 0;
    n += snprintf(msg + n, sizeof(msg) - n, "HG_HAS_POST_LIMIT is ");
#ifdef HG_HAS_POST_LIMIT
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, ", HG_HAS_SELF_FORWARD is ");
#ifdef HG_HAS_SELF_FORWARD
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, ", HG_HAS_EAGER_BULK is ");
#ifdef HG_HAS_EAGER_BULK
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, "\n>>> HG_HAS_CHECKSUMS is ");
#ifdef HG_HAS_CHECKSUMS
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    info(msg);
  }
}

void shuffle_msg_sent(size_t n, void** arg1, void** arg2) {
  pctx.mctx.min_nms++;
  pctx.mctx.max_nms++;
  pctx.mctx.nms++;
}

void shuffle_msg_replied(void* arg1, void* arg2) {
  pctx.mctx.nmd++; /* delivered */
}

void shuffle_msg_received() {
  pctx.mctx.min_nmr++;
  pctx.mctx.max_nmr++;
  pctx.mctx.nmr++;
}
