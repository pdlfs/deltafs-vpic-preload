/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <arpa/inet.h>
#include <assert.h>

#include "common.h"
#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffle.h"

/* xn_local_barrier: perform a barrier across all node-local ranks. */
void xn_local_barrier(xn_ctx_t* ctx) {
  nexus_ret_t nret;
  assert(ctx != NULL && ctx->nx != NULL);
  if (ctx->force_global_barrier) {
    nret = nexus_global_barrier(ctx->nx);
  } else {
    nret = nexus_local_barrier(ctx->nx);
  }
  if (nret != NX_SUCCESS) {
    ABORT("nexus_barrier");
  }
}

/*
 * This function is called at the end of each epoch. We expect there is a long
 * computation phase between two epochs that can serve as a virtual barrier. As
 * such, here we first do a collective local-node flush, and then do a remote
 * flush. Each flush ensures all buffered (or waiting-listed) requests are sent
 * out and their replies received. At the end of this function, however, we
 * still have no idea if we have received all remote requests.
 */
void xn_shuffle_epoch_end(xn_ctx_t* ctx) {
  hg_return_t hret;
  assert(ctx != NULL && ctx->sh != NULL);
  hret = shuffle_flush_originqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush local origin queues", hret);
  }

  // hret = shuffle_flush_originqs(ctx->psh);
  // if (hret != HG_SUCCESS) {
    // RPC_FAILED("fail to flush local priority origin queues", hret);
  // }

  xn_local_barrier(ctx);
  hret = shuffle_flush_remoteqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush remote queues", hret);
  }

  // hret = shuffle_flush_remoteqs(ctx->psh);
  // if (hret != HG_SUCCESS) {
    // RPC_FAILED("fail to flush remote priority queues", hret);
  // }
}

/*
 * This function is called at the beginning of each epoch. Since we assume there
 * is a long computation phase that can serve as a virtual barrier, we may now
 * consider all remote requests sent by other folks at the end of the previous
 * epoch have now been received by us. What we need to do is another collective
 * local flush to forward them to their final destinations.
 */
void xn_shuffle_epoch_start(xn_ctx_t* ctx) {
  hg_return_t hret;
  hg_uint64_t tmpori;
  hg_uint64_t tmprl;
  assert(ctx != NULL && ctx->sh != NULL);

  hret = shuffle_flush_relayqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush local relay queues", hret);
  }

  // hret = shuffle_flush_relayqs(ctx->psh);
  // if (hret != HG_SUCCESS) {
    // RPC_FAILED("fail to flush local priorty relay queues", hret);
  // }

  xn_local_barrier(ctx);
  ctx->last_stat = ctx->stat;
  shuffle_send_stats(ctx->sh, &tmpori, &tmprl, &ctx->stat.remote.sends);
  ctx->stat.local.sends = tmpori + tmprl;

  shuffle_recv_stats(ctx->sh, &ctx->stat.local.recvs, &ctx->stat.remote.recvs);
  hret = shuffle_flush_delivery(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush delivery", hret);
  }

  // hret = shuffle_flush_delivery(ctx->psh);
  // if (hret != HG_SUCCESS) {
    // RPC_FAILED("fail to flush priority delivery", hret);
  // }
}

static void xn_shuffle_deliver(int src, int dst, uint32_t type, void* buf,
                               uint32_t buf_sz) {
  int rv;

  rv = shuffle_handle(NULL, static_cast<char*>(buf), buf_sz, -1, src, dst);

  if (rv != 0) {
    ABORT("plfsdir write failed");
  }
}

void xn_shuffle_enqueue(xn_ctx_t* ctx, void* buf, unsigned char buf_sz,
                        int epoch, int dst, int src) {
  hg_return_t hret;
  assert(ctx->sh != NULL);
  hret = shuffle_enqueue(ctx->sh, dst, 0, buf, buf_sz);

  if (hret != HG_SUCCESS) {
    RPC_FAILED("plfsdir shuffler send failed", hret);
  }
}

void xn_shuffle_priority_send(xn_ctx_t* ctx, void* buf, unsigned char buf_sz,
                               int epoch, int dst, int src) {
  hg_return_t hret;
  assert(ctx->psh != NULL);
  hret = shuffle_enqueue(ctx->psh, dst, 0, buf, buf_sz);

  if (hret != HG_SUCCESS) {
    RPC_FAILED("plfsdir shuffler priority send failed", hret);
  }
}

void xn_shuffle_init(xn_ctx_t* ctx) {
  hg_class_t *hgcls;
  hg_context_t *hgctx;
  struct shuffle_opts so;
  const char* logfile;
  const char* env;
  char uri[100];
  int n;

  assert(ctx != NULL);

  shuffle_opts_init(&so);
  shuffle_prepare_uri(uri);     /* uri is an 'out', XXX: uri fixed sized */
  hgcls = HG_Init(uri, HG_TRUE);
  if (!hgcls) {
    ABORT("xn_shuffle_init:HG_Init net");
  }
  hgctx = HG_Context_create(hgcls);
  if (!hgctx) {
    ABORT("xn_shuffle_init:HG_Context_create net");
  }
  ctx->nethand = mercury_progressor_init(hgcls, hgctx);
  if (!ctx->nethand) {
    ABORT("xn_shuffle_init:progressor_init net");
  }
  ctx->localhand = NULL;    /* NULL means create a new na+sm */

  /*
   * these NEXUS vars migrated here when we added progressor, but
   * we keep the name for backward compat.
   */
  if (is_envset("NEXUS_BYPASS_LOCAL")) {

    /* use remote for all local comm. */
    ctx->localhand = ctx->nethand;

  } else if ((env = maybe_getenv("NEXUS_ALT_LOCAL")) != NULL) {

    /* use an alternate local mercury */
    hgcls = HG_Init(env, HG_TRUE);
    if (!hgcls) {
      ABORT("xn_shuffle_init:HG_Init alt-local");
    }
    hgctx = HG_Context_create(hgcls);
    if (!hgctx) {
      ABORT("xn_shuffle_init:HG_Context_create alt-local");
    }
    ctx->localhand = mercury_progressor_init(hgcls, hgctx);
    if (!ctx->localhand) {
      ABORT("xn_shuffle_init:progressor_init alt-local");
    }
  }

  ctx->nx = nexus_bootstrap(ctx->nethand, ctx->localhand);
  if (ctx->nx == NULL) {
    ABORT("nexus_bootstrap");
  }
  if (pctx.paranoid_checks) {
    if (nexus_global_size(ctx->nx) != pctx.comm_sz ||
        nexus_global_rank(ctx->nx) != pctx.my_rank) {
      ABORT("nx-mpi disagree");
    }
  }

  env = maybe_getenv("SHUFFLE_Local_senderlimit");
  if (env == NULL) {
    so.localsenderlimit = 0;
  } else {
    so.localsenderlimit = atoi(env);
    if (so.localsenderlimit < 0) {
      so.localsenderlimit = 0;
    }
  }

  env = maybe_getenv("SHUFFLE_Remote_senderlimit");
  if (env == NULL) {
    so.remotesenderlimit = 0;
  } else {
    so.remotesenderlimit = atoi(env);
    if (so.remotesenderlimit < 0) {
      so.remotesenderlimit = 0;
    }
  }

  env = maybe_getenv("SHUFFLE_Relay_maxrpc");
  if (env == NULL) {
    so.lrmaxrpc = DEFAULT_OUTSTANDING_RPC;
  } else {
    so.lrmaxrpc = atoi(env);
    if (so.lrmaxrpc <= 0) {
      so.lrmaxrpc = 0;
    }
  }

  env = maybe_getenv("SHUFFLE_Local_maxrpc");
  if (env == NULL) {
    so.lomaxrpc = DEFAULT_OUTSTANDING_RPC;
  } else {
    so.lomaxrpc = atoi(env);
    if (so.lomaxrpc <= 0) {
      so.lomaxrpc = 1;
    }
  }

  env = maybe_getenv("SHUFFLE_Remote_maxrpc");
  if (env == NULL) {
    so.rmaxrpc = DEFAULT_OUTSTANDING_RPC;
  } else {
    so.rmaxrpc = atoi(env);
    if (so.rmaxrpc <= 0) {
      so.rmaxrpc = 1;
    }
  }

  env = maybe_getenv("SHUFFLE_Relay_buftarget");
  if (env == NULL) {
    so.lrbuftarget = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    so.lrbuftarget = atoi(env);
    if (so.lrbuftarget < 24) {
      so.lrbuftarget = 24;
    }
  }

  env = maybe_getenv("SHUFFLE_Local_buftarget");
  if (env == NULL) {
    so.lobuftarget = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    so.lobuftarget = atoi(env);
    if (so.lobuftarget < 24) {
      so.lobuftarget = 24;
    }
  }

  env = maybe_getenv("SHUFFLE_Remote_buftarget");
  if (env == NULL) {
    so.rbuftarget = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    so.rbuftarget = atoi(env);
    if (so.rbuftarget < 24) {
      so.rbuftarget = 24;
    }
  }

  env = maybe_getenv("SHUFFLE_Dq_min");
  if (env == NULL) {
    so.deliverq_threshold = 0;
  } else {
    so.deliverq_threshold = atoi(env);
    if (so.deliverq_threshold < 0) {
      so.deliverq_threshold = 0;
    }
  }

  env = maybe_getenv("SHUFFLE_Dq_max");
  if (env == NULL) {
    so.deliverq_max = DEFAULT_DELIVER_MAX;
  } else {
    so.deliverq_max = atoi(env);
    if (so.deliverq_max <= 0) {
      so.deliverq_max = -1;
    }
  }

  logfile = maybe_getenv("SHUFFLE_Log_file");
#define DEF_CFGLOG_ARGS(log) -1, "INFO", "WARN", NULL, NULL, log, 1, 0, 0, 0
  if (logfile != NULL && logfile[0] != 0 && strcmp(logfile, "/") != 0) {
    shuffle_cfglog(DEF_CFGLOG_ARGS(logfile));
  }

  ctx->sh = shuffle_init(ctx->nx, const_cast<char*>("shuffle_rpc_write"),
                          xn_shuffle_deliver, &so);

  if (ctx->sh == NULL) {
    ABORT("shuffler_init");
  } else if (pctx.my_rank == 0) {
    logf(LOG_INFO,
         "3-HOP confs: sndlim(l/r)=%d/%d, maxrpc(lo/lr/r)=%d/%d/%d, "
         "buftgt(lo/lr/r)=%d/%d/%d, dq(min/max)=%d/%d",
         so.localsenderlimit, so.remotesenderlimit, so.lomaxrpc, so.lrmaxrpc,
         so.rmaxrpc, so.lobuftarget, so.lrbuftarget, so.rbuftarget,
         so.deliverq_threshold, so.deliverq_max);
    if (logfile != NULL && logfile[0] != 0 && strcmp(logfile, "/") != 0) {
      fputs(">>> LOGGING is ON, will log to ...\n --> ", stderr);
      fputs(logfile, stderr);
      fprintf(stderr, ".[0-%d]\n", pctx.comm_sz);
    }
  }

  /*
   * Priority shuffler has identical arguments, except it does not batch RPCs
   * All buftarget values are set to 1 here, and its network/delivery threads
   * are disabled, that job is delegated to the regular shuffler
   *
   * Last argument is false to direct psh to not start network threads
   */

  so.lobuftarget = 1;
  so.lrbuftarget = 1;
  so.rbuftarget = 1;

  ctx->psh = shuffle_init(ctx->nx, const_cast<char*>("shuffle_rpc_priority"),
                           xn_shuffle_deliver, &so);

  if (ctx->psh == NULL) {
    ABORT("priority_shuffler_init");
  } else if (pctx.my_rank == 0) {
    logf(LOG_INFO,
         "PRIORITY 3-HOP active; confs: sndlim(l/r)=%d/%d, "
         "maxrpc(lo/lr/r)=%d/%d/%d, "
         "buftgt(lo/lr/r)=%d/%d/%d, dq(min/max)=%d/%d",
         so.localsenderlimit, so.remotesenderlimit, so.lomaxrpc, so.lrmaxrpc,
         so.rmaxrpc, so.lobuftarget, so.lrbuftarget, so.rbuftarget,
         so.deliverq_threshold, so.deliverq_max);
    if (logfile != NULL && logfile[0] != 0 && strcmp(logfile, "/") != 0) {
      fputs(">>> LOGGING is ON, will log to ...\n --> ", stderr);
      fputs(logfile, stderr);
      fprintf(stderr, ".[0-%d]\n", pctx.comm_sz);
    }
  }

  if (is_envset("SHUFFLE_Force_global_barrier")) {
    ctx->force_global_barrier = 1;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "force global barriers");
    }
  }
}

int xn_shuffle_world_size(xn_ctx_t* ctx) {
  assert(ctx != NULL);
  assert(ctx->nx != NULL);
  int rv = nexus_global_size(ctx->nx);
  assert(rv > 0);
  return rv;
}

int xn_shuffle_my_rank(xn_ctx_t* ctx) {
  assert(ctx != NULL);
  assert(ctx->nx != NULL);
  int rv = nexus_global_rank(ctx->nx);
  assert(rv >= 0);
  return rv;
}

void xn_shuffle_destroy(xn_ctx_t* ctx) {
  if (ctx != NULL) {
    // shutdown the priorty shuffler first
    if (ctx->sh != NULL) {
#ifndef NDEBUG
      hg_uint64_t tmpori;
      hg_uint64_t tmprl;
      shuffle_send_stats(ctx->sh, &tmpori, &tmprl, &ctx->stat.remote.sends);
      ctx->stat.local.sends = tmpori + tmprl;
      shuffle_recv_stats(ctx->sh, &ctx->stat.local.recvs,
                          &ctx->stat.remote.recvs);
#endif
      shuffle_shutdown(ctx->sh);
      ctx->sh = NULL;
    }

    if (ctx->psh != NULL) {
      shuffle_shutdown(ctx->psh);
      ctx->psh = NULL;
    }

    if (ctx->nx != NULL) {
      nexus_destroy(ctx->nx);
      ctx->nx = NULL;
    }
    if (ctx->localhand && ctx->localhand != ctx->nethand) {
      /* for NEXUS_ALT_LOCAL case */
      mercury_progressor_freehandle(ctx->localhand);
    }
    ctx->localhand = NULL;
    if (ctx->nethand) {
      mercury_progressor_freehandle(ctx->nethand);
      ctx->nethand = NULL;
    }
  }
}
