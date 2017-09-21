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

#include <arpa/inet.h>
#include <assert.h>

#include <pdlfs-common/xxhash.h>

#include "common.h"
#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffler.h"

/*
 * This function is called at the end of each epoch. We expect there is a long
 * computation phase between two epochs that can serve as a virtual barrier. As
 * such, here we first do a collective local-node flush, and then do a remote
 * flush. Each flush ensures all buffered (or waiting-listed) requests are sent
 * out and their replies received. At the end of this function, however, we
 * still have no idea if we have received all remote requests.
 */
void xn_shuffler_epoch_end(xn_ctx_t* ctx) {
  hg_return_t hret;
  assert(ctx != NULL);

  assert(ctx->sh != NULL);
  assert(ctx->nx != NULL);

  hret = shuffler_flush_localqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush local queues", hret);
  }
  if (ctx->force_global_barrier) {
    nexus_global_barrier(ctx->nx);
  } else {
    nexus_local_barrier(ctx->nx);
  }
  hret = shuffler_flush_remoteqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush remote queues", hret);
  }
}

/*
 * This function is called at the beginning of each epoch. Since we assume there
 * is a long computation phase that can serve as a virtual barrier, we may now
 * consider all remote requests sent by other folks at the end of the previous
 * epoch have now been received by us. What we need to do is another collective
 * local flush to forward them to their final destinations.
 */
void xn_shuffler_epoch_start(xn_ctx_t* ctx) {
  hg_return_t hret;
  assert(ctx != NULL);

  assert(ctx->sh != NULL);
  assert(ctx->nx != NULL);

  hret = shuffler_flush_localqs(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush local queues", hret);
  }
  if (ctx->force_global_barrier) {
    nexus_global_barrier(ctx->nx);
  } else {
    nexus_local_barrier(ctx->nx);
  }
  ctx->last_stat = ctx->stat;
  shuffler_send_stats(ctx->sh, &ctx->stat.local.sends, &ctx->stat.remote.sends);
  shuffler_recv_stats(ctx->sh, &ctx->stat.local.recvs, &ctx->stat.remote.recvs);
  hret = shuffler_flush_delivery(ctx->sh);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("fail to flush delivery", hret);
  }
}

void xn_shuffler_deliver(int src, int dst, int type, void* buf, int buf_sz) {
  char* input;
  size_t input_left;
  char path[PATH_MAX];
  char msg[200];
  const char* fname;
  size_t fname_len;
  uint32_t r;
  uint16_t e;
  int ha;
  int epoch;
  char* data;
  size_t len;
  int rv;
  int n;

  assert(buf_sz >= 0);
  input_left = static_cast<size_t>(buf_sz);
  input = static_cast<char*>(buf);
  assert(input != NULL);

  /* rank */
  if (input_left < 8) {
    ABORT("rpc msg corrupted");
  }
  memcpy(&r, input, 4);
  if (src != ntohl(r)) ABORT("bad src");
  input_left -= 4;
  input += 4;
  memcpy(&r, input, 4);
  if (dst != ntohl(r)) ABORT("bad dst");
  input_left -= 4;
  input += 4;

  /* vpic fname */
  if (input_left < 1) {
    ABORT("rpc msg corrupted");
  }
  fname_len = static_cast<unsigned char>(input[0]);
  input_left -= 1;
  input += 1;
  if (input_left < fname_len + 1) {
    ABORT("rpc msg corrupted");
  }
  fname = input;
  assert(strlen(fname) == fname_len);
  input_left -= fname_len + 1;
  input += fname_len + 1;

  /* vpic data */
  if (input_left < 1) {
    ABORT("rpc msg corrupted");
  }
  len = static_cast<unsigned char>(input[0]);
  input_left -= 1;
  input += 1;
  if (input_left < len) {
    ABORT("rpc msg corrupted");
  }
  data = input;
  input_left -= len;
  input += len;

  /* epoch */
  if (input_left < 2) {
    ABORT("rpc msg corrupted");
  }
  memcpy(&e, input, 2);
  epoch = ntohs(e);

  assert(pctx.len_plfsdir != 0);
  assert(pctx.plfsdir != NULL);
  snprintf(path, sizeof(path), "%s/%s", pctx.plfsdir, fname);
  rv = preload_foreign_write(path, data, len, epoch);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    ha = pdlfs::xxhash32(data, len, 0); /* data checksum */
    n = snprintf(msg, sizeof(msg),
                 "[RECV] %s %d bytes (e%d) r%d "
                 "<< r%d (hash=%08x)\n",
                 path, int(len), epoch, dst, src, ha);
    n = write(pctx.logfd, msg, n);

    errno = 0;
  }

  if (rv != 0) {
    ABORT("plfsdir write failed");
  }
}

void xn_shuffler_enqueue(xn_ctx_t* ctx, const char* fname,
                         unsigned char fname_len, char* data, size_t len,
                         int epoch, int dst, int src) {
  char buf[200];
  hg_return_t hret;
  uint32_t r;
  uint16_t e;
  int rpc_sz;
  int off;

  assert(fname != NULL);
  assert(fname_len != 0);
  assert(len < 256);

  off = rpc_sz = 0;

  /* get an estimated size of the rpc */
  rpc_sz += 4;                 /* src rank */
  rpc_sz += 4;                 /* dst rank */
  rpc_sz += 1 + fname_len + 1; /* vpic fname */
  rpc_sz += 1 + len;           /* vpic data */
  rpc_sz += 2;                 /* epoch */
  assert(rpc_sz <= sizeof(buf));

  /* rank */
  r = htonl(src);
  memcpy(buf + off, &r, 4);
  off += 4;
  r = htonl(dst);
  memcpy(buf + off, &r, 4);
  off += 4;
  /* vpic fname */
  buf[off] = fname_len;
  off += 1;
  memcpy(buf + off, fname, fname_len);
  off += fname_len;
  buf[off] = 0;
  off += 1;
  /* vpic data */
  buf[off] = static_cast<unsigned char>(len);
  off += 1;
  memcpy(buf + off, data, len);
  off += len;
  /* epoch */
  e = htons(epoch);
  memcpy(buf + off, &e, 2);
  off += 2;
  assert(off == rpc_sz);

  assert(ctx->sh != NULL);
  hret = shuffler_send(ctx->sh, dst, 0, buf, off);

  if (hret != HG_SUCCESS) {
    RPC_FAILED("plfsdir shuffler send failed", hret);
  }
}

void xn_shuffler_init(xn_ctx_t* ctx) {
  int deliverq_max;
  int lmaxrpc;
  int lbuftarget;
  int rmaxrpc;
  int rbuftarget;
  const char* logfile;
  const char* env;
  char msg[5000];
  char uri[100];
  int n;

  assert(ctx != NULL);

  shuffle_prepare_uri(uri);
  ctx->nx = nexus_bootstrap_uri(uri);
  if (ctx->nx == NULL) {
    ABORT("nexus_bootstrap_uri");
  }
  if (pctx.paranoid_checks) {
    if (nexus_global_size(ctx->nx) != pctx.comm_sz ||
        nexus_global_rank(ctx->nx) != pctx.my_rank) {
      ABORT("nx-mpi disagree");
    }
  }

  env = maybe_getenv("SHUFFLE_Local_maxrpc");
  if (env == NULL) {
    lmaxrpc = DEFAULT_OUTSTANDING_RPC;
  } else {
    lmaxrpc = atoi(env);
    if (lmaxrpc <= 0) {
      lmaxrpc = 1;
    }
  }

  env = maybe_getenv("SHUFFLE_Remote_maxrpc");
  if (env == NULL) {
    rmaxrpc = DEFAULT_OUTSTANDING_RPC;
  } else {
    rmaxrpc = atoi(env);
    if (rmaxrpc <= 0) {
      rmaxrpc = 1;
    }
  }

  env = maybe_getenv("SHUFFLE_Local_buftarget");
  if (env == NULL) {
    lbuftarget = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    lbuftarget = atoi(env);
    if (lbuftarget < 24) {
      lbuftarget = 24;
    }
  }

  env = maybe_getenv("SHUFFLE_Remote_buftarget");
  if (env == NULL) {
    rbuftarget = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    rbuftarget = atoi(env);
    if (rbuftarget < 24) {
      rbuftarget = 24;
    }
  }

  env = maybe_getenv("SHUFFLE_Max_deliverq");
  if (env == NULL) {
    deliverq_max = DEFAULT_DELIVER_MAX;
  } else {
    deliverq_max = atoi(env);
    if (deliverq_max <= 0) {
      deliverq_max = 1;
    }
  }

  logfile = maybe_getenv("SHUFFLE_Log_file");
#define DEF_CFGLOG_ARGS(log) -1, "INFO", "WARN", NULL, NULL, log, 1, 0, 0, 0
  if (logfile != NULL && logfile[0] != 0) {
    shuffler_cfglog(DEF_CFGLOG_ARGS(logfile));
  }

  ctx->sh = shuffler_init(ctx->nx, const_cast<char*>("shuffle_rpc_write"),
                          lmaxrpc, lbuftarget, rmaxrpc, rbuftarget,
                          deliverq_max, xn_shuffler_deliver);

  if (ctx->sh == NULL) {
    ABORT("shuffler_init");
  } else if (pctx.my_rank == 0) {
    n = snprintf(msg, sizeof(msg),
                 "shuffler: maxrpc(l/r)=%d/%d buftgt(l/r)=%d/%d dqmax=%d",
                 lmaxrpc, rmaxrpc, lbuftarget, rbuftarget, deliverq_max);
    if (logfile != NULL && logfile[0] != 0) {
      snprintf(msg + n, sizeof(msg) - n,
               "\n>>> LOGGING is ON, will log to ..."
               "\n -----------> %s.[0-%d]",
               logfile, pctx.comm_sz);
    }
    INFO(msg);
  }

  if (is_envset("SHUFFLE_Force_global_barrier")) {
    ctx->force_global_barrier = 1;
    if (pctx.my_rank == 0) {
      WARN("force global barriers");
    }
  }
}

int xn_shuffler_world_size(xn_ctx_t* ctx) {
  assert(ctx != NULL);
  assert(ctx->nx != NULL);
  int rv = nexus_global_size(ctx->nx);
  assert(rv > 0);
  return rv;
}

int xn_shuffler_my_rank(xn_ctx_t* ctx) {
  assert(ctx != NULL);
  assert(ctx->nx != NULL);
  int rv = nexus_global_rank(ctx->nx);
  assert(rv >= 0);
  return rv;
}

void xn_shuffler_destroy(xn_ctx_t* ctx) {
  if (ctx != NULL) {
    if (ctx->sh != NULL) {
#ifndef NDEBUG
      shuffler_send_stats(ctx->sh, &ctx->stat.local.sends,
                          &ctx->stat.remote.sends);
      shuffler_recv_stats(ctx->sh, &ctx->stat.local.recvs,
                          &ctx->stat.remote.recvs);
#endif
      shuffler_shutdown(ctx->sh);
      ctx->sh = NULL;
    }
    if (ctx->nx != NULL) {
      nexus_destroy(ctx->nx);
      ctx->nx = NULL;
    }
  }
}
