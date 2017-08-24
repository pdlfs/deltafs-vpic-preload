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
#include "preload_internal.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffler.h"

void xn_shuffle_deliver(int src, int dst, int type, void* buf, int buf_sz) {
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
    msg_abort("rpc_corruption");
  }
  memcpy(&r, input, 4);
  if (src != ntohl(r)) msg_abort("bad src");
  input_left -= 4;
  input += 4;
  memcpy(&r, input, 4);
  if (dst != ntohl(r)) msg_abort("bad dst");
  input_left -= 4;
  input += 4;

  /* vpic fname */
  if (input_left < 1) {
    msg_abort("rpc_corruption");
  }
  fname_len = static_cast<unsigned char>(input[0]);
  input_left -= 1;
  input += 1;
  if (input_left < fname_len + 1) {
    msg_abort("rpc_corruption");
  }
  fname = input;
  assert(strlen(fname) == fname_len);
  input_left -= fname_len + 1;
  input += fname_len + 1;

  /* vpic data */
  if (input_left < 1) {
    msg_abort("rpc_corruption");
  }
  len = static_cast<unsigned char>(input[0]);
  input_left -= 1;
  input += 1;
  if (input_left < len) {
    msg_abort("rpc_corruption");
  }
  data = input;
  input_left -= len;
  input += len;

  /* epoch */
  if (input_left < 2) {
    msg_abort("rpc_corruption");
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
    msg_abort("xxwrite");
  }
}

void xn_shuffler_write(xn_ctx_t* ctx, const char* fn, char* data, size_t len,
                       int epoch) {
  char buf[200];
  char msg[200];
  hg_return_t hret;
  unsigned long target;
  const char* fname;
  size_t fname_len;
  uint32_t r;
  uint16_t e;
  int ha;
  int src;
  int dst;
  int rpc_sz;
  int sz;
  int n;

  /* sanity checks */
  assert(ctx != NULL);
  assert(ctx->nx != NULL);
  src = nexus_global_rank(ctx->nx);

  assert(pctx.len_plfsdir != 0);
  assert(pctx.plfsdir != NULL);
  assert(strncmp(fn, pctx.plfsdir, pctx.len_plfsdir) == 0);
  assert(fn != NULL);

  fname = fn + pctx.len_plfsdir + 1; /* remove parent path */
  assert(strlen(fname) < 256);
  fname_len = strlen(fname);
  assert(len < 256);

  if (nexus_global_size(ctx->nx) != 1) {
    if (IS_BYPASS_PLACEMENT(pctx.mode)) {
      dst =
          pdlfs::xxhash32(fname, strlen(fname), 0) % nexus_global_size(ctx->nx);
    } else {
      assert(ctx->ch != NULL);
      ch_placement_find_closest(
          ctx->ch, pdlfs::xxhash64(fname, strlen(fname), 0), 1, &target);
      dst = int(target);
    }
  } else {
    dst = src;
  }

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    ha = pdlfs::xxhash32(data, len, 0); /* data checksum */
    n = snprintf(msg, sizeof(msg),
                 "[SEND] %s %d bytes (e%d) r%d >> "
                 "r%d (hash=%08x)\n",
                 fn, int(len), epoch, src, dst, ha);

    n = write(pctx.logfd, msg, n);

    errno = 0;
  }

  sz = rpc_sz = 0;

  /* get an estimated size of the rpc */
  rpc_sz += 4;                 /* src rank */
  rpc_sz += 4;                 /* dst rank */
  rpc_sz += 1 + fname_len + 1; /* vpic fname */
  rpc_sz += 1 + len;           /* vpic data */
  rpc_sz += 2;                 /* epoch */
  assert(rpc_sz <= sizeof(buf));

  /* rank */
  r = htonl(src);
  memcpy(buf + sz, &r, 4);
  sz += 4;
  r = htonl(dst);
  memcpy(buf + sz, &r, 4);
  sz += 4;
  /* vpic fname */
  buf[sz] = static_cast<unsigned char>(fname_len);
  sz += 1;
  memcpy(buf + sz, fname, fname_len);
  sz += fname_len;
  buf[sz] = 0;
  sz += 1;
  /* vpic data */
  buf[sz] = static_cast<unsigned char>(len);
  sz += 1;
  memcpy(buf + sz, data, len);
  sz += len;
  /* epoch */
  e = htons(epoch);
  memcpy(buf + sz, &e, 2);
  sz += 2;
  assert(sz == rpc_sz);

  assert(ctx->sh != NULL);
  hret = shuffler_send(ctx->sh, dst, 0, buf, sz);

  if (hret != HG_SUCCESS) {
    rpc_abort("xxsend", hret);
  }
}

void xn_shuffler_init_ch_placement(xn_ctx_t* ctx) {
  char msg[100];
  const char* proto;
  const char* env;
  int rank; /* nx */
  int size; /* nx */
  int vf;

  assert(ctx->nx != NULL);

  rank = nexus_global_rank(ctx->nx);
  size = nexus_global_size(ctx->nx);

  if (pctx.paranoid_checks) {
    if (size != pctx.comm_sz || rank != pctx.my_rank) {
      msg_abort("nx-mpi disagree");
    }
  }

  if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
    env = maybe_getenv("SHUFFLE_Virtual_factor");
    if (env == NULL) {
      vf = DEFAULT_VIRTUAL_FACTOR;
    } else {
      vf = atoi(env);
    }

    proto = maybe_getenv("SHUFFLE_Placement_protocol");
    if (proto == NULL) {
      proto = DEFAULT_PLACEMENT_PROTO;
    }

    ctx->ch = ch_placement_initialize(proto, size, vf /* vir factor */,
                                      0 /* hash seed */);
    if (ctx->ch == NULL) {
      msg_abort("ch_init");
    }
  }

  if (pctx.my_rank == 0) {
    if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
      snprintf(msg, sizeof(msg),
               "ch-placement group size: %s (vir-factor: %s, proto: %s)",
               pretty_num(size).c_str(), pretty_num(vf).c_str(), proto);
      info(msg);
    } else {
      warn("ch-placement bypassed");
    }
  }
}

void xn_shuffler_init(xn_ctx_t* ctx) {
  char rpc_name[] = "shuffle_rpc_write";
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

  xn_shuffler_init_ch_placement(ctx);

  ctx->sh = shuffler_init(ctx->nx, rpc_name, 4, 4 << 10, 16, 32 << 10, 256,
                          xn_shuffle_deliver);

  if (ctx->sh == NULL) {
    msg_abort("sh_init");
  }
}
