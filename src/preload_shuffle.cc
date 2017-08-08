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

#include "preload_shuffle.h"
#include "preload_internal.h"
#include "preload_mon.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"

#include <deltafs-nexus/deltafs-nexus_api.h>

#include "common.h"

/* shuffle context for the 3-hop shuffler. */
typedef struct _3h_ctx {
  nexus_ctx_t* nx;
  void* sh;
} _3h_ctx_t;

void shuffle_epoch_start(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
  }
}

void shuffle_epoch_end(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
  } else {
    nn_shuffler_flush();
    if (!nnctx.force_sync) {
      /* wait for rpc replies */
      nn_shuffler_wait();
    }
  }
}

int shuffle_write(shuffle_ctx_t* ctx, const char* fn, char* d, size_t n,
                  int epoch) {
  if (ctx->type == SHUFFLE_3HOP) {
    // TODO
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
    if (pctx.myrank == 0 && nps != 0) {
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
  int min_port;
  int max_port;
  const char* env;
  const char* subnet;
  const char* proto;
  nexus_ret_t ret;
  char msg[100];

  subnet = maybe_getenv("SHUFFLE_Subnet");
  if (subnet == NULL) {
    subnet = DEFAULT_SUBNET;
  }

  if (pctx.myrank == 0) {
    snprintf(msg, sizeof(msg), "using subnet %s*", subnet);
    if (strcmp(subnet, "127.0.0.1") == 0) {
      warn(msg);
    } else {
      info(msg);
    }
  }

  env = maybe_getenv("SHUFFLE_Min_port");
  if (env == NULL) {
    min_port = DEFAULT_MIN_PORT;
  } else {
    min_port = atoi(env);
  }

  env = maybe_getenv("SHUFFLE_Max_port");
  if (env == NULL) {
    max_port = DEFAULT_MAX_PORT;
  } else {
    max_port = atoi(env);
  }

  /* sanity check on port range */
  if (max_port - min_port < 0) msg_abort("bad min-max port");
  if (min_port < 1) msg_abort("bad min port");
  if (max_port > 65535) msg_abort("bad max port");

  if (pctx.myrank == 0) {
    snprintf(msg, sizeof(msg), "using port range [%d,%d]", min_port, max_port);
    info(msg);
  }

  proto = maybe_getenv("SHUFFLE_Mercury_proto");
  if (proto == NULL) {
    proto = DEFAULT_HG_PROTO;
  }
  if (pctx.myrank == 0) {
    snprintf(msg, sizeof(msg), "using %s", proto);
    if (strstr(proto, "tcp") != NULL) {
      warn(msg);
    } else {
      info(msg);
    }
  }

  ret = nexus_bootstrap(ctx->nx, min_port, max_port, const_cast<char*>(subnet),
                        const_cast<char*>(proto));
  if (ret != NX_SUCCESS) {
    msg_abort("nexus_bootstrap");
  }
}

void shuffle_init(shuffle_ctx_t* ctx) {
  char msg[150];
  if (is_envset("SHUFFLE_Use_3hop")) {
    ctx->type = SHUFFLE_3HOP;
    if (pctx.myrank == 0) {
      snprintf(msg, sizeof(msg), "using the scalable 3-hop shuffler");
      info(msg);
    }
  } else {
    ctx->type = SHUFFLE_NN;
    if (pctx.myrank == 0) {
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
