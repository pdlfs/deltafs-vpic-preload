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

#pragma once

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>

#include <ch-placement.h>
#include <mercury.h>
#include <mercury_proc_string.h>
#include <mpi.h>
#include <ssg-mpi.h>
#include <ssg.h>

#include <deltafs/deltafs_api.h>
#include "preload_mon.h"

#include "preload.h"
#include "preload_internal.h"

extern "C" {
/*
 * The max allowed size for a single rpc message.
 *
 * A TCP/IP jumbo frame can be as many as 9000 bytes for modern nics.
 *
 * (MTU=9000)
 */
#define MAX_RPC_MESSAGE 65536

/*
 * rpc_abort: abort with a mercury rpc error
 */
static inline void rpc_abort(const char* msg, hg_return_t ret) {
  char tmp[500];
  const char* err = HG_Error_to_string(ret);
  int n = snprintf(tmp, sizeof(tmp), "!!!ABORT!!! %s: %s\n", msg, err);
  n = write(fileno(stderr), tmp, n);
  abort();
}

/*
 * shuffle context:
 *   - run-time state of the preload and shuffle lib
 */
typedef struct shuffle_ctx {
  char my_addr[100]; /* mercury server uri */

  hg_class_t* hg_clz;
  hg_context_t* hg_ctx;
  hg_id_t hg_id;

  int timeout;    /* rpc timeout (in secs) */
  int force_sync; /* avoid async rpc */
  int force_rpc;  /* send rpc even if addr is local */

  /* SSG context */
  ssg_t ssg;

  /* ch-placement context */
  struct ch_placement_instance* chp;

} shuffle_ctx_t;

extern shuffle_ctx_t sctx;

typedef struct write_in {
  char encoding[MAX_RPC_MESSAGE]; /* buffer space for encoded contents */
} write_in_t;

typedef struct write_out {
  hg_int32_t rv; /* ret value of the write operation */
} write_out_t;

typedef struct write_cb {
  int ok; /* non-zero if rpc has completed */
  hg_return_t hret;
} write_cb_t;

typedef struct write_async_cb {
  void* arg1;
  void* arg2;
  void (*cb)(int rv, void*, void*);
  int slot; /* cb slot used */
} write_async_cb_t;

void shuffle_init(void);
void shuffle_init_ssg(void);
hg_return_t shuffle_write_rpc_handler(hg_handle_t handle);
hg_return_t shuffle_write_rpc_handler_wrapper(hg_handle_t handle);
hg_return_t shuffle_write_async_handler(const struct hg_cb_info* info);
hg_return_t shuffle_write_handler(const struct hg_cb_info* info);

/* get current hg class and context instances */
inline void* shuffle_hg_class(void) { return sctx.hg_clz; }
inline void* shuffle_hg_ctx(void) { return sctx.hg_ctx; }

/* wait for outstanding rpc */
void shuffle_wait(void);
/* flush rpc queue */
void shuffle_flush(void);

/*
 * shuffle_write_enqueue: add an incoming write into an rpc queue.
 *
 * rpc maybe bypassed if write destination is local.
 *
 * return 0 on success, or EOF on errors.
 */
int shuffle_write(const char* path, char* data, size_t len, int epoch);

/*
 * shuffle_write_send_async: asynchronously send one or more encoded writes to
 * a remote peer and return immediately without waiting for response.
 *
 * return 0 on success, or EOF on errors.
 */
int shuffle_write_send_async(write_in_t* write_in, int peer_rank,
                             void (*async_cb)(int rv, void* arg1, void* arg2),
                             void* arg1, void* arg2);
/*
 * shuffle_write_send: send one or more encoded writes to a remote peer
 * and wait for its response.
 *
 * return 0 on success, or EOF on errors.
 */
int shuffle_write_send(write_in_t* write_in, int peer_rank);

void shuffle_destroy(void);

}  // extern C
