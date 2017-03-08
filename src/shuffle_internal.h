/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include <mpi.h>
#include <mercury.h>
#include <mercury_proc_string.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include <ch-placement.h>

#include <deltafs/deltafs_api.h>
#include "mon.h"

#include "preload_internal.h"
#include "preload.h"

extern "C" {
/*
 * XXX: threshold that determines whether a write is small enough to use
 * point-to-point rpc; otherwise we fall back to a bulk transfer.
 *
 * Not used so far.
 */
#define SHUFFLE_SMALL_WRITE 1024

/*
 * rpc_abort: abort with a rpc error
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
    char my_addr[100];   /* mercury server uri */

    hg_class_t* hg_clz;
    hg_context_t* hg_ctx;
    hg_id_t hg_id;

    int timeout;  /* rpc timeout (in secs) */
    int force_sync;  /* avoid async rpc */
    int force_rpc;  /* send rpc even if addr is local */

    /* SSG context */
    ssg_t ssg;

    /* ch-placement context */
    struct ch_placement_instance* chp;

} shuffle_ctx_t;

extern shuffle_ctx_t sctx;

typedef struct write_in {
    char encoding[4096];  /* encoded contents */
} write_in_t;

typedef struct write_out {
    hg_int32_t rv;  /* ret value of the write operation */
} write_out_t;

typedef struct write_cb {
    int ok;  /* non-zero if rpc has completed */
    hg_return_t hret;
} write_cb_t;

typedef struct write_async_cb {
    void* arg1;
    void* arg2;
    void(*cb)(int rv, void*, void*);
    int slot;  /* cb slot used */
} write_async_cb_t;

void shuffle_init(void);
void shuffle_init_ssg(void);
hg_return_t shuffle_write_rpc_handler(hg_handle_t handle);
hg_return_t shuffle_write_async_handler(const struct hg_cb_info* info);
hg_return_t shuffle_write_handler(const struct hg_cb_info* info);
void shuffle_wait(void);  /* wait for outstanding rpc */

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
        void(*async_cb)(int rv, void* arg1, void* arg2),
        void* arg1, void* arg2);
/*
 * shuffle_write_send: send one or more encoded writes to a remote peer
 * and wait for its response.
 *
 * return 0 on success, or EOF on errors.
 */
int shuffle_write_send(write_in_t* write_in, int peer_rank);

void shuffle_destroy(void);

} // extern C
