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

#ifndef SHUFFLE_LOG_OUTPUT
#define SHUFFLE_LOG_OUTPUT 1
#endif

#define SHUFFLE_LOG(fmt, ...) \
    do { \
        if (SHUFFLE_LOG_OUTPUT) { \
            fprintf(stderr, fmt, ##__VA_ARGS__); \
        } \
    } while(0)

/*
 * XXX: threshold that determines whether a write is small enough to use
 * point-to-point rpc; otherwise we fall back to a bulk transfer.
 *
 * Not used so far.
 */
#define SHUFFLE_SMALL_WRITE 1024

/*
 * shuffle context:
 *   - run-time state of the preload and shuffle lib
 */
typedef struct shuffle_ctx {
    char my_addr[100];   /* mercury server uri */

    hg_class_t* hg_clz;
    hg_context_t* hg_ctx;
    hg_id_t hg_id;

    /* SSG context */
    ssg_t ssg;

    /* ch-placement context */
    struct ch_placement_instance* chp;

} shuffle_ctx_t;

extern shuffle_ctx_t sctx;

typedef struct write_in {
    hg_const_string_t fname;
    hg_int32_t rank_in;
    hg_uint8_t data_len;
    hg_string_t data;

    char buf[500];
} write_in_t;

typedef struct write_out {
    hg_int32_t rv;  /* ret value of the write operation */
} write_out_t;

typedef struct write_cb {
    int ok;   /* non-zero if rpc has completed */
    hg_return_t hret;
} write_cb_t;

void shuffle_init(void);
void shuffle_init_ssg(void);
hg_return_t shuffle_write_rpc_handler(hg_handle_t handle);
hg_return_t shuffle_write_handler(const struct hg_cb_info* info);
int shuffle_write(const char *fn, char *data, int len);
void shuffle_destroy(void);

} // extern C
