/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
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
#include <mpi.h>
#include <set>

/* ANL libs */
#include <mercury_macros.h>
#include <ssg.h>
#include <ssg-mpi.h>

/* CMU libs */
#include <pdlfs-common/port.h>

#define HG_PROTO "bmi+tcp"

/*
 * Shuffle context: run-time state of the preload and shuffle lib
 */
typedef struct shuffle_ctx {
    const char *root;
    int len_root;                       /* strlen root */
    int testin;                         /* just testing */

    pdlfs::port::Mutex setlock;
    std::set<FILE *> isdeltafs;

    char hgaddr[NI_MAXHOST+20];         /* proto://ip:port of host */

    ssg_t s;                            /* ssg context */
    int shutdown_flag;                  /* XXX: Used for testing */
} shuffle_ctx_t;

MERCURY_GEN_PROC(ping_t, ((int32_t)(rank)))

extern shuffle_ctx_t sctx;

/* shuffle_config.cc */
void msg_abort(const char *msg);
void genHgAddr(void);

/* shuffle_rpc.cc */
hg_return_t ping_rpc_handler(hg_handle_t h);
hg_return_t shutdown_rpc_handler(hg_handle_t h);
