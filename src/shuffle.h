/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <stdio.h>
#include <errno.h>
#include <netdb.h>
#include <mpi.h>

#include <set>

#include <pdlfs-common/port.h>
#include <ssg.h>
#include <ssg-mpi.h>

#define HG_PROTO "bmi+tcp"

/*
 * Shuffle context: run-time state of the preload and shuffle lib
 */
struct shuffle_ctx {
    const char *root;
    int len_root;                       /* strlen root */
    int testin;                         /* just testing */

    pdlfs::port::Mutex setlock;
    std::set<FILE *> isdeltafs;

    char hgaddr[NI_MAXHOST+10];         /* IP:port of host */
};

static shuffle_ctx ctx = { 0 };

/* shuffle_config.c */
void msg_abort(const char *msg);
void genHgAddr(void);
