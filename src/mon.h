/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

extern "C" {

/*
 * Histogram.
 *
 * The first four are num. max, min, and sum.
 */
#define MON_NUM_BUCKETS 154
typedef double (hstg_t)[MON_NUM_BUCKETS + 4];

/* XXX: assuming VPIC has only a single main thread such that no
 * synchronization is needed to protect mon state.
 *
 * XXX: we could use atomic counters in future
 * when VPIC goes openmp.
 *
 * Do NOT update a same counter from different threads.
 *
 * Okay to read in any thread.
 */

typedef struct mon_ctx {

    /* !!! monitoring state !!! */

    unsigned min_wsz;   /* min app write size */
    unsigned max_wsz;   /* max app write size */

    /* writes being shuffled out with remote write successful */
    unsigned long long nwsok;
    /* total num of writes being shuffled out */
    unsigned long long nws;
    /* writes being shuffled in with local write successful */
    unsigned long long nwrok;
    /* total num of writes being shuffled in */
    unsigned long long nwr;

    hstg_t hstgrpcw;      /* rpc write latency distribution */

    /* num of writes to deltafs with rv != EOF */
    unsigned long long nwok;
    /* total num of writes to deltafs */
    unsigned long long nw;
    hstg_t hstgw;      /* deltafs write latency distribution */

    unsigned nb;  /* num of MPI barrier invoked by app */
    unsigned ne;  /* num of epoch flushed */

} mon_ctx_t;


extern mon_ctx_t mctx;

extern int mon_preload_write(const char* fn, char* data, size_t n,
        mon_ctx_t* ctx);
extern int mon_shuffle_write(const char* fn, char* data, size_t n,
        mon_ctx_t* ctx);

extern void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum);
extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);

} // extern C
