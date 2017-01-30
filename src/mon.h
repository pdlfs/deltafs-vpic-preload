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

    int no_mon; /* non-zero if monitoring is disabled */

    /* !!! monitoring state !!! */

    unsigned min_wsz;   /* min app write size */
    unsigned max_wsz;   /* max app write size */

    unsigned long long nwsok; /* num of writes being shuffled out with rv!=EOF */
    unsigned long long nws;   /* total num of writes being shuffled out */
    unsigned long long nwrok; /* num of writes being shuffled in with rv!=EOF */
    unsigned long long nwr;   /* total num of writes being shuffled in */
    unsigned long long nwok;  /* num of writes to deltafs with rv!=EOF */
    unsigned long long nw;    /* total num of writes to deltafs */

    unsigned nb;  /* num of MPI barrier invoked by app */
    unsigned ne;  /* num of epoch flushed */

} mon_ctx_t;


extern mon_ctx_t mctx;

extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);

} // extern C
