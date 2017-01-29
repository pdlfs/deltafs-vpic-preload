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

/* XXX: assuming VPIC has only a single main thread such that
 * no synchronization is needed to update our mon state.
 *
 * XXX: we could use atomic counters in future
 * when VPIC goes openmp.
 *
 * Do NOT update mon state in background rpc threads.
 *
 * Okay to read in any thread.
 */

typedef struct mon_ctx {

    int no_mon; /* non-zero if monitoring is disabled */

    /* !!! monitoring state !!! */

    unsigned min_wsz;   /* min app write size */
    unsigned max_wsz;   /* max app write size */

    long long unsigned nswok; /* num of writes being shuffled with rv!=EOF */
    long long unsigned nsw;   /* total num of writes being shuffled */
    long long unsigned nwok;  /* num of writes to deltafs with rv!=EOF */
    long long unsigned nw;    /* total num of writes to deltafs */

    unsigned nb;  /* num of MPI barrier invoked by app */
    unsigned ne;  /* num of epoch flushed */

} mon_ctx_t;


extern mon_ctx_t mctx;

extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);

} // extern C
