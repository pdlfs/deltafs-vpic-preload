/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <pthread.h>

#include "shuffle.h"

/* Synchronization between the main thread and the background RPC thread. */
static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

shuffle_ctx_t sctx = { 0 };

/*
 * Generate a mercury address
*
 * We have to assign a Mercury address to ourselves.
 * Get the first available IP (any interface that's not localhost)
 * and use the process ID to construct the port (but limit to [5000,60000])
 *
 * TODO: let user specify a CIDR network to match (e.g. "192.168.2.0/24")
 *       rather than pick the first interface.
 * TODO: define port as a function of the node's rank
 */
static void prepare_addr(void)
{
    int family, found = 0, port;
    struct ifaddrs *ifaddr, *cur;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs");

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr == NULL)
            continue;

        family = cur->ifa_addr->sa_family;

        /* For an AF_INET interface address, display the address */
        if (family == AF_INET) {
            if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST))
                msg_abort("getnameinfo");

            if (strcmp("127.0.0.1", host)) {
                found = 1;
                break;
            }
        }
    }

    if (!found)
        msg_abort("No valid IP found");

    port = ((long) getpid() % 55000) + 5000;

    snprintf(sctx.my_addr, sizeof(sctx.my_addr), "%s://%s:%d", DEFAULT_PROTO,
            host, port);
    SHUFFLE_LOG("my_addr is %s\n", sctx.my_addr);

    freeifaddrs(ifaddr);
}

/*
 * Mercury hg_request_class_t progress and trigger callbacks.
 */
static int progress(unsigned int timeout, void *arg)
{
    shuffle_ctx_t *ctx = (shuffle_ctx_t *)arg;
    hg_context_t *h = ctx->hg_ctx;

    SHUFFLE_LOG("%d: calling HG_Progress from request shim\n",
                  ssg_get_rank(ctx->ssg));

    if (HG_Progress(h, timeout) == HG_SUCCESS)
        return HG_UTIL_SUCCESS;
    else
        return HG_UTIL_FAIL;

    SHUFFLE_LOG("%d: done calling HG_Progress from request shim\n",
                  ssg_get_rank(ctx->ssg));
}

static int trigger(unsigned int timeout, unsigned int *flag, void *arg)
{
    shuffle_ctx_t *ctx = (shuffle_ctx_t *)arg;
    hg_context_t *h = ctx->hg_ctx;

    SHUFFLE_LOG("%d: calling HG_Trigger from request shim\n",
            ssg_get_rank(ctx->ssg));

    if (HG_Trigger(h, timeout, 1, flag) != HG_SUCCESS) {
        return HG_UTIL_FAIL;
    } else {
        *flag = (*flag) ? HG_UTIL_TRUE : HG_UTIL_FALSE;
        return HG_UTIL_SUCCESS;
    }

    SHUFFLE_LOG("%d: done calling HG_Trigger from request shim\n",
            ssg_get_rank(ctx->ssg));
}

/*
 * Initialize and configure the shuffle layer
 */
void shuffle_init(void)
{
    hg_return_t hret;
    int rank, worldsz;

    prepare_addr();

    /* Initialize Mercury */
    sctx.hg_clz = HG_Init(sctx.my_addr, HG_TRUE);
    if (!sctx.hg_clz)
        msg_abort("HG_Init");

    /* Register write RPC */
    sctx.hg_id = MERCURY_REGISTER(sctx.hg_clz, "rpc_write",
            write_in_t, write_out_t, &write_rpc_handler);

    hret = HG_Register_data(sctx.hg_clz, sctx.hg_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data");

    sctx.hg_ctx = HG_Context_create(sctx.hg_clz);
    if (!sctx.hg_ctx)
        msg_abort("HG_Context_create");

    /* Initialize ssg with MPI */
    sctx.ssg = ssg_init_mpi(sctx.hg_clz, MPI_COMM_WORLD);
    if (sctx.ssg == SSG_NULL)
        msg_abort("ssg_init_mpi");

    /* Resolve group addresses */
    hret = ssg_lookup(sctx.ssg, sctx.hg_ctx);
    if (hret != HG_SUCCESS)
        msg_abort("ssg_lookup");

    rank = ssg_get_rank(sctx.ssg);
    worldsz = ssg_get_count(sctx.ssg);

    sctx.chp = ch_placement_initialize("ring", worldsz,
                    10 /* virt factor */, 0 /* seed */);
    if (!sctx.chp)
        msg_abort("ch_placement_initialize");
}

void shuffle_destroy(void)
{
    int rank;

    /* Get non-mpi rank */
    rank = ssg_get_rank(sctx.ssg);
    if (rank == SSG_RANK_UNKNOWN || rank == SSG_EXTERNAL_RANK)
        msg_abort("ssg_get_rank: bad rank");

    ch_placement_finalize(sctx.chp);

    ssg_finalize(sctx.ssg);

    HG_Context_destroy(sctx.hg_ctx);
    HG_Finalize(sctx.hg_clz);
}
