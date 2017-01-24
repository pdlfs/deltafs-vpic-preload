/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

#include "shuffle.h"

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
void genHgAddr(void)
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

    sprintf(sctx.hgaddr, "%s://%s:%d", HG_PROTO, host, port);
    SHUFFLE_DEBUG("Address: %s\n", sctx.hgaddr);

    freeifaddrs(ifaddr);
}

/*
 * Mercury hg_request_class_t progress and trigger callbacks.
 */
static int progress(unsigned int timeout, void *arg)
{
    shuffle_ctx_t *ctx = (shuffle_ctx_t *)arg;
    hg_context_t *h = ctx->hgctx;

    SHUFFLE_DEBUG("%d: calling HG_Progress from request shim\n",
                  ssg_get_rank(ctx->s));

    if (HG_Progress(h, timeout) == HG_SUCCESS)
        return HG_UTIL_SUCCESS;
    else
        return HG_UTIL_FAIL;

    SHUFFLE_DEBUG("%d: done calling HG_Progress from request shim\n",
                  ssg_get_rank(ctx->s));
}

static int trigger(unsigned int timeout, unsigned int *flag, void *arg)
{
    shuffle_ctx_t *ctx = (shuffle_ctx_t *)arg;
    hg_context_t *h = ctx->hgctx;

    SHUFFLE_DEBUG("%d: calling HG_Trigger from request shim\n",
            ssg_get_rank(ctx->s));

    if (HG_Trigger(h, timeout, 1, flag) != HG_SUCCESS) {
        return HG_UTIL_FAIL;
    } else {
        *flag = (*flag) ? HG_UTIL_TRUE : HG_UTIL_FALSE;
        return HG_UTIL_SUCCESS;
    }

    SHUFFLE_DEBUG("%d: done calling HG_Trigger from request shim\n",
            ssg_get_rank(ctx->s));
}

/*
 * Initialize and configure the shuffle layer
 */
void shuffle_init(void)
{
    hg_return_t hret;
    int rank, worldsz;

    /* Initialize Mercury */
    sctx.hgcl = HG_Init(sctx.hgaddr, HG_TRUE);
    if (!sctx.hgcl)
        msg_abort("HG_Init");

    /* Register write RPC */
    sctx.write_id = MERCURY_REGISTER(sctx.hgcl, "write",
                                     write_in_t, write_out_t,
                                     &write_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, sctx.write_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (write)");

    /* Register shutdown RPC */
    sctx.shutdown_id = MERCURY_REGISTER(sctx.hgcl, "shutdown",
                                        void, void,
                                        &shutdown_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, sctx.shutdown_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (shutdown)");

#ifdef DELTAFS_SHUFFLE_DEBUG
    /* Register ping RPC */
    sctx.ping_id = MERCURY_REGISTER(sctx.hgcl, "ping",
                                    ping_t, ping_t,
                                    &ping_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, sctx.ping_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (ping)");
#endif /* DELTAFS_SHUFFLE_DEBUG */

    sctx.hgctx = HG_Context_create(sctx.hgcl);
    if (!sctx.hgctx)
        msg_abort("HG_Context_create");

    /* Initialize ssg with MPI */
    sctx.s = SSG_NULL;
    sctx.s = ssg_init_mpi(sctx.hgcl, MPI_COMM_WORLD);
    if (sctx.s == SSG_NULL)
        msg_abort("ssg_init");

    /* Resolve group addresses */
    hret = ssg_lookup(sctx.s, sctx.hgctx);
    if (hret != HG_SUCCESS)
        msg_abort("ssg_lookup");

    /* Get non-mpi rank */
    rank = ssg_get_rank(sctx.s);
    if (rank == SSG_RANK_UNKNOWN || rank == SSG_EXTERNAL_RANK)
        msg_abort("ssg_get_rank: bad rank");

    /* Initialize request shim */
    sctx.hgreqcl = hg_request_init(&progress, &trigger, &sctx); //sctx.hgctx
    if (sctx.hgreqcl == NULL)
        msg_abort("hg_request_init");

    /* Initialize ch-placement instance */
    MPI_Comm_size(MPI_COMM_WORLD, &worldsz);
    sctx.chinst = ch_placement_initialize("ring", worldsz,
                    10 /* virt factor */, 0 /* seed */);
    if (!sctx.chinst)
        msg_abort("ch_placement_initialize");

#ifdef DELTAFS_SHUFFLE_DEBUG
    ping_test(rank);
#endif
}

void shuffle_destroy(void)
{
    int rank;

    /* Get non-mpi rank */
    rank = ssg_get_rank(sctx.s);
    if (rank == SSG_RANK_UNKNOWN || rank == SSG_EXTERNAL_RANK)
        msg_abort("ssg_get_rank: bad rank");

    SHUFFLE_DEBUG("%d: Shutting down shuffle layer\n", rank);
    shuffle_shutdown(rank);

    SHUFFLE_DEBUG("%d: Cleaning up shuffle layer\n", rank);
    ch_placement_finalize(sctx.chinst);
    hg_request_finalize(sctx.hgreqcl, NULL);
    ssg_finalize(sctx.s);
    HG_Context_destroy(sctx.hgctx);
    HG_Finalize(sctx.hgcl);
}
