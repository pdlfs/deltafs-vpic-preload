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
 * msg_abort: abort with a message
 */
void msg_abort(const char *msg)
{
    int err = errno;

    fprintf(stderr, "ABORT: %s", msg);
    if (errno)
        fprintf(stderr, " (%s)\n", strerror(errno));
    else
        fprintf(stderr, "\n");

    abort();
}

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
    fprintf(stderr, "Address: %s\n", sctx.hgaddr);

    freeifaddrs(ifaddr);
}

/*
 * Mercury hg_request_class_t progress and trigger callbacks.
 */
static int progress(unsigned int timeout, void *arg)
{
    if (HG_Progress((hg_context_t *)arg, timeout) == HG_SUCCESS)
        return HG_UTIL_SUCCESS;
    else
        return HG_UTIL_FAIL;
}

static int trigger(unsigned int timeout, unsigned int *flag, void *arg)
{
    if (HG_Trigger((hg_context_t *)arg, timeout, 1, flag) != HG_SUCCESS) {
        return HG_UTIL_FAIL;
    } else {
        *flag = (*flag) ? HG_UTIL_TRUE : HG_UTIL_FALSE;
        return HG_UTIL_SUCCESS;
    }
}

/*
 * Initialize and configure the shuffle layer
 *
 * We use two types of Mercury RPCs:
 * - write: sends out a write request to the right rank
 */
void shuffle_init(void)
{
    hg_return_t hret;
    int rank;
    hg_id_t write_id;

    /* Initialize Mercury */
    sctx.hgcl = HG_Init(sctx.hgaddr, HG_TRUE);
    if (!sctx.hgcl)
        msg_abort("HG_Init");

    sctx.hgctx = HG_Context_create(sctx.hgcl);
    if (!sctx.hgctx)
        msg_abort("HG_Context_create");

    /* Register write RPC */
    write_id = MERCURY_REGISTER(sctx.hgcl, "write",
                               void, void, //XXX: change
                               &write_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, write_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (write)");

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
    sctx.hgreqcl = hg_request_init(&progress, &trigger, sctx.hgctx);
    if (sctx.hgreqcl == NULL)
        msg_abort("hg_request_init");

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

    shuffle_shutdown(rank);

    fprintf(stderr, "%d: Cleaning up\n", rank);
    hg_request_finalize(sctx.hgreqcl, NULL);
    ssg_finalize(sctx.s);
    HG_Context_destroy(sctx.hgctx);
    HG_Finalize(sctx.hgcl);
}
