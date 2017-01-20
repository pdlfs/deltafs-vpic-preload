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

#ifdef DELTAFS_SHUFFLE_DEBUG
/* For testing: ping RPCs go around all ranks in a ring to test Mercury/SSG */
static void ping_test(int rank)
{
    hg_id_t ping_id;
    hg_handle_t ping_handle = HG_HANDLE_NULL;
    ping_t ping_in;
    hg_addr_t peer_addr;
    int peer_rank, ret;
    hg_return_t hret;
    hg_request_t *hgreq;
    unsigned int req_complete_flag = 0;

    /* Register ping RPC */
    ping_id = MERCURY_REGISTER(sctx.hgcl, "ping",
                               ping_t, ping_t,
                               &ping_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, ping_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (ping)");

    /* Sanity check count; if we're on our own, don't bother */
    if (ssg_get_count(sctx.s) == 1)
        return;

    peer_rank = (rank+1) % ssg_get_count(sctx.s);
    peer_addr = ssg_get_addr(sctx.s, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("ssg_get_addr");

    fprintf(stderr, "%d: pinging %d\n", rank, peer_rank);
    hret = HG_Create(sctx.hgctx, peer_addr, ping_id, &ping_handle);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Create");

    hgreq = hg_request_create(sctx.hgreqcl);
    if (hgreq == NULL)
        msg_abort("hg_request_create");

    ping_in.rank = rank;
    hret = HG_Forward(ping_handle, &hg_request_complete_cb, hgreq, &ping_in);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Forward");

    ret = hg_request_wait(hgreq, HG_MAX_IDLE_TIME, &req_complete_flag);
    if (ret == HG_UTIL_FAIL)
        msg_abort("ping failed");
    if (req_complete_flag == 0)
        msg_abort("ping timed out");

    HG_Destroy(ping_handle);
    hg_request_destroy(hgreq);
}
#endif /* DELTAFS_SHUFFLE_DEBUG */

/* Shutdown process used at MPI_Finalize() time to tear down shuffle layer */
static void shuffle_shutdown(int rank)
{
    hg_return_t hret;
    hg_id_t shutdown_id;

    /* Register shutdown RPC */
    shutdown_id = MERCURY_REGISTER(sctx.hgcl, "shutdown",
                               void, void,
                               &shutdown_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, shutdown_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (shutdown)");

    /*
     * Rank 0: initialize the shutdown process
     * Others: enter progress
     */
    if (rank != 0) {
        unsigned int num_trigger;
        do {
            do {
                num_trigger = 0;
                hret = HG_Trigger(sctx.hgctx, 0, 1, &num_trigger);
            } while (hret == HG_SUCCESS && num_trigger == 1);

            hret = HG_Progress(sctx.hgctx,
                               sctx.shutdown_flag ? 100 : HG_MAX_IDLE_TIME);
        } while ((hret == HG_SUCCESS || hret == HG_TIMEOUT) &&
                 !sctx.shutdown_flag);

        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            msg_abort("HG_Progress");
        fprintf(stderr, "%d: shutting down\n", rank);

        /* Trigger/progress remaining */
        do {
            hret = HG_Progress(sctx.hgctx, 0);
        } while (hret == HG_SUCCESS);

        do {
            num_trigger = 0;
            hret = HG_Trigger(sctx.hgctx, 0, 1, &num_trigger);
        } while (hret == HG_SUCCESS && num_trigger == 1);
    } else {
        hg_request_t *hgreq;
        unsigned int req_complete_flag = 0;
        hg_addr_t peer_addr;
        int peer_rank, ret;

        fprintf(stderr, "%d: initiating shutdown\n", rank);
        hg_handle_t shutdown_handle = HG_HANDLE_NULL;

        peer_rank = (rank+1) % ssg_get_count(sctx.s);
        peer_addr = ssg_get_addr(sctx.s, peer_rank);
        if (peer_addr == HG_ADDR_NULL)
            msg_abort("ssg_get_addr");

        hret = HG_Create(sctx.hgctx, peer_addr, shutdown_id, &shutdown_handle);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Create");

        hgreq = hg_request_create(sctx.hgreqcl);
        if (hgreq == NULL)
            msg_abort("hg_request_create");

        hret = HG_Forward(shutdown_handle, &hg_request_complete_cb, hgreq, NULL);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Forward");

        req_complete_flag = 0;
        ret = hg_request_wait(hgreq, HG_MAX_IDLE_TIME, &req_complete_flag);
        if (ret != HG_UTIL_SUCCESS)
            msg_abort("hg_request_wait");
        if (req_complete_flag == 0)
            msg_abort("hg_request_wait timed out");

        HG_Destroy(shutdown_handle);
        hg_request_destroy(hgreq);
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
