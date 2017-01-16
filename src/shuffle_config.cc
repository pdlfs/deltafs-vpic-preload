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

#include <mercury_request.h> /* XXX: Just for initial dev. Will replace */
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
 * XXX: This is just for initial dev. Will replace soon.
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
 */
void shuffle_init(void)
{
    hg_return_t hret;
    int rank, peer_rank, ret;
    hg_addr_t peer_addr;
    hg_id_t ping_id, shutdown_id;
    hg_request_class_t *hgreqcl;
    hg_request_t *hgreq;
    ping_t ping_in;
    hg_handle_t ping_handle = HG_HANDLE_NULL;
    unsigned int req_complete_flag = 0;

    /* Initialize Mercury */
    sctx.hgcl = HG_Init(sctx.hgaddr, HG_TRUE);
    if (!sctx.hgcl)
        msg_abort("HG_Init");

    sctx.hgctx = HG_Context_create(sctx.hgcl);
    if (!sctx.hgctx)
        msg_abort("HG_Context_create");

    /* Register RPCs */
    ping_id = MERCURY_REGISTER(sctx.hgcl, "ping", ping_t, ping_t, &ping_rpc_handler);
    shutdown_id = MERCURY_REGISTER(sctx.hgcl, "shutdown", void, void, &shutdown_rpc_handler);

    hret = HG_Register_data(sctx.hgcl, ping_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (ping)");
    hret = HG_Register_data(sctx.hgcl, shutdown_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data (shutdown)");

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
    hgreqcl = hg_request_init(&progress, &trigger, sctx.hgctx);
    if (hgreqcl == NULL)
        msg_abort("hg_request_init");

    hgreq = hg_request_create(hgreqcl);
    if (hgreq == NULL)
        msg_abort("hg_request_create");

    /* Sanity check count; if we're on our own, don't bother with RPCs */
    if (ssg_get_count(sctx.s) == 1)
        goto cleanup; /* XXX: May need to properly direct this later */

    /* XXX: Just a test to make sure Mercury/ssg work as they should */
    peer_rank = (rank+1) % ssg_get_count(sctx.s);
    peer_addr = ssg_get_addr(sctx.s, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("ssg_get_addr");

    fprintf(stdout, "%d: pinging %d\n", rank, peer_rank);
    hret = HG_Create(sctx.hgctx, peer_addr, ping_id, &ping_handle);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Create");

    ping_in.rank = rank;
    hret = HG_Forward(ping_handle, &hg_request_complete_cb, hgreq, &ping_in);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Forward");

    ret = hg_request_wait(hgreq, HG_MAX_IDLE_TIME, &req_complete_flag);
    if (ret == HG_UTIL_FAIL)
        msg_abort("ping failed");
    if (req_complete_flag == 0)
        msg_abort("ping timed out");

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
        fprintf(stdout, "%d: shutting down\n", rank);

        /* Trigger/progress remaining */
        do {
            hret = HG_Progress(sctx.hgctx, 0);
        } while (hret == HG_SUCCESS);

        do {
            num_trigger = 0;
            hret = HG_Trigger(sctx.hgctx, 0, 1, &num_trigger);
        } while (hret == HG_SUCCESS && num_trigger == 1);
    } else {
        fprintf(stdout, "%d: initiating shutdown\n", rank);
        hg_handle_t shutdown_handle = HG_HANDLE_NULL;

        hret = HG_Create(sctx.hgctx, peer_addr, shutdown_id, &shutdown_handle);
        if (hret != HG_SUCCESS)
            msg_abort("HG_Create");

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
    }

cleanup:
    fprintf(stdout, "%d: Cleaning up\n", rank);
    HG_Destroy(ping_handle);
    hg_request_destroy(hgreq);
    hg_request_finalize(hgreqcl, NULL);
}

void shuffle_destroy(void)
{
    ssg_finalize(sctx.s);
    HG_Context_destroy(sctx.hgctx);
    HG_Finalize(sctx.hgcl);
}
