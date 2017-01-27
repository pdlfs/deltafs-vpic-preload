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

#include "preload_internal.h"
#include "shuffle_internal.h"
#include "shuffle.h"

#include <string>

/* XXX: switch to margo to manage threads for us, */

/*
 * A global mutex shared among the main thread and the bg threads.
 */
static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

/* Used when waiting all bg threads to terminate. */
static pthread_cond_t bg_cv = PTHREAD_COND_INITIALIZER;

/* Used when waiting an on-going rpc to finish. */
static pthread_cond_t rpc_cv = PTHREAD_COND_INITIALIZER;

/*
 * prepare_addr(): obtain the mercury addr to bootstrap the rpc
 *
 * Write the server uri into *buf on success.
 *
 * Abort on errors.
 */
static const char* prepare_addr(char* buf)
{
    int family;
    int port;
    const char* tmp;
    int min_port;
    int max_port;
    struct ifaddrs *ifaddr, *cur;
    MPI_Comm comm;
    int rank;
    const char* subnet;
    char ip[50]; // ip
    int rv;

    /* figure out our ip addr by query the local socket layer */

    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs");

    subnet = getenv("SHUFFLE_Subnet");
    if (subnet == NULL) {
        subnet = DEFAULT_SUBNET;
    }

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr == NULL)
            continue;

        family = cur->ifa_addr->sa_family;

        if (family == AF_INET) {
            if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                            ip, sizeof(ip), NULL, 0, NI_NUMERICHOST))
                msg_abort("getnameinfo");

            if (strcmp(subnet, ip) == 0) {
                break;
            }
        }
    }

    freeifaddrs(ifaddr);

    if (cur == NULL)
        msg_abort("no ip addr");

    /* get port through MPI rank */

    tmp = getenv("SHUFFLE_Min_port");
    if (tmp == NULL) {
        min_port = DEFAULT_MIN_PORT;
    } else {
        min_port = atoi(tmp);
    }

    tmp = getenv("SHUFFLE_Max_port");
    if (tmp == NULL) {
        max_port = DEFAULT_MAX_PORT;
    } else {
        max_port = atoi(tmp);
    }

    if (max_port - min_port < 0)
        msg_abort("bad min-max port");
    if (min_port < 1000)
        msg_abort("bad min port");
    if (max_port > 65535)
        msg_abort("bad max port");

#if MPI_VERSION >= 3
    rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
            MPI_INFO_NULL, &comm);
    if (rv != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type");
#else
    comm = MPI_COMM_WORLD;
#endif

    MPI_Comm_rank(comm, &rank);
    port = min_port + (rank % (max_port - min_port));

    /* add proto */

    tmp = getenv("SHUFFLE_Mercury_proto");
    if (tmp == NULL) tmp = DEFAULT_PROTO;
    snprintf(buf, sizeof(buf), "%s://%s:%d", tmp, ip, port);

    SHUFFLE_LOG("using %s\n", buf);

    return buf;
}

/* main shuffle code */

shuffle_ctx_t sctx = { 0 };

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

    prepare_addr(sctx.my_addr);

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
