/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>

#include <mercury_atomic.h>
#include <mercury_thread.h>
#include <mercury_thread_mutex.h>
#include <mercury_thread_condition.h>

#include "preload_internal.h"
#include "shuffle_internal.h"
#include "shuffle.h"

#include <string>

/* XXX: switch to margo to manage threads for us, */

/*
 * main mutex shared among the main thread and the bg threads.
 */
static hg_thread_mutex_t mtx;;

/* used when waiting an on-going rpc to finish. */
static hg_thread_cond_t rpc_cv;

/* used when waiting all bg threads to terminate. */
static hg_thread_cond_t bg_cv;

/* True iff in shutdown seq */
static hg_atomic_int32_t shutting_down;

/* number of bg threads running */
static int num_bg = 0;

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

    return(buf);
}

static inline int is_shuttingdown() {
    if (hg_atomic_get32(&shutting_down) == 0) {
        return(false);
    } else {
        return(true);
    }
}

/* main shuffle code */

shuffle_ctx_t sctx = { 0 };

/* dedicated thread function to drive mercury progress */
static void* bg_work(void* foo)
{
    hg_return_t ret;
    unsigned int actual_count;

    while(!is_shuttingdown) {
        do {
            ret = HG_Trigger(sctx.hg_ctx, 0, 1, &actual_count);
        } while((ret == HG_SUCCESS) && actual_count && !is_shuttingdown);

        if(!is_shuttingdown) {
            ret = HG_Progress(sctx.hg_ctx, 100);
            if (ret != HG_SUCCESS && ret != HG_TIMEOUT)
                msg_abort("HG_Progress");
        }
    }

    hg_thread_mutex_lock(&mtx);
    assert(num_bg > 0);
    num_bg--;
    hg_thread_cond_broadcast(&bg_cv);
    hg_thread_mutex_unlock(&mtx);

    return(NULL);
}

/* shuffle_init_ssg: init the ssg sublayer */
void shuffle_init_ssg(void)
{
    hg_return_t hret;
    int rank;
    int size;

    sctx.ssg = ssg_init_mpi(sctx.hg_clz, MPI_COMM_WORLD);
    if (sctx.ssg == SSG_NULL)
        msg_abort("ssg_init_mpi");

    hret = ssg_lookup(sctx.ssg, sctx.hg_ctx);
    if (hret != HG_SUCCESS)
        msg_abort("ssg_lookup");

    rank = ssg_get_rank(sctx.ssg);
    size = ssg_get_count(sctx.ssg);

    SHUFFLE_LOG("ssg_rank is %d\n", rank);
    SHUFFLE_LOG("ssg_size is %d\n", size);

    sctx.chp = ch_placement_initialize("ring", size,
                    10 /* virt factor */, 0 /* seed */);
    if (!sctx.chp)
        msg_abort("ch_placement_initialize");

    SHUFFLE_LOG("ssg ok\n");

    return;
}

/* shuffle_init: init the shuffle layer */
void shuffle_init(void)
{
    hg_return_t hret;
    hg_thread_t pid;
    int rv;

    prepare_addr(sctx.my_addr);

    sctx.hg_clz = HG_Init(sctx.my_addr, HG_TRUE);
    if (!sctx.hg_clz)
        msg_abort("HG_Init");

    sctx.hg_id = MERCURY_REGISTER(sctx.hg_clz, "rpc_write",
            write_in_t, write_out_t, &write_rpc_handler);

    hret = HG_Register_data(sctx.hg_clz, sctx.hg_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data");

    sctx.hg_ctx = HG_Context_create(sctx.hg_clz);
    if (!sctx.hg_ctx)
        msg_abort("HG_Context_create");

    shuffle_init_ssg();

    rv = hg_thread_mutex_init(&mtx);
    if (rv) msg_abort("hg_thread_mutex_init");

    rv = hg_thread_cond_init(&rpc_cv);
    if (rv) msg_abort("hg_thread_cond_init");
    rv = hg_thread_cond_init(&bg_cv);
    if (rv) msg_abort("hg_thread_cond_init");

    hg_atomic_set32(&shutting_down, 0);

    num_bg++;

    rv = hg_thread_create(&pid, bg_work, NULL);
    if (rv) msg_abort("hg_thread_create");

    SHUFFLE_LOG("hg ok\n");

    return;
}

void shuffle_destroy(void)
{
    hg_atomic_set32(&shutting_down, 1); // start shutdown seq

    hg_thread_mutex_lock(&mtx);
    while (num_bg != 0) {
        hg_thread_cond_wait(&bg_cv, &mtx);
    }
    hg_thread_mutex_unlock(&mtx);

    ch_placement_finalize(sctx.chp);
    ssg_finalize(sctx.ssg);

    SHUFFLE_LOG("ssg closed\n");

    HG_Context_destroy(sctx.hg_ctx);
    HG_Finalize(sctx.hg_clz);

    SHUFFLE_LOG("hg closed\n");

    return;
}
