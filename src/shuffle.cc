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
#include <pthread.h>

#include <pdlfs-common/xxhash.h>

#include "preload_internal.h"
#include "shuffle_internal.h"
#include "shuffle.h"

#include <string>

/* XXX: switch to margo to manage threads for us, */

/*
 * main mutex shared among the main thread and the bg threads.
 */
static pthread_mutex_t mtx;

/* used when waiting an on-going rpc to finish. */
static pthread_cond_t rpc_cv;

/* used when waiting all bg threads to terminate. */
static pthread_cond_t bg_cv;

/* true iff in shutdown seq */
static int shutting_down = 0;  /* XXX: better if this is atomic */

/* number of bg threads running */
static int num_bg = 0;

/* shuffle context */
shuffle_ctx_t sctx = { 0 };

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
    if (subnet == NULL)
        subnet = DEFAULT_SUBNET;

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                        ip, sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
                    msg_abort("getnameinfo");

                SHUFFLE_LOG("maybe using ip %s\n", ip);

                if (strcmp(subnet, ip) == 0) {
                    break;
                }
            }
        }
    }

    if (cur == NULL)  /* maybe a wrong subnet has been specified */
        msg_abort("no ip addr");

    freeifaddrs(ifaddr);

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

    /* sanity check on port range */
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
    sprintf(buf, "%s://%s:%d", tmp, ip, port);

    SHUFFLE_LOG("using %s\n", buf);

    return(buf);
}

#if defined(__x86_64__) && defined(__GNUC__)
static inline bool is_shuttingdown() {
    bool r = shutting_down;
    // See http://en.wikipedia.org/wiki/Memory_ordering.
    __asm__ __volatile__("" : : : "memory");

    return(r);
}
#else
static inline bool is_shuttingdown() {
    /* XXX: enforce memory order via mutex */
    pthread_mutex_lock(&mtx);
    bool r = shutting_down;
    pthread_mutex_unlock(&mtx);

    return(r);
}
#endif

/* main shuffle code */

extern "C" {

static hg_return_t shuffle_write_in_proc(hg_proc_t proc, void* data)
{
    hg_return_t hret;
    hg_uint8_t fname_len;
    hg_uint16_t enc_len;
    hg_uint16_t dec_len;

    write_in_t* in = reinterpret_cast<write_in_t*>(data);

    hg_proc_op_t op = hg_proc_get_op(proc);

    if (op == HG_ENCODE) {
        enc_len = 2;  /* reserves 2 bytes for the encoding length */

        memcpy(in->buf + enc_len, &in->rank_in, 4);
        enc_len += 4;
        in->buf[enc_len] = in->data_len;
        memcpy(in->buf + enc_len + 1, in->data, in->data_len);

        enc_len += 1 + in->data_len;
        fname_len = strlen(in->fname);
        in->buf[enc_len] = fname_len;
        memcpy(in->buf + enc_len + 1, in->fname, fname_len);

        enc_len += 1 + fname_len;
        assert(enc_len < sizeof(in->buf));

        hret = hg_proc_hg_uint16_t(proc, &enc_len);
        if (hret == HG_SUCCESS)
            hret = hg_proc_memcpy(proc, in->buf + 2, enc_len - 2);

    } else if (op == HG_DECODE) {
        hret = hg_proc_hg_uint16_t(proc, &enc_len);
        dec_len = 0;

        assert(enc_len < sizeof(in->buf));

        if (hret == HG_SUCCESS) {
            hret = hg_proc_memcpy(proc, in->buf + 2, enc_len - 2);
            enc_len -= 2;
            dec_len += 2;
        }

        if (hret == HG_SUCCESS && enc_len >= 4) {
            memcpy(&in->rank_in, in->buf + dec_len, 4);
            enc_len -= 4;
            dec_len += 4;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len >= 1) {
            in->data_len = in->buf[dec_len];
            enc_len -= 1;
            dec_len += 1;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len >= in->data_len) {
            in->data = in->buf + dec_len;
            enc_len -= in->data_len;
            dec_len += in->data_len;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len >= 1) {
            fname_len = in->buf[dec_len];
            enc_len -= 1;
            dec_len += 1;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len >= fname_len) {
            in->fname = in->buf + dec_len;
            enc_len -= fname_len;
            dec_len += fname_len;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len == 0) {
            in->buf[dec_len] = 0;
        } else {
            hret = HG_OTHER_ERROR;
        }

    } else {
        hret = HG_SUCCESS;  /* noop */
    }

    return hret;
}

static hg_return_t shuffle_write_out_proc(hg_proc_t proc, void* data)
{
    hg_return_t ret;

    write_out_t* out = reinterpret_cast<write_out_t*>(data);
    ret = hg_proc_hg_int32_t(proc, &out->rv);

    return ret;
}

/* rpc server-side handler for shuffled writes */
hg_return_t shuffle_write_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    write_out_t out;
    write_in_t in;
    char path[PATH_MAX];
    char buf[1024];
    int rank_in;
    int rank;
    int n;

    hret = HG_Get_input(h, &in);

    if (hret == HG_SUCCESS) {
        rank = ssg_get_rank(sctx.ssg);
        rank_in = in.rank_in;

        assert(pctx.plfsdir != NULL);

        snprintf(path, sizeof(path), "%s%s", pctx.plfsdir, in.fname);

        out.rv = mon_preload_write(path, in.data, in.data_len, &mctx);

        /* write trace if we are in testing mode */
        if (pctx.testin && pctx.logfd != -1) {
            n = snprintf(buf, sizeof(buf), "%s %d bytes r%d->r%d\n", path,
                    int(in.data_len), rank_in, rank);

            n = write(pctx.logfd, buf, n);
        }

        hret = HG_Respond(h, NULL, NULL, &out);
    }

    HG_Free_input(h, &in);

    if (!pctx.nomon) {
        if (hret == HG_SUCCESS && out.rv == 0)
            mctx.nwrok++;
        mctx.nwr++;
    }

    return hret;
}

/* rpc client-side callback for shuffled writes */
hg_return_t shuffle_write_handler(const struct hg_cb_info* info)
{
    pthread_mutex_lock(&mtx);

    write_cb_t* cb = reinterpret_cast<write_cb_t*>(info->arg);
    cb->hret = info->ret;
    cb->ok = 1;

    pthread_cond_broadcast(&rpc_cv);
    pthread_mutex_unlock(&mtx);
    return HG_SUCCESS;
}

/* redirect writes to an appropriate rank for buffering and writing */
int shuffle_write(const char *fn, char *data, size_t len, int* is_local)
{
    hg_return_t hret;
    hg_handle_t handle;
    write_in_t write_in;
    write_out_t write_out;
    write_cb_t write_cb;
    hg_addr_t peer_addr;
    char buf[1024];
    int rv;
    unsigned long target;
    int peer_rank;
    int rank;
    int n;

    *is_local = 0;
    assert(ssg_get_count(sctx.ssg) != 0);
    assert(fn != NULL);

    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    if (ssg_get_count(sctx.ssg) != 1) {
        if (IS_BYPASS_PLACEMENT(pctx.mode)) {
            /* send to next-door neighbor instead of using ch-placement */
            peer_rank = (rank + 1) % ssg_get_count(sctx.ssg);
        } else {
            ch_placement_find_closest(sctx.chp,
                    pdlfs::xxhash64(fn, strlen(fn), 0), 1, &target);
            peer_rank = target;
        }
    } else {
        peer_rank = rank;
    }

    if (peer_rank == rank) {

        /* write trace if we are in testing mode */
        if (pctx.testin && pctx.logfd != -1) {
            n = snprintf(buf, sizeof(buf), "%s %d bytes r%d->r%d\n", fn,
                    int(len), rank, peer_rank);

            n = write(pctx.logfd, buf, n);
        }

        *is_local = 1;

        return(mon_preload_write(fn, data, len, &mctx));
    }

    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        return(EOF);

    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &handle);
    if (hret != HG_SUCCESS)
        return(EOF);

    assert(pctx.plfsdir != NULL);

    write_in.fname = fn + pctx.len_plfsdir;
    write_in.data = data;
    write_in.data_len = len;
    write_in.rank_in = rank;

    write_cb.ok = 0;

    hret = HG_Forward(handle, shuffle_write_handler, &write_cb, &write_in);

    if (hret == HG_SUCCESS) {
        /* here we wait rpc to complete */
        pthread_mutex_lock(&mtx);
        while(write_cb.ok == 0)
            pthread_cond_wait(&rpc_cv, &mtx);
        pthread_mutex_unlock(&mtx);

        hret = write_cb.hret;

        if (hret == HG_SUCCESS) {

            hret = HG_Get_output(handle, &write_out);
            if (hret == HG_SUCCESS)
                rv = write_out.rv;
            HG_Free_output(handle, &write_out);
        }
    }

    HG_Destroy(handle);

    if (hret != HG_SUCCESS || rv != 0) {
        return(EOF);
    } else {
        return(0);
    }
}

/* bg_work(): dedicated thread function to drive mercury progress */
static void* bg_work(void* foo)
{
    hg_return_t ret;
    unsigned int actual_count;

    while(true) {
        do {
            ret = HG_Trigger(sctx.hg_ctx, 0, 1, &actual_count);
        } while(ret == HG_SUCCESS && actual_count != 0);

        if(!is_shuttingdown()) {
            ret = HG_Progress(sctx.hg_ctx, 100);
            if (ret != HG_SUCCESS && ret != HG_TIMEOUT)
                msg_abort("HG_Progress");
        } else {
            break;
        }
    }

    pthread_mutex_lock(&mtx);
    assert(num_bg > 0);
    num_bg--;
    pthread_cond_broadcast(&bg_cv);
    pthread_mutex_unlock(&mtx);

    SHUFFLE_LOG("bg off\n");

    return(NULL);
}

/* shuffle_init_ssg(): init the ssg sublayer */
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

    SHUFFLE_LOG("ssg_rank=%d, ssg_size=%d\n", rank, size);

    sctx.chp = ch_placement_initialize("ring", size, 1 /* vir factor */,
            0 /* hash seed */);
    if (!sctx.chp)
        msg_abort("ch_placement_initialize");

    return;
}

/* shuffle_init(): init the shuffle layer */
void shuffle_init(void)
{
    hg_return_t hret;
    pthread_t pid;
    int rv;

    prepare_addr(sctx.my_addr);

    sctx.hg_clz = HG_Init(sctx.my_addr, HG_TRUE);
    if (!sctx.hg_clz)
        msg_abort("HG_Init");

    sctx.hg_id = HG_Register_name(sctx.hg_clz, "shuffle_rpc_write",
            shuffle_write_in_proc, shuffle_write_out_proc,
            shuffle_write_rpc_handler);

    hret = HG_Register_data(sctx.hg_clz, sctx.hg_id, &sctx, NULL);
    if (hret != HG_SUCCESS)
        msg_abort("HG_Register_data");

    sctx.hg_ctx = HG_Context_create(sctx.hg_clz);
    if (!sctx.hg_ctx)
        msg_abort("HG_Context_create");

    shuffle_init_ssg();

    rv = pthread_mutex_init(&mtx, NULL);
    if (rv) msg_abort("pthread_mutex_init");

    rv = pthread_cond_init(&rpc_cv, NULL);
    if (rv) msg_abort("pthread_cond_init");
    rv = pthread_cond_init(&bg_cv, NULL);
    if (rv) msg_abort("pthread_cond_init");

    shutting_down = 0;

    num_bg++;

    rv = pthread_create(&pid, NULL, bg_work, NULL);
    if (rv) msg_abort("pthread_create");

    pthread_detach(pid);

    SHUFFLE_LOG("shuffle is up\n");

    return;
}

/* shuffle_destroy(): finalize the shuffle layer */
void shuffle_destroy(void)
{
    pthread_mutex_lock(&mtx);
    shutting_down = 1; // start shutdown seq
    while (num_bg != 0) pthread_cond_wait(&bg_cv, &mtx);
    pthread_mutex_unlock(&mtx);

    ch_placement_finalize(sctx.chp);
    ssg_finalize(sctx.ssg);

    HG_Context_destroy(sctx.hg_ctx);
    HG_Finalize(sctx.hg_clz);

    SHUFFLE_LOG("shuffle off\n");

    return;
}

} // extern C
