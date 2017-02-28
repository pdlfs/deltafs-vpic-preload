/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <pthread.h>
#include <time.h>

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

/* used when waiting an on-going rpc to finish */
static pthread_cond_t rpc_cv;

/* used when waiting all bg threads to terminate */
static pthread_cond_t bg_cv;

/* used when waiting for the next available rpc callback slot */
static pthread_cond_t cb_cv;

/* true iff in shutdown seq */
static int shutting_down = 0;  /* XXX: better if this is atomic */

/* number of bg threads running */
static int num_bg = 0;

/* rpc callback slots */
#define MAX_OUTSTANDING_RPC 128  /* hard limit */
static write_async_cb_t cb_slots[MAX_OUTSTANDING_RPC];
static int cb_flags[MAX_OUTSTANDING_RPC] = { 0 };
static int cb_allowed = 1; /* soft limit */
static int cb_left = 1;

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
    const char* env;
    int min_port;
    int max_port;
    struct ifaddrs *ifaddr, *cur;
    MPI_Comm comm;
    int rank;
    const char* subnet;
    char msg[100];
    char ip[50]; // ip
    int rv;
    int n;

    /* figure out our ip addr by query the local socket layer */

    if (getifaddrs(&ifaddr) == -1)
        msg_abort("getifaddrs");

    subnet = maybe_getenv("SHUFFLE_Subnet");
    if (subnet == NULL) {
        subnet = DEFAULT_SUBNET;
    }

    if (pctx.rank == 0) {
        snprintf(msg, sizeof(msg), "using subnet %s*", subnet);
        if (strcmp(subnet, "127.0.0.1") == 0) {
            warn(msg);
        } else {
            info(msg);
        }
    }

    for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
        if (cur->ifa_addr != NULL) {
            family = cur->ifa_addr->sa_family;

            if (family == AF_INET) {
                if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in),
                        ip, sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
                    msg_abort("getnameinfo");

                if (strncmp(subnet, ip, strlen(subnet)) == 0) {
                    break;
                } else if (pctx.testin) {
                    if (pctx.logfd != -1) {
                        n = snprintf(msg, sizeof(msg), "[N] reject %s\n", ip);
                        n = write(pctx.logfd, msg, n);

                        errno = 0;
                    }
                }
            }
        }
    }

    if (cur == NULL)  /* maybe a wrong subnet has been specified */
        msg_abort("no ip addr");

    freeifaddrs(ifaddr);

    /* get port through MPI rank */

    env = maybe_getenv("SHUFFLE_Min_port");
    if (env == NULL) {
        min_port = DEFAULT_MIN_PORT;
    } else {
        min_port = atoi(env);
    }

    env = maybe_getenv("SHUFFLE_Max_port");
    if (env == NULL) {
        max_port = DEFAULT_MAX_PORT;
    } else {
        max_port = atoi(env);
    }

    /* sanity check on port range */
    if (max_port - min_port < 0)
        msg_abort("bad min-max port");
    if (min_port < 0)
        msg_abort("bad min port");
    if (max_port > 65535)
        msg_abort("bad max port");

    if (pctx.rank == 0) {
        snprintf(msg, sizeof(msg), "using port range [%d,%d]",
                min_port, max_port);
        info(msg);
    }

#if MPI_VERSION >= 3
    rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
            MPI_INFO_NULL, &comm);
    if (rv != MPI_SUCCESS)
        msg_abort("MPI_Comm_split_type");
#else
    comm = MPI_COMM_WORLD;
#endif

    MPI_Comm_rank(comm, &rank);
    port = min_port + (rank % (1 + max_port - min_port));

    /* add proto */

    env = maybe_getenv("SHUFFLE_Mercury_proto");
    if (env == NULL) env = DEFAULT_PROTO;
    sprintf(buf, "%s://%s:%d", env, ip, port);
    if (pctx.rank == 0) {
        snprintf(msg, sizeof(msg), "using %s", env);
        if (strstr(env, "tcp") != NULL) {
            warn(msg);
        } else {
            info(msg);
        }
    }

    if (pctx.testin) {
        if (pctx.logfd != -1) {
            n = snprintf(msg, sizeof(msg), "[N] using %s\n", buf);
            n = write(pctx.logfd, msg, n);

            errno = 0;
        }
    }

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

        memcpy(in->buf + enc_len, &in->epoch, 4);
        enc_len += 4;
        memcpy(in->buf + enc_len, &in->rank, 4);
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
            memcpy(&in->epoch, in->buf + dec_len, 4);
            enc_len -= 4;
            dec_len += 4;
        } else {
            hret = HG_OTHER_ERROR;
        }

        if (hret == HG_SUCCESS && enc_len >= 4) {
            memcpy(&in->rank, in->buf + dec_len, 4);
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

extern "C" {

/* rpc server-side handler for shuffled writes */
hg_return_t shuffle_write_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    write_out_t out;
    write_in_t in;
    char path[PATH_MAX];
    char buf[200];
    int ha;
    int epoch;
    int peer_rank;
    int rank;
    int n;

    hret = HG_Get_input(h, &in);

    if (hret == HG_SUCCESS) {
        epoch = in.epoch;
        rank = ssg_get_rank(sctx.ssg);
        peer_rank = in.rank;

        assert(pctx.plfsdir != NULL);

        snprintf(path, sizeof(path), "%s/%s", pctx.plfsdir, in.fname);

        out.rv = mon_preload_write(path, in.data, in.data_len,
                epoch, 1 /* foreign */, &mctx);

        /* write trace if we are in testing mode */
        if (pctx.testin && pctx.logfd != -1) {
            ha = pdlfs::xxhash32(in.data, in.data_len, 0);
            n = snprintf(buf, sizeof(buf), "[R] %s %d bytes (e%d) r%d << r%d "
                    "(hash=%08x)\n", path, int(in.data_len), epoch,
                    rank, peer_rank, ha);
            n = write(pctx.logfd, buf, n);

            errno = 0;
        }

        hret = HG_Respond(h, NULL, NULL, &out);
    }

    HG_Free_input(h, &in);

    HG_Destroy(h);

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Respond", hret);
    }

    return hret;
}

/* rpc client-side callback for async shuffled writes */
hg_return_t shuffle_write_async_handler(const struct hg_cb_info* info)
{
    hg_return_t hret;
    hg_handle_t h;
    write_out_t write_out;
    write_async_cb_t* async_cb;
    int rv;

    hret = info->ret;
    assert(info->type == HG_CB_FORWARD);
    async_cb = reinterpret_cast<write_async_cb_t*>(info->arg);
    h = info->info.forward.handle;
    if (hret == HG_SUCCESS) {
        hret = HG_Get_output(h, &write_out);
        if (hret == HG_SUCCESS) {
            rv = write_out.rv;
        }
        HG_Free_output(h, &write_out);
    }

    /* publish response */
    if (hret == HG_SUCCESS) {
        if (async_cb->cb != NULL) {
            async_cb->cb(rv, async_cb->arg1, async_cb->arg2);
        }
    } else {
        rpc_abort("HG_Forward", hret);
    }

    /* return rpc callback slot */
    pthread_mutex_lock(&mtx);
    cb_flags[async_cb->slot] = 0;
    assert(cb_left < cb_allowed);
    if (cb_left == 0 || cb_left == cb_allowed - 1) {
        pthread_cond_broadcast(&cb_cv);
    }
    cb_left++;
    pthread_mutex_unlock(&mtx);

    HG_Destroy(h);

    return HG_SUCCESS;
}

/* send an incoming write to an appropriate peer and return without waiting */
int shuffle_write_async(const char* fn, char* data, size_t len, int epoch,
                        int* is_local,
                        void(*shuffle_cb)(int rv, void* arg1, void* arg2),
                        void* arg1, void* arg2)
{
    hg_return_t hret;
    hg_addr_t peer_addr;
    hg_handle_t h;
    write_in_t write_in;
    write_async_cb_t* async_cb;
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[200];
    int rv;
    int slot;
    const char* fname;
    unsigned long target;
    int ha;
    int peer_rank;
    int rank;
    int e;
    int n;

    *is_local = 0;
    assert(ssg_get_count(sctx.ssg) != 0);
    assert(pctx.plfsdir != NULL);
    assert(fn != NULL);

    fname = fn + pctx.len_plfsdir + 1;  /* remove parent path */
    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    if (ssg_get_count(sctx.ssg) != 1) {
        if (IS_BYPASS_PLACEMENT(pctx.mode)) {
            /* send to next-door neighbor instead of using ch-placement */
            peer_rank = (rank + 1) % ssg_get_count(sctx.ssg);
        } else {
            ch_placement_find_closest(sctx.chp,
                    pdlfs::xxhash64(fname, strlen(fname), 0), 1, &target);
            peer_rank = target;
        }
    } else {
        peer_rank = rank;
    }

    /* write trace if we are in testing mode */
    if (pctx.testin && pctx.logfd != -1) {
        if (rank != peer_rank || sctx.force_rpc) {
            ha = pdlfs::xxhash32(data, len, 0);
            n = snprintf(buf, sizeof(buf), "[A] %s %d bytes (e%d) r%d >> r%d "
                    "(hash=%08x)\n", fn, int(len), epoch,
                    rank, peer_rank, ha);
        } else {
            n = snprintf(buf, sizeof(buf), "[L] %s %d bytes (e%d)\n",
                    fn, int(len), epoch);
        }

        n = write(pctx.logfd, buf, n);

        errno = 0;
    }

    /* avoid rpc if target is local */
    if (peer_rank == rank && !sctx.force_rpc) {
        *is_local = 1;

        rv = mon_preload_write(fn, data, len, epoch, 0 /* non-foreign */,
                &mctx);

        return(rv);
    }

    delay = 1000; /* 1000 us */

    /* wait for rpc callback slot */
    pthread_mutex_lock(&mtx);
    while (cb_left == 0) {
        if (pctx.testin) {
            pthread_mutex_unlock(&mtx);
            if (pctx.logfd != -1) {
                n = snprintf(buf, sizeof(buf), "[X] %s %llu us\n", fn,
                        (unsigned long long) delay);
                n = write(pctx.logfd, buf, n);

                errno = 0;
            }

            usleep(delay);
            delay <<= 1;

            pthread_mutex_lock(&mtx);
        } else {
            now = time(NULL);
            abstime.tv_sec = now + sctx.timeout;
            abstime.tv_nsec = 0;

            e = pthread_cond_timedwait(&cb_cv, &mtx, &abstime);
            if (e == ETIMEDOUT) {
                msg_abort("HG_Forward timeout");
            }
        }
    }
    for (slot = 0; slot < cb_allowed; slot++) {
        if (cb_flags[slot] == 0) {
            break;
        }
    }
    assert(slot < cb_allowed);
    async_cb = &cb_slots[slot];
    cb_flags[slot] = 1;
    assert(cb_left > 0);
    cb_left--;

    pthread_mutex_unlock(&mtx);

    /* go */
    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("cannot obtain addr");

    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &h);
    if (hret != HG_SUCCESS)
        rpc_abort("HG_Create", hret);

    assert(pctx.plfsdir != NULL);

    write_in.fname = fname;
    write_in.data = data;
    write_in.data_len = len;
    write_in.epoch = epoch;
    write_in.rank = rank;

    async_cb->slot = slot;
    async_cb->cb = shuffle_cb;
    async_cb->arg1 = arg1;
    async_cb->arg2 = arg2;

    hret = HG_Forward(h, shuffle_write_async_handler, async_cb,
            &write_in);

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Forward", hret);
    } else {
        return(0);
    }
}

/* block until all outstanding rpc to finish or timeout abort */
void shuffle_wait()
{
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[50];
    int e;
    int n;

    delay = 1000; /* 1000 us */

    pthread_mutex_lock(&mtx);
    while (cb_left != cb_allowed) {
        if (pctx.testin) {
            pthread_mutex_unlock(&mtx);
            if (pctx.logfd != -1) {
                n = snprintf(buf, sizeof(buf), "[Z] %llu us\n",
                        (unsigned long long) delay);
                n = write(pctx.logfd, buf, n);

                errno = 0;
            }

            usleep(delay);
            delay <<= 1;

            pthread_mutex_lock(&mtx);
        } else {
            now = time(NULL);
            abstime.tv_sec = now + sctx.timeout;
            abstime.tv_nsec = 0;

            e = pthread_cond_timedwait(&cb_cv, &mtx, &abstime);
            if (e == ETIMEDOUT) {
                msg_abort("HG_Forward timeout");
            }
        }
    }

    pthread_mutex_unlock(&mtx);
}

/* rpc client-side callback for shuffled writes */
hg_return_t shuffle_write_handler(const struct hg_cb_info* info)
{
    write_cb_t* cb;
    cb = reinterpret_cast<write_cb_t*>(info->arg);
    assert(info->type == HG_CB_FORWARD);

    pthread_mutex_lock(&mtx);
    cb->hret = info->ret;
    cb->ok = 1;
    pthread_cond_broadcast(&rpc_cv);
    pthread_mutex_unlock(&mtx);

    return HG_SUCCESS;
}

/* send an incoming write to an appropriate peer and wait for its result */
int shuffle_write(const char *fn, char *data, size_t len, int epoch,
                  int* is_local)
{
    hg_return_t hret;
    hg_addr_t peer_addr;
    hg_handle_t h;
    write_in_t write_in;
    write_out_t write_out;
    write_cb_t write_cb;
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[200];
    int rv;
    const char* fname;
    unsigned long target;
    int ha;
    int peer_rank;
    int rank;
    int e;
    int n;

    *is_local = 0;
    assert(ssg_get_count(sctx.ssg) != 0);
    assert(pctx.plfsdir != NULL);
    assert(fn != NULL);

    fname = fn + pctx.len_plfsdir + 1; /* remove parent path */
    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    if (ssg_get_count(sctx.ssg) != 1) {
        if (IS_BYPASS_PLACEMENT(pctx.mode)) {
            /* send to next-door neighbor instead of using ch-placement */
            peer_rank = (rank + 1) % ssg_get_count(sctx.ssg);
        } else {
            ch_placement_find_closest(sctx.chp,
                    pdlfs::xxhash64(fname, strlen(fname), 0), 1, &target);
            peer_rank = target;
        }
    } else {
        peer_rank = rank;
    }

    /* write trace if we are in testing mode */
    if (pctx.testin && pctx.logfd != -1) {
        if (rank != peer_rank || sctx.force_rpc) {
            ha = pdlfs::xxhash32(data, len, 0);
            n = snprintf(buf, sizeof(buf), "[S] %s %d bytes (e%d) r%d >> r%d "
                    "(hash=%08x)\n", fn, int(len), epoch,
                    rank, peer_rank, ha);
        } else {
            n = snprintf(buf, sizeof(buf), "[L] %s %d bytes (e%d)\n",
                    fn, int(len), epoch);
        }

        n = write(pctx.logfd, buf, n);

        errno = 0;
    }

    /* avoid rpc if target is local */
    if (peer_rank == rank && !sctx.force_rpc) {
        *is_local = 1;

        rv = mon_preload_write(fn, data, len, epoch, 0 /* non-foreign */,
                &mctx);

        return(rv);
    }

    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("cannot obtain addr");

    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &h);
    if (hret != HG_SUCCESS)
        rpc_abort("HG_Create", hret);

    assert(pctx.plfsdir != NULL);

    write_in.fname = fname;
    write_in.data = data;
    write_in.data_len = len;
    write_in.epoch = epoch;
    write_in.rank = rank;

    write_cb.ok = 0;

    hret = HG_Forward(h, shuffle_write_handler, &write_cb, &write_in);

    delay = 1000;  /* 1000 us */

    if (hret == HG_SUCCESS) {
        /* here we block until rpc completes */
        pthread_mutex_lock(&mtx);
        while(write_cb.ok == 0) {
            if (pctx.testin) {
                pthread_mutex_unlock(&mtx);
                if (pctx.logfd != -1) {
                    n = snprintf(buf, sizeof(buf), "[X] %s %llu us\n", fn,
                            (unsigned long long) delay);
                    n = write(pctx.logfd, buf, n);

                    errno = 0;
                }

                usleep(delay);
                delay <<= 1;

                pthread_mutex_lock(&mtx);
            } else {
                now = time(NULL);
                abstime.tv_sec = now + sctx.timeout;
                abstime.tv_nsec = 0;

                e = pthread_cond_timedwait(&rpc_cv, &mtx, &abstime);
                if (e == ETIMEDOUT) {
                    msg_abort("HG_Forward timeout");
                }
            }
        }
        pthread_mutex_unlock(&mtx);

        hret = write_cb.hret;

        if (hret == HG_SUCCESS) {

            hret = HG_Get_output(h, &write_out);
            if (hret == HG_SUCCESS)
                rv = write_out.rv;
            HG_Free_output(h, &write_out);
        }
    }

    HG_Destroy(h);

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Forward", hret);
    } else if (rv != 0) {
        return(EOF);
    } else {
        return(0);
    }
}

/* bg_work(): dedicated thread function to drive mercury progress */
static void* bg_work(void* foo)
{
    hg_return_t hret;
    unsigned int actual_count;

    trace("bg on");

    while(true) {
        do {
            hret = HG_Trigger(sctx.hg_ctx, 0, 1, &actual_count);
        } while(hret == HG_SUCCESS && actual_count != 0 && !is_shuttingdown());

        if(!is_shuttingdown()) {
            hret = HG_Progress(sctx.hg_ctx, 100);
            if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
                rpc_abort("HG_Progress", hret);
        } else {
            break;
        }
    }

    pthread_mutex_lock(&mtx);
    assert(num_bg > 0);
    num_bg--;
    pthread_cond_broadcast(&bg_cv);
    pthread_mutex_unlock(&mtx);

    trace("bg off");

    return(NULL);
}

/* shuffle_init_ssg(): init the ssg sublayer */
void shuffle_init_ssg(void)
{
    char tmp[100];
    hg_return_t hret;
    const char* env;
    int vf;
    int rank;
    int size;
    int n;

    env = maybe_getenv("SHUFFLE_Virtual_factor");
    if (env == NULL) {
        vf = DEFAULT_VIRTUAL_FACTOR;
    } else {
        vf = atoi(env);
    }

    sctx.ssg = ssg_init_mpi(sctx.hg_clz, MPI_COMM_WORLD);
    if (sctx.ssg == SSG_NULL)
        msg_abort("ssg_init_mpi");

    hret = ssg_lookup(sctx.ssg, sctx.hg_ctx);
    if (hret != HG_SUCCESS)
        msg_abort("ssg_lookup");

    rank = ssg_get_rank(sctx.ssg);
    size = ssg_get_count(sctx.ssg);

    if (pctx.testin) {
        if (pctx.logfd != -1) {
            n = snprintf(tmp, sizeof(tmp), "[G] ssg_rank=%d ssg_size=%d "
                    "vir_factor=%d\n", rank, size, vf);
            n = write(pctx.logfd, tmp, n);

            errno = 0;
        }
    }

    sctx.chp = ch_placement_initialize("ring", size, vf /* vir factor */,
            0 /* hash seed */);
    if (!sctx.chp)
        msg_abort("ch_init");

    return;
}

/* shuffle_init(): init the shuffle layer */
void shuffle_init(void)
{
    hg_return_t hret;
    pthread_t pid;
    char msg[100];
    const char* env;
    int rv;

    prepare_addr(sctx.my_addr);

    env = maybe_getenv("SHUFFLE_Timeout");
    if (env == NULL) {
        sctx.timeout = DEFAULT_TIMEOUT;
    } else {
        sctx.timeout = atoi(env);
        if (sctx.timeout < 5) {
            sctx.timeout = 5;
        }
    }

    env = maybe_getenv("SHUFFLE_Num_outstanding_rpc");
    if (env == NULL) {
        cb_allowed = DEFAULT_OUTSTANDING_RPC;
    } else {
        cb_allowed = atoi(env);
        if (cb_allowed > MAX_OUTSTANDING_RPC) {
            cb_allowed = MAX_OUTSTANDING_RPC;
        }
        else if (cb_allowed <= 0) {
            cb_allowed = 1;
        }
    }

    cb_left = cb_allowed;

    if (is_envset("SHUFFLE_Force_rpc"))
        sctx.force_rpc = 1;
    if (is_envset("SHUFFLE_Force_sync_rpc"))
        sctx.force_sync = 1;

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

    if (pctx.rank == 0) {
        if (sctx.force_sync) {
            warn("async rpc disabled");
        } else {
            snprintf(msg, sizeof(msg), "num outstanding rpc %d", cb_left);
            if (cb_left <= 1) {
                warn(msg);
            } else {
                info(msg);
            }
        }
    }

    trace(__func__);
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

    trace(__func__);
    return;
}

} // extern C
