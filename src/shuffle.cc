/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/time.h>
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
static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

/* used when waiting for an on-going rpc to finish */
static pthread_cond_t rpc_cv;

/* used when waiting for all bg threads to terminate */
static pthread_cond_t bg_cv;

/* used when waiting for the next available rpc callback slot */
static pthread_cond_t cb_cv;

/* used when waiting for a busy rpc queue */
static pthread_cond_t qu_cv;

/* true iff in shutdown seq */
static int shutting_down = 0;  /* XXX: better if this is atomic */

/* number of bg threads running */
static int num_bg = 0;

/* rpc queue */
typedef struct rpcq {
    uint16_t sz;  /* current queue size */
    int busy;  /* non-0 if queue is locked and is being flushed */
    char* buf;  /* dedicated memory for the queue */
} rpcq_t;
static rpcq_t* rpcqs = NULL;
static size_t max_rpcq_sz = 0;  /* bytes allocated for each queue */
static int nrpcqs = 0;  /* number of queues */

/* rpc callback slots */
#define MAX_OUTSTANDING_RPC 128  /* hard limit */
static write_async_cb_t cb_slots[MAX_OUTSTANDING_RPC];
static int cb_flags[MAX_OUTSTANDING_RPC] = { 0 };
static int cb_allowed = 1; /* soft limit */
static int cb_left = 1;

/* shuffle context */
shuffle_ctx_t sctx = { 0 };

/* read a line from file */
static std::string readline(const char* fname)
{
    char tmp[1000];
    ssize_t n;
    int fd;

    memset(tmp, 0, sizeof(tmp));
    fd = open(fname, O_RDONLY);
    if (fd != -1) {
        n = read(fd, tmp, sizeof(tmp) - 1);
        if (n > 0) tmp[n - 1] = 0;
        close(fd);
        errno = 0;
    }

    return tmp;
}

/*
 * try_scan_sysfs(): scan sysfs for important information ^_%
 */
static void try_scan_sysfs()
{
    DIR* d;
    DIR* dd;
    struct dirent* dent;
    struct dirent* ddent;
    char msg[200];
    char path[PATH_MAX];
    std::string mtu;
    std::string txqlen;
    std::string speed;
    std::string nic;
    int tx;
    int rx;
    int nnics;
    int nnodes;
    int ncpus;
    int n;

    if (pctx.rank != 0) return;
    if (access("/sys", R_OK) != 0) {
        /* give up */
        errno = 0;
        return;
    }

    ncpus = 0;
    d = opendir("/sys/devices/system/cpu");
    if (d != NULL) {
       dent = readdir(d);
       for (; dent != NULL; dent = readdir(d)) {
           if (dent->d_type == DT_DIR || dent->d_type == DT_UNKNOWN) {
               if (sscanf(dent->d_name, "cpu%d", &n) == 1) {
                   ncpus++;
               }
           }
       }
       closedir(d);
       snprintf(msg, sizeof(msg), "num CPU cores: %d", ncpus);
       info(msg);
    }

    nnodes = 0;
    d = opendir("/sys/devices/system/node");
    if (d != NULL) {
        dent = readdir(d);
        for (; dent != NULL; dent = readdir(d)) {
            if (dent->d_type == DT_DIR || dent->d_type == DT_UNKNOWN) {
                if (sscanf(dent->d_name, "node%d", &n) == 1) {
                    nnodes++;
                }
            }
        }
        closedir(d);
        snprintf(msg, sizeof(msg), "num NUMA nodes: %d", nnodes);
        info(msg);
    }

    nnics = 0;
    d = opendir("/sys/class/net");
    if (d != NULL) {
        dent = readdir(d);
        for (; dent != NULL; dent = readdir(d)) {
            if (strcmp(dent->d_name, "lo") != 0 &&
                    strcmp(dent->d_name, ".") != 0 &&
                    strcmp(dent->d_name, "..") != 0) {
                nic = dent->d_name;
                snprintf(path, sizeof(path), "/sys/class/net/%s/tx_queue_len",
                        dent->d_name);
                txqlen = readline(path);
                snprintf(path, sizeof(path), "/sys/class/net/%s/speed",
                        dent->d_name);
                speed = readline(path);
                snprintf(path, sizeof(path), "/sys/class/net/%s/mtu",
                        dent->d_name);
                mtu = readline(path);
                tx = 0;
                rx = 0;
                snprintf(path, sizeof(path), "/sys/class/net/%s/queues",
                        dent->d_name);
                dd = opendir(path);
                if (dd != NULL) {
                    ddent = readdir(dd);
                    for (; ddent != NULL; ddent = readdir(dd)) {
                        if (sscanf(ddent->d_name, "tx-%d", &n) == 1) {
                            tx++;
                        } else if (sscanf(ddent->d_name, "rx-%d", &n) == 1) {
                            rx++;
                        }
                    }
                    closedir(dd);
                }
                nnics++;
                snprintf(msg, sizeof(msg), "%s: speed %s Mbps, tx_queue_len "
                        "%s, mtu %s, rx-irq: %d, tx-irq: %d", nic.c_str(),
                        speed.c_str(), txqlen.c_str(), mtu.c_str(),
                        rx, tx);
                info(msg);
            }
        }
        closedir(d);
    }

    errno = 0;
}

/*
 * misc_checks(): check cpu affinity and rlimits.
 */
static void misc_checks()
{
    struct rlimit rl;
    long long softnofile;
    long long hardnofile;
    long long oknofile;
    long long softmemlock;
    long long hardmemlock;
    cpu_set_t cpuset;
    int ncputset;
    int cpus;
    char msg[200];
    int n;

    if (pctx.rank != 0) return;
    n = getrlimit(RLIMIT_NOFILE, &rl);
    if (n == 0) {
        oknofile = 2 * static_cast<long long>(pctx.size) + 128;
        if (rl.rlim_cur != RLIM_INFINITY)
            softnofile = rl.rlim_cur;
        else
            softnofile = -1;
        if (rl.rlim_max != RLIM_INFINITY)
            hardnofile = rl.rlim_max;
        else
            hardnofile = -1;
        snprintf(msg, sizeof(msg), "max open files per process: "
                "%lld soft, %lld hard, %lld suggested",
                softnofile, hardnofile, oknofile);
        if (softnofile < oknofile) {
            warn(msg);
        } else {
            info(msg);
        }
    }

    n = getrlimit(RLIMIT_MEMLOCK, &rl);
    if (n == 0) {
        if (rl.rlim_cur != RLIM_INFINITY)
            softmemlock = rl.rlim_cur;
        else
            softmemlock = -1;
        if (rl.rlim_max != RLIM_INFINITY)
            hardmemlock = rl.rlim_max;
        else
            hardmemlock = -1;
        snprintf(msg, sizeof(msg), "max memlock size: "
                "%lld soft, %lld hard", softmemlock,
                hardmemlock);
        info(msg);
    }

#if defined(_SC_NPROCESSORS_CONF)
    cpus = sysconf(_SC_NPROCESSORS_CONF);
    if (cpus != -1) {
        n = sched_getaffinity(0, sizeof(cpuset), &cpuset);
        if (n == 0) {
            ncputset = CPU_COUNT(&cpuset);
            snprintf(msg, sizeof(msg), "cpu affinity: %d/%d cores",
                    ncputset, cpus);
            if (ncputset == cpus) {
                warn(msg);
            } else {
                info(msg);
            }
        }
    }
#endif

    errno = 0;
}

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
    struct sockaddr_in addr;
    socklen_t addr_len;
    MPI_Comm comm;
    int rank;
    int size;
    const char* subnet;
    char msg[100];
    char ip[50]; // ip
    int so;
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
                        n = snprintf(msg, sizeof(msg), "[IPV4] skip %s\n", ip);
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
    if (min_port < 1)
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
    MPI_Comm_size(comm, &size);
    port = min_port + (rank % (1 + max_port - min_port));
    for (; port <= max_port; port += size) {
        n = 1;
        /* test port availability */
        so = socket(PF_INET, SOCK_STREAM, 0);
        setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
        if (so != -1) {
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(port);
            n = bind(so, (struct sockaddr*)&addr, sizeof(addr));
            close(so);
            errno = 0;
            if (n == 0) {
                break;  /* done */
            }
        } else {
            msg_abort("socket");
        }
    }

    if (port > max_port) {
        port = 0;
        n = 1;
        warn("no free ports available within the specified range\n>>> "
                "auto detecting ports ...");
        so = socket(PF_INET, SOCK_STREAM, 0);
        setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
        if (so != -1) {
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(0);
            n = bind(so, (struct sockaddr*)&addr, sizeof(addr));
            if (n == 0) {
                n = getsockname(so, (struct sockaddr*)&addr, &addr_len);
                if (n == 0) {
                    port = ntohs(addr.sin_port);
                    /* okay */
                }
            }
            close(so);
            errno = 0;
        } else {
            msg_abort("socket");
        }
    }

    if (port == 0) /* maybe a wrong port range has been specified */
        msg_abort("no free ports");

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
            n = snprintf(msg, sizeof(msg), "[URI] using %s\n", buf);
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
extern "C" {

hg_return_t shuffle_write_in_proc(hg_proc_t proc, void* data)
{
    hg_return_t hret;
    hg_uint16_t sz;

    write_in_t* in = reinterpret_cast<write_in_t*>(data);
    hg_proc_op_t op = hg_proc_get_op(proc);

    if (op == HG_ENCODE) {
        memcpy(&sz, in->encoding, 2);
        hret = hg_proc_hg_uint16_t(proc, &sz);
        if (hret == HG_SUCCESS) {
            hret = hg_proc_memcpy(proc, in->encoding + 2, sz);
        }
    } else if (op == HG_DECODE) {
        hret = hg_proc_hg_uint16_t(proc, &sz);
        if (hret == HG_SUCCESS) {
            memcpy(in->encoding, &sz, 2);
            hret = hg_proc_memcpy(proc, in->encoding + 2, sz);
        }
    } else {
        hret = HG_SUCCESS;  /* noop */
    }

    return(hret);
}

hg_return_t shuffle_write_out_proc(hg_proc_t proc, void* data)
{
    hg_return_t hret;

    write_out_t* out = reinterpret_cast<write_out_t*>(data);
    hg_proc_op_t op = hg_proc_get_op(proc);

    if (op == HG_ENCODE) {
        hret = hg_proc_hg_int32_t(proc, &out->rv);
    } else if (op == HG_DECODE) {
        hret = hg_proc_hg_int32_t(proc, &out->rv);
    } else {
        hret = HG_SUCCESS;  /* noop */
    }

    return(hret);
}

/* server-side rpc handler */
hg_return_t shuffle_write_rpc_handler(hg_handle_t h)
{
    char* input;
    uint16_t input_left;
    uint32_t nrank;
    uint16_t nepoch;
    hg_return_t hret;
    write_out_t write_out;
    write_in_t write_in;
    char* data;
    size_t len;
    const char* fname;
    unsigned char fname_len;
    char path[PATH_MAX];
    char buf[200];
    int ha;
    int epoch;
    int src;
    int dst;
    int peer_rank;
    int rank;
    int n;

    assert(pctx.plfsdir != NULL);
    assert(sctx.ssg != NULL);

    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    hret = HG_Get_input(h, &write_in);

    if (hret == HG_SUCCESS) {
        memcpy(&input_left, write_in.encoding, 2);

        /* sender rank */
        if (input_left < 4) {
            hret = HG_OTHER_ERROR;
        } else {
            memcpy(&nrank, write_in.encoding + 2, 4);
            peer_rank = ntohl(nrank);

            /* write trace if we are in testing mode */
            if (pctx.testin) {
                if (pctx.logfd != -1) {
                    n = snprintf(buf, sizeof(buf), "[IN] %d bytes r%d << r%d\n",
                            int(input_left), rank, peer_rank);
                    n = write(pctx.logfd, buf, n);

                    errno = 0;
                }
            }

            input = write_in.encoding + 2 + 4;
            input_left -= 4;
        }

        /* decode and execute writes */
        while (hret == HG_SUCCESS && input_left != 0) {
            /* rank */
            if (input_left < 8) {
                hret = HG_OTHER_ERROR;
                break;
            } else {
                memcpy(&nrank, input, 4);
                src = ntohl(nrank);
                input_left -= 4;
                input += 4;
                memcpy(&nrank, input, 4);
                dst = ntohl(nrank);
                input_left -= 4;
                input += 4;
            }

            /* fname */
            if (input_left < 1) {
                hret = HG_OTHER_ERROR;
                break;
            } else {
                fname_len = static_cast<unsigned char>(input[0]);
                input_left -= 1;
                input += 1;
                if (input_left < fname_len + 1) {
                    hret = HG_OTHER_ERROR;
                    break;
                } else {
                    fname = input;
                    assert(strlen(fname) == fname_len);
                    input_left -= fname_len + 1;
                    input += fname_len + 1;
                }
            }

            /* data */
            if (input_left < 1) {
                hret = HG_OTHER_ERROR;
                break;
            } else {
                len = static_cast<unsigned char>(input[0]);
                input_left -= 1;
                input += 1;
                if (input_left < len) {
                    hret = HG_OTHER_ERROR;
                    break;
                } else {
                    data = input;
                    input_left -= len;
                    input += len;
                }
            }

            /* epoch */
            if (input_left < 2) {
                hret = HG_OTHER_ERROR;
                break;
            } else {
                memcpy(&nepoch, input, 2);
                epoch = ntohs(nepoch);
                input_left -= 2;
                input += 2;
            }

            snprintf(path, sizeof(path), "%s/%s", pctx.plfsdir, fname);
            write_out.rv = mon_preload_write(path, data, len,
                    epoch, NULL);

            /* write trace if we are in testing mode */
            if (pctx.testin && pctx.logfd != -1) {
                ha = pdlfs::xxhash32(data, len, 0);  /* checksum */
                n = snprintf(buf, sizeof(buf), "[RECV] %s %d bytes (e%d) r%d "
                        "<< r%d (hash=%08x)\n", path, int(len),
                        epoch, dst, src, ha);
                n = write(pctx.logfd, buf, n);

                errno = 0;
            }
        }

        if (hret == HG_SUCCESS) {
            hret = HG_Respond(h, NULL, NULL, &write_out);
        }
    }

    HG_Free_input(h, &write_in);

    HG_Destroy(h);

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Respond", hret);
    }

    return hret;
}

/* callback associated with shuffle_write_send_async(...) */
hg_return_t shuffle_write_async_handler(const struct hg_cb_info* info)
{
    hg_return_t hret;
    hg_handle_t h;
    write_async_cb_t* write_cb;
    write_out_t write_out;
    int rv;

    hret = info->ret;
    assert(info->type == HG_CB_FORWARD);
    write_cb = reinterpret_cast<write_async_cb_t*>(info->arg);
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
        if (write_cb->cb != NULL) {
            write_cb->cb(rv, write_cb->arg1, write_cb->arg2);
        }
    } else {
        rpc_abort("HG_Forward", hret);
    }

    /* return rpc callback slot */
    pthread_mutex_lock(&mtx);
    cb_flags[write_cb->slot] = 0;
    assert(cb_left < cb_allowed);
    if (cb_left == 0 || cb_left == cb_allowed - 1) {
        pthread_cond_broadcast(&cb_cv);
    }
    cb_left++;
    pthread_mutex_unlock(&mtx);

    HG_Destroy(h);

    return HG_SUCCESS;
}

/* send a write request to a specified peer and return without waiting */
int shuffle_write_send_async(write_in_t* write_in, int peer_rank,
                            void(*shuffle_cb)(int rv, void* arg1, void* arg2),
                            void* arg1, void* arg2)
{
    hg_return_t hret;
    hg_addr_t peer_addr;
    hg_handle_t h;
    write_async_cb_t* write_cb;
    uint16_t write_sz;
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[200];
    int slot;
    int rank;
    int e;
    int n;

    assert(write_in != NULL);
    memcpy(&write_sz, write_in->encoding, 2);
    assert(sctx.ssg != NULL);
    rank = ssg_get_rank(sctx.ssg);

    /* write trace if we are in testing mode */
    if (pctx.testin && pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[OUT] %d bytes r%d >> r%d\n",
                int(write_sz), rank, peer_rank);

        n = write(pctx.logfd, buf, n);

        errno = 0;
    }

    delay = 1000; /* 1000 us */

    /* wait for slot */
    pthread_mutex_lock(&mtx);
    while (cb_left == 0) {  /* no slots available */
        if (pctx.testin) {
            pthread_mutex_unlock(&mtx);
            if (pctx.logfd != -1) {
                n = snprintf(buf, sizeof(buf), "[SLOT] %d us\n", int(delay));
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
                msg_abort("timeout");
            }
        }
    }
    for (slot = 0; slot < cb_allowed; slot++) {
        if (cb_flags[slot] == 0) {
            break;
        }
    }
    assert(slot < cb_allowed);
    write_cb = &cb_slots[slot];
    cb_flags[slot] = 1;
    assert(cb_left > 0);
    cb_left--;

    pthread_mutex_unlock(&mtx);

    /* go */
    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("cannot obtain addr");

    assert(sctx.hg_ctx != NULL);
    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &h);
    if (hret != HG_SUCCESS)
        rpc_abort("HG_Create", hret);

    write_cb->slot = slot;
    write_cb->cb = shuffle_cb;
    write_cb->arg1 = arg1;
    write_cb->arg2 = arg2;

    hret = HG_Forward(h, shuffle_write_async_handler, write_cb,
            write_in);

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Forward", hret);
    } else {
        return(0);
    }
}

/* block until all outstanding rpc finishes */
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
                n = snprintf(buf, sizeof(buf), "[WAIT] %d us\n", int(delay));
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
                msg_abort("timeout");
            }
        }
    }

    pthread_mutex_unlock(&mtx);
}

/* callback associated with shuffle_write_send(...) */
hg_return_t shuffle_write_handler(const struct hg_cb_info* info)
{
    write_cb_t* write_cb;
    write_cb = reinterpret_cast<write_cb_t*>(info->arg);
    assert(info->type == HG_CB_FORWARD);

    pthread_mutex_lock(&mtx);
    write_cb->hret = info->ret;
    write_cb->ok = 1;
    pthread_cond_broadcast(&rpc_cv);
    pthread_mutex_unlock(&mtx);

    return HG_SUCCESS;
}

/* send a write request to a specified peer and wait for its response */
int shuffle_write_send(write_in_t* write_in, int peer_rank)
{
    hg_return_t hret;
    hg_addr_t peer_addr;
    hg_handle_t h;
    uint16_t write_sz;
    write_out_t write_out;
    write_cb_t write_cb;
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[200];
    int rv;
    int rank;
    int e;
    int n;

    assert(write_in != NULL);
    memcpy(&write_sz, write_in->encoding, 2);
    assert(sctx.ssg != NULL);
    rank = ssg_get_rank(sctx.ssg);

    /* write trace if we are in testing mode */
    if (pctx.testin && pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[OUT] %d bytes r%d >> r%d\n",
                int(write_sz), rank, peer_rank);

        n = write(pctx.logfd, buf, n);

        errno = 0;
    }

    peer_addr = ssg_get_addr(sctx.ssg, peer_rank);
    if (peer_addr == HG_ADDR_NULL)
        msg_abort("cannot obtain addr");

    assert(sctx.hg_ctx != NULL);
    hret = HG_Create(sctx.hg_ctx, peer_addr, sctx.hg_id, &h);  /* XXX: malloc */
    if (hret != HG_SUCCESS)
        rpc_abort("HG_Create", hret);

    write_cb.ok = 0;

    hret = HG_Forward(h, shuffle_write_handler, &write_cb, write_in);

    delay = 1000;  /* 1000 us */

    if (hret == HG_SUCCESS) {
        /* here we block until rpc completes */
        pthread_mutex_lock(&mtx);
        while(write_cb.ok == 0) {  /* rpc not completed */
            if (pctx.testin) {
                pthread_mutex_unlock(&mtx);
                if (pctx.logfd != -1) {
                    n = snprintf(buf, sizeof(buf), "[WAIT] r%d >> r%d %d us\n",
                            rank, peer_rank, int(delay));
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
                    msg_abort("timeout");
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

    HG_Destroy(h);  /* XXX: reuse */

    if (hret != HG_SUCCESS) {
        rpc_abort("HG_Forward", hret);
    } else if (rv != 0) {
        return(EOF);
    } else {
        return(0);
    }
}

/* add an incoming write into an appropriate rpc sender queue */
int shuffle_write(const char* path, char* data, size_t len, int epoch)
{
    write_in_t write_in;
    uint16_t write_sz;
    uint16_t nepoch;
    uint32_t nrank;
    rpcq_t* rpcq;
    int rpcq_idx;
    size_t rpc_sz;
    time_t now;
    struct timespec abstime;
    useconds_t delay;
    char buf[200];
    int rv;
    const char* fname;
    unsigned char fname_len;
    unsigned long target;
    int ha;
    int peer_rank;
    int rank;
    int e;
    int n;

    assert(sctx.ssg != NULL);
    assert(ssg_get_count(sctx.ssg) != 0);
    assert(pctx.plfsdir != NULL);
    assert(path != NULL);

    fname = path + pctx.len_plfsdir + 1;  /* remove parent path */
    fname_len = static_cast<unsigned char>(strlen(fname));
    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    if (ssg_get_count(sctx.ssg) != 1) {
        if (IS_BYPASS_PLACEMENT(pctx.mode)) {
            /* send to next-door neighbor instead of using ch-placement */
            peer_rank = (rank + 1) % ssg_get_count(sctx.ssg);
        } else {
            assert(sctx.chp != NULL);
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
            ha = pdlfs::xxhash32(data, len, 0);  /* checksum */
            n = snprintf(buf, sizeof(buf), "[SEND] %s %d bytes (e%d) r%d >> "
                    "r%d (hash=%08x)\n", path, int(len), epoch,
                    rank, peer_rank, ha);
        } else {
            n = snprintf(buf, sizeof(buf), "[LO] %s %d bytes (e%d)\n",
                    path, int(len), epoch);
        }

        n = write(pctx.logfd, buf, n);

        errno = 0;
    }

    /* bypass rpc if target is local */
    if (peer_rank == rank && !sctx.force_rpc) {
        rv = mon_preload_write(path, data, len, epoch, NULL);
        return(rv);
    }

    pthread_mutex_lock(&mtx);

    rpcq_idx = peer_rank;  /* XXX: assuming one queue per rank */
    assert(rpcq_idx < nrpcqs);
    rpcq = &rpcqs[rpcq_idx];
    assert(rpcq != NULL);
    assert(fname_len < 256);
    assert(len < 256);
    rpc_sz = 0;

    delay = 1000;  /* 1000 us */

    /* wait for queue */
    while (rpcq->busy != 0) {
        if (pctx.testin) {
            pthread_mutex_unlock(&mtx);
            if (pctx.logfd != -1) {
                n = snprintf(buf, sizeof(buf), "[QUEUE] %d us\n", int(delay));
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

            e = pthread_cond_timedwait(&qu_cv, &mtx, &abstime);
            if (e == ETIMEDOUT) {
                msg_abort("timeout");
            }
        }
    }

    /* get an estimated size of the rpc */
    rpc_sz += 4;  /* src rank */
    rpc_sz += 4;  /* dst rank */
    rpc_sz += 1 + fname_len + 1;  /* vpic fname */
    rpc_sz += 1 + len;  /* vpic data */
    rpc_sz += 2;  /* epoch */

    /* flush queue if full */
    if (rpcq->sz + rpc_sz > max_rpcq_sz) {
        if (rpcq->sz + 2 + 4 > sizeof(write_in.encoding)) {
            /* happens when the total size of queued data is greater than
             * the size limit for an rpc message */
            msg_abort("rpc overflow");
        } else {
            rpcq->busy = 1;  /* force other writers to block */
            pthread_mutex_unlock(&mtx);
            write_sz = rpcq->sz + 4;  /* with sender rank */
            memcpy(write_in.encoding, &write_sz, 2);
            nrank = htonl(rank);
            memcpy(write_in.encoding + 2, &nrank, 4);
            memcpy(write_in.encoding + 2 + 4, rpcq->buf, rpcq->sz);
            if (!sctx.force_sync) {
                rv = mon_shuffle_write_send_async(&write_in, peer_rank, NULL);
            } else {
                rv = mon_shuffle_write_send(&write_in, peer_rank, NULL);
            }
            if (rv != 0) {
                if (pctx.verr) {
                    /* XXX: set errno */
                    error("xxx");
                }
            }
            pthread_mutex_lock(&mtx);
            pthread_cond_broadcast(&qu_cv);
            rpcq->busy = 0;
            rpcq->sz = 0;
        }
    }

    /* enqueue */
    if (rpcq->sz + rpc_sz > max_rpcq_sz) {
        /* happens when the memory reserved for the queue is smaller than
         * a single write */
        msg_abort("rpc overflow");
    } else {
        /* rank */
        nrank = htonl(rank);
        memcpy(rpcq->buf + rpcq->sz, &nrank, 4);
        rpcq->sz += 4;
        nrank = htonl(peer_rank);
        memcpy(rpcq->buf + rpcq->sz, &nrank, 4);
        rpcq->sz += 4;
        /* vpic fname */
        rpcq->buf[rpcq->sz] = fname_len;
        memcpy(rpcq->buf + rpcq->sz + 1, fname, fname_len);
        rpcq->sz += 1 + fname_len;
        rpcq->buf[rpcq->sz] = 0;
        rpcq->sz += 1;
        /* vpic data */
        rpcq->buf[rpcq->sz] = len;
        memcpy(rpcq->buf + rpcq->sz + 1, data, len);
        rpcq->sz += 1 + len;
        /* epoch */
        nepoch = htons(epoch);
        memcpy(rpcq->buf + rpcq->sz, &nepoch, 2);
        rpcq->sz += 2;
    }

    pthread_mutex_unlock(&mtx);

    return(0);
}

/* force flush all rpc queue */
void shuffle_flush()
{
    uint32_t nrank;
    uint16_t write_sz;
    write_in_t write_in;
    rpcq_t* rpcq;
    int rank;
    int rv;
    int i;

    assert(sctx.ssg != NULL);

    rank = ssg_get_rank(sctx.ssg);  /* my rank */

    pthread_mutex_lock(&mtx);

    for (i = 0; i < nrpcqs; i++) {
        rpcq = &rpcqs[i];
        if (rpcq->sz == 0) {  /* skip empty queue */
            continue;
        } else if (rpcq->sz + 2 + 4 > sizeof(write_in.encoding)) {
            msg_abort("rpc overflow");
        } else {
            rpcq->busy = 1;  /* force other writers to block */
            pthread_mutex_unlock(&mtx);
            write_sz = rpcq->sz + 4;  /* with sender rank */
            memcpy(write_in.encoding, &write_sz, 2);
            nrank = htonl(rank);
            memcpy(write_in.encoding + 2, &nrank, 4);
            memcpy(write_in.encoding + 2 + 4, rpcq->buf, rpcq->sz);
            if (!sctx.force_sync) {
                rv = mon_shuffle_write_send_async(&write_in, i, NULL);
            } else {
                rv = mon_shuffle_write_send(&write_in, i, NULL);
            }
            if (rv != 0) {
                if (pctx.verr) {
                    /* XXX: set errno */
                    error("xxx");
                }
            }
            pthread_mutex_lock(&mtx);
            pthread_cond_broadcast(&qu_cv);
            rpcq->busy = 0;
            rpcq->sz = 0;
        }
    }

    pthread_mutex_unlock(&mtx);

    return;
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
    int rank;  /* ssg */
    int size;  /* ssg */
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
            n = snprintf(tmp, sizeof(tmp), "[SSG] ssg_rank=%d ssg_size=%d "
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
    int i;

    try_scan_sysfs();
    prepare_addr(sctx.my_addr);
    misc_checks();

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

    /* rpc queue */
    assert(sctx.ssg != NULL);
    nrpcqs = ssg_get_count(sctx.ssg);
    max_rpcq_sz = 1024;  /* XXX */
    rpcqs = static_cast<rpcq_t*>(malloc(nrpcqs * sizeof(rpcq_t)));
    for (i = 0; i < nrpcqs; i++) {
        rpcqs[i].buf = static_cast<char*>(malloc(max_rpcq_sz));
        rpcqs[i].busy = 0;
        rpcqs[i].sz = 0;
    }

    rv = pthread_cond_init(&rpc_cv, NULL);
    if (rv) msg_abort("pthread_cond_init");
    rv = pthread_cond_init(&bg_cv, NULL);
    if (rv) msg_abort("pthread_cond_init");
    rv = pthread_cond_init(&cb_cv, NULL);
    if (rv) msg_abort("pthread_cond_init");
    rv = pthread_cond_init(&qu_cv, NULL);
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
