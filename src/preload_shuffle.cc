/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <arpa/inet.h>
#include <assert.h>
#include <ifaddrs.h>

#include "preload_internal.h"
#include "preload_mon.h"
#include "preload_shuffle.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffle.h"

#include <mercury_config.h>
#include <pdlfs-common/xxhash.h>
#ifdef PRELOAD_HAS_CH_PLACEMENT
#include <ch-placement.h>
#endif

#include "common.h"

namespace {
void shuffle_prepare_sm_uri(char* buf, const char* proto) {
  int min_port, max_port;
  const char* env;

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
  if (max_port - min_port < 0) ABORT("bad min-max port");
  if (min_port < 1) ABORT("bad min port");
  if (max_port > 65535) ABORT("bad max port");

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using port range [%d,%d]", min_port, max_port);
  }

  /* finalize uri */
  assert(strstr(proto, "sm") != NULL);
  sprintf(buf, "%s://%d:%d", proto, int(getpid()), min_port);
#ifndef NDEBUG
  if (pctx.verbose || pctx.my_rank == 0) {
    logf(LOG_INFO, "[hg] using %s (rank %d)", buf, pctx.my_rank);
  }
#endif
}

void shuffle_determine_ipaddr(char* ip, socklen_t iplen) {
  int family;
  struct ifaddrs *ifaddr, *cur;
  const char* subnet;

  subnet = maybe_getenv("SHUFFLE_Subnet");
  if (!subnet || !subnet[0] || strcmp("0.0.0.0", subnet) == 0) {
    subnet = NULL;
  }
  if (pctx.my_rank == 0) {
    if (!subnet) {
      logf(LOG_WARN,
           "subnet not specified\n>>> will use the 1st non-local ip...");
    } else {
      logf(LOG_INFO, "using subnet %s*", subnet);
    }
  }

  /* settle down an ip addr to use */
  if (getifaddrs(&ifaddr) == -1) {
    ABORT("getifaddrs");
  }

  for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
    if (cur->ifa_addr != NULL) {
      family = cur->ifa_addr->sa_family;

      if (family == AF_INET) {
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip, iplen,
                        NULL, 0, NI_NUMERICHOST) == -1)
          ABORT("getnameinfo");

        if (!subnet && strncmp("127", ip, strlen("127")) != 0) {
          break;
        } else if (subnet && strncmp(subnet, ip, strlen(subnet)) == 0) {
          break;
        } else {
#ifndef NDEBUG
          if (pctx.verbose || pctx.my_rank == 0) {
            logf(LOG_INFO, "[ip] skip %s (rank %d)", ip, pctx.my_rank);
          }
#endif
        }
      }
    }
  }

  if (cur == NULL) /* maybe a wrong subnet has been specified */
    ABORT("no ip addr");
  freeifaddrs(ifaddr);

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using ip %s (rank 0)", ip);
  }
}

}  // namespace

void shuffle_prepare_uri(char* buf) {
  int port;
  const char* env;
  int min_port;
  int max_port;
  struct sockaddr_in addr;
  socklen_t addr_len;
  MPI_Comm comm;
  int rank;
  int size;
  const char* proto;
  char ip[50];
  int opt;
  int so;
  int rv;
  int n;

  proto = maybe_getenv("SHUFFLE_Mercury_proto");
  if (proto == NULL) {
    proto = DEFAULT_HG_PROTO;
  }
  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using %s", proto);
  }

  if (strstr(proto, "sm") != NULL) { /* special handling for sm addrs */
    shuffle_prepare_sm_uri(buf, proto);
    return;
  }

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
  if (max_port - min_port < 0) ABORT("bad min-max port");
  if (min_port < 1) ABORT("bad min port");
  if (max_port > 65535) ABORT("bad max port");

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using port range [%d,%d]", min_port, max_port);
  }

#if MPI_VERSION >= 3
  rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                           MPI_INFO_NULL, &comm);
  if (rv != MPI_SUCCESS) {
    ABORT("MPI_Comm_split_type");
  }
#else
  comm = MPI_COMM_WORLD;
#endif
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &size);
  if (comm != MPI_COMM_WORLD) {
    MPI_Comm_free(&comm);
  }

  /* try and test port availability */
  port = min_port + (rank % (1 + max_port - min_port));
  for (; port <= max_port; port += size) {
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so != -1) {
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(port);
      opt = 1;
      setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
      n = bind(so, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
      close(so);
      if (n == 0) {
        break;
      }
    } else {
      ABORT("socket");
    }
  }

  if (port > max_port) {
    port = 0;
    logf(LOG_WARN,
         "no free ports available within the specified range\n>>> "
         "auto detecting ports ...");
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so != -1) {
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(0);
      opt = 0; /* do not reuse ports */
      setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
      n = bind(so, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
      if (n == 0) {
        addr_len = sizeof(addr);
        n = getsockname(so, reinterpret_cast<struct sockaddr*>(&addr),
                        &addr_len);
        if (n == 0) {
          port = ntohs(addr.sin_port);
        }
      }
      close(so);
    } else {
      ABORT("socket");
    }
  }

  errno = 0;

  if (port == 0) /* maybe a wrong port range has been specified */
    ABORT("no free ports");

  /* finalize uri */
  shuffle_determine_ipaddr(ip, sizeof(ip));
  sprintf(buf, "%s://%s:%d", proto, ip, port);
#ifndef NDEBUG
  if (pctx.verbose || pctx.my_rank == 0) {
    logf(LOG_INFO, "[hg] using %s (rank %d)", buf, pctx.my_rank);
  }
#endif
}

void shuffle_epoch_pre_start(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffle_epoch_start(rep);
  } else {
    nn_shuffler_bgwait();
  }
}

/*
 * This function is called at the beginning of each epoch but before the epoch
 * really starts and before the final stats for the previous epoch are collected
 * and dumped. Therefore, this is a good time for us to copy xn_shuffle's
 * internal stats counters into preload's global mon context.
 */
void shuffle_epoch_start(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffle_epoch_start(rep);
    pctx.mctx.nlmr = rep->stat.local.recvs - rep->last_stat.local.recvs;
    pctx.mctx.min_nlmr = pctx.mctx.max_nlmr = pctx.mctx.nlmr;
    pctx.mctx.nlms = rep->stat.local.sends - rep->last_stat.local.sends;
    pctx.mctx.min_nlms = pctx.mctx.max_nlms = pctx.mctx.nlms;
    pctx.mctx.nlmd = pctx.mctx.nlms;
    pctx.mctx.nmr = rep->stat.remote.recvs - rep->last_stat.remote.recvs;
    pctx.mctx.min_nmr = pctx.mctx.max_nmr = pctx.mctx.nmr;
    pctx.mctx.nms = rep->stat.remote.sends - rep->last_stat.remote.sends;
    pctx.mctx.min_nms = pctx.mctx.max_nms = pctx.mctx.nms;
    pctx.mctx.nmd = pctx.mctx.nms;
  } else {
    nn_shuffler_bgwait();
  }
}

void shuffle_epoch_end(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_epoch_end(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    nn_shuffler_flushq(); /* flush rpc queues */
    if (!nnctx.force_sync) {
      /* wait for rpc replies */
      nn_shuffler_waitcb();
    }
  }
}

int shuffle_target(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz) {
  int world_sz, keylen;
  unsigned long target;
  int rv;

  assert(ctx != NULL);
  assert(buf_sz >= ctx->skey_len);

  world_sz = shuffle_world_sz(ctx);
  /* XXX backward compat: don't hash the trailing null to match old code */
  /* XXX: assumes at this level skey is a filename in a C string */
  keylen = ctx->skey_len - 1;

  if (world_sz != 1) {
#ifdef PRELOAD_HAS_CH_PLACEMENT
    if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
      assert(ctx->chp != NULL);
      ch_placement_find_closest(
          ctx->chp, pdlfs::xxhash64(buf, keylen, 0), 1, &target);
      rv = static_cast<int>(target);
    } else {
#endif
      rv = pdlfs::xxhash32(buf, keylen, 0) % world_sz;
#ifdef PRELOAD_HAS_CH_PLACEMENT
    }
#endif
  } else {
    rv = shuffle_rank(ctx);
  }

  return (rv & ctx->receiver_mask);
}

namespace {
void shuffle_write_debug(shuffle_ctx_t* ctx, char* buf, unsigned char buf_sz,
                         int epoch, int src, int dst) {
  const int h = pdlfs::xxhash32(buf, buf_sz, 0);

  if (src != dst || ctx->force_rpc) {
    fprintf(pctx.trace, "[SH] %u bytes (ep=%d) r%d >> r%d (xx=%08x)\n", buf_sz,
            epoch, src, dst, h);
  } else {
    fprintf(pctx.trace,
            "[LO] %u bytes (ep=%d) "
            "(xx=%08x)\n",
            buf_sz, epoch, h);
  }
}
}  // namespace

// the shuffle key/value should match the preload inkey/invalue
int shuffle_write(shuffle_ctx_t* ctx, const char* skey,
                  unsigned char skey_len, char* svalue,
                  unsigned char svalue_len, int epoch) {
  char buf[255];
  int peer_rank;
  int rank;
  int rv;

  assert(ctx == &pctx.sctx);
  assert(ctx->skey_len + ctx->svalue_len + ctx->extra_data_len <= sizeof(buf));
  if (ctx->skey_len != skey_len) ABORT("bad shuffle key len");
  if (ctx->svalue_len != svalue_len) ABORT("bad shuffle value len");

  unsigned char base_sz = skey_len + svalue_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;
  memcpy(buf, skey, skey_len);
  memcpy(buf + skey_len, svalue, svalue_len);
  if (buf_sz != base_sz) memset(buf + base_sz, 0, buf_sz - base_sz);

  peer_rank = shuffle_target(ctx, buf, buf_sz);
  rank = shuffle_rank(ctx);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_debug(ctx, buf, buf_sz, epoch, rank, peer_rank);

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(skey, skey_len, svalue, svalue_len, epoch);
    return rv;
  }

  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
                       peer_rank, rank);
  } else {
    nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  }

  return 0;
}

namespace {
void shuffle_handle_debug(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz,
                          int epoch, int src, int dst) {
  const int h = pdlfs::xxhash32(buf, buf_sz, 0);

  fprintf(pctx.trace,
          "[RM] %u bytes (ep=%d) r%d << r%d "
          "(xx=%08x)\n",
          buf_sz, epoch, dst, src, h);
}
}  // namespace

int shuffle_handle(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz,
                   int epoch, int src, int dst) {
  int rv;

  /* XXXCDC: xn gives us NULL, nn should give pctx.sctx, make uniform? */
  if (ctx == NULL) {
    ctx = &pctx.sctx;
  } else {
    assert(ctx == &pctx.sctx);
  }
  if (buf_sz != ctx->extra_data_len + ctx->svalue_len + ctx->skey_len)
    ABORT("unexpected incoming shuffle request size");
  rv = exotic_write(buf, ctx->skey_len, buf + ctx->skey_len,
                    ctx->svalue_len, epoch, src);

  if (pctx.testin && pctx.trace != NULL)
    shuffle_handle_debug(ctx, buf, buf_sz, epoch, src, dst);

  return rv;
}

void shuffle_finalize(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN && ctx->rep != NULL) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffle_destroy(rep);
    if (ctx->finalize_pause > 0) {
      sleep(ctx->finalize_pause);
    }
#ifndef NDEBUG
    unsigned long long sum_rpcs[2];
    unsigned long long min_rpcs[2];
    unsigned long long max_rpcs[2];
    unsigned long long rpcs[2];
    rpcs[0] = rep->stat.local.sends;
    rpcs[1] = rep->stat.remote.sends;
    MPI_Reduce(rpcs, sum_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, min_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, max_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0 && (sum_rpcs[0] + sum_rpcs[1]) != 0) {
      logf(LOG_INFO,
           "[rpc] total sends: %s intra-node + %s inter-node = %s overall "
           ".....\n"
           " -> intra-node: %s per rank (min: %s, max: %s)\n"
           " -> inter-node: %s per rank (min: %s, max: %s)\n"
           " //",
           pretty_num(sum_rpcs[0]).c_str(), pretty_num(sum_rpcs[1]).c_str(),
           pretty_num(sum_rpcs[0] + sum_rpcs[1]).c_str(),
           pretty_num(double(sum_rpcs[0]) / pctx.comm_sz).c_str(),
           pretty_num(min_rpcs[0]).c_str(), pretty_num(max_rpcs[0]).c_str(),
           pretty_num(double(sum_rpcs[1]) / pctx.comm_sz).c_str(),
           pretty_num(min_rpcs[1]).c_str(), pretty_num(max_rpcs[1]).c_str());
    }
#endif
    ctx->rep = NULL;
    free(rep);
  } else {
    hstg_t hg_intvl;
    int p[] = {10, 30, 50, 70, 90, 95, 96, 97, 98, 99};
    double d[] = {99.5,  99.7,   99.9,   99.95,  99.97,
                  99.99, 99.995, 99.997, 99.999, 99.9999};
#define NUM_RUSAGE (sizeof(nnctx.r) / sizeof(nn_rusage_t))
    nn_rusage_t total_rusage_recv[NUM_RUSAGE];
    nn_rusage_t total_rusage[NUM_RUSAGE];
    unsigned long long total_writes;
    unsigned long long total_msgsz;
    hstg_t iq_dep;
    nn_shuffler_destroy();
    if (ctx->finalize_pause > 0) {
      sleep(ctx->finalize_pause);
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "[nn] per-thread cpu usage ... (s)");
      logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_rank",
           "SYS_per_rank", "TOTAL_per_rank");
    }
    for (size_t i = 0; i < NUM_RUSAGE; i++) {
      if (nnctx.r[i].tag[0] != 0) {
        MPI_Reduce(&nnctx.r[i].usr_micros, &total_rusage[i].usr_micros, 1,
                   MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&nnctx.r[i].sys_micros, &total_rusage[i].sys_micros, 1,
                   MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
               double(total_rusage[i].usr_micros) / 1000000 / pctx.comm_sz,
               double(total_rusage[i].sys_micros) / 1000000 / pctx.comm_sz,
               double(total_rusage[i].usr_micros + total_rusage[i].sys_micros) /
                   1000000 / pctx.comm_sz);
        }
      }
    }
    if (!shuffle_is_everyone_receiver(ctx)) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_recv",
             "SYS_per_recv", "TOTAL_per_recv");
      }
      for (size_t i = 0; i < NUM_RUSAGE; i++) {
        if (nnctx.r[i].tag[0] != 0 && pctx.recv_comm != MPI_COMM_NULL) {
          MPI_Reduce(&nnctx.r[i].usr_micros, &total_rusage_recv[i].usr_micros,
                     1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, pctx.recv_comm);
          MPI_Reduce(&nnctx.r[i].sys_micros, &total_rusage_recv[i].sys_micros,
                     1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, pctx.recv_comm);
          if (pctx.my_rank == 0) {
            logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
                 double(total_rusage_recv[i].usr_micros) / 1000000 /
                     pctx.recv_sz,
                 double(total_rusage_recv[i].sys_micros) / 1000000 /
                     pctx.recv_sz,
                 double(total_rusage_recv[i].usr_micros +
                        total_rusage_recv[i].sys_micros) /
                     1000000 / pctx.recv_sz);
          }
        }
      }
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_nonrecv",
             "SYS_per_nonrecv", "TOTAL_per_nonrecv");
      }
      for (size_t i = 0; i < NUM_RUSAGE; i++) {
        if (nnctx.r[i].tag[0] != 0 && pctx.recv_comm != MPI_COMM_NULL) {
          if (pctx.my_rank == 0) {
            logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
                 double(total_rusage[i].usr_micros -
                        total_rusage_recv[i].usr_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz),
                 double(total_rusage[i].sys_micros -
                        total_rusage_recv[i].sys_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz),
                 double(total_rusage[i].usr_micros -
                        total_rusage_recv[i].usr_micros +
                        total_rusage[i].sys_micros -
                        total_rusage_recv[i].sys_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz));
            ;
          }
        }
      }
    }
    if (pctx.recv_comm != MPI_COMM_NULL) {
      memset(&hg_intvl, 0, sizeof(hstg_t));
      hstg_reset_min(hg_intvl);
      hstg_reduce(nnctx.hg_intvl, hg_intvl, pctx.recv_comm);
      if (pctx.my_rank == 0 && hstg_num(hg_intvl) >= 1.0) {
        logf(LOG_INFO, "[nn] hg_progress interval ... (ms)");
        logf(LOG_INFO, "  %s samples, avg: %.3f (min: %.0f, max: %.0f)",
             pretty_num(hstg_num(hg_intvl)).c_str(), hstg_avg(hg_intvl),
             hstg_min(hg_intvl), hstg_max(hg_intvl));
        for (size_t i = 0; i < sizeof(p) / sizeof(int); i++) {
          logf(LOG_INFO, "    - %d%% %-12.2f %.4f%% %.2f", p[i],
               hstg_ptile(hg_intvl, p[i]), d[i], hstg_ptile(hg_intvl, d[i]));
        }
      }
      memset(&iq_dep, 0, sizeof(hstg_t));
      hstg_reset_min(iq_dep);
      hstg_reduce(nnctx.iq_dep, iq_dep, pctx.recv_comm);
      MPI_Reduce(&nnctx.total_writes, &total_writes, 1, MPI_UNSIGNED_LONG_LONG,
                 MPI_SUM, 0, pctx.recv_comm);
      MPI_Reduce(&nnctx.total_msgsz, &total_msgsz, 1, MPI_UNSIGNED_LONG_LONG,
                 MPI_SUM, 0, pctx.recv_comm);
      if (pctx.my_rank == 0 && hstg_num(iq_dep) >= 1.0) {
        logf(LOG_INFO,
             "[nn] avg rpc size: %s (%s writes per rpc, %s per write)",
             pretty_size(double(total_msgsz) / hstg_sum(iq_dep)).c_str(),
             pretty_num(double(total_writes) / hstg_sum(iq_dep)).c_str(),
             pretty_size(double(total_msgsz) / double(total_writes)).c_str());
        logf(LOG_INFO, "[nn] rpc incoming queue depth ...");
        logf(LOG_INFO, "  %s samples, avg: %.3f (min: %.0f, max: %.0f)",
             pretty_num(hstg_num(iq_dep)).c_str(), hstg_avg(iq_dep),
             hstg_min(iq_dep), hstg_max(iq_dep));
        for (size_t i = 0; i < sizeof(p) / sizeof(int); i++) {
          logf(LOG_INFO, "    - %d%% %-12.2f %.4f%% %.2f", p[i],
               hstg_ptile(iq_dep, p[i]), d[i], hstg_ptile(iq_dep, d[i]));
        }
      }
    }
#undef NUM_RUSAGE
  }
#ifdef PRELOAD_HAS_CH_PLACEMENT
  if (ctx->chp != NULL) {
    ch_placement_finalize(ctx->chp);
    ctx->chp = NULL;
  }
#endif
}

namespace {
/* convert an integer number to an unsigned char */
unsigned char TOUCHAR(int input) {
  assert(input >= 0 && input <= 255);
  unsigned char rv = static_cast<unsigned char>(input);
  return rv;
}
}  // namespace

void shuffle_init(shuffle_ctx_t* ctx) {
  const char* proto;
  const char* env;
  int world_sz;
  int vf;
  int n;

  assert(ctx != NULL);

  ctx->skey_len = TOUCHAR(pctx.preload_inkey_size);
  ctx->extra_data_len = TOUCHAR(pctx.shuffle_extrabytes);
  ctx->svalue_len = TOUCHAR(pctx.preload_invalue_size);
  if (ctx->extra_data_len + ctx->svalue_len > 255 - ctx->skey_len - 1)
    ABORT("bad shuffle conf: id + data exceeds 255 bytes");
  if (ctx->skey_len == 0) {
    ABORT("bad shuffle conf: id size is zero");
  }

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "shuffle format: K = %u bytes, V = %u bytes",
         ctx->skey_len, ctx->extra_data_len + ctx->svalue_len);
  }

  ctx->receiver_rate = 1;
  ctx->receiver_mask = ~static_cast<unsigned int>(0);
  env = maybe_getenv("SHUFFLE_Recv_radix");
  if (env != NULL) {
    n = atoi(env);
    if (n > 8) n = 8;
    if (n > 0) {
      ctx->receiver_rate <<= n;
      ctx->receiver_mask <<= n;
    }
  }
  ctx->is_receiver = shuffle_is_rank_receiver(ctx, pctx.my_rank);
  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "%u shuffle senders per receiver\n>>> receiver mask is %#x",
         ctx->receiver_rate, ctx->receiver_mask);
  }

  env = maybe_getenv("SHUFFLE_Finalize_pause");
  if (env != NULL) {
    ctx->finalize_pause = atoi(env);
    if (ctx->finalize_pause < 0) {
      ctx->finalize_pause = 0;
    }
  }
  if (pctx.my_rank == 0) {
    if (ctx->finalize_pause > 0) {
      logf(LOG_INFO, "shuffle finalize pause: %d secs", ctx->finalize_pause);
    }
  }
  if (is_envset("SHUFFLE_Force_rpc")) {
    ctx->force_rpc = 1;
  }
  if (pctx.my_rank == 0) {
    if (!ctx->force_rpc) {
      logf(LOG_WARN,
           "shuffle force_rpc is OFF (will skip shuffle if addr is "
           "local)\n>>> "
           "main thread may be blocked on writing");
    } else {
      logf(LOG_INFO,
           "shuffle force_rpc is ON\n>>> "
           "will always invoke shuffle even addr is local");
    }
  }
  if (is_envset("SHUFFLE_Use_multihop")) {
    ctx->type = SHUFFLE_XN;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "using the scalable multi-hop shuffler");
    }
  } else {
    ctx->type = SHUFFLE_NN;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO,
           "using the default NN shuffler: code might not scale well\n>>> "
           "switch to the multi-hop shuffler for better scalability");
    }
  }
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(malloc(sizeof(xn_ctx_t)));
    memset(rep, 0, sizeof(xn_ctx_t));
    xn_shuffle_init(rep, ctx->priority_cb);
    world_sz = xn_shuffle_world_size(rep);
    ctx->rep = rep;
  } else {
    if (ctx->priority_cb)
      ABORT("CFG error: NN shuffle does not support priority callback");
    nn_shuffler_init(ctx);
    world_sz = nn_shuffler_world_size();
  }

#ifdef PRELOAD_HAS_CH_PLACEMENT
  if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
    env = maybe_getenv("SHUFFLE_Virtual_factor");
    if (env == NULL) {
      vf = DEFAULT_VIRTUAL_FACTOR;
    } else {
      vf = atoi(env);
    }

    proto = maybe_getenv("SHUFFLE_Placement_protocol");
    if (proto == NULL) {
      proto = DEFAULT_PLACEMENT_PROTO;
    }

    ctx->chp = ch_placement_initialize(proto, world_sz, vf /* vir factor */,
                                       0 /* hash seed */);
    if (ctx->chp == NULL) {
      ABORT("ch_init");
    }
  }

  if (pctx.my_rank == 0) {
    if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
      logf(LOG_INFO,
           "ch-placement group size: %s (vir-factor: %s, proto: %s)\n>>> "
           "possible protocols are: "
           "static_modulo, hash_lookup3, xor, and ring",
           pretty_num(world_sz).c_str(), pretty_num(vf).c_str(), proto);
    } else {
      logf(LOG_INFO, "ch-placement bypassed");
    }
  }
#endif

  if (pctx.my_rank == 0 && pctx.verbose) {
    logf(LOG_INFO, "HG is configured as follows ...");
    fputs(" > HG_HAS_POST_LIMIT=", stderr);
#ifdef HG_HAS_POST_LIMIT
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);

    fputs(" > HG_HAS_SELF_FORWARD=", stderr);
#ifdef HG_HAS_SELF_FORWARD
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);

    fputs(" > HG_HAS_CHECKSUMS=", stderr);
#ifdef HG_HAS_CHECKSUMS
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);
  }
}

int shuffle_is_everyone_receiver(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  return ctx->receiver_rate == 1;
}

int shuffle_is_rank_receiver(shuffle_ctx_t* ctx, int rank) {
  assert(ctx != NULL);
  if (ctx->receiver_rate == 1) return 1;
  return (rank & ctx->receiver_mask) == rank;
}

int shuffle_world_sz(shuffle_ctx* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    return xn_shuffle_world_size(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    return nn_shuffler_world_size();
  }
}

int shuffle_rank(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    return xn_shuffle_my_rank(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    return nn_shuffler_my_rank();
  }
}

void shuffle_resume(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    // TODO
  } else {
    nn_shuffler_wakeup();
  }
}

void shuffle_pause(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    // TODO
  } else {
    nn_shuffler_sleep();
  }
}

void shuffle_msg_sent(size_t n, void** arg1, void** arg2) {
  pctx.mctx.min_nms++;
  pctx.mctx.max_nms++;
  pctx.mctx.nms++;
}

void shuffle_msg_replied(void* arg1, void* arg2) {
  pctx.mctx.nmd++; /* delivered */
}

void shuffle_msg_received() {
  pctx.mctx.min_nmr++;
  pctx.mctx.max_nmr++;
  pctx.mctx.nmr++;
}

void shuffle_dump_state(shuffle_ctx_t* ctx, int tostderr) {
  if (ctx->type == SHUFFLE_XN) {
    xn_shuffle_dump_state(static_cast<xn_ctx_t*>(ctx->rep), tostderr);
  } else {
    // NN shuffler does not have a statedump function
  }
}
