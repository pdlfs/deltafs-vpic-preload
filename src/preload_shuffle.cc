/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <arpa/inet.h>
#include <assert.h>
#include <ifaddrs.h>

#include "preload_internal.h"
#include "preload_mon.h"
#include "preload_shuffle.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffler.h"

#include <mercury_config.h>

#include "common.h"

char* shuffle_prepare_uri(char* buf) {
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
  char ip[50];  // ip
  int opt;
  int so;
  int rv;
  int n;

  subnet = maybe_getenv("SHUFFLE_Subnet");
  if (subnet == NULL) {
    subnet = DEFAULT_SUBNET;
  }

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "using subnet %s*", subnet);
    if (strcmp(subnet, "127.0.0.1") == 0) {
      WARN(msg);
    } else {
      INFO(msg);
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
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip,
                        sizeof(ip), NULL, 0, NI_NUMERICHOST) == -1)
          ABORT("getnameinfo");

        if (strncmp(subnet, ip, strlen(subnet)) == 0) {
          break;
        } else {
#ifndef NDEBUG
          if (pctx.verr || pctx.my_rank == 0) {
            snprintf(msg, sizeof(msg), "[ip] skip %s (rank %d)", ip,
                     pctx.my_rank);
            INFO(msg);
          }
#endif
        }
      }
    }
  }

  if (cur == NULL) /* maybe a wrong subnet has been specified */
    ABORT("no ip addr");

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
  if (max_port - min_port < 0) ABORT("bad min-max port");
  if (min_port < 1) ABORT("bad min port");
  if (max_port > 65535) ABORT("bad max port");

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "using port range [%d,%d]", min_port, max_port);
    INFO(msg);
  }

#if MPI_VERSION >= 3
  rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                           MPI_INFO_NULL, &comm);
  if (rv != MPI_SUCCESS) ABORT("MPI_Comm_split_type");
#else
  comm = MPI_COMM_WORLD;
#endif
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &size);

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
    WARN(
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

  /* add proto */

  env = maybe_getenv("SHUFFLE_Mercury_proto");
  if (env == NULL) env = DEFAULT_HG_PROTO;
  sprintf(buf, "%s://%s:%d", env, ip, port);
  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "using %s", env);
    if (strstr(env, "tcp") != NULL) {
      WARN(msg);
    } else {
      INFO(msg);
    }
  }

#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[hg] using %s (rank %d)", buf, pctx.my_rank);
    INFO(msg);
  }
#endif

  return buf;
}

void shuffle_epoch_start(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_XN) {
    assert(ctx != NULL);
    xn_shuffler_epoch_start(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    nn_shuffler_bgwait();
  }
}

void shuffle_epoch_end(shuffle_ctx_t* ctx) {
  if (ctx->type == SHUFFLE_XN) {
    assert(ctx != NULL);
    xn_shuffler_epoch_end(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    nn_shuffler_flushq(); /* flush rpc queues */
    if (!nnctx.force_sync) {
      /* wait for rpc replies */
      nn_shuffler_waitcb();
    }
  }
}

int shuffle_write(shuffle_ctx_t* ctx, const char* fn, char* d, size_t n,
                  int epoch) {
  if (ctx->type == SHUFFLE_XN) {
    assert(ctx != NULL);
    xn_shuffler_write(static_cast<xn_ctx_t*>(ctx->rep), fn, d, n, epoch);
    return 0;
  } else {
    return nn_shuffler_write(fn, d, n, epoch);
  }
}

void shuffle_finalize(shuffle_ctx_t* ctx) {
  char msg[200];
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    unsigned long long sum_rpcs[2];
    unsigned long long min_rpcs[2];
    unsigned long long max_rpcs[2];
    unsigned long long rpcs[2];
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffler_destroy(rep);
    rpcs[0] = rep->stat.local.sends;
    rpcs[1] = rep->stat.remote.sends;
    free(rep);
    MPI_Reduce(rpcs, sum_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, min_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, max_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0 && (sum_rpcs[0] + sum_rpcs[1]) != 0) {
      snprintf(msg, sizeof(msg),
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
               pretty_num(min_rpcs[1]).c_str(),
               pretty_num(max_rpcs[1]).c_str());
      INFO(msg);
    }
  } else {
    unsigned long long accqsz;
    unsigned long long nps;
    int min_maxqsz;
    int max_maxqsz;
    int min_minqsz;
    int max_minqsz;
    nn_shuffler_destroy();
    MPI_Reduce(&nnctx.accqsz, &accqsz, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.nps, &nps, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.maxqsz, &min_maxqsz, 1, MPI_INT, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.maxqsz, &max_maxqsz, 1, MPI_INT, MPI_MAX, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.minqsz, &min_minqsz, 1, MPI_INT, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(&nnctx.minqsz, &max_minqsz, 1, MPI_INT, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0 && nps != 0) {
      snprintf(msg, sizeof(msg),
               "[rpc] incoming queue depth: %.3f per rank\n"
               ">>> max: %d - %d, min: %d - %d",
               double(accqsz) / nps, min_maxqsz, max_maxqsz, min_minqsz,
               max_minqsz);
      INFO(msg);
    }
  }
}

void shuffle_init(shuffle_ctx_t* ctx) {
  char msg[200];
  int n;
  if (is_envset("SHUFFLE_Use_multihop")) {
    ctx->type = SHUFFLE_XN;
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg), "using the scalable multi-hop shuffler");
      INFO(msg);
    }
  } else {
    ctx->type = SHUFFLE_NN;
    if (pctx.my_rank == 0) {
      snprintf(msg, sizeof(msg),
               "using the default NN shuffler: code might not scale well\n>>> "
               "switch to the multi-hop shuffler for better scalability");
      WARN(msg);
    }
  }
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(malloc(sizeof(xn_ctx_t)));
    memset(rep, 0, sizeof(xn_ctx_t));
    xn_shuffler_init(rep);
    ctx->rep = rep;
  } else {
    nn_shuffler_init();
  }
  if (pctx.my_rank == 0) {
    n = 0;
    n += snprintf(msg + n, sizeof(msg) - n, "HG_HAS_POST_LIMIT is ");
#ifdef HG_HAS_POST_LIMIT
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, ", HG_HAS_SELF_FORWARD is ");
#ifdef HG_HAS_SELF_FORWARD
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, ", HG_HAS_EAGER_BULK is ");
#ifdef HG_HAS_EAGER_BULK
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    n += snprintf(msg + n, sizeof(msg) - n, "\n>>> HG_HAS_CHECKSUMS is ");
#ifdef HG_HAS_CHECKSUMS
    n += snprintf(msg + n, sizeof(msg) - n, "TRUE");
#else
    n += snprintf(msg + n, sizeof(msg) - n, "FALSE");
#endif
    INFO(msg);
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
