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
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include "common.h"
#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"

#include <vector>

/*
 * a set of mutex shared among the main thread and the bg shuffle threads.
 */
static pthread_mutex_t mtx[5] = {
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
    PTHREAD_MUTEX_INITIALIZER};

static pthread_cond_t cv[5] = {
    PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER, PTHREAD_COND_INITIALIZER,
    PTHREAD_COND_INITIALIZER};

/* used when waiting for all bg threads to terminate */
static const int bg_cv = 0;

/* used when waiting for an on-going rpc to finish */
static const int rpc_cv = 1;

/* used when waiting for the next available rpc callback slot */
static const int cb_cv = 2;

/* used when waiting for a busy rpc queue */
static const int qu_cv = 3;

/* used when waiting for work items */
static const int wk_cv = 4;

/* true iff in shutdown seq */
static int shutting_down = 0; /* XXX: better if this is atomic */

/* number of bg threads running */
static int num_bg = 0;

/* workers */
static std::vector<void*> wk_items;
static int num_wk = 0; /* number of worker threads running */
static size_t items_submitted = 0;
static size_t items_completed = 0;
#define MAX_WORK_ITEM 256

/* rpc queue */
typedef struct rpcq {
  uint32_t sz; /* aggregated size of all pending writes */
  int lepo;    /* epoch number for the last write */
  int busy;    /* non-zero when queue is locked and is being flushed */
  char* buf;   /* heap-allocated memory for the queue */
} rpcq_t;
static rpcq_t* rpcqs = NULL;
static size_t max_rpcq_sz = 0; /* buffer size per rpc queue */
static int nrpcqs = 0;         /* number of queues */

/* rpc callback slots */
#define MAX_OUTSTANDING_RPC 128 /* hard limit */
static hg_handle_t hg_hdls[MAX_OUTSTANDING_RPC] = {0};
static write_async_cb_t cb_slots[MAX_OUTSTANDING_RPC] = {0};
static int cb_flags[MAX_OUTSTANDING_RPC] = {0};
static int cb_allowed = 1; /* soft limit */
static int cb_left = 1;

/* per-thread rusage */
typedef struct rpcu {
  struct rusage r0;
  struct rusage r1;
  char tag[16];

#define RPCU_ALLTHREADS 0
#define RPCU_MAIN 1
#define RPCU_LOOPER 2
#define RPCU_HGPRO 3
#define RPCU_WORKER 4
} rpcu_t;
/* 0:ALL, 1:main, 2:looper, 3:hg_progress, 4:worker */
static rpcu_t rpcus[5] = {0};

static void rpcu_accumulate(nn_rusage_t* r, rpcu_t* u) {
  uint64_t u0, u1, s0, s1;
  memcpy(r->tag, u->tag, sizeof(u->tag));

  u0 = timeval_to_micros(&u->r0.ru_utime);
  u1 = timeval_to_micros(&u->r1.ru_utime);
  r->usr_micros += u1 - u0;

  s0 = timeval_to_micros(&u->r0.ru_stime);
  s1 = timeval_to_micros(&u->r1.ru_stime);
  r->sys_micros += s1 - s0;
}

static inline void rpcu_start(int who, rpcu_t* u) {
  if (getrusage(who, &u->r0) != 0) {
    abort();
  }
}

static inline void rpcu_end(int who, rpcu_t* u) {
  if (getrusage(who, &u->r1) != 0) {
    abort();
  }
}

#if defined(__x86_64__) && defined(__GNUC__)
static inline int is_shuttingdown() {
  int r = shutting_down;
  // See http://en.wikipedia.org/wiki/Memory_ordering.
  __asm__ __volatile__("" : : : "memory");

  return r;
}
#else
static inline int is_shuttingdown() {
  pthread_mtx_lock(&mtx[bg_cv]);
  int r = shutting_down;
  pthread_mtx_unlock(&mtx[bg_cv]);

  return r;
}
#endif

/*
 * main shuffle code
 */
static hg_return_t nn_progress_rusage(hg_context_t* context, int timeout) {
  hg_return_t hret;

#if defined(__linux)
  rpcu_start(RUSAGE_THREAD, &rpcus[RPCU_HGPRO]);
#endif
  hret = HG_Progress(context, timeout);
#if defined(__linux)
  rpcu_end(RUSAGE_THREAD, &rpcus[RPCU_HGPRO]);
#endif

  rpcu_accumulate(&nnctx.r[RPCU_HGPRO], &rpcus[RPCU_HGPRO]);

  return hret;
}

namespace {
/* rpc_explain_timeout: print rpc stats before we abort */
void rpc_explain_timeout() {
  LOG(LOG_SINK, 0,
      "!! [%s] (rank %d) shuffler %d (%s) is about to timeout: "
      "rpc out %d (%d replied), rpc in %d",
      nnctx.my_uname.nodename, pctx.my_rank, mssg_get_rank(nnctx.mssg),
      mssg_get_addr_str(nnctx.mssg, mssg_get_rank(nnctx.mssg)),
      int(pctx.mctx.nms), int(pctx.mctx.nmd), int(pctx.mctx.nmr));
}
}  // namespace

/* rpc_work(): dedicated thread function to process rpc. each work item
 * represents an incoming rpc (encoding a batch of writes). */
static void* rpc_work(void* arg) {
  write_info info;
  size_t total_writes; /* total individual writes processed */
  size_t total_bytes;  /* total rpc msg size */
  std::vector<void*> todo;
  std::vector<void*>::iterator it;
  size_t num_items;
  hg_return_t hret;
  hg_handle_t h;
  int s;

#ifndef NDEBUG
  char msg[100];
#endif

  total_writes = total_bytes = 0;
  hstg_reset_min(nnctx.iq_dep);
  num_items = 0;

  /*
   * mercury by default will only pull at most 256 incoming requests from the
   * underlying network transport so here we also reserve 256 slots. This 256
   * limit can be removed from mercury at the compile time.
   */
  todo.reserve(MAX_WORK_ITEM);
#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[bg] rpc worker up (rank %d)", pctx.my_rank);
    INFO(msg);
  }
#endif

#if defined(__linux)
  rpcu_start(RUSAGE_THREAD, &rpcus[RPCU_WORKER]);
#endif

  while (true) {
    todo.clear();
    pthread_mtx_lock(&mtx[wk_cv]);
    items_completed += num_items;
    if (items_completed == items_submitted) {
      pthread_cv_notifyall(&cv[wk_cv]);
    }
    pthread_mtx_unlock(&mtx[wk_cv]);
    num_items = 0;
    s = is_shuttingdown();
    if (s == 0) {
      pthread_mtx_lock(&mtx[wk_cv]);
      while (wk_items.empty() && is_shuttingdown() == 0) {
        pthread_cv_wait(&cv[wk_cv], &mtx[wk_cv]);
      }
      todo.swap(wk_items);
      pthread_mtx_unlock(&mtx[wk_cv]);
      if (!todo.empty()) {
        num_items = todo.size();
        hstg_add(nnctx.iq_dep, num_items);
        for (it = todo.begin(); it != todo.end(); ++it) {
          h = static_cast<hg_handle_t>(*it);
          if (h != NULL) {
            hret = nn_shuffler_write_rpc_handler(h, &info);
            if (hret != HG_SUCCESS) {
              RPC_FAILED("fail to exec rpc", hret);
            }
            total_writes += info.num_writes;
            total_bytes += info.sz;
          }
        }
      }
    } else if (s < 0) {
#ifndef NDEBUG
      if (pctx.verr || pctx.my_rank == 0) {
        snprintf(msg, sizeof(msg), "[bg] rpc worker will pause ... (rank %d)",
                 pctx.my_rank);
        INFO(msg);
      }
#endif
      pthread_mtx_lock(&mtx[bg_cv]);
      while (shutting_down < 0) {
        pthread_cv_wait(&cv[bg_cv], &mtx[bg_cv]);
      }
      pthread_mtx_unlock(&mtx[bg_cv]);
#ifndef NDEBUG
      if (pctx.verr || pctx.my_rank == 0) {
        snprintf(msg, sizeof(msg), "[bg] rpc worker resumed (rank %d)",
                 pctx.my_rank);
        INFO(msg);
      }
#endif
    } else {
      break;
    }
  }

#if defined(__linux)
  rpcu_end(RUSAGE_THREAD, &rpcus[RPCU_WORKER]);
#endif
  rpcu_accumulate(&nnctx.r[RPCU_WORKER], &rpcus[RPCU_WORKER]);

  pthread_mtx_lock(&mtx[bg_cv]);
  assert(num_wk > 0);
  num_wk--;
  pthread_cv_notifyall(&cv[bg_cv]);
  pthread_mtx_unlock(&mtx[bg_cv]);

  nnctx.total_writes = total_writes;
  nnctx.total_msgsz = total_bytes;
#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[bg] rpc worker down (rank %d)", pctx.my_rank);
    INFO(msg);
  }
#endif

  return NULL;
}

/* nn_shuffler_bgwait: wait for background rpc work execution */
void nn_shuffler_bgwait() {
  useconds_t delay;

#ifndef NDEBUG
  char msg[50];
  int n;
#endif

  delay = 1000; /* 1000 us */

  pthread_mtx_lock(&mtx[wk_cv]);
  while (items_completed < items_submitted) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[wk_cv]);
#ifndef NDEBUG
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[BGWAIT] %d us\n", int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }
#endif
      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[wk_cv]);
    } else {
      pthread_cv_wait(&cv[wk_cv], &mtx[wk_cv]);
    }
  }
  pthread_mtx_unlock(&mtx[wk_cv]);
}

/* nn_shuffler_write_rpc_handler_wrapper: server-side rpc handler wrapper */
hg_return_t nn_shuffler_write_rpc_handler_wrapper(hg_handle_t h) {
  if (num_wk == 0) {
    return nn_shuffler_write_rpc_handler(h, NULL);
  }
  pthread_mtx_lock(&mtx[wk_cv]);
  wk_items.push_back(static_cast<void*>(h));
  if (wk_items.size() == 1) {
    pthread_cv_notifyall(&cv[wk_cv]);
  }
  items_submitted++;
  pthread_mtx_unlock(&mtx[wk_cv]);

  return HG_SUCCESS;
}

namespace {
/* nn_shuffler_debug:
 *   print debug information for an incoming RPC write request.
 *   src and dst are shuffle ranks reported by the RPC message, me
 *   indicates the shuffle rank of my own, and hp is the shuffle
 *   rank calculated via hash placement. */
void nn_shuffler_debug(int src, int dst, int me, int hp) {
  LOG(LOG_SINK, 0,
      "!! [%s] (rank %d) shuffler %d (%s) just "
      "received a problematic write req\n"
      "!! [%s] (rank %d) the req swears it comes from %d (%s), "
      "and was heading to %d (%s)",
      nnctx.my_uname.nodename, pctx.my_rank, me,
      mssg_get_addr_str(nnctx.mssg, me), nnctx.my_uname.nodename, pctx.my_rank,
      src, mssg_get_addr_str(nnctx.mssg, src), dst,
      mssg_get_addr_str(nnctx.mssg, dst));
  if (hp != -1) {
    LOG(LOG_SINK, 0, "!! [%s] (rank %d) we think the req should goto %d (%s)",
        nnctx.my_uname.nodename, pctx.my_rank, hp,
        mssg_get_addr_str(nnctx.mssg, hp));
  }
}
}  // namespace

/* nn_shuffler_write_rpc_handler: server-side rpc handler */
hg_return_t nn_shuffler_write_rpc_handler(hg_handle_t h, write_info_t* info) {
  /* here we assume we will only get called by a single thread.
   * this thread is either a dedicated mercury progressing thread, or a separate
   * rpc worker thread. */
  static char buf[MAX_RPC_MESSAGE];

  char* input;
  uint32_t input_left;
  hg_return_t hret;
  write_out_t write_out;
  write_in_t write_in;
  write_info_t write_info;
  char* req;
  unsigned int req_sz;
  int epoch;
  int src;
  int dst;
  int target_rank;
  int rank;
  int rv;

#ifndef NDEBUG
  char msg[200];
  int n;
#endif

  assert(nnctx.mssg != NULL);
  rank = mssg_get_rank(nnctx.mssg);

  write_in.msg = buf;
  write_in.sz = 0;

  hret = HG_Get_input(h, &write_in);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Get_input", hret);
  }

  shuffle_msg_received();
  if (write_in.hash_sig != nn_shuffler_maybe_hashsig(&write_in)) {
    ABORT("rpc msg corrupted (hash_sig mismatch)");
  }

  dst = write_in.dst;
  src = write_in.src;
  if (dst != rank) {
    ABORT("rpc msg misrouted (bad dst)");
  }
#ifndef NDEBUG
  /* write trace if we are in testing mode */
  if (pctx.testin) {
    if (pctx.logfd != -1) {
      n = snprintf(msg, sizeof(msg), "[IN] %u bytes r%d << r%d\n", write_in.sz,
                   dst, src);
      n = write(pctx.logfd, msg, n);

      errno = 0;
    }
  }
#endif
  write_out.rv = 0;
  input_left = write_info.sz = write_in.sz;
  epoch = write_in.epo;
  write_info.num_writes = 0;
  input = buf;

  /* decode and execute writes */
  while (input_left != 0) {
    if (input_left < 1) {
      ABORT("premature end of msg");
    }
    req_sz = static_cast<unsigned char>(input[0]);
    input_left -= 1;
    input += 1;
    if (input_left < req_sz) {
      ABORT("premature end of msg");
    }
    req = input;
    input_left -= req_sz;
    input += req_sz;

    if (nnctx.paranoid_checks) {
      target_rank = shuffle_target(nnctx.shctx, req, req_sz);
      if (rank != target_rank) {
        nn_shuffler_debug(src, dst, rank, target_rank);
        ABORT("rpc msg misdirected");
      }
    }

    rv = shuffle_handle(nnctx.shctx, req, req_sz, epoch, src, dst);
    write_info.num_writes++;
    if (write_out.rv == 0) {
      write_out.rv = rv;
    }
    if (rv != 0) {
      break;
    }
  }

  hret = HG_Respond(h, NULL, NULL, &write_out);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Respond", hret);
  }
  if (info != NULL) {
    *info = write_info;
  }

  HG_Free_input(h, &write_in);
  HG_Destroy(h);

  return HG_SUCCESS;
}

/*
 * nn_shuffler_write_async_handler: rpc callback associated with
 * shuffle_write_send_async(...)
 */
hg_return_t nn_shuffler_write_async_handler(const struct hg_cb_info* info) {
  hg_return_t hret;
  hg_handle_t h;
  int cache;
  write_async_cb_t* write_cb;
  write_out_t write_out;
  int rv;

  assert(info->type == HG_CB_FORWARD);
  hret = info->ret;
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_CB_FORWARD", hret);
  }

  write_cb = static_cast<write_async_cb_t*>(info->arg);
  h = info->info.forward.handle;

  hret = HG_Get_output(h, &write_out);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Get_output", hret);
  } else {
    rv = write_out.rv;
  }

  HG_Free_output(h, &write_out);
  shuffle_msg_replied(write_cb->arg1, write_cb->arg2);

  /* return rpc callback slot */
  pthread_mtx_lock(&mtx[cb_cv]);
  cache = nnctx.cache_hlds && (h == hg_hdls[write_cb->slot]);
  cb_flags[write_cb->slot] = 0;
  assert(cb_left < cb_allowed);
  if (cb_left == 0 || cb_left == cb_allowed - 1) {
    pthread_cv_notifyall(&cv[cb_cv]);
  }
  cb_left++;
  pthread_mtx_unlock(&mtx[cb_cv]);
  if (!cache) {
    HG_Destroy(h);
  }

  if (rv != 0) {
    ABORT("plfsdir peer write failed");
  }

  return HG_SUCCESS;
}

/*
 * nn_shuffler_write_send_async: send a write request to a specified peer and
 * return without waiting.
 */
int nn_shuffler_write_send_async(write_in_t* write_in, int peer_rank,
                                 void* arg1, void* arg2) {
  hg_return_t hret;
  hg_addr_t peer_addr;
  hg_handle_t h;
  write_async_cb_t* write_cb;
  time_t now;
  struct timespec abstime;
  useconds_t delay;
  int slot;
  int rank;
  int e;

#ifndef NDEBUG
  char msg[200];
  int n;
#endif

  assert(nnctx.mssg != NULL);
  rank = mssg_get_rank(nnctx.mssg);
  assert(write_in != NULL);
  assert(write_in->dst == peer_rank);
  assert(write_in->src == rank);

#ifndef NDEBUG
  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    n = snprintf(msg, sizeof(msg), "[OUT] %u bytes r%d >> r%d\n", write_in->sz,
                 rank, peer_rank);

    n = write(pctx.logfd, msg, n);

    errno = 0;
  }
#endif
  delay = 1000; /* 1000 us */

  /* wait for slot */
  pthread_mtx_lock(&mtx[cb_cv]);
  while (cb_left == 0) { /* no slots available */
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[cb_cv]);
#ifndef NDEBUG
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[BLOCK-SLOT] %d us\n", int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }
#endif
      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[cb_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[cb_cv], &mtx[cb_cv], &abstime);
      if (e == ETIMEDOUT) {
        rpc_explain_timeout();
        ABORT("timeout waiting for rpc slot");
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

  pthread_mtx_unlock(&mtx[cb_cv]);

  /* go */
  peer_addr = mssg_get_addr(nnctx.mssg, peer_rank);
  if (peer_addr == HG_ADDR_NULL) {
    ABORT("mssg_get_addr");
  }
  assert(nnctx.hg_ctx != NULL);
  h = hg_hdls[slot];
  if (h == NULL) {
    hret = HG_Create(nnctx.hg_ctx, peer_addr, nnctx.hg_id, &h);
    if (hret != HG_SUCCESS) {
      RPC_FAILED("HG_Create", hret);
    } else if (nnctx.cache_hlds) {
      hg_hdls[slot] = h;
    }
  } else {
    hret = HG_Reset(h, peer_addr, nnctx.hg_id);
    if (hret != HG_SUCCESS) {
      RPC_FAILED("HG_Reset", hret);
    }
  }

  write_cb->slot = slot;
  write_cb->arg1 = arg1;
  write_cb->arg2 = arg2;

  hret = HG_Forward(h, nn_shuffler_write_async_handler, write_cb, write_in);

  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Forward", hret);
  }

  return 0;
}

/* nn_shuffler_waitcb: block until all outstanding rpc finishes */
void nn_shuffler_waitcb() {
  time_t now;
  struct timespec abstime;
  useconds_t delay;
  int e;

#ifndef NDEBUG
  char msg[50];
  int n;
#endif

  delay = 1000; /* 1000 us */

  pthread_mtx_lock(&mtx[cb_cv]);
  while (cb_left != cb_allowed) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[cb_cv]);
#ifndef NDEBUG
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[WAIT] %d us\n", int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }
#endif
      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[cb_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[cb_cv], &mtx[cb_cv], &abstime);
      if (e == ETIMEDOUT) {
        rpc_explain_timeout();
        ABORT("timeout waiting for all outstanding rpcs to complete");
      }
    }
  }

  pthread_mtx_unlock(&mtx[cb_cv]);
}

/*
 * nn_shuffler_write_handler: rpc callback associated with
 * shuffle_write_send(...)
 */
hg_return_t nn_shuffler_write_handler(const struct hg_cb_info* info) {
  write_cb_t* write_cb;
  write_cb = static_cast<write_cb_t*>(info->arg);
  assert(info->type == HG_CB_FORWARD);

  pthread_mtx_lock(&mtx[rpc_cv]);
  write_cb->hret = info->ret;
  write_cb->ok = 1;
  pthread_cv_notifyall(&cv[rpc_cv]);
  pthread_mtx_unlock(&mtx[rpc_cv]);

  return HG_SUCCESS;
}

/*
 * nn_shuffler_write_send: send a write request to a specified peer and wait for
 * its response
 */
int nn_shuffler_write_send(write_in_t* write_in, int peer_rank) {
  hg_return_t hret;
  hg_addr_t peer_addr;
  hg_handle_t h;
  write_out_t write_out;
  write_cb_t write_cb;
  time_t now;
  struct timespec abstime;
  useconds_t delay;
  int rv;
  int rank;
  int e;

#ifndef NDEBUG
  char msg[200];
  int n;
#endif

  assert(nnctx.mssg != NULL);
  rank = mssg_get_rank(nnctx.mssg);
  assert(write_in != NULL);
  assert(write_in->dst == peer_rank);
  assert(write_in->src == rank);

#ifndef NDEBUG
  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    n = snprintf(msg, sizeof(msg), "[OUT] %u bytes r%d >> r%d\n", write_in->sz,
                 rank, peer_rank);

    n = write(pctx.logfd, msg, n);

    errno = 0;
  }
#endif
  peer_addr = mssg_get_addr(nnctx.mssg, peer_rank);
  if (peer_addr == HG_ADDR_NULL) {
    ABORT("mssg_get_addr");
  }
  assert(nnctx.hg_ctx != NULL);
  hret = HG_Create(nnctx.hg_ctx, peer_addr, nnctx.hg_id, &h);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Create", hret);
  }

  write_cb.ok = 0;

  hret = HG_Forward(h, nn_shuffler_write_handler, &write_cb, write_in);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Forward", hret);
  }

  delay = 1000; /* 1000 us */

  /* here we block until rpc completes */
  pthread_mtx_lock(&mtx[rpc_cv]);
  while (write_cb.ok == 0) { /* rpc not completed */
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[rpc_cv]);
#ifndef NDEBUG
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[WAIT] r%d >> r%d %d us\n", rank,
                     peer_rank, int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }
#endif
      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[rpc_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[rpc_cv], &mtx[rpc_cv], &abstime);
      if (e == ETIMEDOUT) {
        rpc_explain_timeout();
        ABORT("timeout waiting for rpc reply");
      }
    }
  }
  pthread_mtx_unlock(&mtx[rpc_cv]);

  hret = write_cb.hret;
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_CB_FORWARD", hret);
  }

  hret = HG_Get_output(h, &write_out);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Get_output", hret);
  } else {
    rv = write_out.rv;
  }

  HG_Free_output(h, &write_out);
  HG_Destroy(h);

  return rv;
}

/* nn_shuffler_enqueue:
 *   encode a req and append it into a corresponding rpc queue */
void nn_shuffler_enqueue(char* req, unsigned char req_sz, int epoch,
                         int peer_rank, int rank) {
  write_in_t write_in;
  rpcq_t* rpcq;
  int rpcq_idx;
  time_t now;
  struct timespec abstime;
  useconds_t delay;
  int world_sz;
  int rv;
  void* arg1;
  void* arg2;
  int e;

#ifndef NDEBUG
  char msg[200];
  int n;
#endif

  assert(nnctx.mssg != NULL);
  assert(rank == mssg_get_rank(nnctx.mssg));
  world_sz = mssg_get_count(nnctx.mssg);

  if (nnctx.paranoid_checks) {
    if (!shuffle_is_rank_receiver(nnctx.shctx, peer_rank)) {
      ABORT("peer rank is not a receiver");
    }
    if (peer_rank < 0 || peer_rank >= world_sz) {
      ABORT("invalid peer rank");
    }
  }

  pthread_mtx_lock(&mtx[qu_cv]);

  rpcq_idx = peer_rank; /* we have one queue per rank */
  assert(rpcq_idx < nrpcqs);
  rpcq = &rpcqs[rpcq_idx];
  assert(rpcq != NULL);
  assert(rpcq->buf != NULL);

  delay = 1000; /* 1000 us */

  /* wait for queue */
  while (rpcq->busy != 0) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[qu_cv]);
#ifndef NDEBUG
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[BLOCK-QUEUE] %d us\n", int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }
#endif
      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[qu_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[qu_cv], &mtx[qu_cv], &abstime);
      if (e == ETIMEDOUT) {
        rpc_explain_timeout();
        ABORT("timeout waiting for rpc queue to flush");
      }
    }
  }

  /* flush queue if full */
  if (rpcq->sz + req_sz + 1 > max_rpcq_sz) {
    if (rpcq->sz > MAX_RPC_MESSAGE) {
      /* happens when the total size of queued data is greater than
       * the size limit for an rpc message */
      ABORT("rpc overflow");
    } else {
      rpcq->busy = 1; /* force other writers to block */
      /* unlock when sending the rpc */
      pthread_mtx_unlock(&mtx[qu_cv]);
      write_in.dst = peer_rank;
      write_in.src = rank;
      write_in.epo = rpcq->lepo;
      write_in.sz = rpcq->sz;
      write_in.msg = rpcq->buf;
      write_in.hash_sig = nn_shuffler_maybe_hashsig(&write_in);
      if (!nnctx.force_sync) {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send_async(&write_in, peer_rank, arg1, arg2);
      } else {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send(&write_in, peer_rank);
        shuffle_msg_replied(arg1, arg2);
      }
      if (rv != 0) {
        ABORT("plfsdir peer write failed");
      }
      pthread_mtx_lock(&mtx[qu_cv]);
      pthread_cv_notifyall(&cv[qu_cv]);
      rpcq->busy = 0;
      rpcq->sz = 0;
    }
  }

  /* enqueue */
  if (rpcq->sz + req_sz + 1 > max_rpcq_sz) {
    /* happens when the memory reserved for the queue is smaller than
     * a single write */
    ABORT("rpc overflow");
  } else {
    rpcq->lepo = epoch;
    rpcq->buf[rpcq->sz] = req_sz;
    memcpy(rpcq->buf + rpcq->sz + 1, req, req_sz);
    rpcq->sz += req_sz + 1;
  }

  pthread_mtx_unlock(&mtx[qu_cv]);
}

/* nn_shuffler_flushq: force flushing all rpc queue */
void nn_shuffler_flushq() {
  write_in_t write_in;
  rpcq_t* rpcq;
  void* arg1;
  void* arg2;
  int peer_rank;
  int rank;
  int rv;

  assert(nnctx.mssg != NULL);
  rank = mssg_get_rank(nnctx.mssg);

  pthread_mtx_lock(&mtx[qu_cv]);

  for (peer_rank = 0; peer_rank < nrpcqs; peer_rank++) {
    rpcq = &rpcqs[peer_rank];
    if (rpcq->sz == 0) { /* skip empty queue */
      continue;
    } else if (rpcq->sz > MAX_RPC_MESSAGE) {
      ABORT("rpc overflow");
    } else {
      rpcq->busy = 1; /* force other writers to block */
      /* unlock when sending the rpc */
      pthread_mtx_unlock(&mtx[qu_cv]);
      write_in.dst = peer_rank;
      write_in.src = rank;
      write_in.epo = rpcq->lepo;
      write_in.sz = rpcq->sz;
      write_in.msg = rpcq->buf;
      write_in.hash_sig = nn_shuffler_maybe_hashsig(&write_in);
      if (!nnctx.force_sync) {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send_async(&write_in, peer_rank, arg1, arg2);
      } else {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send(&write_in, peer_rank);
        shuffle_msg_replied(arg1, arg2);
      }
      if (rv != 0) {
        ABORT("plfsdir peer write failed");
      }
      pthread_mtx_lock(&mtx[qu_cv]);
      pthread_cv_notifyall(&cv[qu_cv]);
      rpcq->busy = 0;
      rpcq->sz = 0;
    }
  }

  pthread_mtx_unlock(&mtx[qu_cv]);
}

/* bg_work(): dedicated thread function to drive mercury progress */
static void* bg_work(void* foo) {
  hg_return_t hret;
  unsigned int actual_count;
  uint64_t intvl;
  uint64_t last_progress;
  uint64_t now;
  int n;
  int s;

#ifndef NDEBUG
  char msg[100];
#endif

#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[bg] rpc looper up (rank %d)", pctx.my_rank);
    INFO(msg);
  }
#endif

  /* set nice if requested */
  if (nnctx.hg_nice > 0) {
    n = nice(nnctx.hg_nice);
  }

  /* the last time we do mercury progress */
  last_progress = 0;
  hstg_reset_min(nnctx.hg_intvl);
#if defined(__linux)
  rpcu_start(RUSAGE_THREAD, &rpcus[RPCU_LOOPER]);
#endif

  /* num hg progress errors */
  n = 0;

  while (true) {
    do {
      hret = HG_Trigger(nnctx.hg_ctx, 0, 1, &actual_count);
    } while (hret == HG_SUCCESS && actual_count != 0);
    if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
      RPC_FAILED("HG_Trigger", hret);
    }
    s = is_shuttingdown();
    if (s == 0) {
      now = now_micros_coarse() / 1000; /* ms */
      if (last_progress != 0) {
        intvl = now - last_progress;
        hstg_add(nnctx.hg_intvl, intvl);
        if (intvl > nnctx.hg_max_interval) {
          LOG(LOG_SINK, 0,
              "!! [%s] (rank %d) calling HG_Progress() with high interval: %d "
              "ms",
              nnctx.my_uname.nodename, pctx.my_rank, int(intvl));
        }
      }
      last_progress = now;
      if (nnctx.hg_rusage) {
        hret = nn_progress_rusage(nnctx.hg_ctx, nnctx.hg_timeout);
      } else {
        hret = HG_Progress(nnctx.hg_ctx, nnctx.hg_timeout);
      }
      if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
        n++;
        if (n >= nnctx.hg_errors) {
          RPC_FAILED("HG_Progress", hret);
        }
      }
    } else if (s < 0) {
#ifndef NDEBUG
      if (pctx.verr || pctx.my_rank == 0) {
        snprintf(msg, sizeof(msg), "[bg] rpc looper will pause ... (rank %d)",
                 pctx.my_rank);
        INFO(msg);
      }
#endif
      pthread_mtx_lock(&mtx[bg_cv]);
      while (shutting_down < 0) {
        pthread_cv_wait(&cv[bg_cv], &mtx[bg_cv]);
      }
      pthread_mtx_unlock(&mtx[bg_cv]);
#ifndef NDEBUG
      if (pctx.verr || pctx.my_rank == 0) {
        snprintf(msg, sizeof(msg), "[bg] rpc looper resumed (rank %d)",
                 pctx.my_rank);
        INFO(msg);
      }
#endif
      last_progress = 0;
    } else {
      break;
    }
  }

#if defined(__linux)
  rpcu_end(RUSAGE_THREAD, &rpcus[RPCU_LOOPER]);
#endif
  rpcu_accumulate(&nnctx.r[RPCU_LOOPER], &rpcus[RPCU_LOOPER]);

  pthread_mtx_lock(&mtx[bg_cv]);
  assert(num_bg > 0);
  num_bg--;
  pthread_cv_notifyall(&cv[bg_cv]);
  pthread_mtx_unlock(&mtx[bg_cv]);

#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[bg] rpc looper down (rank %d)", pctx.my_rank);
    INFO(msg);
  }
#endif

  return NULL;
}

/* nn_shuffler_sleep: force the background rpc looper to stop running */
void nn_shuffler_sleep() {
  if (is_shuttingdown() != 0) return;
  pthread_mtx_lock(&mtx[bg_cv]);
  shutting_down = -1;
  pthread_mtx_unlock(&mtx[bg_cv]);
}

/* nn_shuffler_wakeup: signal a sleeping looper to resume work */
void nn_shuffler_wakeup() {
  if (is_shuttingdown() >= 0) return;
  pthread_mtx_lock(&mtx[bg_cv]);
  shutting_down = 0;
  pthread_cv_notifyall(&cv[bg_cv]);
  pthread_mtx_unlock(&mtx[bg_cv]);
}

/* nn_shuffler_init_mssg: init the mssg sublayer */
static void nn_shuffler_init_mssg(int is_recv) {
  hg_return_t hret;
  int rank; /* mssg */
  int size; /* mssg */

  assert(nnctx.hg_clz != NULL);
  assert(nnctx.hg_ctx != NULL);

  nnctx.mssg = mssg_init_mpi(nnctx.hg_clz, MPI_COMM_WORLD, is_recv);
  if (nnctx.mssg == NULL) ABORT("!mssg_init_mpi");

  hret = mssg_lookup(nnctx.mssg, nnctx.hg_ctx);
  if (hret != HG_SUCCESS) ABORT("!mssg_lookup");

  size = mssg_get_count(nnctx.mssg);
  rank = mssg_get_rank(nnctx.mssg);

  if (nnctx.paranoid_checks) {
    if (size != pctx.comm_sz || rank != pctx.my_rank) {
      ABORT("mssg-mpi disagree");
    }
  }
}

/* nn_shuffler_init: init the shuffle layer */
void nn_shuffler_init(shuffle_ctx_t* ctx) {
  hg_return_t hret;
  hg_size_t isz;
  hg_size_t osz;
  pthread_t pid;
  char msg[200];
  const char* env;
  int nbufs;
  int rv;
  int i;

  nnctx.shctx = ctx;
  shuffle_prepare_uri(nnctx.my_addr);
  rv = uname(&nnctx.my_uname);
  if (rv) ABORT("uname");

  env = maybe_getenv("SHUFFLE_Timeout");
  if (env == NULL) {
    nnctx.timeout = DEFAULT_TIMEOUT;
  } else {
    nnctx.timeout = atoi(env);
    if (nnctx.timeout < 5) {
      nnctx.timeout = 5;
    }
  }

  env = maybe_getenv("SHUFFLE_Mercury_nice");
  if (env != NULL) {
    nnctx.hg_nice = atoi(env);
    if (nnctx.hg_nice < 0) {
      nnctx.hg_nice = 0;
    }
  }

  env = maybe_getenv("SHUFFLE_Mercury_progress_timeout");
  if (env == NULL) {
    nnctx.hg_timeout = DEFAULT_HG_TIMEOUT;
  } else {
    nnctx.hg_timeout = atoi(env);
    if (nnctx.hg_timeout < 100) {
      nnctx.hg_timeout = 100;
    }
  }

  env = maybe_getenv("SHUFFLE_Mercury_progress_warn_interval");
  if (env == NULL) {
    nnctx.hg_max_interval = DEFAULT_HG_INTERVAL;
  } else {
    nnctx.hg_max_interval = atoi(env);
    if (nnctx.hg_max_interval < 100) {
      nnctx.hg_max_interval = 100;
    }
  }

  env = maybe_getenv("SHUFFLE_Mercury_max_errors");
  if (env == NULL) {
    nnctx.hg_errors = 1;
  } else {
    nnctx.hg_errors = atoi(env);
    if (nnctx.hg_errors < 1) {
      nnctx.hg_errors = 1;
    }
  }

  env = maybe_getenv("SHUFFLE_Num_outstanding_rpc");
  if (env == NULL) {
    cb_allowed = DEFAULT_OUTSTANDING_RPC;
  } else {
    cb_allowed = atoi(env);
    if (cb_allowed > MAX_OUTSTANDING_RPC) {
      cb_allowed = MAX_OUTSTANDING_RPC;
    } else if (cb_allowed <= 0) {
      cb_allowed = 1;
    }
  }

  cb_left = cb_allowed;

  if (is_envset("SHUFFLE_Hash_sig")) nnctx.hash_sig = 1;
  if (is_envset("SHUFFLE_Force_sync_rpc")) nnctx.force_sync = 1;
  if (is_envset("SHUFFLE_Paranoid_checks")) nnctx.paranoid_checks = 1;
  if (is_envset("SHUFFLE_Mercury_cache_handles")) nnctx.cache_hlds = 1;
  if (is_envset("SHUFFLE_Mercury_rusage")) nnctx.hg_rusage = 1;

  nnctx.hg_clz = HG_Init(nnctx.my_addr, HG_TRUE);
  if (!nnctx.hg_clz) ABORT("HG_Init");

  nnctx.hg_id = HG_Register_name(
      nnctx.hg_clz, "shuffle_rpc_write", nn_shuffler_write_in_proc,
      nn_shuffler_write_out_proc, nn_shuffler_write_rpc_handler_wrapper);

  hret = HG_Register_data(nnctx.hg_clz, nnctx.hg_id, &nnctx, NULL);
  if (hret != HG_SUCCESS) ABORT("HG_Register_data");

  nnctx.hg_ctx = HG_Context_create(nnctx.hg_clz);
  if (!nnctx.hg_ctx) ABORT("HG_Context_create");

  nn_shuffler_init_mssg(ctx->is_receiver);

  /* rpc queue */
  assert(nnctx.mssg != NULL);
  nrpcqs = mssg_get_count(nnctx.mssg);
  env = maybe_getenv("SHUFFLE_Buffer_per_queue");
  if (env == NULL) {
    max_rpcq_sz = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    max_rpcq_sz = atoi(env);
    if (max_rpcq_sz > MAX_RPC_MESSAGE) {
      WARN("RPC BUFFER SIZE IS TOO LARGE AND HAS BEEN REDUCED");
      max_rpcq_sz = MAX_RPC_MESSAGE;
    } else if (max_rpcq_sz < 128) {
      WARN("RPC BUFFER SIZE IS TOO SMALL");
      max_rpcq_sz = 128;
    }
  }

  nbufs = 0; /* number sender buffers we actually allocated */

  rpcqs = static_cast<rpcq_t*>(malloc(nrpcqs * sizeof(rpcq_t)));
  for (i = 0; i < nrpcqs; i++) {
    if (shuffle_is_rank_receiver(ctx, i)) {
      rpcqs[i].buf = static_cast<char*>(malloc(max_rpcq_sz));
      nbufs++;
    } else {
      rpcqs[i].buf = NULL;
    }
    rpcqs[i].busy = 0;
    rpcqs[i].lepo = 0;
    rpcqs[i].sz = 0;
  }
  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "rpc buffer: %s x %s (%s total)",
             pretty_num(nbufs).c_str(), pretty_size(max_rpcq_sz).c_str(),
             pretty_size(nbufs * max_rpcq_sz).c_str());
    INFO(msg);
  }

  for (i = 0; i < 5; i++) {
    rv = pthread_mutex_init(&mtx[i], NULL);
    if (rv) ABORT("pthread_mutex_init");
    rv = pthread_cond_init(&cv[i], NULL);
    if (rv) ABORT("pthread_cond_init");
  }

  shutting_down = 0;
  num_bg++;
  rv = pthread_create(&pid, NULL, bg_work, NULL);
  if (rv) ABORT("pthread_create");
  pthread_detach(pid);

  if (is_envset("SHUFFLE_Use_worker_thread")) {
    wk_items.reserve(MAX_WORK_ITEM);
    num_wk++;
    rv = pthread_create(&pid, NULL, rpc_work, NULL);
    if (rv) ABORT("pthread_create");
    pthread_detach(pid);
  } else if (pctx.my_rank == 0) {
    WARN("rpc worker disabled\n>>> some rpc stats collection not available");
  }

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg),
             "HG_Progress() timeout: %d ms, warn interval: %d ms, "
             "fatal rpc timeout: %d s, max error: %d\n>>> "
             "cache hg_handle_t: %s, hash signature: %s\n>>> "
             "bg nice: %d",
             nnctx.hg_timeout, nnctx.hg_max_interval, nnctx.timeout,
             nnctx.hg_errors, nnctx.cache_hlds ? "YES" : "NO",
             nnctx.hash_sig ? "YES" : "NO", nnctx.hg_nice);
    INFO(msg);
    if (nnctx.paranoid_checks) {
      WARN(
          "shuffle paranoid checks enabled: benchmarks unnecessarily slow\n>>> "
          "rerun with \"export SHUFFLE_Paranoid_checks=0\" to disable");
    }
    if (!nnctx.force_sync) {
      isz = HG_Class_get_input_eager_size(nnctx.hg_clz);
      osz = HG_Class_get_output_eager_size(nnctx.hg_clz);
      snprintf(msg, sizeof(msg),
               "HG_input_eager_size: %s, HG_output_eager_size: %s\n>>> "
               "num outstanding rpcs: %d",
               pretty_size(isz).c_str(), /* server-side rpc input buf */
               pretty_size(osz).c_str(), /* rpc output buf */
               cb_left);
      INFO(msg);
    } else {
      WARN("async rpc disabled");
    }
  }

  strcpy(rpcus[RPCU_ALLTHREADS].tag, "<ALL>");
  strcpy(rpcus[RPCU_MAIN].tag, "main");
  strcpy(rpcus[RPCU_LOOPER].tag, "bglooper");
  strcpy(rpcus[RPCU_HGPRO].tag, "-hgpro");
  strcpy(rpcus[RPCU_WORKER].tag, "deliv");

  rpcu_start(RUSAGE_SELF, &rpcus[RPCU_ALLTHREADS]);
#if defined(__linux)
  rpcu_start(RUSAGE_THREAD, &rpcus[RPCU_MAIN]);
#endif
}

/* nn_shuffler_world_size: return comm world size */
int nn_shuffler_world_size() {
  assert(nnctx.mssg != NULL);
  int rv = mssg_get_count(nnctx.mssg);
  assert(rv > 0);
  return rv;
}

/* nn_shuffler_my_rank: return my rank */
int nn_shuffler_my_rank() {
  assert(nnctx.mssg != NULL);
  int rv = mssg_get_rank(nnctx.mssg);
  assert(rv >= 0);
  return rv;
}

/* nn_shuffler_destroy: finalize the shuffle layer */
void nn_shuffler_destroy() {
  int i;
  pthread_mtx_lock(&mtx[bg_cv]);
  pthread_mtx_lock(&mtx[wk_cv]);
  shutting_down = 1;
  pthread_cv_notifyall(&cv[wk_cv]);
  pthread_mtx_unlock(&mtx[wk_cv]);
  pthread_cv_notifyall(&cv[bg_cv]);
  while (num_bg + num_wk != 0) {
    pthread_cv_wait(&cv[bg_cv], &mtx[bg_cv]);
  }
  pthread_mtx_unlock(&mtx[bg_cv]);

#if defined(__linux)
  rpcu_end(RUSAGE_THREAD, &rpcus[RPCU_MAIN]);
#endif
  rpcu_end(RUSAGE_SELF, &rpcus[RPCU_ALLTHREADS]);

  rpcu_accumulate(&nnctx.r[RPCU_ALLTHREADS], &rpcus[RPCU_ALLTHREADS]);
  rpcu_accumulate(&nnctx.r[RPCU_MAIN], &rpcus[RPCU_MAIN]);

  if (rpcqs != NULL) {
    for (i = 0; i < nrpcqs; i++) {
      assert(rpcqs[i].busy == 0);
      assert(rpcqs[i].sz == 0);
      /* not all buffers are allocated */
      if (rpcqs[i].buf) {
        free(rpcqs[i].buf);
      }
    }

    free(rpcqs);
  }

  if (nnctx.mssg != NULL) {
    mssg_finalize(nnctx.mssg);
  }

  for (size_t i = 0; i < sizeof(hg_hdls) / sizeof(hg_handle_t); i++) {
    if (hg_hdls[i] != NULL) {
      HG_Destroy(hg_hdls[i]);
    }
  }

  if (nnctx.hg_ctx != NULL) {
    HG_Context_destroy(nnctx.hg_ctx);
  }

  if (nnctx.hg_clz != NULL) {
    HG_Finalize(nnctx.hg_clz);
  }
}
