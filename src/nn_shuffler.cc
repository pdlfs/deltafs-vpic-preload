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

#include <mercury_proc.h>
#include <mercury_proc_string.h>

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
  uint16_t sz; /* current queue size */
  int busy;    /* non-0 if queue is locked and is being flushed */
  char* buf;   /* dedicated memory for the queue */
} rpcq_t;
static rpcq_t* rpcqs = NULL;
static size_t max_rpcq_sz = 0; /* bytes allocated for each queue */
static int nrpcqs = 0;         /* number of queues */

/* rpc callback slots */
#define MAX_OUTSTANDING_RPC 128 /* hard limit */
static hg_handle_t hg_hdls[MAX_OUTSTANDING_RPC] = {0};
static write_async_cb_t cb_slots[MAX_OUTSTANDING_RPC] = {0};
static int cb_flags[MAX_OUTSTANDING_RPC] = {0};
static int cb_allowed = 1; /* soft limit */
static int cb_left = 1;

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

static hg_return_t nn_shuffler_write_in_proc(hg_proc_t proc, void* data) {
  hg_return_t hret;

  write_in_t* in = static_cast<write_in_t*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  if (op == HG_ENCODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->owner);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else if (op == HG_DECODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->owner);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else {
    hret = HG_SUCCESS; /* noop */
  }

  return hret;
}

static hg_return_t nn_shuffler_write_out_proc(hg_proc_t proc, void* data) {
  hg_return_t hret;

  write_out_t* out = reinterpret_cast<write_out_t*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  if (op == HG_ENCODE) {
    hret = hg_proc_hg_int32_t(proc, &out->rv);
  } else if (op == HG_DECODE) {
    hret = hg_proc_hg_int32_t(proc, &out->rv);
  } else {
    hret = HG_SUCCESS; /* noop */
  }

  return hret;
}

/* rpc_work(): dedicated thread function to process rpc */
static void* rpc_work(void* arg) {
  size_t num_loops;
  size_t num_items;
  std::vector<void*> todo;
  std::vector<void*>::size_type sum_items;
  std::vector<void*>::size_type max_items;
  std::vector<void*>::size_type min_items;
  std::vector<void*>::iterator it;
  hg_return_t hret;
  hg_handle_t h;
  char msg[100];
  int s;

  num_items = 0;
  num_loops = 0;

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
  min_items = INT32_MAX;
  max_items = 0;
  sum_items = 0;

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
        num_loops++;

        min_items = std::min(todo.size(), min_items);
        max_items = std::max(todo.size(), max_items);
        sum_items += num_items;

        for (it = todo.begin(); it != todo.end(); ++it) {
          h = static_cast<hg_handle_t>(*it);
          if (h != NULL) {
            hret = nn_shuffler_write_rpc_handler(h);
            if (hret != HG_SUCCESS) {
              RPC_FAILED("fail to exec rpc", hret);
            }
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

  pthread_mtx_lock(&mtx[bg_cv]);
  assert(num_wk > 0);
  num_wk--;
  pthread_cv_notifyall(&cv[bg_cv]);
  pthread_mtx_unlock(&mtx[bg_cv]);

  nnctx.accqsz = sum_items;
  nnctx.minqsz = int(min_items);
  nnctx.maxqsz = int(max_items);
  nnctx.nps = num_loops;
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
  char buf[50];
  int n;

  delay = 1000; /* 1000 us */

  pthread_mtx_lock(&mtx[wk_cv]);
  while (items_completed < items_submitted) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[wk_cv]);
      if (pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[BGWAIT] %d us\n", int(delay));
        n = write(pctx.logfd, buf, n);

        errno = 0;
      }

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
  if (num_wk == 0) return (nn_shuffler_write_rpc_handler(h));

  pthread_mtx_lock(&mtx[wk_cv]);
  wk_items.push_back(static_cast<void*>(h));
  if (wk_items.size() == 1) {
    pthread_cv_notifyall(&cv[wk_cv]);
  }
  items_submitted++;
  pthread_mtx_unlock(&mtx[wk_cv]);

  return HG_SUCCESS;
}

/* nn_shuffler_write_rpc_handler: server-side rpc handler */
hg_return_t nn_shuffler_write_rpc_handler(hg_handle_t h) {
  char* input;
  uint16_t input_left;
  uint32_t nrank;
  uint16_t nepoch;
  hg_return_t hret;
  char buf[MAX_RPC_MESSAGE];
  write_out_t write_out;
  write_in_t write_in;
  char* data;
  size_t len;
  const char* fname;
  unsigned char fname_len;
  char msg[200];
  int rv;
  int epoch;
  int src;
  int dst;
  int peer_rank;
  int rank;
  int n;

  assert(nnctx.ssg != NULL);
  rank = ssg_get_rank(nnctx.ssg); /* my rank */

  write_in.msg = buf;
  write_in.sz = 0;

  hret = HG_Get_input(h, &write_in);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Get_input", hret);
  }

  shuffle_msg_received();
  peer_rank = ntohl(write_in.owner);
  /* write trace if we are in testing mode */
  if (pctx.testin) {
    if (pctx.logfd != -1) {
      n = snprintf(msg, sizeof(msg), "[IN] %d bytes r%d << r%d\n",
                   int(write_in.sz), rank, peer_rank);
      n = write(pctx.logfd, msg, n);

      errno = 0;
    }
  }

  write_out.rv = 0;

  input_left = write_in.sz;
  input = buf;

  /* decode and execute writes */
  while (input_left != 0) {
    /* rank */
    if (input_left < 8) {
      ABORT("rpc msg corrupted");
    }
    memcpy(&nrank, input, 4);
    src = ntohl(nrank);
    input_left -= 4;
    input += 4;
    memcpy(&nrank, input, 4);
    dst = ntohl(nrank);
    input_left -= 4;
    input += 4;

    /* fname */
    if (input_left < 1) {
      ABORT("rpc msg corrupted");
    }
    fname_len = static_cast<unsigned char>(input[0]);
    input_left -= 1;
    input += 1;
    if (input_left < fname_len + 1) {
      ABORT("rpc msg corrupted");
    }
    fname = input;
    assert(strlen(fname) == fname_len);
    input_left -= fname_len + 1;
    input += fname_len + 1;

    /* data */
    if (input_left < 1) {
      ABORT("rpc msg corrupted");
    }
    len = static_cast<unsigned char>(input[0]);
    input_left -= 1;
    input += 1;
    if (input_left < len) {
      ABORT("rpc msg corrupted");
    }
    data = input;
    input_left -= len;
    input += len;

    /* epoch */
    if (input_left < 2) {
      ABORT("rpc msg corrupted");
    }
    memcpy(&nepoch, input, 2);
    epoch = ntohs(nepoch);
    input_left -= 2;
    input += 2;

    if (dst != rank) ABORT("bad dst");
    if (src != peer_rank) ABORT("bad src");
    rv = shuffle_handle(fname, fname_len, data, len, epoch, peer_rank, rank);
    if (write_out.rv == 0) {
      write_out.rv = rv;
    }
  }

  hret = HG_Respond(h, NULL, NULL, &write_out);
  if (hret != HG_SUCCESS) {
    RPC_FAILED("HG_Respond", hret);
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
  char buf[200];
  int slot;
  int rank;
  int e;
  int n;

  assert(nnctx.ssg != NULL);
  rank = ssg_get_rank(nnctx.ssg);
  assert(write_in != NULL);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    n = snprintf(buf, sizeof(buf), "[OUT] %d bytes r%d >> r%d\n",
                 int(write_in->sz), rank, peer_rank);

    n = write(pctx.logfd, buf, n);

    errno = 0;
  }

  delay = 1000; /* 1000 us */

  /* wait for slot */
  pthread_mtx_lock(&mtx[cb_cv]);
  while (cb_left == 0) { /* no slots available */
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[cb_cv]);
      if (pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[BLOCK-SLOT] %d us\n", int(delay));
        n = write(pctx.logfd, buf, n);

        errno = 0;
      }

      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[cb_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[cb_cv], &mtx[cb_cv], &abstime);
      if (e == ETIMEDOUT) {
        ABORT("rpc timeout");
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
  peer_addr = ssg_get_addr(nnctx.ssg, peer_rank);
  if (peer_addr == HG_ADDR_NULL) {
    ABORT("ssg_get_addr");
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
  char buf[50];
  int e;
  int n;

  delay = 1000; /* 1000 us */

  pthread_mtx_lock(&mtx[cb_cv]);
  while (cb_left != cb_allowed) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[cb_cv]);
      if (pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[WAIT] %d us\n", int(delay));
        n = write(pctx.logfd, buf, n);

        errno = 0;
      }

      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[cb_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[cb_cv], &mtx[cb_cv], &abstime);
      if (e == ETIMEDOUT) {
        ABORT("rpc timeout");
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
  char buf[200];
  int rv;
  int rank;
  int e;
  int n;

  assert(nnctx.ssg != NULL);
  rank = ssg_get_rank(nnctx.ssg);
  assert(write_in != NULL);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.logfd != -1) {
    n = snprintf(buf, sizeof(buf), "[OUT] %d bytes r%d >> r%d\n",
                 int(write_in->sz), rank, peer_rank);

    n = write(pctx.logfd, buf, n);

    errno = 0;
  }

  peer_addr = ssg_get_addr(nnctx.ssg, peer_rank);
  if (peer_addr == HG_ADDR_NULL) {
    ABORT("ssg_get_addr");
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
      if (pctx.logfd != -1) {
        n = snprintf(buf, sizeof(buf), "[WAIT] r%d >> r%d %d us\n", rank,
                     peer_rank, int(delay));
        n = write(pctx.logfd, buf, n);

        errno = 0;
      }

      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[rpc_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[rpc_cv], &mtx[rpc_cv], &abstime);
      if (e == ETIMEDOUT) {
        ABORT("rpc timeout");
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

/* nn_shuffler_enqueue: encode and add an incoming write into an rpc queue */
void nn_shuffler_enqueue(const char* fname, unsigned char fname_len, char* data,
                         size_t len, int epoch, int peer_rank, int rank) {
  write_in_t write_in;
  uint16_t nepoch;
  uint32_t nrank;
  rpcq_t* rpcq;
  int rpcq_idx;
  size_t rpc_sz;
  time_t now;
  struct timespec abstime;
  useconds_t delay;
  char msg[200];
  int rv;
  void* arg1;
  void* arg2;
  int e;
  int n;

  assert(fname != NULL);

  pthread_mtx_lock(&mtx[qu_cv]);

  rpcq_idx = peer_rank; /* XXX: assuming one queue per rank */
  assert(rpcq_idx < nrpcqs);
  rpcq = &rpcqs[rpcq_idx];
  assert(rpcq != NULL);
  assert(len < 256);
  rpc_sz = 0;

  delay = 1000; /* 1000 us */

  /* wait for queue */
  while (rpcq->busy != 0) {
    if (pctx.testin) {
      pthread_mtx_unlock(&mtx[qu_cv]);
      if (pctx.logfd != -1) {
        n = snprintf(msg, sizeof(msg), "[BLOCK-QUEUE] %d us\n", int(delay));
        n = write(pctx.logfd, msg, n);

        errno = 0;
      }

      usleep(delay);
      delay <<= 1;

      pthread_mtx_lock(&mtx[qu_cv]);
    } else {
      now = time(NULL);
      abstime.tv_sec = now + nnctx.timeout;
      abstime.tv_nsec = 0;

      e = pthread_cv_timedwait(&cv[qu_cv], &mtx[qu_cv], &abstime);
      if (e == ETIMEDOUT) {
        ABORT("rpc timeout");
      }
    }
  }

  /* get an estimated size of the rpc */
  rpc_sz += 4;                 /* src rank */
  rpc_sz += 4;                 /* dst rank */
  rpc_sz += 1 + fname_len + 1; /* vpic fname */
  rpc_sz += 1 + len;           /* vpic data */
  rpc_sz += 2;                 /* epoch */

  /* flush queue if full */
  if (rpcq->sz + rpc_sz > max_rpcq_sz) {
    if (rpcq->sz > MAX_RPC_MESSAGE) {
      /* happens when the total size of queued data is greater than
       * the size limit for an rpc message */
      ABORT("rpc overflow");
    } else {
      rpcq->busy = 1; /* force other writers to block */
      /* unlock when sending the rpc */
      pthread_mtx_unlock(&mtx[qu_cv]);
      write_in.owner = htonl(rank);
      write_in.msg = rpcq->buf;
      write_in.sz = rpcq->sz;
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
  if (rpcq->sz + rpc_sz > max_rpcq_sz) {
    /* happens when the memory reserved for the queue is smaller than
     * a single write */
    ABORT("rpc overflow");
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

  pthread_mtx_unlock(&mtx[qu_cv]);
}

/* nn_shuffler_flushq: force flushing all rpc queue */
void nn_shuffler_flushq() {
  write_in_t write_in;
  rpcq_t* rpcq;
  void* arg1;
  void* arg2;
  int rank;
  int rv;
  int i;

  assert(nnctx.ssg != NULL);

  rank = ssg_get_rank(nnctx.ssg); /* my rank */

  pthread_mtx_lock(&mtx[qu_cv]);

  for (i = 0; i < nrpcqs; i++) {
    rpcq = &rpcqs[i];
    if (rpcq->sz == 0) { /* skip empty queue */
      continue;
    } else if (rpcq->sz > MAX_RPC_MESSAGE) {
      ABORT("rpc overflow");
    } else {
      rpcq->busy = 1; /* force other writers to block */
      /* unlock when sending the rpc */
      pthread_mtx_unlock(&mtx[qu_cv]);
      write_in.owner = htonl(rank);
      write_in.msg = rpcq->buf;
      write_in.sz = rpcq->sz;
      if (!nnctx.force_sync) {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send_async(&write_in, i, arg1, arg2);
      } else {
        shuffle_msg_sent(0, &arg1, &arg2);
        rv = nn_shuffler_write_send(&write_in, i);
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
  uint64_t last_progress;
  uint64_t now;
  char msg[100];
  int s;

#ifndef NDEBUG
  if (pctx.verr || pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "[bg] rpc looper up (rank %d)", pctx.my_rank);
    INFO(msg);
  }
#endif

  /* the last time we do mercury progress */
  last_progress = 0;

  while (true) {
    do {
      hret = HG_Trigger(nnctx.hg_ctx, 0, 1, &actual_count);
    } while (hret == HG_SUCCESS && actual_count != 0);
    if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
      RPC_FAILED("HG_Trigger", hret);
    }
    s = is_shuttingdown();
    if (s == 0) {
      now = now_micros_coarse() / 1000;
      if (last_progress != 0 && now - last_progress > nnctx.hg_max_interval) {
        snprintf(msg, sizeof(msg),
                 "calling HG_Progress() with high interval: %d ms (rank %d)",
                 int(now - last_progress), pctx.my_rank);
        WARN(msg);
      }
      last_progress = now;
      hret = HG_Progress(nnctx.hg_ctx, nnctx.hg_timeout);
      if (hret != HG_SUCCESS && hret != HG_TIMEOUT) {
        RPC_FAILED("HG_Progress", hret);
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
  if (is_shuttingdown() == 0) {
    pthread_mtx_lock(&mtx[bg_cv]);
    if (shutting_down == 0) shutting_down = -1;
    pthread_mtx_unlock(&mtx[bg_cv]);
  }
}

/* nn_shuffler_wakeup: signal a sleeping looper to resume work */
void nn_shuffler_wakeup() {
  if (is_shuttingdown() < 0) {
    pthread_mtx_lock(&mtx[bg_cv]);
    if (shutting_down < 0) shutting_down = 0;
    pthread_cv_notifyall(&cv[bg_cv]);
    pthread_mtx_unlock(&mtx[bg_cv]);
  }
}

/* nn_shuffler_init_ssg: init the ssg sublayer */
void nn_shuffler_init_ssg() {
  hg_return_t hret;
  int rank; /* ssg */
  int size; /* ssg */

  assert(nnctx.hg_clz != NULL);
  assert(nnctx.hg_ctx != NULL);

  nnctx.ssg = ssg_init_mpi(nnctx.hg_clz, MPI_COMM_WORLD);
  if (nnctx.ssg == SSG_NULL) ABORT("ssg_init_mpi");

  hret = ssg_lookup(nnctx.ssg, nnctx.hg_ctx);
  if (hret != HG_SUCCESS) ABORT("ssg_lookup");

  size = ssg_get_count(nnctx.ssg);
  rank = ssg_get_rank(nnctx.ssg);

  if (pctx.paranoid_checks) {
    if (size != pctx.comm_sz || rank != pctx.my_rank) {
      ABORT("ssg-mpi disagree");
    }
  }
}

/* nn_shuffler_init: init the shuffle layer */
void nn_shuffler_init() {
  hg_return_t hret;
  hg_size_t isz;
  hg_size_t osz;
  pthread_t pid;
  char msg[200];
  const char* env;
  int rv;
  int i;

  shuffle_prepare_uri(nnctx.my_addr);

  env = maybe_getenv("SHUFFLE_Timeout");
  if (env == NULL) {
    nnctx.timeout = DEFAULT_TIMEOUT;
  } else {
    nnctx.timeout = atoi(env);
    if (nnctx.timeout < 5) {
      nnctx.timeout = 5;
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

  if (is_envset("SHUFFLE_Mercury_cache_handles")) nnctx.cache_hlds = 1;
  if (is_envset("SHUFFLE_Force_sync_rpc")) nnctx.force_sync = 1;

  nnctx.hg_clz = HG_Init(nnctx.my_addr, HG_TRUE);
  if (!nnctx.hg_clz) ABORT("HG_Init");

  nnctx.hg_id = HG_Register_name(
      nnctx.hg_clz, "shuffle_rpc_write", nn_shuffler_write_in_proc,
      nn_shuffler_write_out_proc, nn_shuffler_write_rpc_handler_wrapper);

  hret = HG_Register_data(nnctx.hg_clz, nnctx.hg_id, &nnctx, NULL);
  if (hret != HG_SUCCESS) ABORT("HG_Register_data");

  nnctx.hg_ctx = HG_Context_create(nnctx.hg_clz);
  if (!nnctx.hg_ctx) ABORT("HG_Context_create");

  nn_shuffler_init_ssg();

  /* rpc queue */
  assert(nnctx.ssg != NULL);
  nrpcqs = ssg_get_count(nnctx.ssg);
  env = maybe_getenv("SHUFFLE_Buffer_per_queue");
  if (env == NULL) {
    max_rpcq_sz = DEFAULT_BUFFER_PER_QUEUE;
  } else {
    max_rpcq_sz = atoi(env);
    if (max_rpcq_sz > MAX_RPC_MESSAGE) {
      max_rpcq_sz = MAX_RPC_MESSAGE;
    } else if (max_rpcq_sz < 128) {
      max_rpcq_sz = 128;
    }
  }
  rpcqs = static_cast<rpcq_t*>(malloc(nrpcqs * sizeof(rpcq_t)));
  for (i = 0; i < nrpcqs; i++) {
    rpcqs[i].buf = static_cast<char*>(malloc(max_rpcq_sz));
    rpcqs[i].busy = 0;
    rpcqs[i].sz = 0;
  }
  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg), "rpc buffer: %s x %s (%s total)",
             pretty_num(nrpcqs).c_str(), pretty_size(max_rpcq_sz).c_str(),
             pretty_size(nrpcqs * max_rpcq_sz).c_str());
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
    WARN("rpc worker disabled");
  }

  if (pctx.my_rank == 0) {
    snprintf(msg, sizeof(msg),
             "HG_Progress() timeout: %d ms, warn interval: %d ms, "
             "fatal rpc timeout: %d s\n>>> "
             "cache hg_handle_t: %s",
             nnctx.hg_timeout, nnctx.hg_max_interval, nnctx.timeout,
             nnctx.cache_hlds ? "TRUE" : "FALSE");
    INFO(msg);
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
}

/* nn_shuffler_world_size: return comm world size */
int nn_shuffler_world_size() {
  assert(nnctx.ssg != NULL);
  int rv = ssg_get_count(nnctx.ssg);
  assert(rv > 0);
  return rv;
}

/* nn_shuffler_my_rank: return my rank */
int nn_shuffler_my_rank() {
  assert(nnctx.ssg != NULL);
  int rv = ssg_get_rank(nnctx.ssg);
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

  if (rpcqs != NULL) {
    for (i = 0; i < nrpcqs; i++) {
      assert(rpcqs[i].busy == 0);
      assert(rpcqs[i].sz == 0);

      free(rpcqs[i].buf);
    }

    free(rpcqs);
  }

  if (nnctx.ssg != NULL) {
    ssg_finalize(nnctx.ssg);
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
