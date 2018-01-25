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

/*
 * shuffler_internal.h  shuffler internal data structures
 * 28-Jun-2017  chuck@ece.cmu.edu
 */

/*
 * internal data structures for the 3 hop shuffler.
 */

#include <time.h>

#include <map>
#include <deque>
#include "acnt_wrap.h"
#include "xqueue.h"

struct req_parent;                  /* forward decl, see below */
struct outset;                      /* forward decl, see below */
struct hgthread;                    /* forward decl, see below */

/*
 * request: a structure to describe a single write request.
 * it has a fixed sized header (first four fields), and a
 * variable length data buffer.   we always allocate the header
 * and the data together.   data will be null if datalen == 0.
 */
struct request {
  /* fields that are transmitted over the wire */
  uint32_t datalen;                 /* length of data buffer */
  uint32_t type;                    /* message type (0=normal) */
  int32_t src;                      /* SRC rank */
  int32_t dst;                      /* DST rank */
  void *data;                       /* request data */

  /* internal fields (not sent over the wire) */
  /*
   * the owner is something (the application, a hg_handle_t) that
   * has been flow controlled waiting for the request to be removed
   * a waitq.  owner can be NULL if nothing is waiting.  to change
   * a non-null owner or "next" linkage, lock the req's waitq.
   */
  struct req_parent *owner;         /* waiter that generated the request */
  XSIMPLEQ_ENTRY(request) next;     /* next request in a queue of requests */
};

/*
 * request_queue: a simple queue of request structures
 */
XSIMPLEQ_HEAD(request_queue, request);

/*
 * rpcin_t: a batch of requests (top-level RPC request structure).
 * when we serialize this, we add a request with datalen/type=zero
 * to mark the end of the list (XXX: safer that trying to use
 * hg_proc_get_size_left()?).   note: seq is signed to match acnt32_t.
 */
typedef struct {
  int32_t iseq;                     /* seq# (echoed back), for debugging */
  int32_t forwardrank;              /* rank of proc that initiated rpc */
  struct request_queue inreqs;      /* list of malloc'd requests */
} rpcin_t;

/*
 * rpcout_t: return value from the server.
 */
typedef struct {
  int32_t oseq;                     /* seq# (echoed back), for debugging */
  int32_t respondrank;              /* rank of proc sending response */
  int32_t ret;                      /* return value */
} rpcout_t;

/*
 * req_parent: a structure to describe the owner of a group of
 * one or more waiting requests.  the owner is either the main
 * application thread (via shuffler_send()) or it is an
 * hg_handle_t from an inbound rpc request.   we use this
 * to track when all requests have been processed and the caller
 * can continue or the rpc can be responded to (this is for flow
 * control).
 *
 * "nrefs" atomically tracks the number of pending requests we are
 * waiting on.  fields other than "nrefs" are only changed when there
 * are no active references (either because we just allocated the
 * req_parent or because we just released the last reference).   the
 * exception is "ret" which can be set to something other than
 * HG_SUCCESS on an error to give a hint on what the problem was...
 */
struct req_parent {
  acnt32_t nrefs;                   /* atomic ref counter */
  hg_return_t ret;                  /* return status (HG_SUCCESS, normally) */
  int32_t rpcin_seq;                /* saved copy of rpcin.seq */
  int32_t rpcin_forwrank;           /* saved copy of rpcin.forwardrank */
  hg_handle_t input;                /* RPC input, or NULL for app input */
  int32_t timewstart;               /* time wait started */
  /* next three only used if input == NULL (thus via shuffler_send()) */
  pthread_mutex_t pcvlock;          /* lock for pcv */
  pthread_cond_t pcv;               /* app may block here for flow ctl */
  int need_wakeup;                  /* need wakeup when nrefs cleared */
  /* used when building a list of zero-ref'd req_parent's to free */
  int onfq;                         /* non-zero on an fq list (to be safe) */
  struct req_parent *fqnext;        /* free queue next */
};

/*
 * output: a single output that has been started with HG_Forward()
 * but has not yet completed (i.e. RPC request has been sent, but
 * we have not gotten the callback with the result from the remote
 * end yet...
 */
struct output {
  struct outqueue *oqp;             /* owning output queue */
  hg_handle_t outhand;              /* out handle used with HG_Forward() */
  int ostep;                        /* output step */
  int32_t outseq;                   /* output seq# to use for this output */
  int32_t timestart;                /* time we started output */
#define OSTEP_PREP 0                /* prepare, not at forward_reqs_now yet */
#define OSTEP_SEND 1                /* forward_reqs_now sending */
#define OSTEP_CANCEL (-1)           /* trying to cancel request */
  XTAILQ_ENTRY(output) q;           /* linkage (locked by oqlock) */
};

/*
 * sending_outputs: a list of outputs currently being sent
 */
XTAILQ_HEAD(sending_outputs, output);

/*
 * outqueue: an output queue to a mercury endpoint (either na+sm or
 * network).  we append a request to "loading" each time we get an
 * output until we reach our target buffer size, then we send the batch.
 */
struct outqueue {
  /* config */
  struct outset *myset;             /* output set that owns this queue */
  hg_addr_t dst;                    /* who we send to (nexus owns this) */

  /* the next two are cached from nexus for debug output */
  int grank;                        /* global rank of endpoint */
  int subrank;                      /* local rank or node number */

  pthread_mutex_t oqlock;           /* output queue lock */
  struct request_queue loading;     /* list of requests we are loading */
  int loadsize;                     /* size of loading, send when buftarget */

  struct sending_outputs outs;      /* outputs currently being sent to dst */
  int nsending;                     /* #of sends in progress for dst */

  std::deque<request *> oqwaitq;    /* if queue full, waitq of reqs */

  /* fields for flushing an output queue */
  int oqflushing;                   /* 1 if oq is flushing */
  int oqflush_waitcounter;          /* #of waitq reqs flush is waiting on */
  struct output *oqflush_output;    /* output flush is waiting on */

#ifdef SHUFFLER_COUNT
  /* index 0 is for input==NULL (local reqs), 1 for forwarded reqs */
  int cntoqreqs[2];                 /* number of reqs queued here */
  int cntoqsends;                   /* number of RPCs sent */
  int cntoqflushsend;               /* number of RPCs sent early for flush */
  int cntoqwaits[2];                /* number of reqs that go on oqwaitq */
  unsigned int cntoqmaxwait;        /* max wait queue size */
  int cntoqflushes;                 /* number of flushes on non-empty oq */
  int cntoqflushorder;              /* flush rpc finished in different order */
#endif
};

/*
 * shufsend_waiter: a shuffler_send() API call that has been blocked
 * due to the total number of active outset RPCs exceeding shufsend_rpclimit.
 */
struct shufsend_waiter {
  pthread_mutex_t sw_lock;          /* lock on waiter */
  pthread_cond_t sw_cv;             /* client thread waits here */
  int sw_status;                    /* status of waiter */
#define SHUFSEND_WAIT 0             /* keep waiting for space to clear */
#define SHUFSEND_OKGO 1             /* drop nrpcs and start */
#define SHUFSEND_CANCEL (-1)        /* cancel send */
  XTAILQ_ENTRY(shufsend_waiter) sw_q;  /* linkage off of outset */
};

/*
 * sendwaiterlist: list of shuffler_send() ops waiting for an outset's
 * shufsend_rpclimit to drop.
 */
XTAILQ_HEAD(sendwaiterlist, shufsend_waiter);

/*
 * outset: a set of local or remote output queues
 */
struct outset {
  /* config */
  int maxoqrpc;                     /* max# of outstanding sent RPCs on an oq */
  int buftarget;                    /* target size of an RPC (in bytes) */
  int settype;                      /* remote, origin, or relay */
  int shufsend_rpclimit;            /* block shuffler_send() if past limit */

  /* general state */
  shuffler_t shuf;                  /* shuffler that owns us */
  struct hgthread *myhgt;           /* mercury thread that services us */

  /* shuffler_send() rpc limit */
  pthread_mutex_t os_rpclimitlock;  /* locks next two items */
  int outset_nrpcs;                 /* total# of running RPCs in all oq's */
  struct sendwaiterlist shufsendq;  /* list of waiting shuffler_send() ops */
#ifdef SHUFFLER_COUNT
  int os_senderlimit;               /* #of times we hit shufsend_rpclimit */
#endif

  /* a map of all the output queues we known about */
  std::map<hg_addr_t,struct outqueue *> oqs;

  /* state for tracking a flush op (locked w/"flushlock") */
  int osetflushing;                 /* flushing, want signal on flush_waitcv */
  acnt32_t oqflush_counter;         /* #qs flushing (hold flushlock to init) */
};

/*
 * flush_op: a flush opearion.  may be on pending list waiting to
 * run or may be currently running.   typically stack allocated by
 * caller...   locked with flushlock.
 */
struct flush_op {
  pthread_cond_t flush_waitcv;      /* wait here for our turn or flush done */
  int status;                       /* see below */
/* status values */
#define FLUSHQ_PENDING  0           /* flush is on pending list waiting */
#define FLUSHQ_READY    1           /* flush is running */
#define FLUSHQ_CANCEL  -1           /* flush has been canceled */
  XSIMPLEQ_ENTRY(flush_op) fq; /* linkage */
};

/*
 * flush_queue: queue of flush operations (e.g. pending ops)
 */
XSIMPLEQ_HEAD(flush_queue, flush_op);

/*
 * hgthread: state for a mercury progress/trigger thread
 */
struct hgthread {
  struct shuffler *hgshuf;          /* shuffler that owns us */
  hg_class_t *mcls;                 /* mercury class */
  hg_context_t *mctx;               /* mercury context */
  hg_id_t rpcid;                    /* id of this RPC */
  int nshutdown;                    /* to signal ntask to shutdown */
  int nrunning;                     /* ntask is valid and running */
  pthread_t ntask;                  /* network thread */

#ifdef SHUFFLER_COUNT
  /* stats (only modified/updated by ntask) */
  int nprogress;                    /* mercury progress fn counter */
  int ntrigger;                     /* mercury trigger fn counter */
#endif
};

/*
 * shuffler: top-level shuffler structure
 */
struct shuffler {
  /* general config */
  nexus_ctx_t nxp;                  /* routing table */
  int grank;                        /* my global rank */
  char *funname;                    /* strdup'd copy of mercury func. name */
  int disablesend;                  /* disable new sends (for shutdown) */
  time_t boottime;                  /* time we started */

  /* mercury threads */
  struct hgthread hgt_local;        /* local thread (na+sm) */
  struct hgthread hgt_remote;       /* network thread (bmi+tcp, etc.) */

  /* output queues */
  struct outset local_orq;          /* for origin/client na+sm to local procs */
  struct outset local_rlq;          /* for relay na+sm to local procs */
  struct outset remoteq;            /* for network to remote nodes */
  acnt32_t seqsrc;                  /* source for seq# */

  /* delivery queue cfg */
  int deliverq_max;                 /* max #reqs we queue before blocking */
  int deliverq_threshold;           /* wake dlvr when #reqs on q > threshold */
  shuffler_deliver_t delivercb;     /* callback function ptr */

  /* delivery thread and queue itself */
  pthread_mutex_t deliverlock;      /* locks this block of fields */
  pthread_cond_t delivercv;         /* deliver thread blocks on this */
  std::deque<request *> deliverq;   /* acked reqs being delivered */
  std::deque<request *> dwaitq;     /* unacked reqs waiting for deliver */
  int dflush_counter;               /* #of req's flush is waiting for */
  int dshutdown;                    /* to signal dtask to shutdown */
  int drunning;                     /* dtask is valid and running */
  pthread_t dtask;                  /* delivery thread */

  /* flush operation management - flush ops are serialized */
  pthread_mutex_t flushlock;        /* locks the following fields */
  struct flush_queue fpending;      /* queue of pending flush ops */
  struct flush_op *curflush;        /* currently running flush (or NULL) */
  int flushdone;                    /* set when current op done */
  int flushtype;                    /* current flush's type (for diag/logs) */
  struct outset *flushoset;         /* flush outset if local/remote */
/* possible flush types */
#define FLUSH_NONE       0
#define FLUSH_LOCAL_ORQ  1          /* flushing local origin na+sm queues */
#define FLUSH_LOCAL_RLQ  2          /* flushing local relay na+sm queues */
#define FLUSH_REMOTEQ    3          /* flushing remote network queues */
#define FLUSH_DELIVER    4          /* flushing delivery queue */
#define FLUSH_NTYPES     5          /* number of types */

#ifdef SHUFFLER_COUNT
  /* lock by flushlock */
  int cntflush[FLUSH_NTYPES];       /* number of flush reqs by type */
  int cntflushwait;                 /* number of blocked flush reqs */

  /* lock by deliverlock */
  int cntdblock;                    /* number of times deliver blocks */
  int cntdeliver;                   /* number of times delivery cb called */
  int cntdreqs[2];                  /* number of reqs input */
  int cntdwait[2];                  /* number of reqs on delivery wait q*/
  unsigned int cntdmaxwait;         /* max waitq size */

  /* only accessed by one thread */
  int cntrpcinshm;                  /* #rpcs in on na+sm */
  int cntrpcinnet;                  /* #rpcs in on network */

  int cntstranded;                  /* number of stranded reqs (@shutdown) */
#endif

};
