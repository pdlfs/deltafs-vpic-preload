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
 * shuffler.cc  3 hop shuffle code
 * 28-Jun-2017  chuck@ece.cmu.edu
 */

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include <mercury.h>
#include <mercury_macros.h>
#include <deltafs-nexus/deltafs-nexus_api.h>

#include "shuffler.h"

#define SHUFFLER_COUNT           /* enable/disable internal counters */
#include "shuffler_internal.h"

/*
 * quick reminder:
 *
 * forwarding path:
 *    SRC -na+sm-> SRCREP -network-> DSTREP -na+sm-> DST
 *
 * steps can be skipped (e.g. if SRC == SRCREP, skip the first na+sm hop).
 * since there are 3 possible hops, there are 8 possible paths (but only
 * 6 make sense):
 *    h1 h2 h3
 *    -  -  -  src==dst, deliver directly
 *    Y  -  -  dst is local, src==srcrep
 *    -  Y  -  dst remote, src==srcrep, dst=dstrep
 *    Y  Y  -  dst remote, dst==dstrep
 *    -  -  Y  not possible, redundant with "Y - -" (dst local, src==srcrep)
 *    Y  -  Y  not possible, making 2 local hops is pointless
 *    -  Y  Y  dst remote, src==srcrep
 *    Y  Y  Y  dst remote, full 3 hops!
 *
 * 3 threads: na+sm mercury thread, network mercury thread, delivery thread
 * (the final hop is always via the delivery thread)
 *
 * "send" vs "forward":
 *  - applications "send" a single request via the shuffler_send() API
 *  - shuffler "forwards" RPCs received from shuffler_rpchand() callback,
 *    each RPC contains a batch of one or more requests
 *
 * write back buffering handled by allowing requests to complete when
 * placed in a queue.  if the queue is full, request go in a wait queue
 * and either the app is blocked ("send") or the RPC reply is delayed
 * ("forward").  this is managed with the req_parent structure.
 */

/*
 * start of logging init and helper stuff
 */
#include "shuf_mlog.h"

static struct shufcfglog {
  int on;                  /* set if enabled */
  int max_xtra_rank;       /* rank <= max_xtra_rank: enable xtra logging */
  int defpri;              /* default priority for the rest */
  int stderrpri;           /* stderr priority for everyone else */
  char *mask;              /* mask for non-xtra ranks */
  char *xmask;             /* mask xtra logging ranks (defaut=mask) */
  char *logfile;           /* logfile name (if enabled), we append rank */
  int alllogs;             /* if logfile, do on all ranks, not just xtra */
  int lfbaselen;           /* strlen() of the log file base */
  int msgbufsz;            /* message buf size */
  int stderrlog;           /* always log to stderr for other ranks */
  int xtra_stderrlog;      /* always log to stderr for xtra log ranks */
} shufcfg = { 0 };

/*
 * shuffler_cfglog: setup logging before starting shuffler.  call
 * this before shuffler_init() so that everything can be properly
 * logged...
 */
int shuffler_cfglog(int max_xtra_rank, const char *defpri,
                    const char *stderrpri, const char *mask,
                    const char *xmask, const char *logfile,
                    int alllogs, int msgbufsz, int stderrlog,
                    int xtra_stderrlog) {
  char *tmpbuf;

  shufcfg.max_xtra_rank = max_xtra_rank;

  shufcfg.defpri = (defpri) ? shuf::mlog_str2pri(defpri) : MLOG_WARN;
  if (shufcfg.defpri == -1) {
    fprintf(stderr, "shuffler_cfglog: bad defpri %s\n", defpri);
    return(-1);
  }

  shufcfg.stderrpri = (stderrpri) ?
                        shuf::mlog_str2pri(stderrpri) : MLOG_WARN;
  if (shufcfg.stderrpri == -1) {
    fprintf(stderr, "shuffler_cfglog: bad stderrpri %s\n", stderrpri);
    goto err;
  }

  shufcfg.mask = (mask) ? strdup(mask) : NULL;
  if (mask && !shufcfg.mask) {
    fprintf(stderr, "shuffler_cfglog: mask malloc failed\n");
    goto err;
  }

  shufcfg.xmask = (xmask) ? strdup(xmask) : NULL;
  if (xmask && !shufcfg.xmask) {
    fprintf(stderr, "shuffler_cfglog: xmask malloc failed\n");
    goto err;
  }

  if (logfile) {
    shufcfg.lfbaselen = strlen(logfile);
    tmpbuf = (char *)calloc(shufcfg.lfbaselen + 16, 1);   /* zero fills */
    if (tmpbuf == NULL) {
      fprintf(stderr, "shuffler_cfglog: malloc tmpbuf failed?\n");
      goto err;
    }
    shufcfg.logfile = tmpbuf;  /* we'll add the rank later */
    memcpy(tmpbuf, logfile, shufcfg.lfbaselen);
  }
  shufcfg.alllogs = alllogs;
  shufcfg.msgbufsz = msgbufsz;
  shufcfg.stderrlog = stderrlog;
  shufcfg.xtra_stderrlog = xtra_stderrlog;

  shufcfg.on = 1;
  return(0);

err:
  if (shufcfg.mask) free(shufcfg.mask);
  if (shufcfg.xmask) free(shufcfg.xmask);
  if (shufcfg.logfile) free(shufcfg.logfile);
  return(-1);
}

/*
 * shuffler_openlog: start the log
 *
 * @param myrank the current process' rank
 */
static void shuffler_openlog(int myrank) {
  int stderrlog, am_xtra, rv;
  char *lfile, *usemask;

  if (shufcfg.on == 0)
    return;
  am_xtra = (myrank <= shufcfg.max_xtra_rank);
  if (am_xtra) {
    stderrlog = (shufcfg.xtra_stderrlog) ? MLOG_STDERR : 0;
  } else {
    stderrlog = (shufcfg.stderrlog) ? MLOG_STDERR : 0;
  }

  if (shufcfg.logfile) {  /* append rank to logfilename */
    snprintf(shufcfg.logfile+shufcfg.lfbaselen, 16, ".%d", myrank);
    lfile = (am_xtra || shufcfg.alllogs) ? shufcfg.logfile : NULL;
  } else {
    lfile = NULL;
  }

  rv = shuf::mlog_open("shuf", SHUF_MAXFAC, shufcfg.defpri, shufcfg.stderrpri,
                       lfile, shufcfg.msgbufsz, stderrlog|MLOG_LOGPID, 0);
  if (rv < 0) {
    fprintf(stderr, "shuffler_openlog: failed!  log disabled\n");
    shufcfg.on = 0;
    goto done;
  }

  if (shuf::mlog_namefacility(SHUF_MLOG, "SHUF", NULL) < 0 ||
      shuf::mlog_namefacility(UTIL_MLOG, "UTIL", NULL) < 0 ||
      shuf::mlog_namefacility(CLNT_MLOG, "CLNT", NULL) < 0 ||
      shuf::mlog_namefacility(DLIV_MLOG, "DLIV", NULL) < 0) {
    fprintf(stderr, "shuffler_namefac: failed!  log disabled\n");
    goto done;
  }

  usemask = shufcfg.mask;
  if (am_xtra && shufcfg.xmask != NULL)
    usemask = shufcfg.xmask;
  if (usemask)
    shuf::mlog_setmasks(usemask, -1);  /* ignore errors */

done:
  if (shufcfg.logfile) free(shufcfg.logfile);
  if (shufcfg.mask) free(shufcfg.mask);
  if (shufcfg.xmask) free(shufcfg.xmask);
  shufcfg.logfile = shufcfg.mask = shufcfg.xmask = NULL;
}

/*
 * shuffler_closelog: end the log
 */
static void shuffler_closelog() {
  if (shufcfg.on)
    shuf::mlog_close();
}

static void notify(int lvl, const char *fmt, ...)
  __attribute__((__format__(__printf__, 2, 3)));

/*
 * notify: helper function that will emit errors to mlog (if
 * enabled) and fall back to stderr if mlog is not enabled.
 * only use notify over mlog if you want to ensure something
 * gets printed even if mlog is not enabled.
 *
 * @param lvl the mlog level to use
 * @param fmt printf-style format
 * @param ... printf-style args
 */
static void notify(int lvl, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  if (MLOG_NEVERLOG == 0 && shufcfg.on) {
    shuf::vmlog(lvl, fmt, ap);
  } else {
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
  }
  va_end(ap);
}

/*
 * end of logging init and helper stuff
 */

/*
 * counters: can be compiled in or out as needed
 */

#ifdef SHUFFLER_COUNT
#define shufadd(X,V)  do { (*X) += (V); } while (0)
#define shufcount(X)  do { (*X)++; } while (0)
#define shufcounta(X) do { acnt32_incr(parent->nrefs); } while (0)
#define shufmax(X,V)  do { if ((V) > (*X)) (*X) = (V); } while (0)
#define shuftime()    time(NULL)
#define shufzero(X) do { (*X) = 0; } while (0)
#else
#define shufadd(X,V)   /* nothing */
#define shufcount(X)   /* nothing */
#define shufcounta(X)  /* nothing */
#define shufmax(X,V)   /* nothing */
#define shuftime()     0   /* time disabled, just ret 0 */
#define shufzero(X)    /* nothing */
#endif

/*
 * RPC handler registered with mercury
 */
static hg_return_t shuffler_rpchand(hg_handle_t handle);

/*
 * thread main routines for network and delivery
 */
static void *delivery_main(void *arg);
static void *network_main(void *arg);

/*
 * other prototypes
 */

static bool append_req_to_locked_outqueue(struct outset *oset,
                                          struct outqueue *oq,
                                          struct request *req,
                                          struct request_queue *tosendq,
                                          struct output **newoutputp,
                                          bool flushnow);
static hg_return_t aquire_flush(struct shuffler *sh, struct flush_op *fop,
                                int type, struct outset *oset);
static void clean_qflush(struct shuffler *sh, struct outset *oset);
static void done_oq_flush(struct outqueue *oq);
static void drop_curflush(struct shuffler *sh);
static hg_return_t forw_cb(const struct hg_cb_info *cbi);
static void forw_start_next(struct outqueue *oq, struct output *oput);
static hg_return_t forward_reqs_now(struct request_queue *tosendq,
                                    struct shuffler *sh, struct outset *oset,
                                    struct outqueue *oq, struct output *oput);
static int purge_reqs(struct shuffler *sh);
static int purge_reqs_outset(struct shuffler *sh, struct outset *oset);
static hg_return_t req_parent_init(struct shuffler *sh,
                                   struct req_parent **parentp,
                                   struct request *req, hg_handle_t input,
                                   rpcin_t *rpcin);
static hg_return_t req_to_self(struct shuffler *sh, struct request *req,
                               hg_handle_t input, rpcin_t *rpcin,
                               struct req_parent **parentp);
static hg_return_t req_via_mercury(struct shuffler *sh, struct outset *oset,
                                   struct outqueue *oq, struct request *req,
                                   hg_handle_t input, rpcin_t *rpcin,
                                   struct req_parent **parentp);
static void parent_dref_stopwait(struct shuffler *sh, struct req_parent *parent,
                                 int abort);
static void parent_stopwait(struct shuffler *sh, struct req_parent *parent,
                            int abort);
static hg_return_t shuffler_desthand_cb(const struct hg_cb_info *cbi);
static hg_return_t shuffler_respond_cb(const struct hg_cb_info *cbi);
static int start_threads(struct shuffler *sh);
static void stop_threads(struct shuffler *sh);
static int start_qflush(struct shuffler *sh, struct outset *oset,
                        struct outqueue *oq);

/*
 * functions used to serialize/deserialize our RPCs args (e.g. XDR-like fn).
 */

static int32_t zero = 0;   /* for end of list marker */

/*
 * procheck: helper macro to reduce the verbage ...
 */
#define procheck(R,MSG) if ((R) != HG_SUCCESS) { \
    hg_log_write(HG_LOG_TYPE_ERROR, "HG", __FILE__, __LINE__, __func__, MSG); \
    goto done; \
}

/*
 * hg_proc_rpcin_t: encode/decode the rpcin_t structure
 *
 * @param proc the proc used to serialize/deserialize the data
 * @param data pointer to the data being worked on
 * @return HG_SUCCESS or an error code
 */
static hg_return_t hg_proc_rpcin_t(hg_proc_t proc, void *data) {
  hg_return_t ret = HG_SUCCESS;
  hg_proc_op_t op = hg_proc_get_op(proc);
  rpcin_t *struct_data = (rpcin_t *) data;
  struct request *rp, *nrp;
  int cnt, lcv;
  int32_t dlen, typ;
  mlog(UTIL_CALL, "hg_proc_rpcin_t proc=%p op=%d", proc, op);

  if (op == HG_FREE)               /* we combine free and err handling below */
    goto done;

  if (op == HG_DECODE) {           /* start with an empty inreqs list */
    XSIMPLEQ_INIT(&struct_data->inreqs);
  }

  ret = hg_proc_hg_int32_t(proc, &struct_data->iseq);
  procheck(ret, "Proc err iseq");
  ret = hg_proc_hg_int32_t(proc, &struct_data->forwardrank);
  procheck(ret, "Proc err forwardrank");

  if (op == HG_ENCODE) {   /* serialize list to the proc */
    cnt = 0;
    XSIMPLEQ_FOREACH(rp, &struct_data->inreqs, next) {
      ret = hg_proc_hg_int32_t(proc, &rp->datalen);
      procheck(ret, "Proc en err datalen");
      ret = hg_proc_hg_int32_t(proc, &rp->type);
      procheck(ret, "Proc en err type");
      ret = hg_proc_hg_int32_t(proc, &rp->src);
      procheck(ret, "Proc en err src");
      ret = hg_proc_hg_int32_t(proc, &rp->dst);
      procheck(ret, "Proc en err dst");
      ret = hg_proc_memcpy(proc, rp->data, rp->datalen);
      procheck(ret, "Proc en err data");
      cnt++;
    }
    /* put in the end of list marker */
    for (lcv = 0 ; lcv < 4 ; lcv++) {
      ret = hg_proc_hg_int32_t(proc, &zero);
      procheck(ret, "Proc err zero");
    }
    mlog(UTIL_D1, "hg_proc_rpcin_t proc %p, encoded=%d", proc, cnt);
    goto done;
  }

  /* op == HG_DECODE */
  cnt = 0;
  while (1) {
    ret = hg_proc_hg_int32_t(proc, &dlen);  /* should err if we use up data */
    procheck(ret, "Proc de err datalen");
    ret = hg_proc_hg_int32_t(proc, &typ);
    procheck(ret, "Proc de err type");
    if (dlen == 0 && typ == 0) break;     /* got end of list marker */
    rp = (request*)malloc(sizeof(*rp) + dlen);
    if (rp == NULL) ret = HG_NOMEM_ERROR;
    procheck(ret, "Proc de malloc");
    rp->datalen = dlen;
    rp->type = typ;
    ret = hg_proc_hg_int32_t(proc, &rp->src);
    if (ret == HG_SUCCESS) ret = hg_proc_hg_int32_t(proc, &rp->dst);
    rp->data = ((char *)rp) + sizeof(*rp);
    if (ret == HG_SUCCESS) ret = hg_proc_memcpy(proc, rp->data, dlen);
    rp->owner = NULL;
    if (ret != HG_SUCCESS) {
      free(rp);
      procheck(ret, "Proc decoder");
    }

    /* got it!  put at the end of the decoded list */
    XSIMPLEQ_INSERT_TAIL(&struct_data->inreqs, rp, next);
    cnt++;
  }
  mlog(UTIL_D1, "hg_proc_rpcin_t proc %p, decoded=%d", proc, cnt);

done:
  if ( ((op == HG_DECODE && ret != HG_SUCCESS) || op == HG_FREE) &&
       XSIMPLEQ_FIRST(&struct_data->inreqs) != NULL) {
    XSIMPLEQ_FOREACH_SAFE(rp, &struct_data->inreqs, next, nrp) {
      free(rp);
    }
    XSIMPLEQ_INIT(&struct_data->inreqs);
  }
  return(ret);
}

/*
 * hg_proc_rpcout_t: encode/decode the rpcout_t structure
 *
 * @param proc the proc used to serialize/deserialize the data
 * @param data pointer to the data being worked on
 * @return HG_SUCCESS or an error code
 */
static hg_return_t hg_proc_rpcout_t(hg_proc_t proc, void *data) {
    hg_return_t ret = HG_SUCCESS;
    rpcout_t *struct_data = (rpcout_t *) data;
    /* hg_proc_op_t op = hg_proc_get_op(proc); */  /* don't need it */

    mlog(UTIL_CALL, "hg_proc_rpcout_t proc=%p, op=%d", proc,
         hg_proc_get_op(proc));

    ret = hg_proc_hg_int32_t(proc, &struct_data->oseq);
    procheck(ret, "Proc err oseq");
    ret = hg_proc_hg_int32_t(proc, &struct_data->respondrank);
    procheck(ret, "Proc err src");
    ret = hg_proc_hg_int32_t(proc, &struct_data->ret);
    procheck(ret, "Proc err ret");

done:
    return(ret);
}

/*
 * outset_typstr: outset type as a string
 *
 * @param type the type value
 * @return type string
 */
static const char *outset_typstr(int type) {
  switch (type) {
    case SHUFFLER_REMOTE_QUEUES: return("remote");
    case SHUFFLER_ORIGIN_QUEUES: return("orgin");
    case SHUFFLER_RELAY_QUEUES:  return("relay");
  }
  return("UNKNOWN!");
}

/*
 * shuffler_outset_discard: free anything that was attached to an outset
 * (e.g. for error recovery, shutdown)
 *
 * @param oset the outset to clean
 */
static void shuffler_outset_discard(struct outset *oset) {
  std::map<hg_addr_t,struct outqueue *>::iterator oqit;
  struct outqueue *oq;
  mlog(UTIL_CALL, "shuffler_outset_discard %s", outset_typstr(oset->settype));

  for (oqit = oset->oqs.begin() ; oqit != oset->oqs.end() ; oqit++) {
    oq = oqit->second;
    pthread_mutex_destroy(&oq->oqlock);
    delete oq;
  }

  oset->oqs.clear();
  if (oset->oqflush_counter)
    acnt32_free(&oset->oqflush_counter);
}

/*
 * shuffler_init_outset: init an outset (but does not start network thread)
 *
 * @param oset the structure we are init'ing
 * @param maxrpc max# of outstanding RPCs allowed
 * @param buftarget try and collect at least this many bytes into batch
 * @param shuf the shuffler that owns this oset
 * @param hgt the mercury thread that will service us
 * @param nit nexus iterator for map
 * @return -1 on error, 0 on success
 */
static int shuffler_init_outset(struct outset *oset, int maxrpc, int buftarget,
                                shuffler_t shuf, struct hgthread *hgt,
                                nexus_iter_t nit) {
  int stype;
  hg_addr_t ha;
  struct outqueue *oq;

  if (oset == &shuf->remoteq) {
    stype = SHUFFLER_REMOTE_QUEUES;
  } else if (oset == &shuf->local_orq) {
    stype = SHUFFLER_ORIGIN_QUEUES;
  } else if (oset == &shuf->local_rlq) {
    stype = SHUFFLER_RELAY_QUEUES;
  } else {
    notify(SHUF_CRIT, "init outset with mistmatched pointers?!");
    abort();    /* should never happen */
  }

  mlog(UTIL_CALL, "shuffler_init_outset type=%s", outset_typstr(stype));

  oset->maxrpc = maxrpc;
  oset->buftarget = buftarget;
  oset->settype = stype;
  oset->shuf = shuf;
  oset->myhgt = hgt;
  /* oqs init'd by ctor */
  oset->oqflush_counter = acnt32_alloc();
  if (oset->oqflush_counter == NULL)
    goto err;

  /* now populate the oqs */
  for (/*null*/ ; nexus_iter_atend(nit) == 0 ; nexus_iter_advance(nit)) {
    ha = nexus_iter_addr(nit);
    oq = new struct outqueue;
    if (!oq) goto err;
    oq->myset = oset;
    oq->dst = ha;         /* shared with nexus, nexus owns it */
    oq->subrank = nexus_iter_subrank(nit);
    oq->grank = nexus_iter_globalrank(nit);
    if (pthread_mutex_init(&oq->oqlock, NULL) != 0) {
      delete oq;
      goto err;
    }
    XSIMPLEQ_INIT(&oq->loading);
    XTAILQ_INIT(&oq->outs);
    oq->loadsize = oq->nsending = 0;
    oq->oqflushing = oq->oqflush_waitcounter = 0;
    oq->oqflush_output = NULL;
    shufzero(&oq->cntoqreqs[0]);  shufzero(&oq->cntoqreqs[1]);
    shufzero(&oq->cntoqsends);
    shufzero(&oq->cntoqflushsend);
    shufzero(&oq->cntoqwaits[0]);  shufzero(&oq->cntoqwaits[1]);
    shufzero(&oq->cntoqmaxwait);
    shufzero(&oq->cntoqflushes);
    shufzero(&oq->cntoqflushorder);

    /* waitq init'd by ctor */
    oset->oqs[ha] = oq;    /* map insert, malloc's under the hood */
    mlog(UTIL_D1, "init_outset: add oq=%p rnks=%d.%d addr=%p", oq, oq->grank,
         oq->subrank, ha);
  }

  mlog(UTIL_D1, "init_outset: final size=%zd", oset->oqs.size());
  return(0);

err:
  mlog(UTIL_ERR, "init_outset: failed (type=%s)!", outset_typstr(stype));
  shuffler_outset_discard(oset);
  return(-1);
}

/*
 * shuffler_init_hgthread: init the hg thread state structure.
 * allocates rpcid, but does not start threads.
 *
 * @param sh the shuffler we are working on
 * @param hgt the structure to init
 * @param cls hg class to use
 * @param ctx hc context to use
 * @param rpchand rpc handler function (we register it)
 * @return -1 on error, 0 on success
*/
int shuffler_init_hgthread(struct shuffler *sh, struct hgthread *hgt,
                           hg_class_t *cls, hg_context_t *ctx,
                           hg_rpc_cb_t rpchand) {
  /* zero everything.  setting nrunning to 0 indicates ntask is not valid */
  memset(hgt, 0, sizeof(*hgt));
  hgt->hgshuf = sh;
  hgt->mcls = cls;
  hgt->mctx = ctx;

  /*
   * register with mercury.  XXX: HG_Register_name() can't fail.
   * there is no API to unregister an RPC other than shutting down
   * mercury, so hopefully HG_Register_data() can't fail...
   */
  hgt->rpcid = HG_Register_name(cls, sh->funname,
                 hg_proc_rpcin_t, hg_proc_rpcout_t,  rpchand);
  if (HG_Register_data(cls, hgt->rpcid, hgt, NULL) != HG_SUCCESS)
    return(-1);

  return(0);
}

/*
 * shuffler_flush_discard: discard allocated state for flush mgt
 *
 * @param sh shuffler previously init'd with shuffler_init_flush
 */
static void shuffler_flush_discard(struct shuffler *sh) {
  int nc = 0;
  struct flush_op *fop;
  mlog(UTIL_CALL, "shuffler_flush_discard");

  /* kill any pending flush ops (hopefully none) */
  pthread_mutex_lock(&sh->flushlock);
  while ((fop = XSIMPLEQ_FIRST(&sh->fpending)) != NULL) {
    XSIMPLEQ_REMOVE_HEAD(&sh->fpending, fq);
    fop->status = FLUSHQ_CANCEL;
    pthread_cond_signal(&fop->flush_waitcv);
    nc++;
  }

  if (sh->curflush) {
    sh->curflush->status = FLUSHQ_CANCEL;
    pthread_cond_signal(&sh->curflush->flush_waitcv);
    nc++;
  }
  pthread_mutex_unlock(&sh->flushlock);

  if (nc) {
    notify(UTIL_WARN|MLOG_STDERR,
           "shuffler: flush_discard canceled %d flush op(s)", nc);
    sleep(3);   /* yield to be safe since destroy flushlock is next */
  }
  pthread_mutex_destroy(&sh->flushlock);
}

/*
 * shuffler_init_flush: init flush op management fields in shuffler
 *
 * @param sh shuffler to init
 * @return success, normally
 */
static hg_return_t shuffler_init_flush(struct shuffler *sh) {
  mlog(UTIL_CALL, "shuffler_init_flush");
  XSIMPLEQ_INIT(&sh->fpending);
  sh->curflush = NULL;
  sh->flushdone = 0;
  sh->flushtype = FLUSH_NONE;
  sh->flushoset = NULL;

  if (pthread_mutex_init(&sh->flushlock, NULL) != 0)
    return(HG_NOMEM_ERROR);
  return(HG_SUCCESS);
}

/*
 * shuffler_init: init's the shuffler layer.
 */
shuffler_t shuffler_init(nexus_ctx_t nxp, char *funname,
           int lomaxrpc, int lobuftarget, int lrmaxrpc, int lrbuftarget,
           int rmaxrpc, int rbuftarget, int deliverq_max,
           shuffler_deliver_t delivercb) {
  int myrank, lcv, rv;
  shuffler_t sh;
  nexus_iter_t nit;

  myrank = nexus_global_rank(nxp);
  shuffler_openlog(myrank);

  mlog(SHUF_CALL,
  "shuffler_init maxrpc(lo/lr/r)=%d/%d/%d targ(lo/lr/r)=%d/%d/%d dqmax=%d",
       lomaxrpc, lrmaxrpc, rmaxrpc, lobuftarget, lrbuftarget,
       rbuftarget, deliverq_max);

  sh = new shuffler;    /* aborts w/std::bad_alloc on failure */

  /* make sure these oqflush_counters are not pointing at garbage */
  sh->local_orq.oqflush_counter = NULL;
  sh->local_rlq.oqflush_counter = NULL;
  sh->remoteq.oqflush_counter = NULL;

  sh->grank = myrank;
  for (lcv = 0 ; lcv < FLUSH_NTYPES ; lcv++) {
    shufzero(&sh->cntflush[lcv]);
  }
  shufzero(&sh->cntflushwait);
  shufzero(&sh->cntdblock);
  shufzero(&sh->cntdeliver);
  shufzero(&sh->cntdreqs[0]); shufzero(&sh->cntdreqs[1]);
  shufzero(&sh->cntdwait[0]); shufzero(&sh->cntdwait[1]);
  shufzero(&sh->cntdmaxwait);
  shufzero(&sh->cntrpcinshm);
  shufzero(&sh->cntrpcinnet);
  shufzero(&sh->cntstranded);

  sh->nxp = nxp;
  sh->funname = strdup(funname);
  sh->seqsrc = acnt32_alloc();
  if (!sh->funname || !sh->seqsrc)
    goto err;
  sh->disablesend = 0;
  sh->boottime = shuftime();

  nit = nexus_iter(nxp, 1);
  if (nit == NULL) goto err;
  rv = shuffler_init_outset(&sh->local_orq, lomaxrpc, lobuftarget, sh,
                            &sh->hgt_local, nit);
  nexus_iter_free(&nit);
  if (rv < 0) goto err;

  nit = nexus_iter(nxp, 1);
  if (nit == NULL) goto err;
  rv = shuffler_init_outset(&sh->local_rlq, lrmaxrpc, lrbuftarget, sh,
                            &sh->hgt_local, nit);
  nexus_iter_free(&nit);
  if (rv < 0) goto err;

  nit = nexus_iter(nxp, 0);
  if (nit == NULL) goto err;
  rv = shuffler_init_outset(&sh->remoteq, rmaxrpc, rbuftarget, sh,
                            &sh->hgt_remote, nit);
  nexus_iter_free(&nit);
  if (rv < 0) goto err;
  acnt32_set(sh->seqsrc, 0);

  /* init hg thread state (but don't start yet).  allocs hg rpcid */
  rv = shuffler_init_hgthread(sh, &sh->hgt_local, nexus_hgclass_local(nxp),
                              nexus_hgcontext_local(nxp), shuffler_rpchand);
  if (rv < 0) goto err;
  rv = shuffler_init_hgthread(sh, &sh->hgt_remote, nexus_hgclass_remote(nxp),
                              nexus_hgcontext_remote(nxp), shuffler_rpchand);
  if (rv < 0) goto err;

  sh->deliverq_max = deliverq_max;
  sh->delivercb = delivercb;
  if (pthread_mutex_init(&sh->deliverlock, NULL) != 0)
    goto err;
  if (pthread_cond_init(&sh->delivercv, NULL) != 0) {
    pthread_mutex_destroy(&sh->deliverlock);
    goto err;
  }
  sh->dflush_counter = 0;
  sh->dshutdown = sh->drunning = 0;

  if (shuffler_init_flush(sh) != HG_SUCCESS) {
    pthread_mutex_destroy(&sh->deliverlock);
    pthread_cond_destroy(&sh->delivercv);
    goto err;
  }

  /* now start our three worker threads */
  if (start_threads(sh) != 0) {
    pthread_mutex_destroy(&sh->deliverlock);
    pthread_cond_destroy(&sh->delivercv);
    shuffler_flush_discard(sh);
    goto err;
  }

  return(sh);

err:
  mlog(SHUF_D1, "shuffler_init: FAILED!!!");
  shuffler_outset_discard(&sh->local_orq);     /* ensures maps are empty */
  shuffler_outset_discard(&sh->local_rlq);
  shuffler_outset_discard(&sh->remoteq);
  if (sh->seqsrc) acnt32_free(&sh->seqsrc);
  if (sh->funname) free(sh->funname);
  delete sh;
  shuffler_closelog();
  return(NULL);
}

/*
 * start_threads: attempt to start our three worker threads
 *
 * @param sh the shuffler we are starting
 * @return 0 on success, -1 on error
 */
static int start_threads(struct shuffler *sh) {
  int rv;
  mlog(SHUF_CALL, "start_threads called");

  /* start delivery thread */
  rv = pthread_create(&sh->dtask, NULL, delivery_main, (void *)sh);
   if (rv != 0) {
     notify(SHUF_CRIT, "shuffler:start_threads: delivery_main failed");
     stop_threads(sh);
     return(-1);
   }
   sh->drunning = 1;

   /* start local na+sm thread */
  rv = pthread_create(&sh->hgt_local.ntask, NULL,
                      network_main, (void *)&sh->hgt_local);
  if (rv != 0) {
     notify(SHUF_CRIT, "shuffler:start_threads: na+sm main failed");
     stop_threads(sh);
     return(-1);
  }
  sh->hgt_local.nrunning = 1;

   /* start remote network thread */
  rv = pthread_create(&sh->hgt_remote.ntask, NULL,
                      network_main, (void *)&sh->hgt_remote);
  if (rv != 0) {
     notify(SHUF_CRIT, "shuffler:start_threads: net main failed");
     stop_threads(sh);
     return(-1);
  }
  sh->hgt_remote.nrunning = 1;

  mlog(SHUF_CALL, "start_threads SUCCESS!");
  return(0);
}

/*
 * stop_threads: stop all our worker threads.  this will prevent
 * any requests from progressing, so clear out the queues.
 *
 * @param sh shuffler
 */
static void stop_threads(struct shuffler *sh) {
  int stranded;
  mlog(SHUF_CALL, "stop_threads");

  /* stop network */
  if (sh->hgt_remote.nrunning) {
    mlog(SHUF_D1, "join remote");
    sh->hgt_remote.nshutdown = 1;
    pthread_join(sh->hgt_remote.ntask, NULL);
    sh->hgt_remote.nshutdown = 0;
  }

  /* stop na+sm */
  if (sh->hgt_local.nrunning) {
    mlog(SHUF_D1, "join local");
    sh->hgt_local.nshutdown = 1;
    pthread_join(sh->hgt_local.ntask, NULL);
    sh->hgt_local.nshutdown = 0;
  }

  /* stop delivery */
  if (sh->drunning) {
    mlog(SHUF_D1, "join delivery");
    pthread_mutex_lock(&sh->deliverlock);
    sh->dshutdown = 1;
    pthread_cond_broadcast(&sh->delivercv);
    pthread_mutex_unlock(&sh->deliverlock);
    pthread_join(sh->dtask, NULL);
    sh->dshutdown = 0;
  }

  /* look for stranded requests and warn about them */
  stranded = purge_reqs(sh);
  if (stranded > 0) {
    notify(SHUF_CRIT, "shuffler stop_threads: stranded %d reqs", stranded);
    shufadd(&sh->cntstranded, stranded);
  }
}

/*
 * purge_reqs: we've stopped the network and delivery so no more
 * progress is going to be made.  look for reqs that are still in
 * the system and clear them out.  return number of reqs we cleared
 * (hopefully zero).
 *
 * @param sh the shuffler to purge
 * @return number of items that got purged
 */
static int purge_reqs(struct shuffler *sh) {
  int rv = 0;
  struct request *req;
  mlog(SHUF_CALL, "purge_reqs");

  if (sh->drunning || sh->hgt_local.nrunning || sh->hgt_remote.nrunning) {
    notify(SHUF_CRIT, "ERROR!  purge_reqs called on active system?!!?");
    abort();   /* should never happen */
  }

  /* clear delivery queues */
  while (!sh->dwaitq.empty()) {
    req = sh->dwaitq.front();
    sh->dwaitq.pop_front();
    parent_dref_stopwait(sh, req->owner, 1);
    free(req);
    rv++;
  }
  while (!sh->deliverq.empty()) {
    req = sh->dwaitq.front();
    sh->dwaitq.pop_front();
    free(req);
    rv++;
  }

  /* clear local and remote queeus */
  rv += purge_reqs_outset(sh, &sh->local_orq);
  rv += purge_reqs_outset(sh, &sh->local_rlq);
  rv += purge_reqs_outset(sh, &sh->remoteq);

  mlog(SHUF_D1, "purg_reqs => result = %d", rv);
  return(rv);
}

/*
 * purge_reqs_outset: helper function purge_reqs() that clears an outset.
 * the threads should have been stopped prior to running this (so we are
 * not expecting concurrent access while we are tearing this down).
 *
 * @param sh the shuffler oset belongs to
 * @param oset set to purge
 * @return the number of stranded reqs in the outset
 */
static int purge_reqs_outset(struct shuffler *sh, struct outset *oset) {
  int rv = 0;
  std::map<hg_addr_t,struct outqueue *>::iterator it;
  struct outqueue *oq;
  struct request *req, *nxt;
  struct output *oput;
  mlog(UTIL_CALL, "purge_reqs_outset type=%s", outset_typstr(oset->settype));

  /* need to purge each output queue in the set */
  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

   /* stop flushing */
   if (oq->oqflushing) {
     oq->oqflushing = 0;
     oq->oqflush_waitcounter = 0;
     oq->oqflush_output = NULL;
     acnt32_decr(oset->oqflush_counter);
   }

   /* zap the wait queue */
    while (!oq->oqwaitq.empty()) {
      req = oq->oqwaitq.front();
      oq->oqwaitq.pop_front();
      parent_dref_stopwait(sh, req->owner, 1);
      free(req);
      rv++;
    }

    /* now zap the loading requests */
    XSIMPLEQ_FOREACH_SAFE(req, &oq->loading, next, nxt) {
      free(req);
      rv++;
    }

    /* and dump the requests in progress */
    while ((oput = XTAILQ_FIRST(&oq->outs)) != NULL) {
      XTAILQ_REMOVE(&oq->outs, oput, q);
      /*
       * XXX: what to do with outhand.  should we cancel it?  threads
       * are not running.   seems like we hold a ref we should drop
       * at any rate.
       */
      HG_Destroy(oput->outhand);
      free(oput);
    }
  }

  oset->osetflushing = 0;
  mlog(UTIL_D1, "purge_reqs_outset type=%s =RET=> %d",
       outset_typstr(oset->settype), rv);

  return(rv);
}

/*
 * delivery_main: main routine for delivery thread.  the delivery
 * thread does final delivery of messages to the application (via
 * the delivery callback).   we need this thread because the final
 * delivery can block (e.g. for flow control) and we don't want to
 * block our network threads because of it (since it would stop
 * traffic that we are a REP for).
 *
 * @param arg void* pointer to our shuffler
 */
static void *delivery_main(void *arg) {
  struct shuffler *sh = (struct shuffler *)arg;
  struct request *req;
  struct req_parent *parent;
  mlog(DLIV_CALL, "delivery_main running");

  pthread_mutex_lock(&sh->deliverlock);
  while (sh->dshutdown == 0) {
    if (sh->deliverq.empty()) {
      mlog(DLIV_D1, "queue empty, blocked");
      shufcount(&sh->cntdblock);
      (void)pthread_cond_wait(&sh->delivercv, &sh->deliverlock);
      mlog(DLIV_D1, "woke up after blocking");
      continue;
    }

    /*
     * start first entry of the queue -- this may block, so unlock
     * to allow other threads to append to the queues.   note that
     * this is the only thread that dequeues reqs from deliverq, so
     * it is safe to leave req at the front while we are running the
     * callback...
     */
    req = sh->deliverq.front();
    if (!req) {
      notify(DLIV_CRIT, "notified with empty deliverq?  not possible");
      abort();   /* shouldn't ever happen */
    }

    shufcount(&sh->cntdeliver);
    pthread_mutex_unlock(&sh->deliverlock);
    mlog(DLIV_D1, "deliver %d->%d t=%d, dl=%d req=%p",
         req->src, req->dst, req->type, req->datalen, req);
    /* note: may block in callback */
    sh->delivercb(req->src, req->dst, req->type, req->data, req->datalen);
    mlog(DLIV_D1, "deliver %p complete", req);
    pthread_mutex_lock(&sh->deliverlock);

    /* see if anyone is waiting for us to flush */
    if (sh->dflush_counter > 0) {
      sh->dflush_counter--;
      mlog(DLIV_D1, "drop dflush_counter to %d", sh->dflush_counter);
      if (sh->dflush_counter == 0) {   /* droped to 0, wake up flusher */
        if (sh->curflush)
          pthread_cond_signal(&sh->curflush->flush_waitcv);
      }
    }

    /* dispose of the req we just delivered */
    sh->deliverq.pop_front();
    if (req->owner)        /* should never happen */
      notify(DLIV_CRIT, "delivery_main: freeing req with owner!?!");
    free(req);
    req = NULL;

    /* just made space in deliveryq, see if we can advance one from waitq */
    if (sh->dwaitq.empty())
      continue;                 /* waitq empty, loop back up */

    /* move it to deliveryq */
    req = sh->dwaitq.front();
    sh->dwaitq.pop_front();
    sh->deliverq.push_back(req); /* deliverq should be full again */
    mlog(DLIV_D1, "promoted %p from dwaitq", req);

    /*
     * now we need to tell req's parent it can stop waiting.  since
     * we are holding the deliver lock (covers the dwaitq) we can
     * clear the owner to detach the req from the parent.   then
     * we need to call parent_dref_stopwait() to drop the parent's
     * reference counter.
     *
     * XXX: be safe and drop deliverlock when calling parent_dref_stopwait().
     * normally parent_dref_stopwait() will just drop the reference count and
     * if it drops to zero it will call HG_Reply (if parent->input !NULL)
     * pthread_cond_signal (if parent->input == NULL).  the main worry
     * is HG_Reply() since that code is external to us and we can't
     * know what it (or any mercury NA layer under it) will do.
     */
    parent = req->owner;
    req->owner = NULL;
    pthread_mutex_unlock(&sh->deliverlock);
    parent_dref_stopwait(sh, parent, 0);
    pthread_mutex_lock(&sh->deliverlock);
  }
  sh->drunning = 0;
  pthread_mutex_unlock(&sh->deliverlock);

  mlog(DLIV_CALL, "delivery_main exiting");
  return(NULL);
}

/*
 * parent_dref_stopwait: drop parent reference count and stop waiting
 * if we've dropped the last reference
 *
 * @param sh our shuffler
 * @param parent waiting parent of request that is no longer waiting
 * @param abort cancels further processing of parent (e.g. for error handling)
 */
static void parent_dref_stopwait(struct shuffler *sh, struct req_parent *parent,
                                 int abort) {
  int nw;

  if (parent == NULL) {
    /* this should never happen */
    notify(SHUF_CRIT, "parent_dref_stopwait: ERROR - waiting req w/no parent");
    return;
  }

  /* atomically drop the reference counter and get new value */
  nw = acnt32_decr(parent->nrefs);
  if (parent->input)
    mlog(SHUF_CALL, "parent_dref_stopwait %p R%d-%d new-nref=%d", parent,
         parent->rpcin_forwrank, parent->rpcin_seq, nw);
  else
    mlog(SHUF_CALL, "parent_dref_stopwait %p CLI new-nref=%d", parent, nw);


  if (nw > 0) {   /* still active reqs, let it keep waiting ... */
    return;
  }

  parent_stopwait(sh, parent, abort);
}


/*
 * parent_stopwait: we had a req on a waitq (i.e. a parent waiting on it)
 * and it finished.  we removed the req from the waitq and dropped the
 * ref count.   the ref count has dropped to zero, so we need to unblock
 * the parent and set it up for disposal.
 *
 * NOTE: caller must ensure that assume that nrefs is < 1
 *
 * @param sh our shuffler
 * @param parent waiting parent of request that is no longer waiting
 * @param abort cancels further processing of parent (e.g. for error handling)
 */
static void parent_stopwait(struct shuffler *sh, struct req_parent *parent,
                            int abort) {
  rpcout_t reply;
  hg_return_t rv;

  mlog(SHUF_CALL, "parent_stopwait: %p input?=%d abort=%d", parent,
       (parent && parent->input != NULL), abort);

  if (parent == NULL) {
    /* this should never happen */
    notify(SHUF_CRIT, "stopwait: ERROR - waiting req w/no parent");
    return;
  }

  /*
   * parent no longer has any active refs besides us, so no thread other
   * than us can access it anymore (meaning it is safe to access it
   * without additional locking).
   */

  /* set a non-success code if we are aborting ... */
  if (abort)
    parent->ret = HG_CANCELED;

  /*
   * we've launched the last waiting request, so now the parent no
   * longer needs to wait.   we have two types of parents: the
   * application process or an input hg_handle_t.
   *
   * if the parent is the application process (input==NULL), it is
   * waiting on parent->pcv and needs to be woken up (this can only
   * happen when sending with SRC == DST and the app is flow controlled).
   * the application will free the req_parent.
   */
  if (parent->input == NULL) {
    pthread_mutex_lock(&parent->pcvlock);
    parent->need_wakeup = 0;   /* XXX: needed?  vs. nrefs==0 */
    mlog(SHUF_D1, "parent_stopwait: wake parent %p", parent);
    pthread_cond_signal(&parent->pcv);
    pthread_mutex_unlock(&parent->pcvlock);
    return;
  }

  /*
   * ok, the parent is a flow controlled hg_handle_t that we can
   * now respond to.   once we've stopped the wait, we can dispose
   * of the req_parent (we have the only reference to it now, so we
   * can just free it).
   */
  reply.oseq = parent->rpcin_seq;
  reply.respondrank = sh->grank;
  reply.ret = parent->ret;

  /* only respond if we are not aborting */
  if (!abort) {
    mlog(SHUF_D1, "parent_stopwait: RPC respond %d %p R%d-%d",
         reply.ret, parent, parent->rpcin_forwrank, parent->rpcin_seq);
    rv = HG_Respond(parent->input, shuffler_respond_cb, parent, &reply);
  } else {
    rv = HG_CANCELED;
  }

  if (rv != HG_SUCCESS) {
    struct hg_cb_info cbi;   /* fake, for err/abort! */
    if (!abort) {
      notify(SHUF_WARN, "parent_stopwait: %p reply failed %d!", parent, rv);
    }
    /* note: we know shuffler_respond_cb() only looks at cbi.arg */
    cbi.arg = parent;
    rv = shuffler_respond_cb(&cbi);
    /* ignore return value */
  }
}

/*
 * shuffler_respond_cb: we've either send a response or we tried
 * to send a response and failed.  either way, we need to do final
 * cleanup.  note that we only use the field cbi->arg in cbi (in
 * the failure case we are getting a fake cbi, see above).
 *
 * @param cbi the arg for the callback
 * @return success
 */
static hg_return_t shuffler_respond_cb(const struct hg_cb_info *cbi) {
  struct req_parent *parent = (struct req_parent *)cbi->arg;
  mlog(SHUF_CALL, "shuffler_respond_cb parent=%p", parent);

  /*
   * XXX: if we want to cache a list of free parent structures,
   * we could do it here...
   */

  HG_Destroy(parent->input);
  acnt32_free(&parent->nrefs);
  free(parent);

  return(HG_SUCCESS);
}

/*
 * network_main: network support pthread.   need to call progress to
 * push the network and then trigger to run the callback.  we do this
 * all in one thread (meaning that we shouldn't block in the trigger
 * function, or we won't make progress)
 *
 * @param arg void* pointer to our outset
 */

static void *network_main(void *arg) {
  struct hgthread *hgt = (struct hgthread *)arg;
  hg_return_t ret;
  unsigned int actual;

  mlog(SHUF_CALL, "network_main start (local=%d)",
       (hgt == &hgt->hgshuf->hgt_local));
  while (hgt->nshutdown == 0) {

    do {
      ret = HG_Trigger(hgt->mctx, 0, 1, &actual); /* triggers callbacks */
      shufcount(&hgt->ntrigger);
    } while (ret == HG_SUCCESS && actual);
    if (ret != HG_SUCCESS && ret != HG_TIMEOUT) {
      notify(SHUF_CRIT, "ERROR! calling HG_Trigger returning error: %s(%d)",
          HG_Error_to_string(ret), int(ret));
      abort();
    }

    ret = HG_Progress(hgt->mctx, 100);
    if (ret != HG_SUCCESS && ret != HG_TIMEOUT) {
      notify(SHUF_CRIT, "ERROR! calling HG_Progress returning error: %s(%d)",
              HG_Error_to_string(ret), int(ret));
      abort();
    }

    shufcount(&hgt->nprogress);
  }
  mlog(SHUF_CALL, "network_main exiting (local=%d)",
       (hgt == &hgt->hgshuf->hgt_local));

  hgt->nrunning = 0;
  return(NULL);
}

/*
 * req_parent_init: helper function called when we need to attach a
 * req to an init'd req_parent structure.   if the structure is already
 * init'd, we just bump the req_parent's reference count and set the
 * req's owner.   otherwise, we are starting a 0ew req_parent.
 * this happens when we send a req on a full queue and need to wait
 * on a waitq or on dwaitq.  it also happens when we recv an inbound
 * RPC handle (i.e. input != NULL) that contains a req that needs to
 * wait on a waitq or on dwaitq (e.g. before doing HG_Respond()).
 *
 * note: "parentp" is a pointer to a pointer.  if *parentp is NULL, we
 * will malloc a new req_parent structure.
 *
 * @param sh the shuffler we are working with
 * @param parentp ptr to ptr to the req_parent to init
 * @param req the request that we are waiting on
 * @param input inbound RPC handle (NULL if we are an app shuffler_send())
 * @param rpcin inbound rpcin_t struct (only used if input != NULL)
 * @return status (normally success)
 */
static hg_return_t req_parent_init(struct shuffler *sh,
                                   struct req_parent **parentp,
                                   struct request *req, hg_handle_t input,
                                   rpcin_t *rpcin) {
  struct req_parent *parent;

  parent = *parentp;
  if (rpcin)
    mlog(SHUF_CALL, "req_parent_init: parent=%p req=%p R%d-%d", parent, req,
         rpcin->forwardrank, rpcin->iseq);
  else
    mlog(SHUF_CALL, "req_parent_init: parent=%p req=%p CLI", parent, req);

  /* can just bump nrefs for RPCs w/previously allocated parent */
  if (input && parent) {
    acnt32_incr(parent->nrefs);  /* just add a reference */
    req->owner = parent;
    return(HG_SUCCESS);
  }

  /* parent should be NULL only if we are handling an inbound RPC in input */
  if (parent == NULL) {
    if (input == NULL) {   /* sanity check: should be an input RPC */
      notify(SHUF_CRIT, "shuffler: req_parent_init usage error");
      return(HG_INVALID_PARAM);  /* should never happen */
    }
    /* NOTE: we only malloc() parent if input != NULL */
    parent = (struct req_parent *)malloc(sizeof(*parent));
    if (parent) {
      parent->nrefs = acnt32_alloc();
      if (parent->nrefs == NULL) {
        free(parent);
        parent = NULL;
      }
    }
    mlog(SHUF_D1, "req_parent_init: malloc parent=%p for %p", parent, req);
    if (parent == NULL) {
      return(HG_NOMEM_ERROR);
    }
    *parentp = parent;
  }

  /*
   * if we are doing a shuffler_send(), then input is NULL and the
   * caller has provided us a parent structure (we never malloc it
   * in this case).   we need to init nrefs.  also, we need pcv/pcvlock
   * setup in this case (they are only used when input == NULL).
   */
  if (input == NULL) {
    mlog(SHUF_D1, "req_parent_init: caller parent=%p for %p", parent, req);

    /* setup nrefs */
    parent->nrefs = acnt32_alloc();
    if (parent->nrefs == NULL) {
      return(HG_OTHER_ERROR);
    }

    /* set up mutex/cv */
    if (pthread_mutex_init(&parent->pcvlock, NULL)) {  /* only for pcv */
      acnt32_free(&parent->nrefs);
      return(HG_OTHER_ERROR);
    }
    if (pthread_cond_init(&parent->pcv, NULL)) {
      pthread_mutex_destroy(&parent->pcvlock);
      acnt32_free(&parent->nrefs);
      return(HG_OTHER_ERROR);
    }
  }

  /*
   * we always set the initial value for nrefs to 2.  one for the
   * req we are adding, and one for our caller in order to hold
   * the req_parent in memory until we are completely done with it.
   * (want to avoid unlikely case where RPC completes before we
   * start waiting for the result..)
   */
  acnt32_set(parent->nrefs, 2);
  parent->ret = HG_SUCCESS;
  if (rpcin) {
    parent->rpcin_seq = rpcin->iseq;
    parent->rpcin_forwrank = rpcin->forwardrank;
  } else {
    parent->rpcin_seq = parent->rpcin_forwrank = -1;  /* inited, but !used */
  }
  parent->input = input;
  parent->timewstart = shuftime() - sh->boottime;
  parent->need_wakeup = (input == NULL) ? 1 : 0;
  parent->onfq = 0;
  parent->fqnext = NULL;    /* to be safe */

  /* parent now owns req */
  req->owner = parent;

  return(HG_SUCCESS);
}



/*
 * shuffler_send: start the sending of a message via the shuffle.
 */
hg_return_t shuffler_send(shuffler_t sh, int dst, int type,
                          void *d, int datalen) {
  nexus_ret_t nexus;
  int rank;
  hg_addr_t dstaddr;
  struct request *req;
  struct req_parent parent_store, *parent;
  hg_return_t rv;
  struct outset *oset;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;

  mlog(CLNT_CALL, "shuffler_send: dst=%d t=%d dl=%d", dst, type, datalen);

  /* first, check to see if send is generally disabled */
  if (sh->disablesend)
    return(HG_OTHER_ERROR);

  /* determine next hop */
  nexus = nexus_next_hop(sh->nxp, dst, &rank, &dstaddr);

  /*
   * we always have to malloc and copy the data from the user to one
   * of our buffers because we return to the sender before the is
   * complete (and we don't want to sender to reuse the buffer before
   * we are done with it).
   *
   * XXX: for output queues that have room, it would be nice if we
   * could directly copy into their hg_handle_t buffer as we receive
   * new requests until the hg_handle_t is full and ready to be
   * send, but mercury doesn't give us an API to do that (we've got
   * HG_Forward() which takes an unpacked set of requests and packs
   * them all at once... there is no way to incrementally add data).
   */
  req = (struct request *) malloc(sizeof(*req) + datalen);
  if (req == NULL) {
    mlog(CLNT_ERR, "shuffler_send: dst=%d dl=%d malloc failed", dst, datalen);
    return(HG_NOMEM_ERROR);
  }
  mlog(CLNT_D1, "shuffler_send: %d->%d nexus=%d rank=%d addr=%p req=%p",
       sh->grank, dst, nexus, rank, dstaddr, req);

  req->datalen = datalen;
  req->type = type;
  req->src = sh->grank;
  req->dst = dst;
  req->data = (char *)req + sizeof(*req);
  memcpy(req->data, d, datalen);    /* DATA COPY HERE */
  req->owner = NULL;
  req->next.sqe_next = NULL;        /* to be safe */

  /* case 1: sending to ourselves */
  if (nexus == NX_DONE || req->src == dst) {

    parent = &parent_store;
    parent->nrefs = NULL;
    mlog(CLNT_D1, "shuffler_send: req=%p to self", req);
    rv = req_to_self(sh, req, NULL, NULL, &parent);  /* can block */
    return(rv);
  }

  /* case 2: not for us, sending request over mercury */

  /*
   * we are the SRC.  possible sub-cases:
   *  NX_ISLOCAL: dst is on local machine, use na+sm to send it
   *  NX_SRCREP: dst is remote, use na+sm to send to remote's SRCREP
   *  NX_DESTREP: dst is remote, we are SRCREP, send over network
   */
  if (nexus != NX_ISLOCAL && nexus != NX_SRCREP && nexus != NX_DESTREP) {
    /* nexus doesn't know dst, return error */
    mlog(CLNT_ERR, "shuffler_send: bogus nexus value %d", nexus);
    return(HG_INVALID_PARAM);
  }

  /*
   * need to find correct output queue for dstaddr.  for the local
   * queues, shuffler_send always goes to the origin (local_orq) outset.
   */
  oset = (nexus == NX_DESTREP) ? &sh->remoteq : &sh->local_orq;
  it = oset->oqs.find(dstaddr);
  if (it == oset->oqs.end()) {
    /*
     * nexus knew the addr, but we couldn't find a a queue!
     * this should not happen!!!
     */
    mlog(CLNT_ERR, "shuffler_send: no route to dst %d", dst);
    return(HG_INVALID_PARAM);
  }

  oq = it->second;    /* now we have the correct output queue */

  parent = &parent_store;
  parent->nrefs = NULL;
  rv = req_via_mercury(sh, oset, oq, req, NULL, NULL, &parent); /* can block */

  return(rv);
}

/*
 * drop_reqs: helper fn that drops and frees requests that can't be
 * sent due to some unexpected error.  we route though this function
 * to put the error handling in one place and make it easy to find
 * places where data is dropped.   the reqs should not be attached
 * to any parent (or we'll lose a reference).  we print an errmsg
 * if 'msg' is not NULL (if it is null, the caller should print the msg).
 *
 * @param reqp ptr to request to free (or null).  we set to null
 * @param reqq list of reqs to free (or null).  re-init'd to empty
 * @param msg err msg string, if NULL we don't print anything
 */
void drop_reqs(struct request **reqp, struct request_queue *reqq,
               const char *msg) {
  struct request *rp, *nrp;
  int owned;

  if (reqp) {
    rp = *reqp;
    owned = (rp->owner != NULL);
    if (msg) {
      notify(SHUF_CRIT, "drop_reqs: drop %p(o=%d) due to err (%s), data LOST!",
           rp, owned, msg);
    }
    free(rp);
    *reqp = NULL;
  }

  if (reqq) {
    XSIMPLEQ_FOREACH_SAFE(rp, reqq, next, nrp) {
      owned = (rp->owner != NULL);
      if (msg) {
        notify(SHUF_CRIT,
            "drop_reqs: drop %p(O=%d) due to err (%s) - data LOST!",
             rp, owned, msg);
      }
      free(rp);
    }
    XSIMPLEQ_INIT(reqq);
  }

}

/*
 * req_to_self: sending/forward a req to ourself via the delivery thread.
 *
 * for apps sending (i.e. input==NULL, we are called via shuffler_send())
 * we will block if the delivery queue is full.  for blocking we'll use a
 * req_parent's condvar provided by the caller to block.  the caller
 * typically allocates the req_parent on the stack.
 *
 * for reqs generated by the shuffler_rpchand() callback function
 * (input != NULL), if we get put on a wait queue we'll let
 * req_parent_init() malloc a req_parent to save the input handle
 * on until.   we'll hold the HG_Respond() call until all reqs in
 * the input batch have cleared the waitqs.
 *
 * as noted in req_parent_init(), freshly malloc'd req_parent structures
 * have their reference count set to 2 (one for the RPC and one for us
 * to hold the structure in memory until we've launched everything).
 *
 * all this blocking structure is in place to support write back
 * buffering with flow control in all cases.
 *
 * rules: if input is NULL, then this is part of a "send" operation
 * and the caller provides a req_parent (typically stack allocated)
 * in parentp.  otherwise, if input is not NULL then this is part of
 * a "forward" operation and a req_parent is malloc'd on demand and
 * placed in parentp.
 *
 * if this fails, we free the request (what else can we do?) which
 * means it gets dropped ...
 *
 * @param sh the shuffler involved
 * @param req the request to send/forward to self
 * @param input the inbound handle that generated the req
 * @param rpcin ptr to the rcpin value of the inbound req (input != NULL case)
 * @param parentp parent ptr (will allocate a new one if needed)
 * @return status
 */
static hg_return_t req_to_self(struct shuffler *sh, struct request *req,
                               hg_handle_t input, rpcin_t *rpcin,
                               struct req_parent **parentp) {
  hg_return_t rv = HG_SUCCESS;
  int qsize, needwait;
  struct req_parent *parent;
  if (rpcin)
    mlog(SHUF_CALL, "req_to_self req=%p, handle=%p R%d-%d", req, input,
         rpcin->forwardrank, rpcin->iseq);
  else
    mlog(SHUF_CALL, "req_to_self req=%p, handle=%p CLI", req, input);

  /* this allows delivery to be turned off for debugging... */
  if (sh->deliverq_max < 0) {
    mlog(SHUF_D1, "req_to_self: req=%p discarded (delivery disabled)", req);
    free(req);
    return(rv);
  }

  pthread_mutex_lock(&sh->deliverlock);
  qsize = sh->deliverq.size();
  needwait = (qsize >= sh->deliverq_max); /* wait if no room in deliverq */
  shufcount(&sh->cntdreqs[input != NULL]);

  if (!needwait) {

    /* easy!  just queue and wake delivery thread (if needed) */
    mlog(SHUF_D1, "req_to_self: deliverq req=%p qsize=%d", req, qsize);
    sh->deliverq.push_back(req);
    if (qsize == 0)
      pthread_cond_signal(&sh->delivercv);  /* empty->!empty: wake thread */

  } else {

    /* sad!  we need to block on the waitq for delivery ... */
    shufcount(&sh->cntdwait[input != NULL]);
    rv = req_parent_init(sh, parentp, req, input, rpcin);

    if (rv == HG_SUCCESS) {
      mlog(SHUF_D1, "req_to_self: dwaitq! req=%p parent=%p", req, req->owner);
      sh->dwaitq.push_back(req); /* add req to wait queue */
      shufmax(&sh->cntdmaxwait, sh->dwaitq.size());
    } else {
      notify(SHUF_CRIT, "shuffler: req_to_self parent init failed (%d)", rv);
      drop_reqs(&req, NULL, "req_to_self"); /* error means we can't send it */
    }

  }
  pthread_mutex_unlock(&sh->deliverlock);

  /*
   * if we are sending (!input) and need to wait, we'll block here.
   */
  if (!input && needwait && rv == HG_SUCCESS) {   /* wait now if needed */
    parent = *parentp;

    pthread_mutex_lock(&parent->pcvlock);
    /* drop extra parent ref created by req_parent_init() before waiting */
    acnt32_decr(parent->nrefs);
    while (acnt32_get(parent->nrefs) > 0) {
      mlog(CLNT_D1, "req_to_self: blocked! req=%p, parent=%p", req, parent);
      pthread_cond_wait(&parent->pcv, &parent->pcvlock);  /*BLOCK HERE*/
      mlog(CLNT_D1, "req_to_self: UNblocked! req=%p, parent=%p", req, parent);
    }
    pthread_mutex_unlock(&parent->pcvlock);

    /*
     * we are done now, since the thread that woke us up also
     * should have pulled our req off the dwaitq and put it in
     * the delivery queue.
     */

    acnt32_free(&parent->nrefs);
    pthread_cond_destroy(&parent->pcv);
    pthread_mutex_destroy(&parent->pcvlock);
  }

  /* done! */
  return(rv);
}

/*
 * req_via_mercury: send a req via mercury.  as usual there are two
 * cases: input == NULL: app sending directly via shuffler_send()
 *        input != NULL: forwarding req recv'd via mercury RPC
 *
 * flow control blocking is handled the same way as req_to_self()
 * (see discussion above).
 *
 * @param sh the shuffler we are sending with
 * @param oset the output queue set we are using
 * @param oq the output queue to use
 * @param req the request to send
 * @param input input RPC handle (null if via app shuffler_send call)
 * @param rpcin ptr to the rcpin value of the inbound req (input != NULL case)
 * @param parentp parent ptr (will allocate a new one if needed)
 * @return status, normally success
 */
static hg_return_t req_via_mercury(struct shuffler *sh, struct outset *oset,
                                   struct outqueue *oq, struct request *req,
                                   hg_handle_t input, rpcin_t *rpcin,
                                   struct req_parent **parentp) {
  hg_return_t rv = HG_SUCCESS;
  int needwait;
  bool tosend;
  struct request_queue tosendq;
  struct output *oput;
  struct req_parent *parent;

  if (rpcin)
    mlog(SHUF_CALL, "req_via_mercury: req=%p type=%s r=[%d.%d] dst=%p R%d-%d",
         req, outset_typstr(oset->settype), oq->grank, oq->subrank, oq->dst,
         rpcin->forwardrank, rpcin->iseq);
  else
    mlog(SHUF_CALL, "req_via_mercury: req=%p type=%s rnk=[%d.%d] dst=%p CLI",
         req, outset_typstr(oset->settype), oq->grank, oq->subrank, oq->dst);

  pthread_mutex_lock(&oq->oqlock);
  needwait = (oq->nsending >= oset->maxrpc);
  tosend = false;
  shufcount(&oq->cntoqreqs[input != NULL]);

  if (!needwait) {

    /* we can start sending this req now, no need to wait */
    mlog(SHUF_D1, "req_via_mercury: !needwait, send req=%p", req);
    tosend = append_req_to_locked_outqueue(oset, oq, req,
                                           &tosendq, &oput, false);

  } else {

    /* sad!  we need to block on the output queue till it clears some */
    shufcount(&oq->cntoqwaits[input != NULL]);
    rv = req_parent_init(sh, parentp, req, input, rpcin);

    if (rv == HG_SUCCESS) {
      mlog(SHUF_D1, "req_via_mercury: oqwaitq, req=%p, parent=%p",
           req, req->owner);
      oq->oqwaitq.push_back(req); /* add req to oq's waitq */
      shufmax(&oq->cntoqmaxwait, oq->oqwaitq.size());
    } else {
      notify(SHUF_CRIT, "shuffler: req_via_mercury parent init failed (%d)",
              rv);
      drop_reqs(&req, NULL, "req_via_mercury"); /* error, can't send it */
    }
  }
  pthread_mutex_unlock(&oq->oqlock);

  if (tosend) {   /* have a batch ready to send? */

    mlog(SHUF_D1, "req_via_mercury: got a batch to send now!");
    rv = forward_reqs_now(&tosendq, sh, oset, oq, oput);

  } else if (!input && needwait && rv == HG_SUCCESS) { /* wait now if needed */
    parent = *parentp;

    pthread_mutex_lock(&parent->pcvlock);
    /* drop extra parent ref created by req_parent_init() before waiting */
    acnt32_decr(parent->nrefs);
    while (acnt32_get(parent->nrefs) > 0) {
      mlog(CLNT_D1, "req_via_mercury: blocking req=%p, parent=%p", req, parent);
      pthread_cond_wait(&parent->pcv, &parent->pcvlock);  /* BLOCK HERE */
      mlog(CLNT_D1, "req_via_mercury: UNblock req=%p, parent=%p", req, parent);
    }
    pthread_mutex_unlock(&parent->pcvlock);

    /*
     * we are done now, since the thread that woke us up also
     * should have pulled our req off the waitq and set it up
     * for sending.
     */

    acnt32_free(&parent->nrefs);
    pthread_cond_destroy(&parent->pcv);
    pthread_mutex_destroy(&parent->pcvlock);
    mlog(CLNT_D1, "req_via_mercury: req=%p complete", req);
  }

  /* done! */
  return(rv);
}

/*
 * append_req_to_locked_outqueue: append a req to a locked output
 * queue.  this may result in a message that we need to forward
 * (e.g. if we fill a batch or if we are flushing).  in that case
 * we bump nsending, allocate an output structure (with a null
 * handle, that will be allocated later), put the output structure
 * on the outs list, generate a list of reqs to send, and return
 * TRUE.  note that req is allowed to be NULL (e.g. if we just
 * want to flush and are not sending anything).
 *
 * if we encounter an allocation error (i.e. malloc fails),
 * we have no choice but to drop requests and complain about
 * it since data will be lost.
 *
 * @param oset the output set that our outq belongs to
 * @param oq the locked output queue (we've already checked for room)
 * @param req the request to append to the queue (NULL is ok)
 * @param tosend a queue of requests ready to send (OUT, if ret true)
 * @param newoutputp output struct for tosend (OUT, if ret is true)
 * @param flushnow don't wait for buftarget bytes, flush now
 * @return true a list of requests to send is in "tosend"
 */
static bool append_req_to_locked_outqueue(struct outset *oset,
                                          struct outqueue *oq,
                                          struct request *req,
                                          struct request_queue *tosend,
                                          struct output **newoutputp,
                                          bool flushnow) {
  struct request *rv;
  int newloadsize;
  struct output *newoutput;
  mlog(SHUF_CALL, "append_to_locked: req=%p, dst=%p, flush=%d",
       req, oq->dst, flushnow == true);

  /* what is new loadsize?  it may not change if req is null */
  newloadsize = (req) ? oq->loadsize + req->datalen : oq->loadsize;

  /*
   * see if there is enough space in loading for us to just queue
   * the req (if not NULL) and return without doing anything else.
   * if we are flushing then we have to send now if we have anything.
   */
  if (newloadsize == 0 ||
      (newloadsize < oset->buftarget && !flushnow) ) {
    if (req) {
      XSIMPLEQ_INSERT_TAIL(&oq->loading, req, next);
      oq->loadsize = newloadsize;
    }
    mlog(SHUF_D1, "append_to_locked: still room dst=%p, sz=%d, targ=%d",
         oq->dst, oq->loadsize, oset->buftarget);
    return(false);
  }

  /*
   * now we know we need to send something.  we need to allocate an
   * output structure, add the current request in (if not NULL),
   * bump nsending, return back the output and list of reqs to send,
   * and finally reset loading.   if the allocation of the output
   * structure fails we are in a bad place and discard reqs (so we
   * drop data!).  we complain loudly if we have to do this.
   */
  newoutput = (struct output *) malloc(sizeof(*newoutput));
  if (newoutput == NULL) {
    mlog(SHUF_ERR, "append_to_locked malloc failed!  data likely lost!");
    if (flushnow) {
      drop_reqs(&req, &oq->loading, "append_to_locked (f)");
      oq->loadsize = 0;
    } else {
      drop_reqs(&req, NULL, "append_to_locked");
    }
    return(false);
  }

  /* init output and put on the oq */
  newoutput->oqp = oq;
  newoutput->outhand = NULL;
  newoutput->ostep = OSTEP_PREP;    /* preparing, not sent yet */
  newoutput->outseq = -1;           /* not available yet */
  XTAILQ_INSERT_TAIL(&oq->outs, newoutput, q);
  *newoutputp = newoutput;

  /*
   * bump nsending, pass back list of reqs to send, and reset loading
   * list...
   */
  XSIMPLEQ_INIT(tosend);
  XSIMPLEQ_CONCAT(tosend, &oq->loading);
  if (req) {
    XSIMPLEQ_INSERT_TAIL(tosend, req, next);
  }
  /* note: "CONCAT" re-init's &oq->loading to empty */
  oq->loadsize = 0;
  oq->nsending++;
  shufcount(&oq->cntoqsends);
  if (req == NULL && flushnow)
    shufcount(&oq->cntoqflushsend);  /* sent early due to flush */

  mlog(SHUF_D1, "append_to_locked: send NOW dst=%p nsending=%d",
       oq->dst, oq->nsending);
  return(true);
}

/*
 * forward_reqs_now: actually send a batch of requests now.  oq->nsending
 * has already been bumped up and an output struct has been allocated
 * and placed on the outs list, but we hold the only reference to the
 * reqs in the tosend request_queue.   if we fail we have to drop nsending
 * and remove the output from the list.
 *
 * @param tosend a list of reqs to send
 * @param sh shuffler we are sending with
 * @param oset the output queue set we are working with
 * @param oq the output queue we are sending on
 * @param oput the freshly allocated output for the send
 * @return status (hopefully success)
 */
static hg_return_t forward_reqs_now(struct request_queue *tosend,
                                    struct shuffler *sh, struct outset *oset,
                                    struct outqueue *oq, struct output *oput) {
  hg_return_t rv = HG_SUCCESS;
  hg_handle_t newhand = NULL;
  rpcin_t in;
  struct request *rp, *nrp;

  mlog(SHUF_CALL, "forward_now: to dst=%p", oq->dst);

  /* always rehome the requests to in */
  XSIMPLEQ_INIT(&in.inreqs);
  XSIMPLEQ_CONCAT(&in.inreqs, tosend);

  /* allocate new handle */
  rv = HG_Create(oset->myhgt->mctx, oq->dst, oset->myhgt->rpcid, &newhand);
  mlog(SHUF_CALL, "forward_now: output=%p rnk=[%d.%d] %s dst=%p hand=%p",
       oput, oq->grank, oq->subrank, outset_typstr(oq->myset->settype),
       oq->dst, newhand);

  /* install in output structure */
  if (rv == HG_SUCCESS) {
    pthread_mutex_lock(&oq->oqlock);
    switch (oput->ostep) {
      case OSTEP_CANCEL:
        HG_Destroy(newhand);
        rv = HG_CANCELED;
        break;
      case OSTEP_PREP:
        oput->outhand = newhand;
        oput->ostep = OSTEP_SEND;
        oput->outseq = acnt32_incr(sh->seqsrc);
        oput->timestart = shuftime() - sh->boottime;

        /* also init "in" since we are going to forward now */
        in.iseq = oput->outseq;
        in.forwardrank = sh->grank;

        break;
      default:   /* should never happen */
        notify(SHUF_CRIT, "forward_now: BAD STEP %d", oput->ostep);
        abort();
    }
    pthread_mutex_unlock(&oq->oqlock);
  }

  if (rv == HG_SUCCESS) {
    mlog(SHUF_D1, "forward_now: HG_Forward R%d-%d to [%d.%d] dst=%p",
         in.forwardrank, in.iseq, oq->grank, oq->subrank, oq->dst);
    rv = HG_Forward(oput->outhand, forw_cb, oput, &in);   /* SEND HERE! */
  }

  /* look for error (e.g. setup failed or HG_Forward failed) */
  if (rv != HG_SUCCESS) {
    /*
     * terrible!  we failed to forward.  no pretty way to recover,
     * we have to drop the data ...
     */
    notify(SHUF_CRIT, "forward request failed (%d)!  data likely lost!", rv);
    drop_reqs(NULL, &in.inreqs, "forward_reqs_now");
    forw_start_next(oq, oput);

  } else {

    /* success!  the data was copied to the handle, so we can free reqs */
    XSIMPLEQ_FOREACH_SAFE(rp, &in.inreqs, next, nrp) {
      free(rp);
    }
  }

  return(rv);
}

/*
 * forw_cb: normally the callback from an HG_Forward() operation
 * (runs in the context of the network thread via HG_Trigger()).
 * also directly called from forward_reqs_now() on an error.
 * our job is to mark this send as complete (dropping nsending) and
 * then see if there is anything on the wait queue that we can
 * advance (now that we just made space).
 *
 * @param cbi callback info (our arg, handle)
 * @return success
 */
static hg_return_t forw_cb(const struct hg_cb_info *cbi) {
  struct output *oput = (struct output *)cbi->arg;
  hg_handle_t hand;
  rpcout_t out;

  mlog(SHUF_CALL, "forw_cb: oput=%p success=%d", oput, cbi->ret == HG_SUCCESS);

  if (cbi->type != HG_CB_FORWARD) {
    notify(SHUF_CRIT, "cbi->type != FORWARD, impossible!");
    abort();
  }
  if (cbi->ret != HG_SUCCESS) {
    notify(SHUF_CRIT, "shuffle: forw_cb() failed (%d) - lost data?", cbi->ret);
  }
  hand = cbi->info.forward.handle;

  if (hand && cbi->ret == HG_SUCCESS) {
    if (HG_Get_output(hand, &out) != HG_SUCCESS) {
      /* shouldn't ever happen, output is just 3 numbers */
      notify(SHUF_CRIT, "shuffler: forw_cb: get output failed");
    } else {
      if (out.ret != HG_SUCCESS) {
        notify(SHUF_CRIT, "shuffler: forw_cb: RPC %d failed (%d)",
          out.oseq, out.ret);
      }
      HG_Free_output(hand, &out);
    }
  }

  /* destroy handle, drop nsending, and start next req */
  forw_start_next(oput->oqp, oput);

  return(HG_SUCCESS);
}

/*
 * forw_start_next: we have finished processing a handle (success
 * or failure) and need to destroy the handle, remove anything we
 * sent from the queues, drop nsending, and then start anything on
 * the waitq that can go.
 *
 * @param oq the output queue we are working on
 * @param oput output we just sent (can be NULL if we had an error)
 */
static void forw_start_next(struct outqueue *oq, struct output *oput) {
  bool tosend, flush_done, flushloadingnow, empty_outs;
  struct request_queue tosendq;
  struct output *nxtoput;
  struct req_parent *fq, **fq_end, *parent, *nparent;
  struct request *req;

  if (oput)
    mlog(SHUF_CALL, "forw_start_next: to=[%d.%d] %s dst=%p, oput=%p (R%d-%d)",
       oq->grank, oq->subrank, outset_typstr(oq->myset->settype), oq->dst,
       oput, oq->myset->shuf->grank, oput->outseq);
  else
    mlog(SHUF_CALL, "forw_start_next: to=[%d.%d] %s dst=%p, oput=null",
       oq->grank, oq->subrank, outset_typstr(oq->myset->settype), oq->dst);

  /*
   * get rid of handle if we've got one (XXX should we try and recycle
   * it?  how much memory does caching handles cost us?)
   */
  if (oput && oput->outhand) {
    HG_Destroy(oput->outhand);
    oput->outhand = NULL;
  }

  /* now lock the queue so we can drop nsending and advance */
  pthread_mutex_lock(&oq->oqlock);

  if (oq->oqflushing && oq->myset->shuf->curflush == NULL) {
      notify(SHUF_CRIT, "shuffler: forw_start_next: flush sanity check fail!");
      notify(SHUF_CRIT, "shuffler: oq=%p [%d.%d]", oq, oq->grank, oq->subrank);
      shuffler_statedump(oq->myset->shuf, 0);
      abort();
  }

  flush_done = false;
  if (oput) {

    /* flushing?  see if we finished everying at and before oqflush_output */
    if (oq->oqflushing && oput == oq->oqflush_output) {

      if (oput == XTAILQ_FIRST(&oq->outs)) {  /* nothing before us? */
        flush_done = true; /* so we call done_oq_flush() after unlock */
        oq->oqflushing = 0;
        oq->oqflush_output = NULL;
        mlog(SHUF_D1, "forw_start_next: flush done!");
      } else {
        /* set oqflush_output to pending earlier request */
        oq->oqflush_output = XTAILQ_PREV(oput, sending_outputs, q);
        mlog(SHUF_D1, "forw_start_next: flush update to %p",
             oq->oqflush_output);
        shufcount(&oq->cntoqflushorder);
      }

    }

    XTAILQ_REMOVE(&oq->outs, oput, q);
    mlog(SHUF_D1, "forw_start_next: done with output=%p, oseq=%d",
         oput, oput->outseq);
    free(oput);
    oput = NULL;
  }
  if (oq->nsending > 0) oq->nsending--;
  mlog(SHUF_D1, "forw_start_next: dst=%p nsending=%d", oq->dst, oq->nsending);

  tosend = false;
  flushloadingnow = false;
  XSIMPLEQ_INIT(&tosendq);   /* to be safe */
  fq = NULL;
  fq_end = &fq;
  while (!oq->oqwaitq.empty() && tosend == false) {
    req = oq->oqwaitq.front();
    oq->oqwaitq.pop_front();

    /* if flushing, see if we pulled the last req of interest */
    if (oq->oqflushing && oq->oqflush_waitcounter > 0) {
      oq->oqflush_waitcounter--;
      if (oq->oqflush_waitcounter == 0) {
        mlog(SHUF_D1, "forw_start_next: dst=%p cleared waitcount", oq->dst);
        flushloadingnow = true;   /* done first phase of flush */
      }
    }

    parent = req->owner;
    req->owner = NULL;      /* detach req from owner now it is unblocked */
    if (parent == NULL) {

      /* should never happen */
      notify(SHUF_CRIT, "shuffle: forw_cb: waitq req w/o owner?!?!");

    } else if (acnt32_decr(parent->nrefs) < 1) {   /* drop reference */

      if (parent->onfq) {               /* onfq is a sanity check */
        /* should never happen */
        notify(SHUF_CRIT, "shuffle_forw_cb: failed onfq sanity check!!!");
      } else {
        /* done with parent, put on a list for stopwait()... */
        *fq_end = parent;
        fq_end = &parent->fqnext;
        parent->onfq = 1;              /* now on an fq list */
      }

    }

    /* this bumps nsending back up if it returns a "tosend" list */
    mlog(SHUF_D1, "forw_start_next: dst=%p, pull req=%p from waitq",
         oq->dst, req);
    tosend = append_req_to_locked_outqueue(oq->myset, oq, req,
                                           &tosendq, &nxtoput, false);
  }

  /* if flushing, ensure our req got pushed out */
  if (flushloadingnow && !tosend) {
    mlog(SHUF_D1, "forw_start_next: dst=%p need to push output queue", oq->dst);
    tosend = append_req_to_locked_outqueue(oq->myset, oq, NULL,
                                           &tosendq, &nxtoput, true);
    mlog(SHUF_D1, "forw_start_next: after push dst=%p tosend=%d",
         oq->dst, tosend == true);
  }
  pthread_mutex_unlock(&oq->oqlock);

  /*
   * now we can stopwait() any parent whose nrefs dropped to zero.
   * (we've saved them all on the "fq" list so we could delay the
   * actual calls to stopwait() until after we've released the oqlock.)
   */
  if (fq)
    mlog(SHUF_D1, "forw_start_next: dst=%p stopwait new zero refs", oq->dst);
  for (parent = fq ; parent != NULL ; parent = nparent) {
    nparent = parent->fqnext;  /* save copy, we are going to free parent */
    parent_stopwait(oq->myset->shuf, parent, 0);   /* might HG_Respond, etc. */
  }

  /* if waitq gave us enough to start sending, do it now */
  if (tosend) {

    /* if flushing and drained oqwaitq, start output tracking before send */
    if (flushloadingnow) {
      pthread_mutex_lock(&oq->oqlock);
      oq->oqflush_output = XTAILQ_LAST(&oq->outs, sending_outputs);
      empty_outs = (oq->oqflush_output == NULL);
      mlog(SHUF_D1, "forw_start_next: dst=%p set oqflush_output=%p, e=%d",
           oq->dst, oq->oqflush_output, empty_outs == true);
      if (empty_outs) {     /* unlikely, but possible */
        oq->oqflushing = 0;
        flush_done = true;  /* trigger call to done_oq_flush, below */
      }
      pthread_mutex_unlock(&oq->oqlock);

    }

    /* this will print an warning on failure */
    mlog(SHUF_D1, "forw_start_next: dst=%p, sending next", oq->dst);
    (void) forward_reqs_now(&tosendq, oq->myset->shuf, oq->myset, oq, nxtoput);
  }

  /* if we finished the flush, pass that info upward */
  if (flush_done) {
    mlog(SHUF_D1, "forw_start_next: dst=%p, flush_done set", oq->dst);
    done_oq_flush(oq);
  }

  mlog(SHUF_D1, "forw_start_next: done!");
}

/*
 * shuffler_rpchand: mercury callback when we recv an RPC.  we need to
 * unpack the requests in the batch and use nexus to forward them on
 * to their next hop.  we'll allocate a req_parent to own any req that
 * gets placed on a waitq.  being placed on a waitq will cause our
 * HG_Respond() to be delayed until everything clears the wait queue.
 *
 * @param handle the handle from the RPC request
 * @return success
 */
static hg_return_t shuffler_rpchand(hg_handle_t handle) {
  const struct hg_info *hgi;
  struct hgthread *inhgt;
  struct outset *outoset;
  struct shuffler *sh;
  int islocal, rank;
  hg_return_t ret;
  rpcin_t in;
  struct request *req;
  nexus_ret_t nexus;
  hg_addr_t dstaddr;
  struct req_parent *parent = NULL;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;
  rpcout_t reply;

  mlog(SHUF_CALL, "rpchand: rpc recv'd.  handle=%p", handle);

  /* recover output queue set from handle and see if it is local or remote */
  hgi = HG_Get_info(handle);
  if (!hgi) {
    notify(SHUF_CRIT, "shuffler_rpchand: no hg_info (%p)", handle);
    abort();   /* should never happen */
  }
  inhgt = (struct hgthread *) HG_Registered_data(hgi->hg_class, hgi->id);
  if (!inhgt) {
    notify(SHUF_CRIT, "shuffler_rpchand: no registered data (%p)", handle);
    abort();   /* should never happen */
  }
  sh = inhgt->hgshuf;
  islocal = (inhgt == &sh->hgt_local);
  mlog(SHUF_D1, "rpchand: got request hand=%p local=%d", handle, islocal);
  if (islocal)
    shufcount(&sh->cntrpcinshm);
  else
    shufcount(&sh->cntrpcinnet);

  /* if sending is disabled, we don't want new requests */
  if (sh->disablesend) {
    HG_Destroy(handle);
    mlog(SHUF_WARN, "rpchand: drop req due to disablesend");
    return(HG_CANCELED);
  }

  /* decode RPC input into an rpcin_t */
  ret = HG_Get_input(handle, &in);
  if (ret != HG_SUCCESS) {
    notify(SHUF_CRIT, "rpchand: drop req due to get input error");
    HG_Destroy(handle);
    return(ret);
  }
  mlog(SHUF_D1, "rpchand: hand=%p is R%d-%d", handle, in.forwardrank, in.iseq);

  /*
   * now we've got a list of reqs to either deliver local or forward
   * to their next hop...   if any requests get put on a wait queue,
   * then we need to allocate a req_parent to track the state of this
   * RPC so that we can delay the HG_Respond() until everything clears
   * the wait queue (this is for flow control).   we delay the allocation
   * of the req_parent until its first use (in case we don't need it).
   */
  while ((req = XSIMPLEQ_FIRST(&in.inreqs)) != NULL) {

    /* remove req from front of list */
    XSIMPLEQ_REMOVE_HEAD(&in.inreqs, next);

    /* determine next hop */
    nexus = nexus_next_hop(sh->nxp, req->dst, &rank, &dstaddr);
    mlog(SHUF_D1, "rpchand: new req=%p dst=%d nexus=%d", req, req->dst, nexus);

    /* case 1: we are dst of this request */
    if (nexus == NX_DONE) {

      mlog(SHUF_D1, "rpchand: req=%p to_self", req);
      ret = req_to_self(sh, req, handle, &in, &parent);

      continue;
    }

    /* case 2: not for us, sending request over mercury */

    /*
     * possible sub-cases for case 2:
     *   NX_ISLOCAL:  dst is on local node, use na+sm to output to final dst
     *                 -> input should be from network (we are DSTREP)
     *   NX_DSTREP:   dst is on remote node, use network to output
     *                 -> input should be from shm (we are SRCREP)
     *   NX_SRCREP:   dst is on remote node, need to forw to SRCREP
     *                 -> can't happen!  only happens at SRC.
     *                    all SRC routing happens in shuffler_send(),
     *                    never in shuffler_rpchand().
     *
     * sanity check it here to avoid network loops
     */
    if ((nexus != NX_ISLOCAL && nexus != NX_DESTREP) ||
        (nexus == NX_ISLOCAL && islocal)             ||
        (nexus == NX_DESTREP && !islocal)) {
      notify(SHUF_ERR, "rpchand: nexus PANIC!  "
                       "%d: %d->%d len=%d code=%d, l=%d, R%d-%d", sh->grank,
                       req->src, req->dst, req->datalen, nexus, islocal,
                       in.forwardrank, in.iseq);
      drop_reqs(&req, NULL, NULL);  /* no msg, we already printed one */
      continue;
    }

    /* need to find correct output queue for dstaddr */
    outoset = (nexus == NX_DESTREP) ? &sh->remoteq : &sh->local_rlq;
    it = outoset->oqs.find(dstaddr);
    if (it == outoset->oqs.end()) {
      /*
       * nexus knew the addr, but we couldn't find a a queue!
       * this should not happen!!!
       */
      notify(SHUF_ERR, "rpchand: no route for %d (%d)", req->dst, nexus);
      drop_reqs(&req, NULL, NULL);  /* no msg, we already printed one */
      continue;
    }

    oq = it->second;    /* now we have the correct output queue */

    mlog(SHUF_D1, "rpchand: req=%p via mercury [%d.%d] oq=%p", req,
         oq->grank, oq->subrank, oq);
    ret = req_via_mercury(sh, outoset, oq, req, handle, &in, &parent);

  }

  /*
   * if we malloc'd a req_parent via req_parent_init() [called in either
   * req_to_self or req_via_mercury], then we are holding an additional
   * reference to the parent to keep it in place until we exit the
   * while loop above (req_parent_init set the inital value of nrefs to 2).
   * now we can drop that extra reference, since we are all done
   * processing.
   *
   * on the other hand, if we did not malloc a req_parent then the
   * RPC is done and we can respond right now.
   */
  if (parent != NULL) {
    mlog(SHUF_D1, "rpchand: flowctrl handle=%p, new parent=%p", handle,
         parent);
    (void) HG_Free_input(handle, &in);
    parent_dref_stopwait(sh, parent, 0);
  } else {
    mlog(SHUF_D1, "rpchand: done! handle=%p, ret=%d", handle, ret);
    reply.oseq = in.iseq;
    reply.respondrank = sh->grank;
    reply.ret = ret;
    (void) HG_Free_input(handle, &in);
    ret = HG_Respond(handle, shuffler_desthand_cb, handle, &reply);
    if (ret != HG_SUCCESS)
      HG_Destroy(handle);
  }

  mlog(SHUF_CALL, "rpchand: DONE.  handle=%p", handle);
  return(HG_SUCCESS);
}

/*
 * shuffler_desthand_cb: sent reply, drop the handle
 *
 * @param cbi the arg for the callback
 * @return success
 */
static hg_return_t shuffler_desthand_cb(const struct hg_cb_info *cbi) {
  hg_handle_t handle = (hg_handle_t)cbi->arg;
  mlog(SHUF_CALL, "desthand_cb: HG_Respond done.  handle=%p", handle);
  HG_Destroy(handle);
  return(HG_SUCCESS);
}

/*
 * aquire_flush: flush operations are serialized.  this function
 * blocks until a flush can run...  flush type is one of localq,
 * remoteq, or deliver.
 *
 * for localq/remoteq if we are successful we set oqflushing=1
 * and init the oqflush_counter to 1 (to hold it until the caller
 * can sleep).
 *
 * @param sh the shuffler we are using
 * @param fop the op that needs to flush, has not been init'd
 * @param type the type of flush we are planning to run (FLUSH_*)
 * @param oset the output set (if type is localq or remoteq)
 * @return status (normally success after waiting)
 */
static hg_return_t aquire_flush(struct shuffler *sh, struct flush_op *fop,
                                int type, struct outset *oset) {
  hg_return_t rv = HG_SUCCESS;
  mlog(CLNT_CALL, "aquire_flush: type=%d fop=%p oset=%p", type, fop, oset);

  /* first init the flush operation's CV */
  if (pthread_cond_init(&fop->flush_waitcv, NULL) != 0) {
    notify(CLNT_CRIT, "shuffler: flush cv init failed!");
    return(HG_OTHER_ERROR);
  }

  pthread_mutex_lock(&sh->flushlock);
  fop->status = (sh->curflush != NULL) ? FLUSHQ_PENDING : FLUSHQ_READY;
  shufcount(&sh->cntflush[type]);
  if (fop->status == FLUSHQ_PENDING) shufcount(&sh->cntflushwait);

  /* if flush is busy, our op needs to wait for it */
  if (fop->status == FLUSHQ_PENDING) {
    XSIMPLEQ_INSERT_TAIL(&sh->fpending, fop, fq);
    while (fop->status == FLUSHQ_PENDING) {
     mlog(CLNT_D1, "aquire_flush: blocking fop=%p", fop);
      pthread_cond_wait(&fop->flush_waitcv, &sh->flushlock);
     mlog(CLNT_D1, "aquire_flush: UNblocking fop=%p", fop);
    }

    /* wakeup removed us from pending queue, see if we were canceled */
    if (fop->status == FLUSHQ_CANCEL) {
      notify(CLNT_CRIT, "shuffler: aqflush: cancel while waiting");
      pthread_cond_destroy(&fop->flush_waitcv);
      return(HG_CANCELED);
    }
  }
  mlog(CLNT_D1, "aquire_flush: got flush for fop=%p", fop);

  /* setup state for this flush */
  sh->curflush = fop;
  sh->flushdone = 0;
  sh->flushtype = type;
  sh->flushoset = oset;

  /* if we have an oset, then additional work todo while holding flushlock */
  if (oset) {
    oset->osetflushing = 1;
    acnt32_set(oset->oqflush_counter, 1);
  }

  pthread_mutex_unlock(&sh->flushlock);

  /* make sure we are still running or we might block forever... */
  if (( (type == FLUSH_LOCAL_ORQ || type == FLUSH_LOCAL_RLQ)  &&
        (sh->hgt_local.nshutdown  != 0 || sh->hgt_local.nrunning  == 0)) ||
      (type == FLUSH_REMOTEQ &&
        (sh->hgt_remote.nshutdown != 0 || sh->hgt_remote.nrunning == 0)) ||
      (type == FLUSH_DELIVER && (sh->dshutdown != 0 || sh->drunning == 0)) ) {

    drop_curflush(sh);
    rv = HG_CANCELED;
  }

  mlog(CLNT_D1, "aquire_flush: fop=%p, return=%d", fop, rv);
  return(rv);
}

/*
 * drop_curflush: we are the flusher, but we are done with it.  drop it
 * and wake up anyone waiting on the pending list to flush.
 *
 * @param sh the shuffler we are using
 */
static void drop_curflush(struct shuffler *sh) {
  struct flush_op *nxtfop;
  mlog(CLNT_CALL, "drop_curflush");

  pthread_mutex_lock(&sh->flushlock);
  if (sh->curflush) {
    pthread_cond_destroy(&sh->curflush->flush_waitcv);
    sh->curflush = NULL;
    sh->flushtype = FLUSH_NONE;   /* to be safe */
    if (sh->flushoset) {
      sh->flushoset->osetflushing = 0;
      /* no need to set oqflush_counter */
      sh->flushoset = NULL;
    }
  } else {
    notify(CLNT_CRIT, "drop_curflush: drop, but no flush in progress!?!");
    abort();    /* this shouldn't happen */
  }

  nxtfop = XSIMPLEQ_FIRST(&sh->fpending);
  if (nxtfop != NULL) {
    XSIMPLEQ_REMOVE_HEAD(&sh->fpending, fq);
    nxtfop->status = FLUSHQ_READY;
    pthread_cond_signal(&nxtfop->flush_waitcv);
  }
  pthread_mutex_unlock(&sh->flushlock);
}

/*
 * shuffler_flush_delivery: flush the delivery queue.  this function
 * blocks until all requests currently in the delivery queues (both
 * deliverq and dwaitq) are delivered.
 */
hg_return_t shuffler_flush_delivery(shuffler_t sh) {
  struct flush_op fop;
  hg_return_t rv;
  int waitcount;
  mlog(CLNT_CALL, "shuffler_flush_delivery");

  rv = aquire_flush(sh, &fop, FLUSH_DELIVER, NULL);    /* may BLOCK here */
  if (rv != HG_SUCCESS)
    return(rv);
  mlog(CLNT_D1, "shuffler_flush_delivery: aquired flush");

  /*
   * we now own the current flush operation, set counter and wait.
   * counter is dropped after we deliver a req with the callback
   * and will send us a cond_signal when it drops from 1 to zero.
   */
  pthread_mutex_lock(&sh->deliverlock);
  sh->dflush_counter = sh->deliverq.size() + sh->dwaitq.size();
  mlog(CLNT_D1, "shuffler_flush_delivery: count=%d", sh->dflush_counter);
  while (sh->dflush_counter > 0 && fop.status == FLUSHQ_READY) {
    pthread_cond_wait(&fop.flush_waitcv, &sh->deliverlock);  /* BLOCK HERE */
  }
  sh->dflush_counter = 0;
  pthread_mutex_unlock(&sh->deliverlock);

  drop_curflush(sh);

  rv = (fop.status == FLUSHQ_CANCEL) ? HG_CANCELED : HG_SUCCESS;
  mlog(CLNT_D1, "shuffler_flush_delivery: done rv=%d", rv);
  return(rv);
}

/*
 * shuffler_flush_qs: flush either local or remote output queues.
 * this function blocks until all requests currently in the specified
 * output queues are delivered. We make no claims about requests that
 * arrive after the flush has been started.
 */
hg_return_t shuffler_flush_qs(shuffler_t sh, int whichqs) {
  struct flush_op fop;
  hg_return_t rv;
  int ftype, r;
  struct outset *oset;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;
  mlog(CLNT_CALL, "shuffler_flush_qs: type=%s", outset_typstr(whichqs));

  /* no point trying to flush if we can't send */
  if (sh->disablesend)
    return(HG_CANCELED);

  switch (whichqs) {
    case SHUFFLER_REMOTE_QUEUES:
      oset = &sh->remoteq;
      ftype = FLUSH_REMOTEQ;
      break;
    case SHUFFLER_ORIGIN_QUEUES:
      oset = &sh->local_orq;
      ftype = FLUSH_LOCAL_ORQ;
      break;
    case SHUFFLER_RELAY_QUEUES:
      oset = &sh->local_rlq;
      ftype = FLUSH_LOCAL_RLQ;
      break;
    default:
      mlog(CLNT_ERR, "shuffler_flush_qs(%d): bad whichqs", whichqs);
      return(HG_OTHER_ERROR);
  }

  rv = aquire_flush(sh, &fop, ftype, oset);         /* may BLOCK here */
  if (rv != HG_SUCCESS) {
    return(rv);
  }
  mlog(CLNT_D1, "shuffler_flush_qs: type=%s AQUIRED!", outset_typstr(whichqs));

  /*
   * we've aquired the flush, including setting oqflushing and
   * oqflush_counter both to 1.  (initing oqflush_counter to 1
   * rather than 0 keeps the flush active while we are setting
   * it up -- we'll drop the extra reference before we block).
   * now we need to look for output queues to be flushed. ...
   */
  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

    /*
     * if we start a queue flush on this queue, bump the counter
     * (keeping track of the number of outqueues being flushed).
     */
    if (start_qflush(sh, oset, oq)) {
      acnt32_incr(oset->oqflush_counter);
    }
  }

  /*
   * now wait for the flush to finish ... we use the flushlock here,
   * since the outset structure doesn't have a lock
   */
  pthread_mutex_lock(&sh->flushlock);
  r = acnt32_decr(oset->oqflush_counter);  /* drop our reference */
  if (r == 0)
    oset->osetflushing = 0;

  mlog(CLNT_D1, "shuffler_flush_qs: waiting... type=%s osetflushing=%d!",
       outset_typstr(whichqs), oset->osetflushing);
  while (oset->osetflushing != 0 && fop.status == FLUSHQ_READY) {
    pthread_cond_wait(&fop.flush_waitcv, &sh->flushlock);  /* BLOCK HERE */
  }
  pthread_mutex_unlock(&sh->flushlock);
  mlog(CLNT_D1, "shuffler_flush_qs: wait done!");

  /*
   * done!   drop the flush and return...
   */
  if (fop.status == FLUSHQ_CANCEL) {
    clean_qflush(sh, oset);    /* clear out state of cancel'd flush */
  }
  drop_curflush(sh);
  rv = (fop.status == FLUSHQ_CANCEL) ? HG_CANCELED : HG_SUCCESS;
  mlog(CLNT_D1, "shuffler_flush_qs: done! type=%s rv=%d!",
       outset_typstr(whichqs), rv);
  return(rv);
}

/*
 * start_qflush: start flushing an output queue.  if the queue flush
 * is pending, we return 1.  otherwise 0.   flushing an output queue
 * is a multi-step process.  first we must wait for the waitq to drain.
 * second, if there are any pending buffered reqs in the loading list
 * waiting for enough data to build a batch then we need to stop waiting
 * and send them now.  third, we need to wait for all sending_outputs
 * on oq->outs at the time of the flush to finish.   depending on the
 * state of the queue we may be able to skip some or all of these
 * steps (e.g. if queue empty, then we're done!).
 *
 * @param sh the shuffler we are using
 * @param oset the output set being flushed
 * @param oq the output queue to flush
 * @return 1 if flush is pending, otherwise zero
 */
static int start_qflush(struct shuffler *sh, struct outset *oset,
                        struct outqueue *oq) {
  int rv = 0;
  bool tosend;
  struct request_queue tosendq;
  struct output *oput;
  mlog(UTIL_CALL, "start_qflush: oset=%p, oq=%p rnk=[%d.%d]", oset, oq,
       oq->grank, oq->subrank);

  pthread_mutex_lock(&oq->oqlock);

  if (oq->oqflushing) {
    notify(UTIL_CRIT, "shuffler: start_qflush: oq->flushing already set?!");
    abort();    /* this shouldn't happen */
  }

  /* first, look for waiting requests in the oq->waitq */
  if (!oq->oqwaitq.empty()) {
    oq->oqflush_waitcounter = oq->oqwaitq.size();
    oq->oqflush_output = NULL;   /* to be safe */
    oq->oqflushing = 1;
    rv = 1;
    mlog(UTIL_D1, "start_qflush: WAITQ: oset=%p, oq=%p, waitqcnt=%d",
         oset, oq, oq->oqflush_waitcounter);
    goto done;
  }

  /* second, flush the loading list (req==NULL in below call) */
  tosend = append_req_to_locked_outqueue(oset, oq, NULL,
                                         &tosendq, &oput, true);

  /* send?  drop oq lock to be safe since we are calling out to mercury */
  if (tosend) {
    mlog(UTIL_D1, "start_qflush: LOADING FLUSH: oset=%p, oq=%p",
         oset, oq);
    pthread_mutex_unlock(&oq->oqlock);
    if (forward_reqs_now(&tosendq, sh, oset, oq, oput) != HG_SUCCESS) {
      /* XXX: no good recovery from this */
      notify(UTIL_CRIT, "shuffler: start_qflush: forward_reqs_now failed?!");
    }
    pthread_mutex_lock(&oq->oqlock);
  }

  /* third, check oq->outs */
  if (XTAILQ_FIRST(&oq->outs) != NULL) {
    oq->oqflush_waitcounter = 0;
    oq->oqflush_output = XTAILQ_LAST(&oq->outs, sending_outputs);
    oq->oqflushing = 1;
    rv = 1;
    mlog(UTIL_D1, "start_qflush: SENDERS: oset=%p, oq=%p, waitfor=%p",
         oset, oq, oq->oqflush_output);
  }

done:
  if (rv != 0)
    shufcount(&oq->cntoqflushes);
  pthread_mutex_unlock(&oq->oqlock);
  mlog(UTIL_D1, "start_qflush: oset=%p, oq=%p, flushpending=%d", oset, oq, rv);
  return(rv);
}

/*
 * clean_qflush: clean out state of a flush that has been canceled.
 * we own the flush so it is safe to clear out oset once all the
 * output queues have been reset...
 *
 * @param sh the shuffler we are using
 * @param oset the output set being flushed
 * @return 1 if flush is pending, otherwise zero
 */
static void clean_qflush(struct shuffler *sh, struct outset *oset) {
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;
  mlog(UTIL_CALL, "clean_qflush: oset=%p", oset);

  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

    pthread_mutex_lock(&oq->oqlock);
    oq->oqflushing = 0;
    oq->oqflush_waitcounter = 0;
    oq->oqflush_output = NULL;
    pthread_mutex_unlock(&oq->oqlock);
  }

  oset->osetflushing = 0;
  acnt32_set(oset->oqflush_counter, 0);
}

/*
 * done_oq_flush: finished flushing an outqueue.  need to update the
 * outset and signal flusher if we dropped the ref to zero!
 *
 * @param oq the output queue we just finished flushing
 */
static void done_oq_flush(struct outqueue *oq) {
  struct outset *oset = oq->myset;
  struct shuffler *sh = oset->shuf;
  int r;

  r = acnt32_decr(oset->oqflush_counter);
  mlog(UTIL_CALL, "done_oq_flush: oq=%p, newrefcnt=%d", oq, r);

  /* signal main flusher if we dropped the last reference */
  if (r == 0) {
    mlog(UTIL_CALL, "done_oq_flush: dropped last oq ref, flush done!");
    pthread_mutex_lock(&sh->flushlock);  /* protects oqflushing */
    oset->osetflushing = 0;
    assert(sh->curflush != NULL);
    pthread_cond_broadcast(&sh->curflush->flush_waitcv);
    pthread_mutex_unlock(&sh->flushlock);
  }
}

/*
 * dumpstats: dump stats to mlog NOTE
 *
 * @param sh the shuffler to dump
 */
static void dumpstats(shuffler_t sh) {
#ifdef SHUFFLER_COUNT
  std::map<hg_addr_t,struct outqueue *>::iterator oqit;
  const char *names[3] = { "local_origin", "local_relay", "remote" };
  struct outset *o[3] = { &sh->local_orq, &sh->local_rlq, &sh->remoteq }, *os;
  struct outqueue *oq;
  int lcv;

  mlog(SHUF_NOTE, "stat counter dump follows");
  mlog(SHUF_NOTE, "deliver-thread: dblock=%d, delivery=%d", sh->cntdblock,
       sh->cntdeliver);
  mlog(SHUF_NOTE, "deliver: reqs=%d/%d, waits=%d/%d, mxwait=%d",
       sh->cntdreqs[0], sh->cntdreqs[1], sh->cntdwait[0], sh->cntdwait[1],
       sh->cntdmaxwait);
  mlog(SHUF_NOTE, "recvs: local=%d, network=%d", sh->cntrpcinshm,
       sh->cntrpcinnet);
  mlog(SHUF_NOTE,
       "flush: rem=%d, loc_o=%d, loc_r=%d dlvr=%d, waits=%d, strand=%d",
       sh->cntflush[FLUSH_REMOTEQ], sh->cntflush[FLUSH_LOCAL_ORQ],
       sh->cntflush[FLUSH_LOCAL_RLQ], sh->cntflush[FLUSH_DELIVER],
       sh->cntflushwait, sh->cntstranded);
  mlog(SHUF_NOTE, "oset-size: local_or=%ld, local_rl=%ld, remote=%ld",
       sh->local_orq.oqs.size(), sh->local_rlq.oqs.size(),
       sh->remoteq.oqs.size());
  mlog(SHUF_NOTE, "local_hgt: nprogress=%d, ntrigger=%d",
       sh->hgt_local.nprogress, sh->hgt_local.ntrigger);
  mlog(SHUF_NOTE, "remote_hgt: nprogress=%d, ntrigger=%d",
       sh->hgt_remote.nprogress, sh->hgt_remote.ntrigger);
  for (lcv = 0; lcv < 3 ; lcv++) {
    mlog(SHUF_NOTE, "outqueue-stats: %s", names[lcv]);
    os = o[lcv];
    for (oqit = os->oqs.begin() ; oqit != os->oqs.end() ; oqit++) {
      oq = oqit->second;
      mlog(SHUF_NOTE, "oq[%d.%d]: reqs=%d/%d, snds=%d, flsnd=%d, "
                      "waits=%d/%d, fl=%d, mxwait=%d, order=%d",
      oq->grank, oq->subrank, oq->cntoqreqs[0], oq->cntoqreqs[1],
      oq->cntoqsends, oq->cntoqflushsend, oq->cntoqwaits[0], oq->cntoqwaits[1],
      oq->cntoqflushes, oq->cntoqmaxwait, oq->cntoqflushorder);
    }
  }
#endif
}

/*
 * shuffler_send_stats: report number of rpcs sent.
 */
hg_return_t shuffler_send_stats(shuffler_t sh, hg_uint64_t* local_origin,
                                hg_uint64_t* local_relay, hg_uint64_t* remote) {
  *local_origin = *local_relay = *remote = 0;
#ifdef SHUFFLER_COUNT
  std::map<hg_addr_t,struct outqueue *>::iterator oqit;
  struct outset *o[3] = { &sh->local_orq, &sh->local_rlq, &sh->remoteq }, *os;

  for (oqit = o[0]->oqs.begin() ; oqit != o[0]->oqs.end() ; oqit++)
    *local_origin += static_cast<hg_uint64_t>(oqit->second->cntoqsends);
  for (oqit = o[1]->oqs.begin() ; oqit != o[1]->oqs.end() ; oqit++)
    *local_relay += static_cast<hg_uint64_t>(oqit->second->cntoqsends);
  for (oqit = o[2]->oqs.begin() ; oqit != o[2]->oqs.end() ; oqit++)
    *remote += static_cast<hg_uint64_t>(oqit->second->cntoqsends);

#endif
  return(HG_SUCCESS);
}

/*
 * shuffler_recv_stats: report number of rpcs received.
 */
hg_return_t shuffler_recv_stats(shuffler_t sh, hg_uint64_t* local,
                                hg_uint64_t* remote) {
#ifdef SHUFFLER_COUNT
  *local = static_cast<hg_uint64_t>(sh->cntrpcinshm);
  *remote = static_cast<hg_uint64_t>(sh->cntrpcinnet);
#endif
  return(HG_SUCCESS);
}

/*
 * statedump_oset: helper fn for shuffler statedump
 */
static void statedump_oset(shuffler_t sh, int lvl, const char *name,
  struct outset *oset) {
  std::map<hg_addr_t,struct outqueue *>::iterator oqit;
  std::deque<request *>::iterator reqit;
  struct request *req;
  struct req_parent *parent;
  struct outqueue *oq;
  int lck_rv, ql, idx, lsz;
  struct output *out;
  int32_t rtime;

  notify(lvl, "oset %s: run/shut=%d/%d, fl=%d, flcnt=%d", name,
         oset->myhgt->nrunning, oset->myhgt->nshutdown, oset->osetflushing,
         acnt32_get(oset->oqflush_counter));

  for (oqit = oset->oqs.begin() ; oqit != oset->oqs.end() ; oqit++) {
    oq = oqit->second;
    lck_rv = pthread_mutex_trylock(&oq->oqlock);

    ql = oq->oqwaitq.size();
    notify(lvl, "[%d.%d] waslck=%d, loadsz=%d, nsend=%d, nwait=%d, fl=%d/%d",
           oq->grank, oq->subrank, lck_rv != 0, oq->loadsize, oq->nsending,
           ql, oq->oqflushing, oq->oqflush_waitcounter);

    for (idx = 0, reqit = oq->oqwaitq.begin() ;
         reqit != oq->oqwaitq.end() ; reqit++, idx++) {
      req = *reqit;
      parent = req->owner;

      if (parent == NULL) {
        mlog(SHUF_INFO, "oqwaitq[%d] req %p with NULL PARENT?", idx, req);
        continue;
      }
      if (sh->boottime)
        rtime = (shuftime() - sh->boottime) - parent->timewstart;
      else
        rtime = 0;
      if (parent->rpcin_forwrank == -1 && parent->rpcin_seq == -1)
        mlog(SHUF_INFO,
             "oqwaitq[%d], %d->%d, CLI, refs=%d, hand?=%d, time=%d",
                idx, req->src, req->dst, acnt32_get(parent->nrefs),
                parent->input != NULL, rtime);
      else
        mlog(SHUF_INFO,
             "oqwaitq[%d], %d->%d, R%d-%d, refs=%d, hand?=%d, time=%d",
                idx, req->src, req->dst, parent->rpcin_forwrank,
                parent->rpcin_seq, acnt32_get(parent->nrefs),
                parent->input != NULL, rtime);
    }

    /* sanity checks */
    lsz = 0;
    XSIMPLEQ_FOREACH(req, &oq->loading, next) {
      lsz += req->datalen;
    }
    if (lsz != oq->loadsize) {
      notify(lvl, "[%d.%d] LOADSIZE CHECK FAILED: %d != %d",
             oq->grank, oq->subrank, lsz, oq->loadsize);
    }

    lsz = 0;
    XTAILQ_FOREACH(out, &oq->outs, q) {
      if (sh->boottime)
        rtime = (shuftime() - sh->boottime) - out->timestart;
      else
        rtime = 0;

      /* use info for this, since it is more chatty */
      mlog(SHUF_INFO, "outs[%d] R%d-%d to %d.%d, ostep=%d, time=%d",
           lsz, sh->grank, out->outseq, oq->grank, oq->subrank,
           out->ostep, rtime);
      lsz++;
    }
    if (lsz != oq->nsending) {
      notify(lvl, "[%d.%d] NSENDING CHECK FAILED: %d != %d",
             oq->grank, oq->subrank, lsz, oq->nsending);
    }

    if (lck_rv == 0) pthread_mutex_unlock(&oq->oqlock);
  }

}

/*
 * shuffler_statedump: dump out current state of shuffle for diagnostics
 */
void shuffler_statedump(shuffler_t sh, int tostderr) {
  int lvl, lck_rv, qsz, wsz, idx, rtime;
  std::deque<request *>::iterator reqit;
  struct request *req;
  struct req_parent *parent;

  dumpstats(sh);   /* dump stats first */

  lvl = SHUF_NOTE;
  if (tostderr) lvl |= MLOG_STDERR;
  notify(lvl, "shuffler state dump:");
  notify(lvl, "rank=%d, disablesend=%d, seqsrc=%d", sh->grank,
         sh->disablesend, acnt32_get(sh->seqsrc));

  lck_rv = pthread_mutex_trylock(&sh->deliverlock);
  qsz = sh->deliverq.size();
  wsz = sh->dwaitq.size();
  notify(lvl, "dlvr: waslck=%d, wait=%d, inprog=%d, flcnt=%d, run/shut=%d/%d",
         lck_rv != 0, qsz, wsz, sh->dflush_counter,
         sh->drunning, sh->dshutdown);

  for (idx = 0, reqit = sh->dwaitq.begin() ;
       reqit != sh->dwaitq.end() ; reqit++, idx++) {
    req = *reqit;
    parent = req->owner;

    if (parent == NULL) {
      mlog(SHUF_INFO, "dwaitq[%d] req %p with NULL PARENT?", idx, req);
      continue;
    }
    if (sh->boottime)
      rtime = (shuftime() - sh->boottime) - parent->timewstart;
    else
      rtime = 0;
   if (parent->rpcin_forwrank == -1 && parent->rpcin_seq == -1)
        mlog(SHUF_INFO,
             "dwaitq[%d], %d->%d, CLI, refs=%d, hand?=%d, time=%d",
                idx, req->src, req->dst, acnt32_get(parent->nrefs),
                parent->input != NULL, rtime);
      else
        mlog(SHUF_INFO,
             "dwaitq[%d], %d->%d, R%d-%d, refs=%d, hand?=%d, time=%d",
                idx, req->src, req->dst, parent->rpcin_forwrank,
                parent->rpcin_seq, acnt32_get(parent->nrefs),
                parent->input != NULL, rtime);

  }

  if (lck_rv == 0) pthread_mutex_unlock(&sh->deliverlock);

  notify(lvl, "flsh: cur=%p, typ=%d, done=%d", sh->curflush, sh->flushtype,
         sh->flushdone);
  statedump_oset(sh, lvl, "local_orgin", &sh->local_orq);
  statedump_oset(sh, lvl, "local_relay", &sh->local_rlq);
  statedump_oset(sh, lvl, "remote", &sh->remoteq);
}

/*
 * shuffler_shutdown: stop all threads, release all memory.
 * does not shutdown mercury (since we didn't start it, nexus did),
 * but mercury should not be restarted once we call this.
 */
hg_return_t shuffler_shutdown(shuffler_t sh) {
  int cnt;
  mlog(CLNT_CALL, "shuffer_shutdown");

  /* stop all new inbound requests */
  sh->disablesend = 1;

  /* cancel any flush ops that are queued or running */
  shuffler_flush_discard(sh);

  /* stop all threads */
  stop_threads(sh);

  /* purge any orphaned reqs */
  cnt = purge_reqs(sh);
  if (cnt) {
    notify(CLNT_CRIT, "shuffler: shutdown warning: %d orphans", cnt);
  }

  /* dump counters */
  dumpstats(sh);

  /* now free remaining structure */
  shuffler_outset_discard(&sh->local_orq);     /* ensures maps are empty */
  shuffler_outset_discard(&sh->local_rlq);
  shuffler_outset_discard(&sh->remoteq);
  if (sh->funname) free(sh->funname);
  if (sh->seqsrc) acnt32_free(&sh->seqsrc);
  pthread_mutex_destroy(&sh->deliverlock);
  pthread_cond_destroy(&sh->delivercv);
  pthread_mutex_destroy(&sh->flushlock);
  delete sh;
  mlog(CLNT_CALL, "shuffer_shutdown: DONE closing log...");
  shuffler_closelog();

  return(HG_SUCCESS);
}
