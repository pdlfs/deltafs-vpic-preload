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
 * shuffler.h  shuffler API
 * 28-Jun-2017  chuck@ece.cmu.edu
 */

/*
 * the shuffler uses mercury RPCs to send a message from a SRC process
 * to a DST process.  our goal is to reduce per-process memory usage by
 * reducing the number of output queues and connections to manage on
 * each node (we are assuming an environment where each node has
 * multiple CPUs and there is an app with processes running on each
 * CPU).
 *
 * as an example, consider Trinity Haswell nodes: there are 10,000 nodes
 * with 32 cores on each node.   an application running on these nodes
 * would have 320,000 processes (10,000 nodes * 32 cores/node).  if
 * all processes communicate with each other, each process will need
 * memory for ~320,000 output queues!
 *
 * to reduce the memory usage, we add layers of indirection to the system
 * (in the form of additional mercury RPC hops).   the result is a 3
 * hop shuffle:
 *
 *  SRC  ---na+sm--->   SRCREP ---network--->   DSTREP   ---na+sm--->  DST
 *            1                      2                        3
 *
 * note: "na+sm" is mercury's shared memory transport, "REP" == representative
 *
 * we divide the job for sending to each remote node among the local
 * cores.  furthermore, we only send to one process on each remote node.
 * we expect the remote receiving process to forward our message to
 * the final destination (if it isn't the final destination).
 *
 * thus, on Trinity, each SRC process has 31 na+sm output queues to
 * talk to other local processes, and each SRC process has 10,000/32
 * (~313) network output queues to talk to the remote nodes it is
 * responsible for.   this is much less than the ~320,000 output queues
 * needed in the all-to-all case.
 *
 * a msg from a SRC to a remote DST on node N flows like this:
 *  1. SRC find the local proc responsible for talking to N.  this is
 *     the SRCREP.   it forward the msg to the SRCREP over na+sm.
 *  2. the SRCREP forwards all messages for node N to one process on
 *     node N over the network.   this is the DSTREP.
 *  3. the DSTREP receives the message and looks for its na+sm connection
 *     to the DST (which is also on node N) and sends the msg to DST.
 * at that point the DST will deliver the msg.   note that it is
 * possible to skip hops (e.g. if SRC==SRCREP, the first hop can be
 * skipped).
 *
 * the shuffler library manages this three hop communication.  it
 * has support for batching multiple messages into a larger batch
 * message (to reduce the overhead of small writes), and it also
 * supports write-behind buffering (so that the application can
 * queue data in the buffer and continue, rather than waiting for
 * the RPC).  if/when the buffers fill, the library provides flow
 * control to stop the application from overfilling the buffers.
 *
 * for output queues we provide:
 *  - maxrpc:    the max number of outstanding RPCs we allow at one time.
 *               additional requests are queued on a wait list until
 *               something completes.
 *  - buftarget: controls batching of requests... we batch multiple
 *               requests into a single RPC until we have at least
 *               "buftarget" bytes in the batch.  setting this value
 *               small effectively disables batching.
 *
 * also for delivery, we have a "deliverq_max" which is the max
 * number of delivery requests we will buffer before we start
 * putting additional requests on the waitq (waitq requests are
 * not ack'd until space is available... this triggers flow control).
 *
 * note that we identify endpoints by a global rank number (the
 * rank number is assigned by MPI... MPI is also used to determine
 * the topology -- i.e. which ranks are on the local node.  see
 * deltafs-nexus for details...).
 */

#pragma once

#include <deltafs-nexus/deltafs-nexus_api.h> /* for nexus_ctx_t */

#include <mercury_types.h>

/*
 * shuffler_t: handle to shuffler state (a pointer)
 */
typedef struct shuffler *shuffler_t;

/*
 * shuffler_deliver_t: pointer to a callback function used to
 * deliver a msg to the DST.  this function may block if the
 * DST is busy/full.
 */
typedef void (*shuffler_deliver_t)(int src, int dst, int type,
                                   void *d, int datalen);


/*
 * shuffler_init: init's the shuffler layer.  if this returns an
 * error, you'll need to shutdown and reinit mercury and nexus to
 * retry...
 *
 * @param nxp the nexus context (routing info, already init'd)
 * @param funname rpc function name (for making a mercury RPC id number)
 * @param lmaxrpc max# of outstanding local na+sm RPCs
 * @param lbuftarget target number of bytes in batch na+sm RPC
 * @param rmaxrpc max# of outstanding remote RPCs
 * @param rbuftarget target number of bytes in remote batch RPC
 * @param deliverq_max max# reqs in deliverq before we switch to deliver waitq
 * @param delivercb application callback to deliver data
 * @return handle to shuffler (a pointer) or NULL on error
 */
shuffler_t shuffler_init(nexus_ctx_t nxp, char *funname,
           int lmaxrpc, int lbuftarget, int rmaxrpc, int rbuftarget,
           int deliverq_max, shuffler_deliver_t delivercb);


/*
 * shuffler_send: start the sending of a message via the shuffle.
 * this is not end-to-end, it returns success once the message has
 * been queued for the next hop (the message data is copied into
 * the output queue, so the buffer passed in as an arg can be reused
 * when this function returns).   this is called by the main client
 * thread.
 *
 * @param sh shuffler service handle
 * @param dst target to send to
 * @param type message type (normally 0)
 * @param d data buffer
 * @param datalen length of data
 * @return status (success if we've queued the data)
 */
hg_return_t shuffler_send(shuffler_t sh, int dst, int type,
                          void *d, int datalen);


/*
 * shuffler_flush_delivery: flush the delivery queue.  this function
 * blocks until all requests currently in the delivery queues are
 * delivered.   We make no claims about requests that arrive after
 * the flush has been started.
 *
 * @param sh shuffler service handle
 * @return status
 */
hg_return_t shuffler_flush_delivery(shuffler_t sh);

/*
 * shuffler_flush_qs: flush either local or remote output queues.
 * this function blocks until all requests currently in the specified
 * output queues are delivered. We make no claims about requests that
 * arrive after the flush has been started.
 *
 * @param sh shuffler service handle
 * @param islocal set for localq, zero for remoteqs
 * @return status
 */
hg_return_t shuffler_flush_qs(shuffler_t sh, int islocal);


/*
 * shuffler_flush_localqs: flush localqs (wrapper for shuffler_flush_qs)
 *
 * @param sh shuffler service handle
 * @return status
 */
static hg_return_t shuffler_flush_localqs(shuffler_t sh) {
  return(shuffler_flush_qs(sh, 1));
}


/*
 * shuffler_flush_remoteqs: flush remoteqs (wrapper for shuffler_flush_qs)
 *
 * @param sh shuffler service handle
 * @return status
 */
static hg_return_t shuffler_flush_remoteqs(shuffler_t sh) {
  return(shuffler_flush_qs(sh, 0));
}


/*
 * shuffler_shutdown: stop all threads, release all memory.
 * does not shutdown mercury (since we didn't start it, nexus did),
 * but mercury should not be restarted once we call this.
 *
 * @param sh shuffler service handle
 * @return status
 */
hg_return_t shuffler_shutdown(shuffler_t sh);


/*
 * shuffler_cfglog: setup logging before starting shuffler (for
 * debugging).  call this before shuffler_init() so that everything
 * can be properly logged... priority strings are: EMERG, ALERT, CRIT,
 * ERR, WARN, NOTE, INFO, DBG, DBG0, DBG1, DBG2, DBG3.
 * masks take the form: [facility1=]priority1,[facility2]=priority2,...
 * facilities: CLNT (client), DLIV (delivery),  SHUF (general shuffler)
 *
 * @param max_xtra_rank ranks <= this get extra logging
 * @param defpri default log priority
 * @param stderrpri if msg is logged: print to stderr if at this priority
 * @param mask log mask for non-xtra ranks
 * @param xmask log mask for xtra ranks (default=use mask)
 * @param logfile file to log to (we append the rank# to filename)
 * @param alllogs if logfile, do on all ranks (not just xtra ones)
 * @param msgbufsz size of in-memory message buffer, 0 disables
 * @param stderrlog always print log msgs to stderr, ignore stderrmask
 * @param xtra_stderlog as above, for extra ranks
 * @return 0 on success, -1 on error
 */
int shuffler_cfglog(int max_xtra_rank, const char *defpri,
                    const char *stderrpri, const char *mask,
                    const char *xmask, const char *logfile,
                    int alllogs, int msgbufsz, int stderrlog,
                    int xtra_stderrlog);

/*
 * shuffler_send_stats: retrieve shuffle sender statistics
 * @param sh shuffler service handle
 * @param local accumulated number of remote sends
 * @param remote accumulated number of local sends
 * @return status
 */
hg_return_t shuffler_send_stats(shuffler_t sh, hg_uint64_t* local,
                                hg_uint64_t* remote);

/*
 * shuffler_recv_stats: retrieve shuffler receiver statistics
 * @param sh shuffler service handle
 * @param rrpcs accumulated number of remote receives
 * @param lrpcs accumulated number of local receives
 * @return status
 */
hg_return_t shuffler_recv_stats(shuffler_t sh, hg_uint64_t* local,
                                hg_uint64_t* remote);

