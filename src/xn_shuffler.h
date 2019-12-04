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

/*
 * xn_shuffler.h  wrapper implementation for the embedded 3-hop shuffler.
 *
 * A list of all environmental variables used by us:
 *
 *  SHUFFLE_Mercury_proto
 *    Mercury rpc proto for the remote hop
 *  SHUFFLE_Log_file
 *    Log file to store shuffler stats
 *  SHUFFLE_Remote_senderlimit
 *    Total num of outstanding rpcs for the remote hop
 *  SHUFFLE_Remote_buftarget
 *    Memory allocated for each remote rpc queue
 *  SHUFFLE_Remote_maxrpc
 *    Max num of outstanding rpcs allowed for each remote outgoing queue
 *  SHUFFLE_Local_senderlimit
 *    Total num of outstanding rpcs for the local hops
 *  SHUFFLE_Local_buftarget
 *    Memory allocated for each local rpc queue
 *  SHUFFLE_Local_maxrpc
 *    Max num of outstanding rpcs allowed for each local outgoing queue
 *  SHUFFLE_Relay_buftarget
 *    Memory allocated for each local relay rpc queue
 *  SHUFFLE_Relay_maxrpc
 *    Max num of outstanding rpcs allowed for each local relay outgoing queue
 *  SHUFFLE_Dq_min
 *    Min queue size for the final delivery queue
 *      Delay flushing the queue until queue size exceeds the minimum
 *  SHUFFLE_Dq_max
 *    Max queue size for the final delivery queue
 *      Set to "-1" to disable msg delivery so all msgs will be discarded
 *  SHUFFLE_Min_port
 *    The min port number we can use
 *  SHUFFLE_Max_port
 *    The max port number we can use
 *  SHUFFLE_Subnet
 *    IP prefix of the subnet
 */

#pragma once

#include <stddef.h>

#include <deltafs-nexus/deltafs-nexus_api.h>
#include <mercury_types.h>

#include "shuffler/shuffler.h"

typedef struct xn_stat {
  struct {
    hg_uint64_t recvs; /* total rpcs received */
    hg_uint64_t sends; /* total rpcs sent */
  } local;
  struct {
    hg_uint64_t recvs; /* total rpcs received */
    hg_uint64_t sends; /* total rpcs sent */
  } remote;
} xn_stat_t;

/* shuffle context for the multi-hop shuffler */
typedef struct xn_ctx {
  /* replace all local barriers with global barriers */
  int force_global_barrier;
  xn_stat_t last_stat;
  xn_stat_t stat;
  progressor_handle_t *nethand;
  progressor_handle_t *localhand;
  nexus_ctx_t nx; /* nexus handle */
  shuffler_t sh;
} xn_ctx_t;

/* xn_shuffler_init: init the shuffler or die */
extern void xn_shuffler_init(xn_ctx_t* ctx);

/* xn_shuffler_world_size: return comm world size */
extern int xn_shuffler_world_size(xn_ctx_t* ctx);

/* xn_shuffler_my_rank: return my rank id */
extern int xn_shuffler_my_rank(xn_ctx_t* ctx);

void xn_shuffler_enqueue(xn_ctx_t* ctx, void* buf, unsigned char buf_sz,
                         int epoch, int dst, int src);

/* xn_shuffler_epoch_end: do necessary flush at the end of an epoch */
extern void xn_shuffler_epoch_end(xn_ctx_t* ctx);

/* xn_shuffler_epoch_start: do necessary flush at the beginning of an epoch */
extern void xn_shuffler_epoch_start(xn_ctx_t* ctx);

/* xn_shuffler_destroy: shutdown the shuffler */
extern void xn_shuffler_destroy(xn_ctx_t* ctx);

/*
 * Default size of the rpc delivery queue.
 */
#define DEFAULT_DELIVER_MAX 256
