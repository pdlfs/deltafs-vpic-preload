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
 * xn_shuffler.h  wrapper implementation for the embedded 3-hop shuffler.
 *
 * A list of all environmental variables used by us:
 *
 *  SHUFFLE_Mercury_proto
 *    Mercury rpc proto for the remote hop
 *  SHUFFLE_Placement_protocol
 *    Protocol name for initializing placement groups
 *      such as hash_spooky, hash_lookup3, xor, as well as ring
 *  SHUFFLE_Virtual_factor
 *    Virtual factor used by nodes in a placement group
 *  SHUFFLE_Log_file
 *    Log file to store shuffler stats
 *  SHUFFLE_Remote_buftarget
 *    Memory allocated for each remote rpc queue
 *  SHUFFLE_Remote_maxrpc
 *    Max num of outstanding rpcs allowed for the remote hop
 *  SHUFFLE_Local_buftarget
 *    Memory allocated for each local rpc queue
 *  SHUFFLE_Local_maxrpc
 *    Max num of outstanding rpcs allowed for the local hops
 *  SHUFFLE_Max_deliverq
 *    Queue size for final rpc delivery
 *  SHUFFLE_Min_port
 *    The min port number we can use
 *  SHUFFLE_Max_port
 *    The max port number we can use
 *  SHUFFLE_Subnet
 *    IP prefix of the subnet
 */

#pragma once

#include <stddef.h>

#include <ch-placement.h>
typedef struct ch_placement_instance* ch_t;
#include <deltafs-nexus/deltafs-nexus_api.h>
#include <mercury_types.h>

#include "shuffler/shuffler.h"

/* shuffle context for the multi-hop shuffler */
typedef struct xn_ctx {
  int global_barrier;  /* replace all local barriers with global barriers */
  hg_uint64_t rpcs[2]; /* total local/remote rpcs received */
  shuffler_t sh;
  nexus_ctx_t nx;
  ch_t ch;
} xn_ctx_t;

/* xn_shuffler_init: init the shuffler or die */
extern void xn_shuffler_init(xn_ctx_t* ctx);

/* xn_shuffler_write: shuffle a file write or die */
extern void xn_shuffler_write(xn_ctx_t* ctx, const char* fn, char* data,
                              size_t len, int epoch);

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
