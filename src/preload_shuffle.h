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
 * preload_shuffle.h  abstract shuffle service api.
 *
 * A list of all environmental variables used by us:
 *
 *  SHUFFLE_Use_multihop
 *    Use the three-hop shuffler instead of the default NN shuffler
 *  SHUFFLE_Force_rpc
 *    Send rpcs even if target is local
 *  SHUFFLE_Placement_protocol
 *    Protocol name for initializing placement groups
 *      such as static_modulo, hash_spooky, hash_lookup3, xor, as well as ring
 *  SHUFFLE_Virtual_factor
 *    Virtual factor used by nodes in a placement group
 *  SHUFFLE_Recv_radix
 *    Number of senders (1**radix) per receiver
 *  SHUFFLE_Finalize_pause
 *    Number of secs to sleep after releasing the shuffle instance
 *      for shuffle bg threads to complete shutdown
 */
#pragma once

#include <inttypes.h>
#include <signal.h>
#include <stddef.h>

/*
 * shuffle_priority_cb_t: pointer to a callback function used to
 * deliver a priority msg.  this function is allowed to block
 * (though that may trigger flow control).
 */
typedef void (*shuffle_priority_cb_t)(int src, int dst, uint32_t type,
                                      void *d, uint32_t datalen);

typedef struct shuffle_ctx {
  /* internal shuffle impl */
  void* rep;
#ifdef PRELOAD_HAS_CH_PLACEMENT
  /* consistent hash context */
  struct ch_placement_instance* chp;
#endif
  /* whether shuffle should never be bypassed
   * even when destination is local. it is often necessary to
   * avoid bypassing the shuffle. this is because the main thread
   * will be used to do the final write to the memtable if shuffle
   * is bypassed and destination is local. so there is a chance where the main
   * thread is blocked and cannot go send more writes. */
  int force_rpc;
  /* number of secs to sleep after releasing the shuffle instance so
   * shuffle bg threads can complete shutdown in the meantime. */
  int finalize_pause;
  unsigned int receiver_rate; /* only 1/receiver_rate ranks are receivers */
  /* (rank & receiver_mask) -> receiver_rank */
  unsigned int receiver_mask;
  int is_receiver;
  unsigned char skey_len;
  unsigned char extra_data_len;
  unsigned char svalue_len;
  /* shuffle type */
  int type;
#define SHUFFLE_NN 0 /* default */
#define SHUFFLE_XN 1
  shuffle_priority_cb_t priority_cb;   /* NULL to disable priority shuffle */
} shuffle_ctx_t;

/*
 * shuffle_prepare_uri: obtain the mercury server uri to bootstrap the rpc.
 * write the server uri into *buf on success, or abort on errors.
 */
void shuffle_prepare_uri(char* buf);

/* return 0 if some ranks are sender-only, 1 otherwise. */
int shuffle_is_everyone_receiver(shuffle_ctx_t* ctx);

/* return 0 if a specific rank is not a receiver, 1 otherwise. */
int shuffle_is_rank_receiver(shuffle_ctx_t* ctx, int rank);

/* return the global index for a shuffle participant. */
int shuffle_rank(shuffle_ctx_t* ctx);

/* return the total number of shuffle participant */
int shuffle_world_sz(shuffle_ctx_t* ctx);

/*
 * shuffle_write_mux: shuffle a write request through an underlying transport.
 *
 * a multiplexer function that may call shuffle_write or one of the utility
 * shuffles dedveloped for various benchmarks.
 *
 * return 0 on success, or EOF or errors.
 */
int shuffle_write_mux(shuffle_ctx_t* ctx, const char* skey,
                      unsigned char skey_len, char* svalue,
                      unsigned char svalue_len, int epoch);

/*
 * shuffle_write: shuffle a write request through an underlying transport.
 *
 * shuffle may be bypassed if destination is local.
 * write requests may be buffered and batched with other write
 * requests to amortize the cost of native rpcs.
 * batched write requests may be forwarded through multiple
 * hops to reduce per core memory for network buffers.
 *
 * return 0 on success, or EOF or errors.
 */
int shuffle_write(shuffle_ctx_t* ctx, const char* skey,
                  unsigned char skey_len, char* svalue,
                  unsigned char svalue_len, int epoch);

/*
 * shuffle_epoch_start: perform necessary flushes at the
 * beginning of an epoch.
 *
 * abort on errors.
 */
void shuffle_epoch_start(shuffle_ctx_t* ctx);

/*
 * shuffle_epoch_pre_start: pre-flush the shuffle directly at the end of an
 * epoch.
 *
 * abort on errors.
 */
void shuffle_epoch_pre_start(shuffle_ctx_t* ctx);

/*
 * shuffle_epoch_end: perform necessary flushes at the
 * end of an epoch.
 *
 * abort on errors.
 */
void shuffle_epoch_end(shuffle_ctx_t* ctx);

/*
 * shuffle_finalize: shutdown the shuffle service and release resources.
 */
void shuffle_finalize(shuffle_ctx_t* ctx);

/*
 * shuffle_init: initialize the shuffle service or die.
 */
void shuffle_init(shuffle_ctx_t* ctx);

/*
 * shuffle_pause: temporarily stop running background threads.
 */
void shuffle_pause(shuffle_ctx_t* ctx);

/*
 * shuffle_resume: resume stopped background threads.
 */
void shuffle_resume(shuffle_ctx_t* ctx);

/*
 * shuffle_data_target: get shuffle target from indexed prop
 */
int shuffle_data_target2(const float& indexed_prop);

/*
 * shuffle_target: return the shuffle destination for a given req.
 */
int shuffle_target(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz);

/*
 * shuffle_handle: process an incoming shuffled write. here "peer_rank" refers
 * to the original sender, and "rank" refers to us.
 *
 * return 0 on success, or EOF on errors.
 */
int shuffle_handle(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz,
                   int epoch, int peer_rank, int rank);

/*
 * shuffle_msg_sent: callback for a shuffle sender to
 * notify the main system of the sending of an rpc request.
 *
 * note: the main system may pass 1 or 2 opaque
 * arguments back to the shuffler for
 * temporary storage.
 */
void shuffle_msg_sent(size_t n, void** arg1, void** arg2);

/*
 * shuffle_msg_replied: callback for a shuffler sender to
 * notify the main system of the reception of an rpc response.
 *
 * note: a shuffler must return any arguments
 * previously obtained from the main system.
 */
void shuffle_msg_replied(void* arg1, void* arg2);

/*
 * shuffle_msg_received: callback for a shuffler receiver
 * to notify the main system of the reception of
 * an rpc request.
 */
void shuffle_msg_received();
