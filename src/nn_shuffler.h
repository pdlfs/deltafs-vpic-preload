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
 * nn_shuffler.h  a simple shuffle implementation with N-N rpc endpoints.
 *
 * A list of all environmental variables used by us:
 *
 *  SHUFFLE_Mercury_proto
 *    Mercury rpc proto
 *  SHUFFLE_Mercury_progress_timeout
 *    Timeout for calling HG_Progress
 *  SHUFFLE_Mercury_progress_warn_interval
 *    Time between two HG_Progress calls that starts
 *      to cause warning messages
 *  SHUFFLE_Mercury_cache_handles
 *    Reuse mercury handles to avoid freq mallocs
 *  SHUFFLE_Mercury_max_errors
 *    Max errors before we abort
 *  SHUFFLE_Mercury_nice
 *    Nice value to be applied to the looper thread
 *  SHUFFLE_Hash_sig
 *    Generate a hash signature for each rpc message
 *  SHUFFLE_Paranoid_checks
 *    Enable paranoid checks on rpc messages
 *  SHUFFLE_Force_sync_rpc
 *    Disallow async rpcs
 *  SHUFFLE_Num_outstanding_rpc
 *    Max num of outstanding rpcs allowed
 *  SHUFFLE_Use_worker_thread
 *    Allocate a dedicated worker thread
 *  SHUFFLE_Subnet
 *    IP prefix of the subnet we prefer to use
 *  SHUFFLE_Min_port
 *    The min port number we can use
 *  SHUFFLE_Max_port
 *    The max port number we can use
 *  SHUFFLE_Buffer_per_queue
 *    Memory allocated for each rpc queue
 *  SHUFFLE_Timeout
 *    RPC timeout
 */

#pragma once

#include <stddef.h>

/* nn_shuffler_init: initialize the shuffle service or die. */
extern void nn_shuffler_init();

/* nn_shuffler_world_size: return comm world size */
extern int nn_shuffler_world_size();

/* nn_shuffler_my_rank: return my rank */
extern int nn_shuffler_my_rank();

/* nn_shuffler_enqueue: put an incoming write into an rpc queue. */
extern void nn_shuffler_enqueue(const char* fname, unsigned char fname_len,
                                char* data, size_t len, int epoch,
                                int peer_rank, int rank);

/* nn_shuffler_waitcb: wait for all outstanding rpcs to finish. */
extern void nn_shuffler_waitcb();

/* nn_shuffler_flushq: force flushing local rpc queues. */
extern void nn_shuffler_flushq();

/* nn_shuffler_bgwait: wait for all background rpc work to finish. */
extern void nn_shuffler_bgwait();

/* nn_shuffler_destroy: close the shuffler. */
extern void nn_shuffler_destroy();

/* nn_shuffler_sleep: put the background looper to sleep. */
extern void nn_shuffler_sleep();

/* nn_shuffler_wakeup: wake up a sleeping looper. */
extern void nn_shuffler_wakeup();

/*
 * The default min.
 */
#define DEFAULT_MIN_PORT 50000

/*
 * The default max.
 */
#define DEFAULT_MAX_PORT 59999

/*
 * Default amount of memory allocated for each rpc queue.
 *
 * This is considered a soft limit. There is also a hard limit
 * set for each rpc message.
 */
#define DEFAULT_BUFFER_PER_QUEUE 4096

/*
 * Default num of outstanding rpc.
 *
 * This is considered a soft limit. There is also a hard limit
 * set at compile time.
 *
 * Ignored if rpc is forced to be sync.
 */
#define DEFAULT_OUTSTANDING_RPC 16

/*
 * Default rpc timeout (in secs).
 *
 * Abort when a rpc fails to complete within this amount of time.
 *
 * A server may not be able to finish rpc in time if its
 * in-memory write buffer is full and the background compaction
 * progress is unable to keep up.
 *
 * Timeout ignored in testing mode.
 */
#define DEFAULT_TIMEOUT 300

/*
 * Default placement protocol.
 */
#define DEFAULT_PLACEMENT_PROTO "ring"

/*
 * Default virtual factor.
 *
 * Require a reasonably large number to achieve a more
 * uniform distribution.
 */
#define DEFAULT_VIRTUAL_FACTOR 1024

/*
 * The default subnet.
 *
 * Guaranteed to be wrong in production.
 */
#define DEFAULT_SUBNET "127.0.0.1"

/*
 * If "mercury_proto" is not specified, we set it to the follows.
 *
 * This assumes the mercury linked by us has been
 * built with this specific transport.
 *
 * Use of tcp is subject to high latency.
 */
#define DEFAULT_HG_PROTO "bmi+tcp"

/*
 * The default timeout for calling HG_Progress.
 *
 * Specified in milliseconds.
 */
#define DEFAULT_HG_TIMEOUT 100

/*
 * Max time between two HG_Progress calls that is still considered okay.
 *
 * Specified in milliseconds.
 */
#define DEFAULT_HG_INTERVAL 200
