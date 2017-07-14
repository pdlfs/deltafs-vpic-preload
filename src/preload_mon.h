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

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>

#include <deltafs/deltafs_api.h>

/*
 * Histogram.
 *
 * The first four are num. max, min, and sum.
 */
#define MON_NUM_BUCKETS 142
typedef double(hstg_t)[MON_NUM_BUCKETS + 4];

/*
 * the current timestamp in microseconds.
 */
static inline uint64_t now_micros() {
  uint64_t t;

#if defined(__linux) && defined(PRELOAD_USE_CLOCK_GETTIME)
  struct timespec tp;

#if defined(CLOCK_MONOTONIC_RAW)
  clock_gettime(CLOCK_MONOTONIC_RAW, &tp);
#else
  clock_gettime(CLOCK_MONOTONIC, &tp);
#endif

  t = static_cast<uint64_t>(tp.tv_sec) * 1000000;
  t += tp.tv_nsec / 1000;
#else
  struct timeval tv;

  gettimeofday(&tv, NULL);
  t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
  t += tv.tv_usec;
#endif

  return (t);
}

/*
 * statistics maintained by deltafs
 */
typedef struct dir_stat {
  /* total time spent on compaction */
  unsigned long long total_compaction_time;

  /* bytes of data pushed to index log */
  unsigned long long total_index_size;
  /* bytes of data pushed to data log */
  unsigned long long total_data_size;

} dir_stat_t;

/*
 * XXX: we could use atomic counters in future
 * when VPIC goes openmp.
 *
 */
typedef struct mon_ctx {
  /* !!! auxiliary state !!! */
  dir_stat_t last_dir_stat;
  uint64_t last_write_micros; /* timestamp of the previous write */
  uint64_t epoch_start;       /* the start time of an epoch */
  int epoch_seq;              /* epoch seq num */

  int global; /* is stats global or local (per-rank) */

  /* !!! main monitoring state !!! */

  unsigned min_fnl; /* min file name length */
  unsigned max_fnl; /* max file name length */

  /* total file name length */
  unsigned long long sum_fnl;

  unsigned min_wsz; /* min app write size */
  unsigned max_wsz; /* max app write size */

  /* total app write size */
  unsigned long long sum_wsz;

  /* writes being shuffled out with remote write successful */
  unsigned long long nwsok;
  /* total num of writes being shuffled out */
  unsigned long long nws;

  /* num of writes sent per rank */
  unsigned long long max_nws;
  unsigned long long min_nws;

  /* writes being shuffled in with local write successful */
  unsigned long long nwrok;
  /* total num of writes being shuffled in */
  unsigned long long nwr;

  /* num of writes received per rank */
  unsigned long long max_nwr;
  unsigned long long min_nwr;

  hstg_t hstgrpcw; /* rpc write latency distribution */

  /* num of writes to deltafs with rv != EOF */
  unsigned long long nwok;
  /* total num of writes to deltafs */
  unsigned long long nw;

  /* num of writes executed per rank */
  unsigned long long max_nw;
  unsigned long long min_nw;

  hstg_t hstgarr; /* write interval distribution (mean time to arrive) */
  hstg_t hstgw;   /* deltafs write latency distribution */

  unsigned long long dura; /* epoch duration */

  /* !!! collected by deltafs !!! */
  dir_stat_t dir_stat;

} mon_ctx_t;

extern mon_ctx_t mctx;

extern int mon_fetch_plfsdir_stat(deltafs_plfsdir_t* dir, dir_stat_t* buf);

extern int mon_preload_write(const char* fn, char* data, size_t n, int epoch,
                             mon_ctx_t* ctx);

extern int mon_shuffle_write_received(mon_ctx_t* ctx);
extern int mon_shuffle_write_send_async(void* write_in, int peer_rank,
                                        mon_ctx_t* ctx);
extern int mon_shuffle_write_send(void* write_in, int peer_rank,
                                  mon_ctx_t* ctx);

extern void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum);
extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);
