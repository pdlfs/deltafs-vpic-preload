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

#include <deltafs/deltafs_api.h>

#ifdef PRELOAD_NEED_HISTO
/* compact histogram: the first four are num. max, min, and sum. */
#define MON_NUM_BUCKETS 142
typedef double(hstg_t)[MON_NUM_BUCKETS + 4];
#endif

/* statistics for an opened plfsdir */
typedef struct dir_stat {
  long long min_num_keys; /* min number of keys inserted per rank */
  long long max_num_keys; /* max number of keys inserted per rank */

  long long num_keys; /* total number of keys inserted */

  long long num_dropped_keys; /* total number of keys rejected by fs code */
  long long num_sstables;     /* total number of sst generated */

  /* total size of all sst bloom filter blocks */
  long long total_fblksz;
  /* total size of all sst index blocks */
  long long total_iblksz;
  /* total size of all data blocks */
  long long total_dblksz;

  /* total size of all user data */
  long long total_datasz;

} dir_stat_t;

typedef struct cpu_stat {
  unsigned long long sys_micros; /* system-level cpu time */
  unsigned long long usr_micros; /* user-level cpu time */

  unsigned long long micros; /* real time */

  /* per rank cpu usage */
  int min_cpu; /* from 0 to 100 */
  int max_cpu; /* from 0 to 100 */

} cpu_stat_t;

/*  NOTE
 * -------
 * + remote write:
 *     write executed on behalf of another rank
 * + local write:
 *     write directly executed locally (shortcut the shuffle path)
 */
typedef struct mon_ctx {
  /* !!! main monitoring state !!! */
  unsigned long long dura; /* total epoch duration */

  /* num of write batches sent per rank */
  unsigned long long max_nbs;
  unsigned long long min_nbs;
  /* total num of write batches being shuffled out */
  unsigned long long nbs;

  /* num of write batches received per rank */
  unsigned long long max_nbr;
  unsigned long long min_nbr;
  /* total num of write batches being shuffled in */
  unsigned long long nbr;

  /* total num of remote writes */
  unsigned long long nrw;
  /* total num of local writes */
  unsigned long long nlw;

  /* num of particle writes handled per rank */
  unsigned long long min_nw;
  unsigned long long max_nw;
  /* total num of particle writes */
  unsigned long long nw;

  /* !!! collected by deltafs !!! */
  dir_stat_t dir_stat;

  /* !!! collected by os !!!*/
  cpu_stat_t cpu_stat;

  /* !!! auxiliary state !!! */
  int global; /* is stats global or local (per-rank) */

  int epoch_seq; /* epoch seq num */

#define MON_BUF_SIZE 512
} mon_ctx_t;

extern int mon_fetch_plfsdir_stat(deltafs_plfsdir_t* dir, dir_stat_t* buf);

extern int mon_remote_write(const char* fn, char* data, size_t n, int epoch);
extern int mon_local_write(const char* fn, char* data, size_t n, int epoch);

extern int mon_shuffle_write_received();
extern int mon_shuffle_write_send_async(void* write_in, int peer_rank);
extern int mon_shuffle_write_send(void* write_in, int peer_rank);

extern void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum);
extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);
