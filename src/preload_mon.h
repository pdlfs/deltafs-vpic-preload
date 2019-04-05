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

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <deltafs/deltafs_api.h>

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
  unsigned long long vcs; /* voluntary context switches */
  unsigned long long ics; /* involuntary context switches */

  unsigned long long sys_micros; /* sys-level cpu time */
  unsigned long long usr_micros; /* usr-level cpu time */

  unsigned long long micros; /* real time */

  /* per rank cpu usage */
  int min_cpu; /* from 0 to 100 */
  int max_cpu; /* from 0 to 100 */

} cpu_stat_t;

typedef struct mem_stat {
#define MAX_PAPI_EVENTS 4
  long long max[MAX_PAPI_EVENTS]; /* per rank max */
  long long min[MAX_PAPI_EVENTS]; /* ... min */

  long long num[MAX_PAPI_EVENTS];
} mem_stat_t;

/*  NOTE
 * -------
 * + foreign write:
 *     write executed on behalf of another rank
 * + local write:
 *     write executed locally (shortcutting the shuffle path)
 */
typedef struct mon_ctx {
  /* !!! main monitoring state !!! */
  unsigned long long min_dura;
  unsigned long long max_dura;

  /* num of messages sent per rank */
  unsigned long long max_nms;
  unsigned long long min_nms;
  /* total num of messages being shuffled out */
  unsigned long long nms;
  /* total num of messages delivered */
  unsigned long long nmd;

  /* num of local messages sent per rank */
  unsigned long long max_nlms;
  unsigned long long min_nlms;
  /* total num of local messages being shuffled out */
  unsigned long long nlms;
  /* total num of local messages delivered */
  unsigned long long nlmd;

  /* num of messages received per rank */
  unsigned long long max_nmr;
  unsigned long long min_nmr;
  /* total num of messages being shuffled in */
  unsigned long long nmr;

  /* num of local messages received per rank */
  unsigned long long max_nlmr;
  unsigned long long min_nlmr;
  /* total num of local messages being shuffled in */
  unsigned long long nlmr;

  /* total num of foreign writes */
  unsigned long long nfw;
  /* total num of local writes */
  unsigned long long nlw;

  /* total num of particles with name collisions (conflicts) */
  unsigned long long ncw;
  /* num of particle writes handled per rank */
  unsigned long long min_nw;
  unsigned long long max_nw;
  /* total num of particle writes */
  unsigned long long nw;

  /* !!! collected by deltafs !!! */
  dir_stat_t dir_stat;

  /* !!! collected by the os !!!*/
  cpu_stat_t cpu_stat;

  /* !!! collected by papi !!! */
  mem_stat_t mem_stat;

  /* !!! auxiliary state !!! */
  int global; /* is stats global or local (per-rank) */

  int epoch_seq; /* epoch seq num */

#define MON_BUF_SIZE 512
} mon_ctx_t;

extern int mon_fetch_plfsdir_stat(deltafs_plfsdir_t* dir, dir_stat_t* buf);

extern void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum);
extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);
