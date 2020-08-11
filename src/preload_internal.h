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

#include <mpi.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <deltafs/deltafs_api.h>

#include "common.h"
#include "preload_mon.h"
#include "preload_perfstats.h"
#include "preload_shuffle.h"
#include "preload_range.h"
#include "range_rtp.h"

#include "preload.h"

#include <map>
#include <set>
#include <vector>

/*
 * preload context:
 *   - run-time state of the preload layer
 */
typedef struct preload_ctx {
  const char* deltafs_mntp; /* deltafs mount point */
  size_t len_deltafs_mntp;  /* strlen */

  const char* local_root; /* localfs root */
  size_t len_local_root;  /* strlen */

  const char** ignore_dirs; /* dirs where file i/o should be ignored */
  size_t* len_ignore_dirs;  /* strlens */
  size_t num_ignore_dirs;

  const char* log_home; /* home for log dumps */
  size_t len_log_home;  /* strlen */

  int mpi_wait; /* number of millisecs to wait for MPI async operations */
  int mode;     /* operating mode */

  int paranoid_checks; /* various checks on vpic writes */

  /* MPI barriers at the beginning of an epoch */
  int paranoid_barrier;      /* right before an epoch flush */
  int paranoid_post_barrier; /* after an epoch flush */

  /* MPI barriers at the end of an epoch */
  int paranoid_pre_barrier; /* right before a soft epoch flush */

  int pre_flushing; /* force a soft flush at the end of an epoch */
  int pre_flushing_wait;
  int pre_flushing_sync;

  int my_rank; /* my MPI world rank */
  int comm_sz; /* my MPI world size */
  int my_cpus; /* num of available cpu cores */

  int particle_buf_size;
  int particle_size;       /* bytes in each particle */
  int particle_extra_size; /* extra padding for each particle shuffled */
  int particle_id_size;
  int particle_count;

  /* since some ranks may be sender-only, so we have a dedicated MPI
   * communicator formed specifically for receivers. note that each receiver may
   * be a sender as well. for those sender-only ranks, their receiver
   * communicators will be defined as MPI_COMM_NULL. */

  MPI_Comm recv_comm; /* dedicated communicator for receivers */
  int recv_rank;
  int recv_sz;

  const char* plfsdir; /* path to the plfsdir */
  size_t len_plfsdir;  /* strlen */

  deltafs_plfsdir_t* plfshdl; /* opaque handle to an opened plfsdir */
  deltafs_env_t* plfsenv;     /* opaque handle to an env instance */
  deltafs_tp_t* plfstp; /* opaque handle to a dedicated bg compaction pool */

  int plfsparts; /* num of memtable partitions */
  int plfsfd;    /* fd for the plfsdir */

#ifdef PRELOAD_HAS_PAPI
  std::vector<const char*>* papi_events;
  int papi_set; /* opaque event set descriptor */
#endif

  std::set<FILE*>* isdeltafs;    /* open files owned by deltafs */
  std::set<std::string>* fnames; /* used for checking unique file names */

  std::map<std::string, int>* smap; /* sampled particle names */

  int sthres;   /* sample threshold (num samples per 1 million input names) */
  int sampling; /* enable particle name sampling */
  int sideft;   /* use the bloomy format */
  int sideio;   /* use the wisc-key format */

  shuffle_ctx_t sctx; /* shuffle context */

  int testin; /* developer mode - for debug use only */
  int noscan; /* do not probe sys info */

  /* rank# less than this will get tapped */
  int pthread_tap;

  mon_ctx_t mctx; /* mon stats */

  /* temporary mon stats */
  uint64_t last_sys_usage_snaptime;
  struct rusage last_sys_usage;
  dir_stat_t last_dir_stat;
  uint64_t epoch_start;

  int nomon;  /* skip monitoring */
  int nopapi; /* skip papi monitoring  */
  int nodist; /* skip releasing mon and sampling results */
  int monfd;  /* descriptor for the mon dump file */

  int bgdepth;       /* number of background threads to launch */
  int bgpause;       /* no background activities during compute */
  int print_meminfo; /* if mem info should be collected and printed */
  int verbose;       /* verbose mode */

  FILE* trace;

  perfstats_ctx_t perf_ctx;
  /* TODO: clean up pvt common code from this ctx */
  range_ctx_t rctx;

  /* Contains main thread state for range queries */
  pivot_ctx_t pvt_ctx;

  struct reneg_ctx rtp_ctx;
  float data[40];
  int data_len;
  pthread_mutex_t data_mutex;
} preload_ctx_t;

extern preload_ctx_t pctx;

/*
 * exotic_write: perform a write on behalf of a remote rank.
 * return 0 on success, or EOF on errors.
 */
extern int exotic_write(const char* fname, unsigned char fname_len, char* data,
                        unsigned char data_len, int epoch, int src);

/*
 * native_write: perform a direct local write.
 * return 0 on success, or EOF on errors.
 */
extern int native_write(const char* fname, unsigned char fname_len, char* data,
                        unsigned char data_len, int epoch);

/*
 * PRELOAD_Barrier: perform a collective barrier operation
 * on the give communicator.
 */
extern void PRELOAD_Barrier(MPI_Comm comm);
