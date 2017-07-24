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

#include <sys/resource.h>
#include <sys/time.h>

#include <deltafs/deltafs_api.h>

#include "common.h"
#include "preload_mon.h"

#include <set>

/*
 * preload context:
 *   - run-time state of the preload layer
 */
typedef struct preload_ctx {
  const char* deltafs_root; /* deltafs root */
  size_t len_deltafs_root;  /* strlen */

  const char* local_root; /* localfs root */
  size_t len_local_root;  /* strlen */

  int mode; /* operating mode */

  int paranoid_checks; /* various checks on vpic writes */

  /* MPI barriers at the beginning of an epoch */
  int paranoid_barrier;      /* right before an epoch flush */
  int paranoid_post_barrier; /* after an epoch flush */

  /* MPI barriers at the end of an epoch */
  int paranoid_pre_barrier; /* right before a soft epoch flush */

  int pre_flushing; /* force a soft flush at the end of an epoch */

  int myrank; /* my MPI world rank */
  int commsz; /* my MPI world size */
  int mycpus; /* num of available cpu cores */

  const char* plfsdir;  /* path to the plfsdir */
  size_t len_plfsdir;   /* strlen */
  deltafs_tp_t* plfstp; /* opaque handle to a dedicated bg compaction pool */
  deltafs_plfsdir_t* plfsh; /* opaque handle to an opened plfsdir */

  int plfsparts; /* num of memtable partitions */

  int plfsfd; /* fd for the plfsdir */

  std::set<FILE*>* isdeltafs; /* open files owned by deltafs */

  std::set<std::string>* fnames; /* used for checking unique file names */

  int testin;    /* developer mode - for debug use only */
  int fake_data; /* replace vpic output with fake data - for debug only */
  int noscan;    /* do not probe sys info */

  mon_ctx_t mctx; /* mon stats */

  /* temporary mon stats */
  uint64_t last_sys_usage_snaptime;
  struct rusage last_sys_usage;
  dir_stat_t last_dir_stat;
  uint64_t epoch_start;

  int nomon;  /* skip monitoring */
  int nodist; /* skip copying mon files out */

  int logfd; /* descriptor for the testing log file */
  int monfd; /* descriptor for the mon dump file */

  int vmon; /* verbose mon stats */
  int verr; /* verbose error */

} preload_ctx_t;

extern preload_ctx_t pctx;
