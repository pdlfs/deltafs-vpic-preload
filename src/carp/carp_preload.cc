/*
 * Copyright (c) 2022 Carnegie Mellon University,
 * Copyright (c) 2022 Triad National Security, LLC, as operator of
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
 * carp_preload.cc  preload library init interface to carp
 * 01-Aug-2022  chuck@ece.cmu.edu
 */

#include <assert.h>
#include "carp.h"
#include "../common.h"
#include "../preload_internal.h"

namespace pdlfs {
namespace carp {

/*
 * carp_priority_callback: carp priority shuffle callback function.
 * we bounce this out to the preload's carp object.
 */
static void carp_priority_callback(int src, int dst, uint32_t type,
                                   void *d, uint32_t datalen) {
  pctx.carp->HandleMessage(d, datalen, src, type);
}

/*
 * preload_init_carpopts: called from preload_init() when carp is enabled
 * to allocate and fill out carp options based on environment variable
 * settings.  returned option structure must be deleted by caller.
 */
struct CarpOptions *preload_init_carpopts(shuffle_ctx_t *sx) {
  struct CarpOptions *opts = new pdlfs::carp::CarpOptions();
  const char* tmp;

  tmp = maybe_getenv("PRELOAD_Particle_indexed_attr_size");
  if (tmp != NULL) {
    opts->index_attr_size = atoi(tmp);
    assert(opts->index_attr_size == sizeof(float)); /* XXX: allow double */
  } else {
    opts->index_attr_size = sizeof(float);
  }

  tmp = maybe_getenv("PRELOAD_Particle_indexed_attr_offset");
  if (tmp != NULL) {
    opts->index_attr_offset = atoi(tmp);
    assert(opts->index_attr_offset >= 0);
  } else {
    opts->index_attr_offset = 0;
  }

  tmp = maybe_getenv("RANGE_Oob_size");
  if (tmp != NULL) {
    opts->oob_sz = atoi(tmp);
    assert(opts->oob_sz > 0);
  } else {
    opts->oob_sz = DEFAULT_OOBSZ;
  }

  tmp = maybe_getenv("RANGE_Reneg_policy");
  if (tmp != NULL) {
    opts->reneg_policy = tmp;
  } else {
    opts->reneg_policy = pdlfs::kDefaultRenegPolicy;
  }

  tmp = maybe_getenv("RANGE_Reneg_interval");
  if (tmp != NULL) {
    opts->reneg_intvl = atoi(tmp);
    opts->dynamic_intvl = atoi(tmp);
  } else {
    opts->reneg_intvl = pdlfs::kRenegInterval;
    opts->dynamic_intvl = pdlfs::kRenegInterval;
  }

  tmp = maybe_getenv("RANGE_Dynamic_threshold");
  if (tmp != NULL) {
    opts->dynamic_thresh = atof(tmp);
  } else {
    opts->dynamic_thresh = pdlfs::kDynamicThreshold;
  }

#define INIT_PVTCNT(arr, idx)                \
  tmp = maybe_getenv("RANGE_Pvtcnt_s" #idx); \
  if (tmp != NULL) {                         \
    (arr)[(idx)] = atoi(tmp);                \
    assert(arr[(idx)] > 0);                  \
  } else {                                   \
    (arr)[(idx)] = DEFAULT_PVTCNT;           \
  }

  opts->rtp_pvtcnt[0] = 0;          /* note: first element is not used */
  INIT_PVTCNT(opts->rtp_pvtcnt, 1);
  INIT_PVTCNT(opts->rtp_pvtcnt, 2);
  INIT_PVTCNT(opts->rtp_pvtcnt, 3);

#undef INIT_PVTCNT

  opts->env = Env::Default();

  /* save shuffle context and configure priority callback */
  opts->sctx = sx;
  sx->priority_cb = carp_priority_callback;

  /* final values of these are set at MPI_Init() time */
  opts->my_rank = opts->num_ranks = 0;
  opts->enable_perflog = 0;
  opts->log_home = NULL;

  return opts;
}

/*
 * preload_mpiinit_carpopts: called from preload MPI_Init() function
 * to init the MPI-related CarpOptions fields after MPI_Init() has
 * run.
 */
void preload_mpiinit_carpopts(preload_ctx_t *pc, struct CarpOptions *copts,
                              const char *strippedpath) {
  copts->my_rank = pc->my_rank;
  copts->num_ranks = pc->comm_sz;
  copts->mount_path = pc->local_root;
  copts->mount_path += "/";
  copts->mount_path += strippedpath;
  /* use pctx.nomon to control perflog for now */
  if (!pctx.nomon && pctx.log_home) {
    copts->enable_perflog = 1;
    copts->log_home = pctx.log_home;
  }

  if (copts->my_rank == 0) {
    logf(LOG_INFO, "[carp] CARP enabled!");
    logf(LOG_INFO, "[carp] reneg_intvl: %" PRIu64
                   ", reneg_thresh: %.3f, perflog: %d",
         copts->reneg_intvl, copts->dynamic_thresh, copts->enable_perflog);
  }

}

/*
 * preload_finalize_carp: clear out carp from a preload context.
 * called at MPI_Finalize time.
 */
void preload_finalize_carp(preload_ctx_t *pc) {
  delete pc->carp;
  pc->carp = nullptr;

  delete pc->opts;
  pc->opts = nullptr;

  pc->carp_on = 0;   /* to be safe */
}

}  // carp namespace
}  // pdlfs namespace
