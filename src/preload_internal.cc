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

#include "preload_internal.h"

#include <mpi.h>

/* The global preload context */
preload_ctx_t pctx = {0};

/* exotic_write: expects preload_inkey and preload_invalue */
int exotic_write(const char* pkey, unsigned char pkey_len, char* pvalue,
                 unsigned char pvalue_len, int epoch, int src) {
  int rv;

  rv = preload_write(pkey, pkey_len, pvalue, pvalue_len, epoch, src);

  pctx.mctx.nfw++;

  return rv;
}

/* native_write: expects preload_inkey and preload_invalue */
int native_write(const char* pkey, unsigned char pkey_len, char* pvalue,
                 unsigned char pvalue_len, int epoch) {
  int rv;

  rv = preload_write(pkey, pkey_len, pvalue, pvalue_len, epoch, pctx.my_rank);

  pctx.mctx.nlw++;

  return rv;
}

namespace {
struct barrier_state {
  double time;
  int rank;
};
}  // namespace

void PRELOAD_Barrier(MPI_Comm comm) {
  struct barrier_state start;
  struct barrier_state min;
  double dura;
  int ok;

  if (pctx.my_rank == 0) {
    flog(LOG_INFO, "barrier ...\n   MPI Barrier");
  }
  start.time = MPI_Wtime();
  start.rank = pctx.my_rank;
  if (pctx.mpi_wait >= 0) {
    MPI_Status status;
    MPI_Request req;
    MPI_Iallreduce(&start, &min, 1, MPI_DOUBLE_INT, MPI_MINLOC, comm, &req);
    for (ok = 0; !ok;) {
      usleep(pctx.mpi_wait * 1000);
      MPI_Test(&req, &ok, &status);
    }
  } else {
    MPI_Allreduce(&start, &min, 1, MPI_DOUBLE_INT, MPI_MINLOC, comm);
  }

  if (pctx.my_rank == 0) {
    dura = MPI_Wtime() - min.time;
#ifdef PRELOAD_BARRIER_VERBOSE
    flog(LOG_INFO, "barrier ok (\n /* rank %d waited longest */\n %s+\n)",
         min.rank, pretty_dura(dura * 1000000).c_str());
#else
    flog(LOG_INFO, "barrier %s+", pretty_dura(dura * 1000000).c_str());
#endif
  }
}
