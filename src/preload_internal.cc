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

#include "preload_internal.h"

#include <mpi.h>

/* The global preload context */
preload_ctx_t pctx = {0};

int exotic_write(const char* id, unsigned char id_sz, char* data,
                 unsigned char data_len, int epoch) {
  int rv;

  rv = preload_write(id, data, data_len, epoch);
  pctx.mctx.nfw++;

  return rv;
}

int native_write(const char* id, unsigned char id_sz, char* data,
                 unsigned char data_len, int epoch) {
  int rv;

  rv = preload_write(id, data, data_len, epoch);
  pctx.mctx.nlw++;

  return rv;
}

namespace {
struct barrier_state {
  double time;
  int rank;
};
}  // namespace

void preload_barrier(MPI_Comm comm) {
  char msg[100];
  struct barrier_state start;
  struct barrier_state min;
  double dura;

  if (pctx.my_rank == 0) {
    INFO("barrier ...");
  }
  start.time = MPI_Wtime();
  start.rank = pctx.my_rank;
  MPI_Allreduce(&start, &min, 1, MPI_DOUBLE_INT, MPI_MINLOC,
                static_cast<MPI_Comm>(comm));
  if (pctx.my_rank == 0) {
    dura = MPI_Wtime() - min.time;
    if (pctx.verr) {
      snprintf(msg, sizeof(msg),
               "barrier ok (\n /* rank %d waited longest */\n %s+\n)", min.rank,
               pretty_dura(dura * 1000000).c_str());
    } else {
      snprintf(msg, sizeof(msg), "barrier %s+",
               pretty_dura(dura * 1000000).c_str());
    }
    INFO(msg);
  }
}
