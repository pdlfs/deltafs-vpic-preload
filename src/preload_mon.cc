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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "nn_shuffler_internal.h"

#include "preload_internal.h"
#include "preload_mon.h"

int mon_fetch_plfsdir_stat(deltafs_plfsdir_t* dir, dir_stat_t* buf) {
  assert(dir != NULL);
  assert(buf != NULL);
#define get_property deltafs_plfsdir_get_integer_property

  buf->total_datasz = get_property(dir, "total_user_data");
  buf->total_fblksz = get_property(dir, "sstable_filter_bytes");
  buf->total_iblksz = get_property(dir, "sstable_index_bytes");
  buf->total_dblksz = get_property(dir, "sstable_data_bytes");
  buf->num_sstables = get_property(dir, "num_sstables");
  buf->num_dropped_keys = get_property(dir, "num_dropped_keys");
  buf->num_keys = get_property(dir, "num_keys");
  buf->min_num_keys = buf->num_keys;
  buf->max_num_keys = buf->num_keys;

#undef get_property
  return 0;
}

namespace {

void dir_stat_reduce(const dir_stat_t* src, dir_stat_t* sum) {
  MPI_Reduce(const_cast<long long*>(&src->num_keys), &sum->num_keys, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->min_num_keys), &sum->min_num_keys, 1,
             MPI_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->max_num_keys), &sum->max_num_keys, 1,
             MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<long long*>(&src->total_fblksz), &sum->total_fblksz, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->total_iblksz), &sum->total_iblksz, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->total_dblksz), &sum->total_dblksz, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<long long*>(&src->total_datasz), &sum->total_datasz, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->num_dropped_keys),
             &sum->num_dropped_keys, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->num_sstables), &sum->num_sstables, 1,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
}

void cpu_stat_reduce(const cpu_stat_t* src, cpu_stat_t* sum) {
  MPI_Reduce(const_cast<unsigned long long*>(&src->vcs), &sum->vcs, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->ics), &sum->ics, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->sys_micros),
             &sum->sys_micros, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->usr_micros),
             &sum->usr_micros, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->micros), &sum->micros, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<int*>(&src->min_cpu), &sum->min_cpu, 1, MPI_INT,
             MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<int*>(&src->max_cpu), &sum->max_cpu, 1, MPI_INT,
             MPI_MAX, 0, MPI_COMM_WORLD);
}

void mem_stat_reduce(const mem_stat_t* src, mem_stat_t* sum) {
  MPI_Reduce(const_cast<long long*>(src->num), sum->num, MAX_PAPI_EVENTS,
             MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(src->min), sum->min, MAX_PAPI_EVENTS,
             MPI_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(src->max), sum->max, MAX_PAPI_EVENTS,
             MPI_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
}

}  // namespace

void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum) {
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_dura), &sum->min_dura, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_dura), &sum->max_dura, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nmd), &sum->nmd, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->nms), &sum->nms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nms), &sum->min_nms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nms), &sum->max_nms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nlmd), &sum->nlmd, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->nlms), &sum->nlms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nlms), &sum->min_nlms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nlms), &sum->max_nlms, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nmr), &sum->nmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nmr), &sum->min_nmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nmr), &sum->max_nmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nlmr), &sum->nlmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nlmr), &sum->min_nlmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nlmr), &sum->max_nlmr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nfw), &sum->nfw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->nlw), &sum->nlw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->ncw), &sum->ncw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->nw), &sum->nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nw), &sum->min_nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nw), &sum->max_nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  dir_stat_reduce(&src->dir_stat, &sum->dir_stat);
  cpu_stat_reduce(&src->cpu_stat, &sum->cpu_stat);
  mem_stat_reduce(&src->mem_stat, &sum->mem_stat);
}

#define DUMP(fd, buf, fmt, ...)                             \
  {                                                         \
    int n = snprintf(buf, sizeof(buf), fmt, ##__VA_ARGS__); \
    buf[n] = '\n';                                          \
    n = write(fd, buf, n + 1);                              \
  }

void mon_dumpstate(int fd, const mon_ctx_t* ctx) {
  char buf[1024];
  if (!ctx->global) {
    DUMP(fd, buf, "\n--- epoch-[%d] (rank %d) ---", ctx->epoch_seq,
         pctx.my_rank);
    DUMP(fd, buf, "!!! NON GLOBAL !!!");
  } else {
    DUMP(fd, buf, "\n--- epoch-[%d] ---", ctx->epoch_seq);
  }
  DUMP(fd, buf, "[M] epoch dura: %llu us", ctx->max_dura);
  DUMP(fd, buf, "[M] total sst filter bytes: %lld bytes",
       ctx->dir_stat.total_fblksz);
  DUMP(fd, buf, "[M] total sst indexes: %lld bytes",
       ctx->dir_stat.total_iblksz);
  DUMP(fd, buf, "[M] total sst data: %lld bytes", ctx->dir_stat.total_dblksz);
  DUMP(fd, buf, "[M] total num sst: %lld", ctx->dir_stat.num_sstables);
  DUMP(fd, buf, "[M] total rpc sent: %llu", ctx->nms);
  DUMP(fd, buf, "[M] min rpc sent per rank: %llu", ctx->min_nms);
  DUMP(fd, buf, "[M] max rpc sent per rank: %llu", ctx->max_nms);
  DUMP(fd, buf, "[M] total rpc received: %llu", ctx->nmr);
  DUMP(fd, buf, "[M] min rpc received per rank: %llu", ctx->min_nmr);
  DUMP(fd, buf, "[M] max rpc received per rank: %llu", ctx->max_nmr);
  DUMP(fd, buf, "[M] total remote writes: %llu", ctx->nfw);
  DUMP(fd, buf, "[M] total direct writes: %llu", ctx->nlw);
  DUMP(fd, buf, "[M] min num writes per rank: %llu", ctx->min_nw);
  DUMP(fd, buf, "[M] max num writes per rank: %llu", ctx->max_nw);
  DUMP(fd, buf, "[M] total writes: %llu", ctx->nw);
  if (!ctx->global) DUMP(fd, buf, "!!! NON GLOBAL !!!");
  DUMP(fd, buf, "--- end ---\n");
}

void mon_reinit(mon_ctx_t* ctx) {
  mon_ctx_t tmp = {0};
  *ctx = tmp;
}
