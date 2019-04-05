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

#include "nn_shuffler_internal.h"

#include <assert.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>

#include <pdlfs-common/xxhash.h>
#define ORD(x, seed) pdlfs::xxhash32(&x, sizeof(x), seed)
#define HASH(msg, sz) pdlfs::xxhash32(msg, sz, 0)

#include <algorithm>
#include <vector>

/* The global nn shuffler context */
nn_ctx_t nnctx = {0};

namespace {
struct std_less_than {
  explicit std_less_than(int seed) : seed_(seed) {}
  bool operator()(int i, int j) { return (ORD(i, seed_) < ORD(j, seed_)); }
  int seed_;
};
}  // namespace

void nn_vector_random_shuffle(int seed, std::vector<int>* input) {
  std::sort(input->begin(), input->end(), std_less_than(seed));
}

namespace {
/* nn_shuffler_hashsig: generates a 32-bits hash signature for a given input */
hg_uint32_t nn_shuffler_hashsig(const write_in_t* in) {
  char buf[20];
  uint32_t tmp;
  assert(in != NULL);

  memcpy(buf, &in->sz, 4);

  memcpy(buf + 1 * 4, &in->dst, 4);
  memcpy(buf + 2 * 4, &in->src, 4);
  memcpy(buf + 3 * 4, &in->epo, 4);

  assert(in->msg != NULL);
  tmp = HASH(in->msg, in->sz);
  memcpy(buf + 16, &tmp, 4);

  return HASH(buf, 20);
}
}  // namespace

/* nn_shuffler_maybe_hashsig: return the hash signature */
hg_uint32_t nn_shuffler_maybe_hashsig(const write_in_t* in) {
  if (nnctx.hash_sig) {
    return nn_shuffler_hashsig(in);
  } else {
    return 0;
  }
}

hg_return_t nn_shuffler_write_in_proc(hg_proc_t proc, void* data) {
  hg_return_t hret;

  write_in_t* const in = static_cast<write_in_t*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  if (op == HG_ENCODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->hash_sig);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);

    hret = hg_proc_hg_int32_t(proc, &in->dst);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_int32_t(proc, &in->src);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_int32_t(proc, &in->epo);
    if (hret != HG_SUCCESS) return (hret);

    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else if (op == HG_DECODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->hash_sig);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);

    hret = hg_proc_hg_int32_t(proc, &in->dst);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_int32_t(proc, &in->src);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_int32_t(proc, &in->epo);
    if (hret != HG_SUCCESS) return (hret);

    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else {
    hret = HG_SUCCESS; /* noop */
  }

  return hret;
}

hg_return_t nn_shuffler_write_out_proc(hg_proc_t proc, void* data) {
  hg_return_t hret;

  write_out_t* const out = reinterpret_cast<write_out_t*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  if (op == HG_ENCODE) {
    hret = hg_proc_hg_int32_t(proc, &out->rv);
  } else if (op == HG_DECODE) {
    hret = hg_proc_hg_int32_t(proc, &out->rv);
  } else {
    hret = HG_SUCCESS; /* noop */
  }

  return hret;
}

void rpc_failed(hg_return_t hret, const char* msg, const char* func,
                const char* file, int line) {
  fputs("*** RPC *** ", stderr);
  fprintf(stderr, "@@ %s:%d @@ %s] ", file, line, func);
  fputs(msg, stderr);
  const char* hstr = HG_Error_to_string(hret);
  fprintf(stderr, ": %s (hret=%d)", hstr, int(hret));
  fputc('\n', stderr);
  print_meminfo();
  abort();
}
