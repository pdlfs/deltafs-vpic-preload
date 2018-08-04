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

#include "nn_shuffler_internal.h"

#include <mercury_proc.h>
#include <mercury_proc_string.h>
#include <pdlfs-common/xxhash.h>
#define HASH(msg, sz) pdlfs::xxhash32(msg, sz, 0)

#include <assert.h>

/* The global nn shuffler context */
nn_ctx_t nnctx = {0};

/* nn_shuffler_hashsig: generates a 32-bits hash signature for a given input */
static hg_uint32_t nn_shuffler_hashsig(const write_in_t* in) {
  char buf[16];
  uint32_t tmp;
  assert(in != NULL);

  memcpy(buf, &in->dst, 4);
  memcpy(buf + 4, &in->src, 4);
  memcpy(buf + 8, &in->ep, 2);
  memcpy(buf + 10, &in->sz, 2);

  assert(in->msg != NULL);
  tmp = HASH(in->msg, in->sz);
  memcpy(buf + 12, &tmp, 4);

  return HASH(buf, 16);
}

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

  write_in_t* in = static_cast<write_in_t*>(data);
  hg_proc_op_t op = hg_proc_get_op(proc);

  if (op == HG_ENCODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->hash_sig);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->dst);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->src);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->ep);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else if (op == HG_DECODE) {
    hret = hg_proc_hg_uint32_t(proc, &in->hash_sig);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->dst);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint32_t(proc, &in->src);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->ep);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_hg_uint16_t(proc, &in->sz);
    if (hret != HG_SUCCESS) return (hret);
    hret = hg_proc_memcpy(proc, in->msg, in->sz);

  } else {
    hret = HG_SUCCESS; /* noop */
  }

  return hret;
}

hg_return_t nn_shuffler_write_out_proc(hg_proc_t proc, void* data) {
  hg_return_t hret;

  write_out_t* out = reinterpret_cast<write_out_t*>(data);
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
