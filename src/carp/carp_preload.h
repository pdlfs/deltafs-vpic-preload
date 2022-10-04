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
 * carp_preload.h  preload library interface to carp
 * 01-Aug-2022  chuck@ece.cmu.edu
 */

#include "carp.h"

namespace pdlfs {
namespace carp {

/*
 * preload_init_carpopts: called from preload_init() when carp is enabled
 * to allocate and fill out carp options based on environment variable
 * settings.  we pass in the shuffle context so we can both put it in the
 * options and set the priority callback function.  the returned option
 * structure must be deleted by caller.
 */
struct CarpOptions *preload_init_carpopts(shuffle_ctx_t *sx);

/*
 * preload_mpiinit_carpopts: called from preload MPI_Init() function
 * to init the MPI-related CarpOptions fields after MPI_Init() has
 * run.
 */
void preload_mpiinit_carpopts(preload_ctx_t *pc, struct CarpOptions *copts,
                              const char *strippedpath);

/*
 * preload_finalize_carp: clear out carp from a preload context.
 */
void preload_finalize_carp(preload_ctx_t *pc);

}  // carp namespace
}  // pdlfs namespace
