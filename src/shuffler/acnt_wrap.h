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

/*
 * acnt_wrap.h  wrapper for mercury atomic counters
 * 25-Aug-2017  chuck@ece.cmu.edu
 */

/*
 * mercury provides portable atomic counters for us using what ever
 * is available on the current system (e.g. OPA library, stdatomic.h,
 * OSX's OSAtomic.h, the windows API).   while this works fine for
 * C, there are C++ issues when mercury decides to use stdatomic.h.
 * ( see https://gcc.gnu.org/bugzilla/show_bug.cgi?id=60932 ).
 * 
 * for C, both gcc and clang provide an "_Atomic" keyword that stdatomic
 * uses to declare atomic values:   
 *      _Atomic int foo;
 *
 * and mercury_atomic.h makes use of that.   unfortunately, for C++
 * g++ does not define _Atomic and you get compile errors.   clang
 * does define _Atomic (even though that isn't in the C++ standard).
 * the gcc people say that you shouldn't use _Atomic in C++ code,
 * and instead you should use <atomic>, but that isn't part of older
 * C++ (it is c++11?), so you can't count on it being there.   see
 * discussion in the 60932 bug report above).
 *
 * all we really want for shuffler is an atomic counter without having
 * to recreate all the portability tests that mercury has already done
 * for us.   to hack around this problem, we provide some C code that
 * wraps the mercury API and let our C++ code call that.
 */

#include <mercury.h>          /* typedefs */

#if defined(__cplusplus)
extern "C" {
#endif

/* 
 * acnt32_t: wraps an hg_atomic_int32_t defn in an opaque structure
 * (the hg_atomic_int32_t may use _Atomic keywords, so it can't be 
 * directly used in C++ code).
 */
struct acnt32_val;
typedef struct acnt32_val *acnt32_t;

/**
 * acnt32_alloc: allocate a 32 bit atomic counter and set to zero
 * @return NULL on falure, otherwise a pointer
 */
acnt32_t acnt32_alloc(void);

/**
 * acnt32_free: free a 32 bit atomic counter and set pointer to NULL
 * @param ac pointer to cnt we are freeing
 */
void acnt32_free(acnt32_t *ac);

/**
 * acnt32_decr: decr the counter and return the new value
 * @param ac the value to subtract 1 from
 * @return the new value
 */
int32_t acnt32_decr(acnt32_t ac);

/**
 * acnt32_get: get the current counter value
 * @return the current value
 */
int32_t acnt32_get(acnt32_t ac);

/**
 * acnt32_decr: incr the counter and return the new value
 * @param ac the value to add 1 to
 * @return the new value
 */
int32_t acnt32_incr(acnt32_t ac);

/**
 * acnt32_set: set the value of a counter
 * @param ac counter to set
 * @param value the new value
 */
void acnt32_set(acnt32_t ac, int32_t value);

#if defined(__cplusplus)
}  /* extern "C" */
#endif
