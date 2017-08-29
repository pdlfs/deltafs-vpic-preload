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
 * acnt_wrap.c  wrapper for mercury atomic counters
 * 25-Aug-2017  chuck@ece.cmu.edu
 */

#include <stdlib.h>
#include <mercury_atomic.h>
#include "acnt_wrap.h"

/*
 * actual internal defn of acnt32_t
 */
struct acnt32_val {
  hg_atomic_int32_t val;
};

/*
 * acnt32_alloc: allocate a 32 bit atomic counter and set to zero
 */
acnt32_t acnt32_alloc(void) {
  acnt32_t rv;
  rv = (acnt32_t)malloc(sizeof(*rv));
  if (rv)
     hg_atomic_set32(&rv->val, 0);
  return(rv);
}

/*
 * acnt32_free: free a 32 bit atomic counter and set pointer to NULL
 */
void acnt32_free(acnt32_t *ac) {
  if (ac && *ac) {
    free(*ac);
    *ac = NULL;
  }
}

/*
 * acnt32_decr: decr the counter and return the new value
 */
int32_t acnt32_decr(acnt32_t ac) {
  return(hg_atomic_decr32(&ac->val));
}

/*
 * acnt32_get: get the current counter value
 */
int32_t acnt32_get(acnt32_t ac) {
  return(hg_atomic_get32(&ac->val));
}

/*
 * acnt32_decr: incr the counter and return the new value
 */
int32_t acnt32_incr(acnt32_t ac) {
  return(hg_atomic_incr32(&ac->val));
}

/*
 * acnt32_set: set the value of a counter
 */
void acnt32_set(acnt32_t ac, int32_t value) {
  hg_atomic_set32(&ac->val, value);
}
