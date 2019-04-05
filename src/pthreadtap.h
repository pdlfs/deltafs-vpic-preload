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

/*
 * pthreadtap.h  tap pthread creates to collect thread stats
 * 17-Jan-2018  chuck@ece.cmu.edu
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __USE_GNU
#include <sys/resource.h>
#else
#define __USE_GNU /* XXX: needed to get RUSAGE_THREAD */
#include <sys/resource.h>
#undef __USE_GNU
#endif

/*
 * tapuseprobe: start-end usage state
 */
struct tapuseprobe {
  int who;               /* flag to getrusage */
  struct timeval t0, t1; /* time at start/end */
  struct rusage r0, r1;  /* resource usage at start/end */
};

/**
 * tapuseprobe_print: a probe usage function that prints results to a FILE*
 *
 * @param out the output FILE*
 * @param up the tapuseprobe with the results in it
 * @param tag a tag string provided by the caller when creating the thread
 * @param n an int tag to prepend to output (use -1 to disable)
 */
void tapuseprobe_print(FILE* out, struct tapuseprobe* up, const char* tag,
                       int n);

/**
 * pthread_create_tap: create new thread with a usage tap added.
 * the first 4 args are the same as normal pthread_create().
 *
 * @param thread new thread returned here
 * @param attr new thread attrs
 * @param start_routine the main function for the user's thread
 * @param startarg arg for start routine
 * @param tag user-provided tag for outputing results
 * @param tagarg arg for tag_routine
 * @param tag_routine user-provided tag routine (NULL means use default)
 * @param nxt the real pthread_create function ptr when preloaded
 * @return 0 on success, errno on failure
 */
int pthread_create_tap(
    pthread_t* thread, const pthread_attr_t* attr,
    void* (*start_routine)(void*), void* startarg, const char* tag,
    void* tagarg, void*(tag_routine)(const char*, void*, struct tapuseprobe*),
    int (*nxt)(pthread_t* thread, const pthread_attr_t* attr,
               void* (*start_routine)(void*), void* startarg));
