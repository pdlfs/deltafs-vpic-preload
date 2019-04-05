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
#include "pthreadtap.h"

/*
 * tapuseprobe_start:  load starting values into useprobe
 *
 * @param up the tapuseprobe to fill out
 * @param who should always be RUSAGE_THREAD
 */
static void tapuseprobe_start(struct tapuseprobe* up, int who) {
  up->who = who;
  if (gettimeofday(&up->t0, NULL) < 0 || getrusage(up->who, &up->r0) < 0) {
    fprintf(stderr, "tapuseprobe_start syscall failed?!\n");
    abort();
  }
}

/*
 * tapuseprobe_end:  load ending values into tapuseprobe
 *
 * @param up the tapuseprobe to fill out
 */
static void tapuseprobe_end(struct tapuseprobe* up) {
  if (gettimeofday(&up->t1, NULL) < 0 || getrusage(up->who, &up->r1) < 0) {
    fprintf(stderr, "tapuseprobe_end syscall failed?!\n");
    abort();
  }
}

/*
 * tapuseprobe_print: default output routine (user can override)
 */
void tapuseprobe_print(FILE* out, struct tapuseprobe* up, const char* tag,
                       int n) {
  char nstr[32];
  double start, end;
  double ustart, uend, sstart, send;
  long nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw;

  if (n >= 0) {
    snprintf(nstr, sizeof(nstr), "%d: ", n);
  } else {
    nstr[0] = '\0';
  }

  start = up->t0.tv_sec + (up->t0.tv_usec / 1000000.0);
  end = up->t1.tv_sec + (up->t1.tv_usec / 1000000.0);

  ustart = up->r0.ru_utime.tv_sec + (up->r0.ru_utime.tv_usec / 1000000.0);
  uend = up->r1.ru_utime.tv_sec + (up->r1.ru_utime.tv_usec / 1000000.0);

  sstart = up->r0.ru_stime.tv_sec + (up->r0.ru_stime.tv_usec / 1000000.0);
  send = up->r1.ru_stime.tv_sec + (up->r1.ru_stime.tv_usec / 1000000.0);

  nminflt = up->r1.ru_minflt - up->r0.ru_minflt;
  nmajflt = up->r1.ru_majflt - up->r0.ru_majflt;
  ninblock = up->r1.ru_inblock - up->r0.ru_inblock;
  noublock = up->r1.ru_oublock - up->r0.ru_oublock;
  nnvcsw = up->r1.ru_nvcsw - up->r0.ru_nvcsw;
  nnivcsw = up->r1.ru_nivcsw - up->r0.ru_nivcsw;

  fprintf(out,
          "!! %s%s\n"
          "!! %stimes: wall=%f, usr=%f, sys=%f (secs)\n"
          "!! %sminflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
          nstr, tag, nstr, end - start, uend - ustart, send - sstart, nstr,
          nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
}

/*
 * pthreadtap: tap on a pthread
 */
struct pthreadtap {
  struct tapuseprobe up;              /* stats stored here */
  void* (*user_start_routine)(void*); /* user's callback */
  void* user_start_arg;               /* arg for above */
  const char* tag;                    /* output tag */
  void* tagarg;                       /* arg for tag_routine */
  /* optional user-provided output routine */
  void* (*tag_routine)(const char*, void*, struct tapuseprobe*);
};

/*
 * tap_cleanup: we got canceled, so our routine isn't going to return
 */
static void tap_cleanup(void* arg) {
  struct pthreadtap* tap = (struct pthreadtap*)arg;

  tapuseprobe_end(&tap->up);

  if (tap->tag_routine == NULL) {
    tapuseprobe_print(stderr, &tap->up, tap->tag, getpid());
  } else {
    tap->tag_routine(tap->tag, tap->tagarg, &tap->up);
  }

  free(tap);
}

/*
 * tap_wrap: wrapper function for user thread start_routine.
 * pthread_create() calls this, then we chain to the user's code.
 */
static void* tap_wrap(void* arg) {
  struct pthreadtap* tap = (struct pthreadtap*)arg;
  void* rv;
#if defined(__linux)
  tapuseprobe_start(&tap->up, RUSAGE_THREAD); /* linux only! */
#else
  tapuseprobe_start(&tap->up, RUSAGE_SELF);
#endif
  pthread_cleanup_push(tap_cleanup, tap);
  rv = tap->user_start_routine(tap->user_start_arg);
  pthread_cleanup_pop(0);
  tapuseprobe_end(&tap->up);

  if (tap->tag_routine == NULL) {
    tapuseprobe_print(stderr, &tap->up, tap->tag, getpid());
  } else {
    tap->tag_routine(tap->tag, tap->tagarg, &tap->up);
  }

  free(tap);
  return (rv);
}

/*
 * pthread_create_tap: create new thread with a usage tap added
 */
int pthread_create_tap(
    pthread_t* thread, const pthread_attr_t* attr,
    void* (*start_routine)(void*), void* startarg, const char* tag,
    void* tagarg, void*(tag_routine)(const char*, void*, struct tapuseprobe*),
    int (*nxt)(pthread_t* thread, const pthread_attr_t* attr,
               void* (*start_routine)(void*), void* startarg)) {
  struct pthreadtap* tap;
  int rv;

  tap = (struct pthreadtap*)malloc(sizeof(*tap));
  if (!tap) {
    fprintf(stderr, "pthread_create_tap: malloc failed\n");
    return (ENOMEM);
  }

  tap->user_start_routine = start_routine;
  tap->user_start_arg = startarg;
  tap->tag = tag;
  tap->tagarg = tagarg;
  tap->tag_routine = tag_routine;

  if (!nxt) {
    rv = pthread_create(thread, attr, tap_wrap, tap);
  } else {
    rv = nxt(thread, attr, tap_wrap, tap);
  }
  if (rv != 0) {
    free(tap);
  }

  return (rv);
}
