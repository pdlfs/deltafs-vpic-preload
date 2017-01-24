#pragma once

/*
 * Copyright (c) 2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

extern "C" {

#ifndef PDLFS_MUTEX_LOCKING

typedef int maybe_mutex_t;
typedef int maybe_mutexattr_t;
static inline int maybe_mutex_lock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_unlock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_trylock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_init(maybe_mutex_t* __mut,
        maybe_mutexattr_t* __attr) { return 0; }
static inline int maybe_mutex_destroy(maybe_mutex_t* __mut) { return 0; }
#define MAYBE_MUTEX_INITIALIZER 0

#else

typedef pthread_mutex_t maybe_mutex_t;
typedef pthread_mutexattr_t maybe_mutexattr_t;
#define maybe_mutex_lock(__mut) pthread_mutex_lock(__mut)
#define maybe_mutex_unlock(__mut) pthread_mutex_unlock(__mut)
#define maybe_mutex_trylock(__mut) pthread_mutex_trylock(__mut)
#define maybe_mutex_init(__mut, __attr) pthread_mutex_init(__mut, __attr)
#define maybe_mutex_destroy(__mut) pthread_mutex_destroy(__mut)
#define MAYBE_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER

#endif

/*
 * msg_abort: abort with a message
 */
static inline void msg_abort(const char *msg) {
    const char* err = strerror(errno);
    int d;   /* XXX to avoid compiler warning about write ret val */
    d = write(fileno(stderr), "!!!ABORT!!! ", sizeof("!!!ABORT!!! ") - 1);
    d = write(fileno(stderr), msg, strlen(msg));
    d = write(fileno(stderr), ": ", 2);
    d = write(fileno(stderr), err, strlen(err));
    d = write(fileno(stderr), "\n", 1);
    abort();
}

static inline void must_lockmutex(maybe_mutex_t* __mut) {
    int r = maybe_mutex_lock(__mut);
    if (r != 0) {
        msg_abort("mtx_lock");
    }
}

static inline void must_unlock(maybe_mutex_t* __mut) {
    int r = maybe_mutex_unlock(__mut);
    if (r != 0) {
        msg_abort("mtx_unlock");
    }
}

} // extern C
