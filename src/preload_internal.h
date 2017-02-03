/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

#include <deltafs/deltafs_api.h>

#include "mon.h"

#include <set>

extern "C" {

#ifndef PRELOAD_MUTEX_LOCKING

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
 * verbose_error: print error with a message
 */
static inline void verbose_error(const char* msg) {
    char tmp[500];
    int n = snprintf(tmp, sizeof(tmp), "!!!ERROR!!! %s: %s\n", msg,
            strerror(errno));
    n = write(fileno(stderr), tmp, n);
}

/*
 * msg_abort: abort with a message
 */
static inline void msg_abort(const char *msg) {
    char tmp[500];
    int err_num = errno;
    const char* err = strerror(err_num);
    if (err_num != 0) {
        snprintf(tmp, sizeof(tmp), "!!!ABORT!!! %s: %s\n", msg, err);
    } else {
        snprintf(tmp, sizeof(tmp), "!!!ABORT!!! %s\n", msg);
    }
    int d = write(fileno(stderr), tmp, strlen(tmp));
    abort();
}

static inline void must_maybelockmutex(maybe_mutex_t* __mut) {
    int r = maybe_mutex_lock(__mut);
    if (r != 0) {
        msg_abort("mtx_lock");
    }
}

static inline void must_maybeunlock(maybe_mutex_t* __mut) {
    int r = maybe_mutex_unlock(__mut);
    if (r != 0) {
        msg_abort("mtx_unlock");
    }
}

static inline bool is_envset(const char* key) {
    const char* env = getenv(key);
    if (env == NULL) {
        return(false);
    } else if (strlen(env) == 0) {
        return(false);
    } else if (strlen(env) == 1 && strcmp(env, "0") == 0) {
        return(false);
    } else {
        return(true);
    }
}

/*
 * preload_write(fn, data, n):
 *   - ship data to deltafs
 */
extern int preload_write(const char* fn, char* data, size_t n);

/*
 * preload context:
 *   - run-time state of the preload layer
 */
typedef struct preload_ctx {
    const char* deltafs_root;     /* deltafs root */
    size_t len_deltafs_root;      /* strlen */

    const char* local_root;       /* localfs root */
    size_t len_local_root;        /* strlen */

    int mode;    /* operating mode */

    const char* plfsdir;      /* path to the plfsdir */
    size_t len_plfsdir;       /* strlen */
    DELTAFS_PLFSDIR* plfsh;   /* opaque handle to an opened plfsdir */
    int plfsfd;               /* fd for the plfsdir */

    std::set<FILE*>* isdeltafs;         /* open files owned by deltafs */

    int nomon;       /* skip monitoring */
    int verbose;     /* verbose error */
    int logfd;       /* opened descriptor of the log file */
    int testin;      /* developer mode */

} preload_ctx_t;


extern preload_ctx_t pctx;

} // extern C
