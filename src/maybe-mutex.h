#pragma once

/*
 * Copyright (c) 2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <pthread.h>
#include <stdlib.h>

extern "C" {

#ifndef PDLFS_MUTEX_LOCKING

typedef int pdlfs_maybe_mutex_t;
typedef int pdlfs_maybe_mutexattr_t;
inline int pdlfs_maybe_mutex_lock(pdlfs_maybe_mutex_t* __mut) { return 0; }
inline int pdlfs_maybe_mutex_unlock(pdlfs_maybe_mutex_t* __mut) { return 0; }
inline int pdlfs_maybe_mutex_trylock(pdlfs_maybe_mutex_t* __mut) { return 0; }
inline int pdlfs_maybe_mutex_init(pdlfs_maybe_mutex_t* __mut,
        pdlfs_maybe_mutexattr_t* __attr) { return 0; }
inline int pdlfs_maybe_mutex_destroy(pdlfs_maybe_mutex_t* __mut) { return 0; }
#define PDLFS_MAYBE_MUTEX_INITIALIZER 0

#else

typedef pthread_mutex_t pdlfs_maybe_mutex_t;
typedef pthread_mutexattr_t pdlfs_maybe_mutexattr_t;
#define pdlfs_maybe_mutex_lock(__mut) pthread_mutex_lock(__mut)
#define pdlfs_maybe_mutex_unlock(__mut) pthread_mutex_unlock(__mut)
#define pdlfs_maybe_mutex_trylock(__mut) pthread_mutex_trylock(__mut)
#define pdlfs_maybe_mutex_init(__mut, __attr) pthread_mutex_init(__mut, __attr)
#define pdlfs_maybe_mutex_destroy(__mut) pthread_mutex_destroy(__mut)
#define PDLFS_MAYBE_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER

#endif

inline void pdlfs_must_mutex_lock(pdlfs_maybe_mutex_t* __mut) {
    int r = pdlfs_maybe_mutex_lock(__mut);
    if (r != 0) {
        abort();
    }
}

inline void pdlfs_must_mutex_unlock(pdlfs_maybe_mutex_t* __mut) {
    int r = pdlfs_maybe_mutex_unlock(__mut);
    if (r != 0) {
        abort();
    }
}

} // extern C
