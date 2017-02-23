/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#pragma once

#include <deltafs/deltafs_api.h>
#include <sys/time.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

extern "C" {

/*
 * Histogram.
 *
 * The first four are num. max, min, and sum.
 */
#define MON_NUM_BUCKETS 142
typedef double (hstg_t)[MON_NUM_BUCKETS + 4];

/*
 * the current timestamp in microseconds.
 */
static inline uint64_t now_micros() {
    uint64_t t;

#if defined(__linux)
    struct timespec tp;

#if defined(CLOCK_MONOTONIC_RAW)
    clock_gettime(CLOCK_MONOTONIC_RAW, &tp);
#else
    clock_gettime(CLOCK_MONOTONIC, &tp);
#endif

    t = static_cast<uint64_t>(tp.tv_sec) * 1000000;
    t += tp.tv_nsec / 1000;
#else
    struct timeval tv;

    gettimeofday(&tv, NULL);
    t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
    t += tv.tv_usec;
#endif

    return(t);
}

/*
 * statistics maintained by deltafs
 */
typedef struct dir_stat {
    /* total time spent on compaction */
    unsigned long long total_compaction_time;

    /* bytes of data pushed to index log */
    unsigned long long total_index_size;
    /* bytes of data pushed to data log */
    unsigned long long total_data_size;

} dir_stat_t;

/*
 * XXX: we could use atomic counters in future
 * when VPIC goes openmp.
 *
 */
typedef struct mon_ctx {

    /* !!! auxiliary state !!! */
    dir_stat_t last_dir_stat;
    uint64_t last_write_micros;  /* timestamp of the previous write */
    uint64_t epoch_start;   /* the start time of an epoch */
    int epoch_seq;   /* epoch seq num */

    int global;   /* is stats global or local (per-rank) */

    /* !!! main monitoring state !!! */

    unsigned min_fnl;   /* min file name length */
    unsigned max_fnl;   /* max file name length */

    /* total file name length */
    unsigned long long sum_fnl;

    unsigned min_wsz;   /* min app write size */
    unsigned max_wsz;   /* max app write size */

    /* total app write size */
    unsigned long long sum_wsz;

    /* writes being shuffled out with remote write successful */
    unsigned long long nwsok;
    /* total num of writes being shuffled out */
    unsigned long long nws;

    /* num of writes sent per rank */
    unsigned long long max_nws;
    unsigned long long min_nws;

    /* writes being shuffled in with local write successful */
    unsigned long long nwrok;
    /* total num of writes being shuffled in */
    unsigned long long nwr;

    /* num of writes received per rank */
    unsigned long long max_nwr;
    unsigned long long min_nwr;

    hstg_t hstgrpcw;      /* rpc write latency distribution */

    /* num of writes to deltafs with rv != EOF */
    unsigned long long nwok;
    /* total num of writes to deltafs */
    unsigned long long nw;

    /* num of writes executed per rank */
    unsigned long long max_nw;
    unsigned long long min_nw;

    hstg_t hstgarr;    /* write interval distribution (mean time to arrive) */
    hstg_t hstgw;      /* deltafs write latency distribution */

    unsigned long long dura;   /* epoch duration */

    /* !!! collected by deltafs !!! */
    dir_stat_t dir_stat;

} mon_ctx_t;


extern mon_ctx_t mctx;

extern int mon_preload_write(const char* fn, char* data, size_t n,
        int epoch, int is_foreign, mon_ctx_t* ctx);
extern int mon_shuffle_write_async(const char* fn, char* data, size_t n,
        int epoch, mon_ctx_t* ctx);
extern int mon_shuffle_write(const char* fn, char* data, size_t n,
        int epoch, mon_ctx_t* ctx);

extern void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum);
extern void mon_dumpstate(int fd, const mon_ctx_t* ctx);
extern void mon_reinit(mon_ctx_t* ctx);

} // extern C
