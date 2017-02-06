/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

#include "preload_internal.h"
#include "shuffle_internal.h"

#include "mon.h"

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

mon_ctx_t mctx = { 0 };

static const double BUCKET_LIMITS[MON_NUM_BUCKETS] = {
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
  50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
  500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
  3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
  16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
  70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
  250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
  900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
  3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
  9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
  25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
  70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
  180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
  450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
  1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
  2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
  5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
  1e200,
};

static void hstg_reset_min(hstg_t& h) {
    h[2] = BUCKET_LIMITS[MON_NUM_BUCKETS - 1];  /* min */
}

static void hstg_add(hstg_t& h, double d) {
    int b = 0;
    while (b < MON_NUM_BUCKETS - 1 && BUCKET_LIMITS[b] <= d) {
      b++;
    }
    h[4 + b] += 1.0;
    h[0] += 1.0;             /* num */
    if (h[1] < d) h[1] = d;  /* max */
    if (h[2] > d) h[2] = d;  /* min */
    h[3] += d;               /* sum */
}

static double hstg_ptile(const hstg_t& h, double p) {
    double threshold = h[0] * (p / 100.0);
    double sum = 0;
    for (int b = 0; b < MON_NUM_BUCKETS; b++) {
        sum += h[4 + b];
        if (sum >= threshold) {
            double left_point = (b == 0) ? 0 : BUCKET_LIMITS[b - 1];
            double right_point = BUCKET_LIMITS[b];
            double left_sum = sum - h[4 + b];
            double right_sum = sum;
            double pos = (threshold - left_sum) / (right_sum - left_sum);
            double r = left_point + (right_point - left_point) * pos;
            if (r < h[2]) r = h[2];  /* min */
            if (r > h[1]) r = h[1];  /* max */

            return(r);
        }
    }

    return(h[1]);  /* max */
}

static double hstg_max(const hstg_t& h) { return(h[1]); }

static double hstg_min(const hstg_t& h) { return(h[2]); }

static double hstg_avg(const hstg_t& h) {
  if (h[0] < 1.0) return(0);
  return(h[3] / h[0]);
}

static void hstg_reduce(const hstg_t&src, hstg_t& sum) {
    MPI_Reduce(const_cast<double*>(&src[0]), &sum[0], 1, MPI_DOUBLE, MPI_SUM,
            0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<double*>(&src[1]), &sum[1], 1, MPI_DOUBLE, MPI_MAX,
            0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<double*>(&src[2]), &sum[2], 1, MPI_DOUBLE, MPI_MIN,
            0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<double*>(&src[3]), &sum[3], 1, MPI_DOUBLE, MPI_SUM,
            0, MPI_COMM_WORLD);

    MPI_Reduce(const_cast<double*>(&src[4]), &sum[4], MON_NUM_BUCKETS,
            MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
}

static uint64_t now_micros() {
    struct timeval tv;
    uint64_t t;

    gettimeofday(&tv, NULL);
    t = static_cast<uint64_t>(tv.tv_sec) * 1000000;
    t += tv.tv_usec;

    return(t);
}

extern "C" {

int mon_preload_write(const char* fn, char* data, size_t n, mon_ctx_t* ctx) {
    uint64_t start;
    uint64_t end;
    size_t l;
    int rv;

    assert(fn != NULL && pctx.plfsdir != NULL);
    assert(strncmp(fn, pctx.plfsdir, pctx.len_plfsdir) == 0);
    assert(strlen(fn) > pctx.len_plfsdir + 1);

    if (!pctx.nomon) {
        pthread_mutex_lock(&mtx);
        start = now_micros();
        if (ctx->last_write_micros != 0) {
            hstg_add(ctx->hstgarr, start - ctx->last_write_micros);
        } else {
            ctx->last_write_micros = start;
        }
    }

    rv = preload_write(fn, data, n);

    if (!pctx.nomon) {
        if (rv == 0) {
            end = now_micros();
            hstg_add(ctx->hstgw, end - start);

            l = strlen(fn) - pctx.len_plfsdir - 1;

            if (l > ctx->max_fnl) ctx->max_fnl = l;
            if (l < ctx->min_fnl) ctx->min_fnl = l;
            if (n > ctx->max_wsz) ctx->max_wsz = n;
            if (n < ctx->min_wsz) ctx->min_wsz = n;

            ctx->sum_fnl += l;
            ctx->sum_wsz += n;

            ctx->nwok++;
        }

        ctx->nw++;
        pthread_mutex_unlock(&mtx);
    }

    return(rv);
}

int mon_shuffle_write(const char* fn, char* data, size_t n, mon_ctx_t* ctx) {
    uint64_t start;
    uint64_t end;
    int local;
    int rv;

    if (!pctx.nomon) start = now_micros();

    rv = shuffle_write(fn, data, n, &local);

    if (!pctx.nomon && !local) {
        if (rv == 0) {
            end = now_micros();
            hstg_add(ctx->hstgrpcw, end - start);

            ctx->nwsok++;
        }

        ctx->nws++;
    }

    return(rv);
}

void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum) {
    MPI_Reduce(const_cast<unsigned*>(&src->min_fnl), &sum->min_fnl, 1,
            MPI_UNSIGNED, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned*>(&src->max_fnl), &sum->max_fnl, 1,
            MPI_UNSIGNED, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->sum_fnl), &sum->sum_fnl, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    MPI_Reduce(const_cast<unsigned*>(&src->min_wsz), &sum->min_wsz, 1,
            MPI_UNSIGNED, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned*>(&src->max_wsz), &sum->max_wsz, 1,
            MPI_UNSIGNED, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->sum_wsz), &sum->sum_wsz, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    MPI_Reduce(const_cast<unsigned long long*>(&src->nwsok), &sum->nwsok, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->nws), &sum->nws, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->nwrok), &sum->nwrok, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->nwr), &sum->nwr, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    hstg_reduce(src->hstgrpcw, sum->hstgrpcw);

    MPI_Reduce(const_cast<unsigned long long*>(&src->nwok), &sum->nwok, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned long long*>(&src->nw), &sum->nw, 1,
            MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    hstg_reduce(src->hstgarr, sum->hstgarr);
    hstg_reduce(src->hstgw, sum->hstgw);

    MPI_Reduce(const_cast<unsigned*>(&src->nb), &sum->nb, 1, MPI_UNSIGNED,
            MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<unsigned*>(&src->ne), &sum->ne, 1, MPI_UNSIGNED,
            MPI_MAX, 0, MPI_COMM_WORLD);
}

#define DUMP(fd, buf, fmt, ...) { \
    int n = snprintf(buf, sizeof(buf), fmt, ##__VA_ARGS__); \
    buf[n] = '\n'; \
    n = write(fd, buf, n + 1); \
}

void mon_dumpstate(int fd, const mon_ctx_t* ctx) {
    char buf[1024];
    DUMP(fd, buf, "\n--- mon ---")
    DUMP(fd, buf, "[M] max fname len: %u chars", ctx->max_fnl);
    DUMP(fd, buf, "[M] min fname len: %u chars", ctx->min_fnl);
    DUMP(fd, buf, "[M] total fname len: %llu chars", ctx->sum_fnl);
    DUMP(fd, buf, "[M] max write: %u bytes", ctx->max_wsz);
    DUMP(fd, buf, "[M] min write: %u bytes", ctx->min_wsz);
    DUMP(fd, buf, "[M] total write: %llu bytes", ctx->sum_wsz);
    DUMP(fd, buf, "[M] rpc sent ok: %llu", ctx->nwsok);
    DUMP(fd, buf, "[M] rpc sent: %llu", ctx->nws);
    DUMP(fd, buf, "[M] rpc received ok: %llu", ctx->nwrok);
    DUMP(fd, buf, "[M] rpc received: %llu", ctx->nwr);
    DUMP(fd, buf, "[M] rpc minm lat: %.0f us", hstg_min(ctx->hstgrpcw));
    DUMP(fd, buf, "[M] rpc avge lat: %.0f us", hstg_avg(ctx->hstgrpcw));
    DUMP(fd, buf, "[M] rpc 70th lat: %.0f us", hstg_ptile(ctx->hstgrpcw, 70));
    DUMP(fd, buf, "[M] rpc 90th lat: %.0f us", hstg_ptile(ctx->hstgrpcw, 90));
    DUMP(fd, buf, "[M] rpc 99th lat: %.0f us", hstg_ptile(ctx->hstgrpcw, 99));
    DUMP(fd, buf, "[M] rpc maxm lat: %.0f us", hstg_max(ctx->hstgrpcw));
    DUMP(fd, buf, "[M] write ok: %llu", ctx->nwok);
    DUMP(fd, buf, "[M] write: %llu", ctx->nw);
    DUMP(fd, buf, "[M] write minm lat: %.0f us", hstg_min(ctx->hstgw));
    DUMP(fd, buf, "[M] write avge lat: %.0f us", hstg_avg(ctx->hstgw));
    DUMP(fd, buf, "[M] write 70th lat: %.0f us", hstg_ptile(ctx->hstgw, 70));
    DUMP(fd, buf, "[M] write 90th lat: %.0f us", hstg_ptile(ctx->hstgw, 90));
    DUMP(fd, buf, "[M] write 99th lat: %.0f us", hstg_ptile(ctx->hstgw, 99));
    DUMP(fd, buf, "[M] write maxm lat: %.0f us", hstg_max(ctx->hstgw));
    DUMP(fd, buf, "[M] minm ttw arr: %.0f us", hstg_min(ctx->hstgarr));
    DUMP(fd, buf, "[M] avge ttw arr: %.0f us", hstg_avg(ctx->hstgarr));
    DUMP(fd, buf, "[M] 70th ttw arr: %.0f us", hstg_ptile(ctx->hstgarr, 70));
    DUMP(fd, buf, "[M] 90th ttw arr: %.0f us", hstg_ptile(ctx->hstgarr, 90));
    DUMP(fd, buf, "[M] 99th ttw arr: %.0f us", hstg_ptile(ctx->hstgarr, 99));
    DUMP(fd, buf, "[M] maxm ttw arr: %.0f us", hstg_max(ctx->hstgarr));
    DUMP(fd, buf, "[M] num deltafs epoches: %u", ctx->ne);
    DUMP(fd, buf, "[M] num mpi barriers: %u", ctx->nb);

    return;
}

void mon_reinit(mon_ctx_t* ctx) {
    mon_ctx_t tmp = { 0 };
    tmp.min_fnl = 0xffffffff;
    tmp.min_wsz = 0xffffffff;
    hstg_reset_min(tmp.hstgrpcw);
    hstg_reset_min(tmp.hstgarr);
    hstg_reset_min(tmp.hstgw);

    *ctx = tmp;

    return;
}

} // extern C
