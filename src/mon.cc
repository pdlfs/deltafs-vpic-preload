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

static double hstg_percentile(const hstg_t& h, double p) {
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
    int rv;

    start = now_micros();
    rv = preload_write(fn, data, n);
    end = now_micros();

    if (rv == 0) {
        hstg_add(ctx->hstgw, end - start);
    }

    return(rv);
}

int mon_shuffle_write(const char* fn, char* data, size_t n, mon_ctx_t* ctx) {
    uint64_t start;
    uint64_t end;
    int rv;

    start = now_micros();
    rv = shuffle_write(fn, data, n);
    end = now_micros();

    if (rv == 0) {
        hstg_add(ctx->hstgrpcw, end - start);
    }

    return(rv);
}

#define DUMP(fd, buf, fmt, ...) { \
    int n = snprintf(buf, sizeof(buf), fmt, ##__VA_ARGS__); \
    buf[n] = '\n'; \
    n = write(fd, buf, n + 1); \
}

void mon_dumpstate(int fd, const mon_ctx_t* ctx) {
    char buf[1024];
    DUMP(fd, buf, "\n--- mon ---")
    DUMP(fd, buf, "max write: %u bytes", ctx->max_wsz);
    DUMP(fd, buf, "min write: %u bytes", ctx->min_wsz);
    DUMP(fd, buf, "rpc sent: %llu/%llu", ctx->nwsok, ctx->nws);
    DUMP(fd, buf, "rpc received: %llu/%llu", ctx->nwrok, ctx->nwr);
    DUMP(fd, buf, "rpc avge lat: %.0f", hstg_avg(ctx->hstgrpcw));
    DUMP(fd, buf, "rpc 50th lat: %.0f", hstg_percentile(ctx->hstgrpcw, 50));
    DUMP(fd, buf, "rpc 70th lat: %.0f", hstg_percentile(ctx->hstgrpcw, 70));
    DUMP(fd, buf, "rpc 90th lat: %.0f", hstg_percentile(ctx->hstgrpcw, 90));
    DUMP(fd, buf, "rpc 99th lat: %.0f", hstg_percentile(ctx->hstgrpcw, 99));
    DUMP(fd, buf, "rpc maxm lat: %.0f", hstg_max(ctx->hstgrpcw));
    DUMP(fd, buf, "write: %llu/%llu", ctx->nwok, ctx->nw);
    DUMP(fd, buf, "write avge lat: %.0f", hstg_avg(ctx->hstgw));
    DUMP(fd, buf, "write 50th lat: %.0f", hstg_percentile(ctx->hstgw, 50));
    DUMP(fd, buf, "write 70th lat: %.0f", hstg_percentile(ctx->hstgw, 70));
    DUMP(fd, buf, "write 90th lat: %.0f", hstg_percentile(ctx->hstgw, 90));
    DUMP(fd, buf, "write 99th lat: %.0f", hstg_percentile(ctx->hstgw, 99));
    DUMP(fd, buf, "write maxm lat: %.0f", hstg_max(ctx->hstgw));
    DUMP(fd, buf, "num deltafs epoches: %u", ctx->ne);
    DUMP(fd, buf, "num mpi barriers: %u", ctx->nb);

    return;
}

void mon_reinit(mon_ctx_t* ctx) {
    mon_ctx_t tmp = { 0 };
    tmp.min_wsz = 0xffffffff;
    hstg_reset_min(tmp.hstgrpcw);
    hstg_reset_min(tmp.hstgw);

    *ctx = tmp;

    return;
}

} // extern C
