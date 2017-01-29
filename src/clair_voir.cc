/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "clair_voir.h"

#include <stdio.h>
#include <unistd.h>

mon_ctx_t mctx = { 0 };

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
    DUMP(fd, buf, "writes sent: %llu/%llu", ctx->nwsok, ctx->nws);
    DUMP(fd, buf, "writes received: %llu/%llu", ctx->nwrok, ctx->nwr);
    DUMP(fd, buf, "writes: %llu/%llu", ctx->nwok, ctx->nw);
    DUMP(fd, buf, "num deltafs epoches: %u", ctx->ne);
    DUMP(fd, buf, "num mpi barriers: %u", ctx->nb);

    return;
}

void mon_reinit(mon_ctx_t* ctx) {
    mon_ctx_t tmp = { 0 };
    tmp.min_wsz = 0xffffffff;
    *ctx = tmp;

    return;
}
