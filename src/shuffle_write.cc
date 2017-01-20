/*
 * Copyright (c) 2017 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <unistd.h>

#include "shuffle.h"

static int shuffle_posix_write(const char *fn, char *data, int len)
{
    int fd, rv;
    ssize_t wrote;

    fd = open(fn, O_WRONLY|O_CREAT|O_APPEND, 0666);
    if (fd < 0) {
        if (sctx.testmode)
            fprintf(stderr, "shuffle_posix_write: %s: open failed (%s)\n", fn,
                    strerror(errno));
        return(EOF);
    }

    wrote = write(fd, data, len);
    if (wrote != len && sctx.testmode)
        fprintf(stderr, "shuffle_posix_write: %s: write failed: %d (want %d)\n",
                fn, (int)wrote, (int)len);

    rv = close(fd);
    if (rv < 0 && sctx.testmode)
        fprintf(stderr, "shuffle_posix_write: %s: close failed (%s)\n", fn,
                strerror(errno));

    return((wrote != len || rv < 0) ? EOF : 0);
}

static int shuffle_deltafs_write(const char *fn, char *data, int len)
{
    int fd, rv;
    ssize_t wrote;

    fd = deltafs_open(fn, O_WRONLY|O_CREAT|O_APPEND, 0666);
    if (fd < 0) {
        if (sctx.testmode)
            fprintf(stderr, "shuffle_deltafs_write: %s: open failed (%s)\n", fn,
                    strerror(errno));
        return(EOF);
    }

    wrote = deltafs_write(fd, data, len);
    if (wrote != len && sctx.testmode)
        fprintf(stderr, "shuffle_deltafs_write: %s: write failed: %d (want %d)\n",
                fn, (int)wrote, (int)len);

    rv = deltafs_close(fd);
    if (rv < 0 && sctx.testmode)
        fprintf(stderr, "shuffle_deltafs_write: %s: close failed (%s)\n", fn,
                strerror(errno));

    return((wrote != len || rv < 0) ? EOF : 0);
}

/*
 * shuffle_write_local(): write directly to deltafs or posix after shuffle.
 * If used for debugging we will print msg on any err.
 * Returns 0 or EOF on error.
 */
int shuffle_write_local(const char *fn, char *data, int len)
{
    char testpath[PATH_MAX];

    if (sctx.testmode &&
        snprintf(testpath, PATH_MAX, REDIRECT_TEST_ROOT "%s", fn) < 0)
        msg_abort("fclose:snprintf");

    switch (sctx.testmode) {
        case NO_TEST:
            return shuffle_deltafs_write(fn, data, len);
        case DELTAFS_NOPLFS_TEST:
            return shuffle_deltafs_write(testpath, data, len);
        case PRELOAD_TEST:
        case SHUFFLE_TEST:
        case PLACEMENT_TEST:
            return shuffle_posix_write(testpath, data, len);
    }
}

int shuffle_write(const char *fn, char *data, int len)
{
    /* TODO: Do the shuffle */
    return shuffle_write_local(fn, data, len);
}
