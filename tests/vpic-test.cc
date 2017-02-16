/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>
#include <mpi.h>

#define ASSERT_OK(expr) {\
    if (!(expr)) {\
        fprintf(stderr, "!!!ABORT!!! '%s' failed\n", #expr); \
        abort(); \
    } \
}

int main(int argc, char **argv) {
    int rank;
    int r;
#if 0
    /*
     * XXX: all mkdir and write operations are converted to noop.
     *
     * "deltafs_root" is used to trigger plfsdir so we still have shuffle.
     *
     * "local_root" is only used to store trace and statistics.
     */
    ASSERT_OK(setenv("PRELOAD_Bypass_write", "1", 0) == 0);
    ASSERT_OK(setenv("PRELOAD_Bypass_placement", "1", 0) == 0);
    ASSERT_OK(setenv("PRELOAD_Local_root", "/tmp/vpic-deltafs-test", 0) == 0);
    ASSERT_OK(setenv("PRELOAD_Deltafs_root", "d", 0) == 0);
#endif
    ASSERT_OK(MPI_Init(&argc, &argv) == MPI_SUCCESS);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    const char* rt = getenv("PRELOAD_Deltafs_root");
    ASSERT_OK(rt != NULL);
    char dname[PATH_MAX];
    snprintf(dname, sizeof(dname), "%s", rt);
    DIR* d = opendir(dname);
    ASSERT_OK(d != NULL);
    char fname[PATH_MAX];

    double start = MPI_Wtime();
    static const int NUM_PARTICLES = 400000;  /* 400k */
    for (int i = 0; i < NUM_PARTICLES; i++) {
        snprintf(fname, sizeof(fname), "%s/%03d%03d", dname, rank, i);
        FILE* fp = fopen(fname, "a");
        ASSERT_OK(fp != NULL);
        fwrite("1234", 4, 1, fp);
        fwrite("5678", 1, 4, fp);
        fwrite("9", 1, 1, fp);
        fwrite("0", 1, 1, fp);
        fwrite("abcdefghijk", 1, 11, fp);
        fwrite("lmnopqrstuv", 1, 11, fp);
        fwrite("12345678", 1, 8, fp);
        r = fclose(fp);  /* 40 bytes */
        ASSERT_OK(r == 0);
        fp = NULL;
    }

    double dura = MPI_Wtime() - start;
    fprintf(stderr, "%d: %d rpc, %.2f sec (%3f sec/op)\n", rank, NUM_PARTICLES,
            dura, dura / NUM_PARTICLES);
    closedir(d);
    MPI_Finalize();
    exit(0);
}
