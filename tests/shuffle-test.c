/*
 * Copyright (c) 2017 Carnegie Mellon University.
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#ifdef NDEBUG
#undef NDEBUG    /* we always want to assert */
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <mpi.h>
#include <sys/stat.h>
#include <limits.h> /* Just for PATH_MAX */

#include "../src/preload.h"

/*
 * Warning: Meant to be run on a shared namespace.
 *          All processes generate files in same dir.
 */
int main(int argc, char **argv) {
    int world_size, world_rank;
    char dname[PATH_MAX], fname[PATH_MAX], rstr[5];
    FILE *fp;

    memset(dname, 0, sizeof(dname));

    /* No arguments. We will create a temporary dir in /tmp. */
    if (argc != 1) {
        fprintf(stderr, "usage: %s\n", *argv);
        exit(1);
    }

    /* Get down to business */
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error - MPI_Init failed");
        exit(1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    /* Rank 0 creates the temporary dir */
    if (!world_rank) {
        char *dir, tmpl[PATH_MAX];

        /* Generate temporary dir name */
        snprintf(tmpl, sizeof(tmpl), DEFAULT_ROOT "/shuffle-test.XXXXXX");
        dir = mkdtemp(tmpl);
        assert(dir);
        strncpy(dname, dir, PATH_MAX);

        snprintf(fname, sizeof(fname), REDIRECT_TEST_ROOT "%s",
                 dname+strlen(DEFAULT_ROOT));
        mkdir(fname, S_IRWXU|S_IRWXG|S_IRWXO);

        fprintf(stderr, "Generated dir %s\n", dname);
    }

    /* Broadcast the dir name */
    MPI_Bcast(dname, sizeof(dname), MPI_CHAR, 0, MPI_COMM_WORLD);

    /* Create a file with my rank, and write 32b of data */
    snprintf(fname, sizeof(fname), "%s/file%d", dname, world_rank);
    fprintf(stderr, "%d: Created %s\n", world_rank, fname);

    fp = fopen(fname, "w");
    if (!fp) {
        perror("Error - fopen failed");
        goto error;
    }

    /* Write 32b of data */
    snprintf(rstr, sizeof(rstr), "%04d", world_rank);
    assert(fwrite(rstr, 4, 1, fp) == 1);
    assert(fwrite("5678", 1, 4, fp) == 4);
    assert(fwrite("9", 1, 1, fp) == 1);
    assert(fwrite("0", 1, 1, fp) == 1);
    assert(fwrite("abcdefghijklmnopqrstuv", 1, 22, fp) == 22);

    assert(fclose(fp) == 0);

    MPI_Finalize();

    /*
     * Rank 0 checks persisted data from all ranks.
     * Use unbuffered I/O (not preloaded).
     */
    if (world_rank)
        exit(0);

    for (int i = 0; i < world_size; i++) {
        int fd;
        char buf[33] = { 0 };
        char tst[33] = { 0 };

        snprintf(fname, sizeof(fname), REDIRECT_TEST_ROOT "%s/file%d",
                 dname+strlen(DEFAULT_ROOT), i);
        fd = open(fname, O_RDONLY);
        if (fd < 0) {
            perror("Error - open failed");
            exit(1);
        }

        if (read(fd, buf, 32) != 32) {
            perror("Error - read failed");
            close(fd);
            exit(1);
        }

        close(fd);

        snprintf(tst, sizeof(tst), "%04d567890abcdefghijklmnopqrstuv", i);
        if (strcmp(buf, tst)) {
            fprintf(stderr, "Error: output of %s did not match\n"
                    "Want: %04d567890abcdefghijklmnopqrstuv\n"
                    "Got:  %s\n", fname, i, buf);
            exit(1);
        }
    }

    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
