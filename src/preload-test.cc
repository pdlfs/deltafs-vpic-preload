/*
 * Copyright (c) 2016-2017 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {
void ABORT(const char* msg) {
  int err = errno;
  fprintf(stderr, "!!! ABORT !!! %s", msg);
  if (err != 0) fprintf(stderr, ": %s", strerror(err));
  fprintf(stderr, "\n");
  abort();
}
}  // namespace

int main(int argc, char** argv) {
  int rank;
  int r = MPI_Init(&argc, &argv);
  if (r == MPI_SUCCESS) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  } else {
    ABORT("MPI_init");
  }
  const char* mntp = getenv("PRELOAD_Deltafs_mntp");
  if (mntp == NULL) {
    ABORT("no deltafs mntp");
  } else if (mntp[0] == '/') {
    ABORT("deltafs mount point must be relative");
  }
  if (rank == 0) {
    fprintf(stderr, "deltafs_mntp is %s\n", mntp);
  }
  char dname[PATH_MAX];
  snprintf(dname, sizeof(dname), "%s", mntp);
  r = (rank == 0) ? mkdir(dname, 0777) : 0;
  if (r != 0) {
    ABORT("mkdir");
  }

  MPI_Barrier(MPI_COMM_WORLD);

  DIR* d = opendir(dname);
  char fname[PATH_MAX];
  for (int i = 0; i < 10; i++) {
    snprintf(fname, sizeof(fname), "%s/%05x-%05x", dname, rank, i);
    FILE* fp = fopen(fname, "a");
    if (fp == NULL) {
      ABORT("fopen");
    }
    fwrite("1234", 1, 4, fp);
    fwrite("5678", 1, 4, fp);
    fputc('9', fp);
    fputc('0', fp);
    fwrite("abcdefghijk", 1, 11, fp);
    fwrite("lmnopqrstuv", 1, 11, fp);
    fwrite("~!@#$%^", 1, 7, fp);
    fwrite("&", 1, 1, fp);
    r = fclose(fp);
    if (r != 0) {
      ABORT("fclose");
    }
  }
  closedir(d);

  MPI_Finalize();

  char rname[PATH_MAX];
  const char* lo = getenv("PRELOAD_Local_root");
  if (lo == NULL) {
    ABORT("no local root");
  }
  if (rank == 0) {
    fprintf(stderr, "local_root is %s\n", lo);
  }
  for (int i = 0; i < 10; i++) {
    snprintf(rname, sizeof(rname), "%s/%05x-%05x", lo, rank, i);
    int fd = open(rname, O_RDONLY);
    if (fd == -1) {
      ABORT("open");
    }
    char buf[32];
    ssize_t nr = read(fd, buf, 32);
    if (nr != 32) {
      ABORT("read");
    }
    int cmp = memcmp(buf, "1234567890abcdefghijklmnopqrstuv", 32);
    if (cmp != 0) {
      ABORT("data lost");
    }
    close(fd);
  }

  exit(0);
}
