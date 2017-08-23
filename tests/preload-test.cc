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

static inline void msg_abort(const char* msg) {
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

int main(int argc, char** argv) {
  int rank;
  int r = MPI_Init(&argc, &argv);
  if (r == MPI_SUCCESS) {
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  } else {
    msg_abort("MPI_init");
  }
  const char* mntp = getenv("PRELOAD_Deltafs_mntp");
  if (mntp == NULL) {
    msg_abort("no deltafs mntp");
  } else if (mntp[0] == '/') {
    msg_abort("deltafs mount point must be relative");
  }
  if (rank == 0) {
    fprintf(stderr, "deltafs_mntp is %s\n", mntp);
  }
  char dname[PATH_MAX];
  snprintf(dname, sizeof(dname), "%s", mntp);
  r = (rank == 0) ? mkdir(dname, 0777) : 0;
  if (r != 0) {
    msg_abort("mkdir");
  }

  MPI_Barrier(MPI_COMM_WORLD);

  DIR* d = opendir(dname);
  char fname[PATH_MAX];
  for (int i = 0; i < 10; i++) {
    snprintf(fname, sizeof(fname), "%s/%03d%03d", dname, rank, i);
    FILE* fp = fopen(fname, "a");
    if (fp == NULL) {
      msg_abort("fopen");
    }
    fwrite("1234", 4, 1, fp);
    fwrite("5678", 1, 4, fp);
    fwrite("9", 1, 1, fp);
    fwrite("0", 1, 1, fp);
    fwrite("abcdefghijk", 1, 11, fp);
    fwrite("lmnopqrstuv", 1, 11, fp);
    fwrite("~!@#$%^&", 8, 1, fp);
    r = fclose(fp);
    if (r != 0) {
      msg_abort("fclose");
    }
  }
  closedir(d);

  MPI_Finalize();

  char rname[PATH_MAX];
  const char* lo = getenv("PRELOAD_Local_root");
  if (lo == NULL) {
    msg_abort("no local root");
  }
  if (rank == 0) {
    fprintf(stderr, "local_root is %s\n", lo);
  }
  for (int i = 0; i < 10; i++) {
    snprintf(rname, sizeof(rname), "%s/%s", lo, fname);
    int fd = open(rname, O_RDONLY);
    if (fd == -1) {
      msg_abort("open");
    }
    char buf[32];
    ssize_t nr = read(fd, buf, 32);
    if (nr != 32) {
      msg_abort("read");
    }
    int cmp = memcmp(buf, "1234567890abcdefghijklmnopqrstuv", 32);
    if (cmp != 0) {
      msg_abort("data lost");
    }
    close(fd);
  }

  exit(0);
}
