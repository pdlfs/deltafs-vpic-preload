/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * reader.cc
 *
 * a simple program for reading data out of a plfsdir.
 */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <deltafs/deltafs_api.h>

#include <algorithm>
#include <string>
#include <vector>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
static char* argv0;      /* argv[0], program name */
static deltafs_tp_t* tp; /* plfsdir worker thread pool */
static char conf[500];   /* plfsdir conf */

/*
 * vcomplain/complain about something and exit.
 */
static void vcomplain(const char* format, va_list ap) {
  fprintf(stderr, "!!! ERROR !!! %s: ", argv0);
  vfprintf(stderr, format, ap);
  fprintf(stderr, "\n");
  exit(1);
}

static void complain(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vcomplain(format, ap);
  va_end(ap);
}

/*
 * print info messages.
 */
static void vinfo(const char* format, va_list ap) {
  printf("-INFO- ");
  vprintf(format, ap);
  printf("\n");
}

static void info(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vinfo(format, ap);
  va_end(ap);
}

/*
 * now: get current time in micros
 */
static uint64_t now() {
  struct timeval tv;
  uint64_t rv;

  gettimeofday(&tv, NULL);
  rv = tv.tv_sec * 1000000LLU + tv.tv_usec;

  return rv;
}

/*
 * end of helper/utility functions.
 */

/*
 * default values
 */
#define DEF_TIMEOUT 300 /* alarm timeout */

/*
 * gs: shared global data (e.g. from the command line)
 */
struct gs {
  int w;         /* number of ranks to read */
  int d;         /* number of names to read per rank */
  int bg;        /* number of background worker threads */
  char* in;      /* path to the input dir */
  char* dirname; /* dir name (path to dir storage) */
  int dirradix;  /* dir radix (lg memtable partitions per rank) ) */
  int dirsz;     /* dir size (total dir ranks) */
  int crc32c;    /* verify checksums */
  int paranoid;  /* paranoid checks */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

/*
 * ms: measurements
 */
struct ms {
  uint64_t tr; /* total ranks touched */
  uint64_t tn; /* total names touched */
  uint64_t td; /* total user data read out (in bytes) */
  uint64_t tb; /* total data block fetched (total seeks) */
  uint64_t ts; /* total sstable opened */
  uint64_t tt; /* total time past (in micros)*/
} m;

/*
 * report: print performance measurements
 */
static void report() { printf("total read ops: %lu", m.tn); }

/*
 * alarm signal handler
 */
static void sigalarm(int foo) {
  fprintf(stderr, "!!! SIGALRM detected !!!\n");
  fprintf(stderr, "Alarm clock\n");
  exit(1);
}

/*
 * usage
 */
static void usage(const char* msg) {
  if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
  fprintf(stderr, "usage: %s [options] plfsdir indir\n", argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-s num    dir size (total dir ranks)\n");
  fprintf(stderr, "\t-x num    dir radix (lg memtable partitions)\n");
  fprintf(stderr, "\t-w num    number of ranks to read\n");
  fprintf(stderr, "\t-d num    number of names to read per rank\n");
  fprintf(stderr, "\t-j num    number of background worker threads\n");
  fprintf(stderr, "\t-t sec    timeout (alarm), in seconds\n");
  fprintf(stderr, "\t-c        verify crc32c (for both data and indexes)\n");
  fprintf(stderr, "\t-k        force paranoid checks\n");
  fprintf(stderr, "\t-v        be verbose\n");
  exit(1);
}

/*
 * prepare_conf: generate plfsdir conf
 */
static void prepare_conf(int rank) {
  int n;

  n = snprintf(conf, sizeof(conf), "rank=%d", rank);
  n += snprintf(conf + n, sizeof(conf) - n, "&verify_checksums=%d", g.crc32c);
  n += snprintf(conf + n, sizeof(conf) - n, "&skip_checksums=%d", !g.crc32c);
  snprintf(conf + n, sizeof(conf) - n, "&lg_parts=%d", g.dirradix);

#ifndef NDEBUG
  info(conf);
#endif
}

/*
 * do_read: read from plfsdir and measure the performance.
 */
static void do_read(deltafs_plfsdir_t* dir, const char* name) {
  char* data;
  uint64_t start;
  uint64_t end;
  size_t table_seeks;
  size_t seeks;
  size_t sz;

  start = now();

  data = static_cast<char*>(
      deltafs_plfsdir_readall(dir, name, &sz, &table_seeks, &seeks));
  if (data == NULL) {
    complain("error reading %s: %s", name, strerror(errno));
  } else if (sz == 0) {
    complain("file %s is empty!!", name);
  }

  end = now();

  free(data);

  m.tt += end - start;
  m.ts += table_seeks;
  m.tb += seeks;
  m.td += sz;
  m.tn++;
}

/*
 * get_names: load names from an input source.
 */
static void get_names(int rank, std::vector<std::string>* results) {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  results->clear();

  snprintf(fname, sizeof(fname), "%s/r-%08d.txt", g.in, rank);
  f = fopen(fname, "r");
  if (!f) complain("error opening %s: %s", fname, strerror(errno));

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
    if (strlen(ch) >= 100) complain("name len overflow");
    results->push_back(std::string(ch, strlen(ch) - 1));
  }

  if (ferror(f)) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  fclose(f);
}

/*
 * run_queries: open plfsdir and do reads on a specific rank.
 */
static void run_queries(int rank) {
  std::vector<std::string> names;
  deltafs_plfsdir_t* dir;
  int r;

  get_names(rank, &names);
  if (g.v) info("%d names available for rank %d", int(names.size()), rank);
  std::random_shuffle(names.begin(), names.end());
  prepare_conf(rank);

  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 0); /* we don't need this */
  if (tp) deltafs_plfsdir_set_thread_pool(dir, tp);

  r = deltafs_plfsdir_open(dir, g.dirname);
  if (r) complain("error opening plfsdir: %s", strerror(errno));

  if (g.v) info("do %d reads ...", std::min(g.d, int(names.size())));
  for (int i = 0; i < g.d && i < int(names.size()); i++) {
    do_read(dir, names[i].c_str());
  }
  if (g.v) info("ok");

  deltafs_plfsdir_free_handle(dir);

  m.tr++;
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  int ch;

  argv0 = argv[0];
  memset(conf, 0, sizeof(conf));
  tp = NULL;

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  /* setup default to zero/null, except as noted below */
  memset(&g, 0, sizeof(g));
  g.timeout = DEF_TIMEOUT;
  while ((ch = getopt(argc, argv, "s:x:w:d:j:t:ckv")) != -1) {
    switch (ch) {
      case 's':
        g.dirsz = atoi(optarg);
        if (g.dirsz < 0) usage("bad size");
        break;
      case 'x':
        g.dirradix = atoi(optarg);
        if (g.dirradix < 0) usage("bad radix");
        break;
      case 'w':
        g.w = atoi(optarg);
        if (g.w < 0) usage("bad w");
        break;
      case 'd':
        g.d = atoi(optarg);
        if (g.d < 0) usage("bad d");
        break;
      case 'j':
        g.bg = atoi(optarg);
        if (g.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      case 'c':
        g.crc32c = 1;
        break;
      case 'k':
        g.paranoid = 1;
        break;
      case 'v':
        g.v = 1;
        break;
      default:
        usage(NULL);
    }
  }
  argc -= optind;
  argv += optind;

  if (argc != 2) /* plfsdir and indir must be provided on cli */
    usage("bad args");
  g.dirname = argv[0];
  g.in = argv[1];

  printf("\n%s options:\n", argv0);
  printf("\t          num ranks: %d\n", g.w);
  printf("\t num names per rank: %d\n", g.d);
  printf("\t     num bg threads: %d\n", g.bg);
  printf("\t          inpur dir: %s\n", g.in);
  printf("\t       plfsdir name: %s\n", g.dirname);
  printf("\t      plfsdir radix: %d\n", g.dirradix);
  printf("\t         plfsdir sz: %d\n", g.dirsz);
  printf("\t      alarm timeout: %d\n", g.timeout);
  printf("\n");

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  if (g.v) info("ok - i'm doing it ...");
  for (int i = 0; i < g.w && i < g.dirsz; i++) {
    run_queries(i);
  }
  if (g.v) info("all done!");
  if (g.v) info("bye");

  report();

  exit(0);
}
