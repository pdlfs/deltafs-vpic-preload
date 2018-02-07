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
 * preload_plfsdir_reader.cc
 *
 * a simple reader program for reading data out of a plfsdir.
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
static char cf[500];     /* plfsdir conf str */
static struct deltafs_conf {
  int num_epochs;
  int key_size;
  int filter_bits_per_key;
  int lg_parts;
  int skip_crc32c;
  int use_leveldb;
  int comm_sz;
} c; /* plfsdir conf */

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
  int r;         /* number of ranks to read */
  int d;         /* number of names to read per rank */
  int bg;        /* number of background worker threads */
  char* in;      /* path to the input dir */
  char* dirname; /* dir name (path to dir storage) */
  int nobf;      /* ignore bloom filters */
  int crc32c;    /* verify checksums */
  int paranoid;  /* paranoid checks */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

/*
 * ms: measurements
 */
struct ms {
  uint64_t ranks;          /* num ranks touched */
  uint64_t ops;            /* num read ops */
  uint64_t bytes;          /* total amount of user data retrieved */
  uint64_t seeks[3];       /* sum/min/max data block fetched */
  uint64_t table_seeks[3]; /* sum/min/max sstable opened */
  uint64_t t[3];           /* sum/min/max time past (in micros)*/
#define SUM 0
#define MIN 1
#define MAX 2
} m;

/*
 * report: print performance measurements
 */
static void report() {
  if (m.ops == 0) return;
  printf("\n");
  printf("+++ Query Results +++\n");
  printf("[R] Total Ranks: %lu\n", m.ranks);
  printf("[R] Total Read Ops: %lu\n", m.ops);
  printf("[R] Total Particle Bytes: %lu (%lu per particle epoch)\n", m.bytes,
         m.bytes / m.ops / c.num_epochs);
  printf("[R] Num Epochs: %d\n", c.num_epochs);
  printf("[R] Latency: %.3f (min: %.3f, max %.3f) ms per op\n",
         double(m.t[SUM]) / 1000 / m.ops, double(m.t[MIN]) / 1000,
         double(m.t[MAX]) / 1000);
  printf("[R] SST Seeks: %.3f (min: %lu, max: %lu) per op\n",
         double(m.table_seeks[SUM]) / m.ops, m.table_seeks[MIN],
         m.table_seeks[MAX]);
  printf("[R] Seeks: %.3f (min: %lu, max: %lu) per op\n",
         double(m.seeks[SUM]) / m.ops, m.seeks[MIN], m.seeks[MAX]);
  printf("[R] BF Bits: %d\n", c.filter_bits_per_key);
  printf("\n");
}

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
  fprintf(stderr, "usage: %s [options] plfsdir infodir\n", argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-r ranks  number of ranks to read\n");
  fprintf(stderr, "\t-d depth  number of names to read per rank\n");
  fprintf(stderr, "\t-j num    number of background worker threads\n");
  fprintf(stderr, "\t-t sec    timeout (alarm), in seconds\n");
  fprintf(stderr, "\t-i        ignore bloom filters\n");
  fprintf(stderr, "\t-c        verify crc32c (for both data and indexes)\n");
  fprintf(stderr, "\t-k        force paranoid checks\n");
  fprintf(stderr, "\t-v        be verbose\n");
  exit(1);
}

/*
 * get_manifest: parse the conf from the dir manifest file
 */
static void get_manifest() {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  snprintf(fname, sizeof(fname), "%s/MANIFEST", g.in);
  f = fopen(fname, "r");
  if (!f) complain("error opening %s: %s", fname, strerror(errno));

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
    if (strncmp(ch, "num_epochs=", strlen("num_epochs=")) == 0) {
      c.num_epochs = atoi(ch + strlen("num_epochs="));
      if (c.num_epochs < 0) complain("bad num_epochs from manifest");
    } else if (strncmp(ch, "key_size=", strlen("key_size=")) == 0) {
      c.key_size = atoi(ch + strlen("key_size="));
      if (c.key_size < 0) complain("bad key_size from manifest");
    } else if (strncmp(ch, "filter_bits_per_key=",
                       strlen("filter_bits_per_key=")) == 0) {
      c.filter_bits_per_key = atoi(ch + strlen("filter_bits_per_key="));
      if (c.filter_bits_per_key < 0)
        complain("bad filter_bits_per_key from manifest");
    } else if (strncmp(ch, "lg_parts=", strlen("lg_parts=")) == 0) {
      c.lg_parts = atoi(ch + strlen("lg_parts="));
      if (c.lg_parts < 0) complain("bad lg_parts from manifest");
    } else if (strncmp(ch, "skip_checksums=", strlen("skip_checksums=")) == 0) {
      c.skip_crc32c = atoi(ch + strlen("skip_checksums="));
      if (c.skip_crc32c < 0) complain("bad skip_checksums from manifest");
    } else if (strncmp(ch, "use_leveldb=", strlen("use_leveldb=")) == 0) {
      c.use_leveldb = atoi(ch + strlen("use_leveldb="));
      if (c.use_leveldb < 0) complain("bad use_leveldb from manifest");
    } else if (strncmp(ch, "comm_sz=", strlen("comm_sz=")) == 0) {
      c.comm_sz = atoi(ch + strlen("comm_sz="));
      if (c.comm_sz < 0) complain("bad comm_sz from manifests");
    }
  }

  if (ferror(f)) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  if (c.num_epochs == 0 || c.key_size == 0 || c.filter_bits_per_key == 0 ||
      c.comm_sz == 0) {
    complain("bad manifest");
  }

  fclose(f);
}

/*
 * prepare_conf: generate plfsdir conf
 */
static void prepare_conf(int rank, int* io_engine) {
  int n;

  if (g.bg && !tp) tp = deltafs_tp_init(g.bg);
  if (g.bg && !tp) complain("fail to init thread pool");

  n = snprintf(cf, sizeof(cf), "rank=%d", rank);
  n += snprintf(cf + n, sizeof(cf) - n, "&key_size=%d", c.key_size);
  n += snprintf(cf + n, sizeof(cf) - n, "&skip_checksums=%d", c.skip_crc32c);
  n += snprintf(cf + n, sizeof(cf) - n, "&verify_checksums=%d", g.crc32c);
  n += snprintf(cf + n, sizeof(cf) - n, "&paranoid_checks=%d", g.paranoid);
  n += snprintf(cf + n, sizeof(cf) - n, "&parallel_reads=%d", g.bg != 0);
  n += snprintf(cf + n, sizeof(cf) - n, "&ignore_filters=%d", g.nobf);
  snprintf(cf + n, sizeof(cf) - n, "&lg_parts=%d", c.lg_parts);

  *io_engine =
      c.use_leveldb ? DELTAFS_PLFSDIR_LEVELDB : DELTAFS_PLFSDIR_DEFAULT;
#ifndef NDEBUG
  info(cf);
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
      deltafs_plfsdir_read(dir, name, -1, &sz, &table_seeks, &seeks));
  if (data == NULL) {
    complain("error reading %s: %s", name, strerror(errno));
  } else if (sz == 0) {
    complain("file %s is empty!!", name);
  }

  end = now();

  free(data);

  m.t[SUM] += end - start;
  m.t[MIN] = std::min(end - start, m.t[MIN]);
  m.t[MAX] = std::max(end - start, m.t[MAX]);
  m.table_seeks[SUM] += table_seeks;
  m.table_seeks[MIN] = std::min(table_seeks, m.table_seeks[MIN]);
  m.table_seeks[MAX] = std::max(table_seeks, m.table_seeks[MAX]);
  m.seeks[SUM] += seeks;
  m.seeks[MIN] = std::min(seeks, m.seeks[MIN]);
  m.seeks[MAX] = std::max(seeks, m.seeks[MAX]);
  m.bytes += sz;
  m.ops++;
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

  snprintf(fname, sizeof(fname), "%s/NAMES-%07d.txt", g.in, rank);
  f = fopen(fname, "r");
  if (!f) complain("error opening %s: %s", fname, strerror(errno));

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
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
  int io_engine;
  int r;

  get_names(rank, &names);
  std::random_shuffle(names.begin(), names.end());
  prepare_conf(rank, &io_engine);

  dir = deltafs_plfsdir_create_handle(cf, O_RDONLY, io_engine);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 0); /* we don't need this */
  if (tp) deltafs_plfsdir_set_thread_pool(dir, tp);

  r = deltafs_plfsdir_open(dir, g.dirname);
  if (r) complain("error opening plfsdir: %s", strerror(errno));

  if (g.v)
    info("rank %d (%d reads) ...\t\t(%d samples available)", rank,
         std::min(g.d, int(names.size())), int(names.size()));
  for (int i = 0; i < g.d && i < int(names.size()); i++) {
    do_read(dir, names[i].c_str());
  }

  deltafs_plfsdir_free_handle(dir);

  m.ranks++;
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  std::vector<int> ranks;
  int ch;

  argv0 = argv[0];
  memset(cf, 0, sizeof(cf));
  tp = NULL;

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  /* setup default to zero/null, except as noted below */
  memset(&g, 0, sizeof(g));
  g.timeout = DEF_TIMEOUT;
  while ((ch = getopt(argc, argv, "r:d:j:t:ickv")) != -1) {
    switch (ch) {
      case 'r':
        g.r = atoi(optarg);
        if (g.r < 0) usage("bad rank number");
        break;
      case 'd':
        g.d = atoi(optarg);
        if (g.d < 0) usage("bad depth");
        break;
      case 'j':
        g.bg = atoi(optarg);
        if (g.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      case 'i':
        g.nobf = 1;
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

  if (argc != 2) /* plfsdir and infodir must be provided on cli */
    usage("bad args");
  g.dirname = argv[0];
  g.in = argv[1];

  if (access(g.dirname, R_OK) != 0)
    complain("cannot access %s: %s", g.dirname, strerror(errno));
  if (access(g.in, R_OK) != 0)
    complain("cannot access %s: %s", g.in, strerror(errno));

  memset(&c, 0, sizeof(c));
  get_manifest();

  printf("\n%s\n==options:\n", argv0);
  printf("\tqueries: %d x %d\n", g.r, g.d);
  printf("\tnum bg threads: %d\n", g.bg);
  printf("\tinfodir: %s\n", g.in);
  printf("\tplfsdir: %s\n", g.dirname);
  printf("\ttimeout: %d\n", g.timeout);
  printf("\tignore bloom filters: %d\n", g.nobf);
  printf("\tverify crc32: %d\n", g.crc32c);
  printf("\tparanoid checks: %d\n", g.paranoid);
  printf("\tverbose: %d\n", g.v);
  printf("\n==dir manifest\n");
  printf("\tnum epochs: %d\n", c.num_epochs);
  printf("\tkey size: %d\n", c.key_size);
  printf("\tfilter bits per key: %d\n", c.filter_bits_per_key);
  printf("\tskip crc32c: %d\n", c.skip_crc32c);
  printf("\tuse_leveldb: %d\n", c.use_leveldb);
  printf("\tlg parts: %d\n", c.lg_parts);
  printf("\tcomm sz: %d\n", c.comm_sz);
  printf("\n");

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  memset(&m, 0, sizeof(m));
  m.table_seeks[MIN] = ULONG_LONG_MAX;
  m.seeks[MIN] = ULONG_LONG_MAX;
  m.t[MIN] = ULONG_LONG_MAX;
  for (int i = 0; i < c.comm_sz; i++) {
    ranks.push_back(i);
  }
  std::random_shuffle(ranks.begin(), ranks.end());
  if (g.v) info("start queries (%d ranks) ...", std::min(g.r, c.comm_sz));
  for (int i = 0; i < g.r && i < c.comm_sz; i++) {
    run_queries(ranks[i]);
  }
  report();

  if (tp) deltafs_tp_close(tp);

  if (g.v) info("all done!");
  if (g.v) info("bye");

  exit(0);
}
