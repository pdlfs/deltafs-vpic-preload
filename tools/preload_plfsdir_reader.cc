/*
 * Copyright (c) 2018-2019, Carnegie Mellon University and
 *     Los Alamos National Laboratory.
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
 * a benchmark program for evaluating plfsdir read performance
 */

#include "preload_plfsdir_reader.h"

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

#include <algorithm>
#include <string>
#include <vector>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
static char* argv0;                  /* program name */
static plfsdir_tp_t* tp;             /* plfsdir background worker thread pool */
static struct plfsdir_reader_conf r; /* plfsdir reader conf */
static struct plfsdir_conf c;        /* plfsdir conf */
static char cf[500];                 /* plfsdir conf str */

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
  int a;         /* anti-shuffle mode (query rank 0 names across all ranks)*/
  int r;         /* number of ranks to read */
  int d;         /* number of names to read per rank */
  char* in;      /* path to the dir input dir */
  char* dirname; /* dir name (path to dir storage) */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

/*
 * ms: measurements
 */
struct ms {
  std::vector<uint64_t>* latencies;
  uint64_t partitions;     /* num data partitions touched */
  uint64_t extra_ops;      /* num extra read ops */
  uint64_t extra_okops;    /* num extra read ops that return non-empty data */
  uint64_t extra_bytes;    /* total extra amount of data read */
  uint64_t ops;            /* num read ops */
  uint64_t okops;          /* num read ops that return non-empty data */
  uint64_t bytes;          /* total amount of data read */
  uint64_t under_bytes;    /* total amount of underlying data retrieved */
  uint64_t under_files;    /* total amount of underlying files opened */
  uint64_t under_seeks;    /* total amount of underlying storage seeks */
  uint64_t table_seeks[3]; /* sum/min/max sstable opened */
  uint64_t seeks[3];       /* sum/min/max data block fetched */
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
  printf("=== Query Results ===\n");
  printf("[R] Total Epochs: %d\n", c.num_epochs);
  printf("[R] Total Data Partitions: %d (%lu queried)\n", c.comm_sz,
         m.partitions);
  if (c.io_engine == DELTAFS_PLFSDIR_DEFAULT)
    printf("[R] Total Data Subpartitions: %d\n", c.comm_sz * (1 << c.lg_parts));
  if (m.extra_ops != 0)
    printf("[R] Total Extra Query Ops: %lu (%lu ok ops)\n", m.extra_ops,
           m.extra_okops);
  if (m.extra_okops != 0)
    printf(
        "[R] Total Extra Data Queried: %lu bytes (%lu per entry per epoch)\n",
        m.extra_bytes, m.extra_bytes / m.extra_okops / c.num_epochs);
  printf("[R] Total Query Ops: %lu (%lu ok ops)\n", m.ops, m.okops);
  if (m.okops != 0)
    printf("[R] Total Data Queried: %lu bytes (%lu per entry per epoch)\n",
           m.bytes, m.bytes / m.okops / c.num_epochs);
  if (c.io_engine == DELTAFS_PLFSDIR_DEFAULT)
    printf("[R] SST Touched Per Query: %.3f (min: %lu, max: %lu)\n",
           double(m.table_seeks[SUM]) / m.ops, m.table_seeks[MIN],
           m.table_seeks[MAX]);
  if (c.io_engine == DELTAFS_PLFSDIR_DEFAULT)
    printf("[R] SST Data Blocks Fetched Per Query: %.3f (min: %lu, max: %lu)\n",
           double(m.seeks[SUM]) / m.ops, m.seeks[MIN], m.seeks[MAX]);
  printf("[R] Total Under Storage Seeks: %lu\n", m.under_seeks);
  printf("[R] Total Under Data Read: %lu bytes\n", m.under_bytes);
  printf("[R] Total Under Files Opened: %lu\n", m.under_files);
  std::vector<uint64_t>* const lat = m.latencies;
  if (lat && lat->size() != 0) {
    std::sort(lat->begin(), lat->end());
    uint64_t sum = 0;
    std::vector<uint64_t>::iterator it = lat->begin();
    for (; it != lat->end(); ++it) sum += *it;
    printf(
        "[R] Latency Per Rank (Partition): "
        "%.3f (med: %3f, min: %.3f, max %.3f) ms\n",
        double(sum) / m.partitions / 1000,
        double((*lat)[(lat->size() - 1) / 2]) / 1000, double((*lat)[0]) / 1000,
        double((*lat)[lat->size() - 1]) / 1000);
    printf("[R] Total Read Latency: %.6f s\n", double(sum) / 1000 / 1000);
  }
  printf("[R] Dir IO Engine: %d\n", c.io_engine);
  printf("[R] MemTable Size: %s\n", c.memtable_size);
  printf("[R] BF Bits: %s\n", c.filter_bits_per_key);
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
  fprintf(stderr, "\t-a        enable the special anti-shuffle mode\n");
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
 * prepare_dir: generate plfsdir conf and initialize thread pool
 */
static char* prepare_dir(int myrank) {
  if (g.r && !tp) tp = deltafs_tp_init(r.bg);
  if (g.r && !tp) complain("fail to init thread pool");
  gen_conf(&r, &c, myrank, cf, sizeof(cf));

#ifndef NDEBUG
  info(cf);
#endif
  return cf;
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

static void do_extra_reads(int rank, off_t off) {
  const int sz = c.particle_size;
  deltafs_plfsdir_t* dir;
  char conf[20];
  ssize_t n;

  snprintf(conf, sizeof(conf), "rank=%d", rank);
  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, DELTAFS_PLFSDIR_NOTHING);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 1);

  if (g.v) info("  open rank %d ... (%s)", rank, __func__);
  if (deltafs_plfsdir_open(dir, g.dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));
  if (deltafs_plfsdir_io_open(dir, g.dirname) != 0)
    complain("error opening plfsdir io: %s", strerror(errno));

  char* buf = static_cast<char*>(malloc(sz));
  n = deltafs_plfsdir_io_pread(dir, buf, sz, off);
  if (n != sz) complain("error reading extra data: %s", strerror(errno));

  free(buf);

  m.under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  m.under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  m.under_seeks += deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");
  m.extra_bytes += n;
  if (n != 0) m.extra_okops++;
  m.extra_ops++;

  deltafs_plfsdir_free_handle(dir);
}

/*
 * do_read: perform a read operation against a specific
 * name under a given plfsdir.
 */
static void do_read(deltafs_plfsdir_t* dir, const char* name) {
  char* data;
  size_t table_seeks;
  size_t seeks;
  size_t sz;
  uint64_t off;
  int rank;

  table_seeks = seeks = 0;

  data = static_cast<char*>(
      deltafs_plfsdir_read(dir, name, -1, &sz, &table_seeks, &seeks));
  if (!data) {
    complain("error reading %s: %s", name, strerror(errno));
  } else if (sz == 0 && c.value_size != 0 && !c.bloomy_fmt && !g.a &&
             !c.bypass_shuffle) {
    complain("file %s is empty!!", name);
  }

  if (c.wisc_fmt) {
    if (sz != 12) complain("file %s's wisc record size is wrong!!", name);
    memcpy(&rank, data, 4);
    memcpy(&off, data + 4, 8);

    do_extra_reads(rank, off);
  }

  free(data);

  m.table_seeks[SUM] += table_seeks;
  m.table_seeks[MIN] = std::min<uint64_t>(table_seeks, m.table_seeks[MIN]);
  m.table_seeks[MAX] = std::max<uint64_t>(table_seeks, m.table_seeks[MAX]);
  m.seeks[SUM] += seeks;
  m.seeks[MIN] = std::min<uint64_t>(seeks, m.seeks[MIN]);
  m.seeks[MAX] = std::max<uint64_t>(seeks, m.seeks[MAX]);
  m.bytes += sz;
  if (sz != 0) m.okops++;
  m.ops++;
}

/*
 * do_reads: perform one or more read operations against
 * a series of names under a given data partition.
 */
static void do_reads(int rank, std::string* names, int num_names) {
  deltafs_plfsdir_t* dir;
  char* conf = prepare_dir(rank);

  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, c.io_engine);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 1);
  deltafs_plfsdir_force_leveldb_fmt(dir, c.force_leveldb_format);
  deltafs_plfsdir_set_unordered(dir, c.unordered_storage);
  deltafs_plfsdir_set_fixed_kv(dir, 1);
  if (tp) deltafs_plfsdir_set_thread_pool(dir, tp);

  if (g.v) info(" open rank %d ... (%s)", rank, __func__);
  if (deltafs_plfsdir_open(dir, g.dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));

  for (int i = 0; i < num_names; i++) {
    do_read(dir, names[i].c_str());
  }

  m.under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  m.under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  m.under_seeks += deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");

  deltafs_plfsdir_free_handle(dir);
}

/*
 * do_reads_ft: perform read operations with a filter against
 * a series of names under a given data partition.
 */
static void do_reads_ft(int rank, std::string* names, int num_names) {
  deltafs_plfsdir_t* dir;
  size_t num_ranks;
  char conf[20];

  snprintf(conf, sizeof(conf), "rank=%d", rank);
  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, DELTAFS_PLFSDIR_NOTHING);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 1);
  if (deltafs_plfsdir_open(dir, g.dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));
  if (deltafs_plfsdir_filter_open(dir, g.dirname) != 0)
    complain("error opening plfsdir filter: %s", strerror(errno));

  for (int i = 0; i < num_names; i++) {
    int* ranks = deltafs_plfsdir_filter_get(dir, names[i].data(),
                                            names[i].size(), &num_ranks);
    if (ranks) {
      for (int r = 0; r < num_ranks; r++) {
        do_reads(ranks[r], &names[i], 1);
      }
    }

    free(ranks);
  }

  m.under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  m.under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  m.under_seeks += deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");

  deltafs_plfsdir_free_handle(dir);
}

/*
 * run_queries: perform reads against a specific data partition.
 */
static void run_queries(int rank) {
  std::vector<std::string> names;
  get_names((g.a || c.bypass_shuffle) ? 0 : rank, &names);
  std::random_shuffle(names.begin(), names.end());
  uint64_t start;

  const int reads = std::min(g.d, int(names.size()));

  if (g.v)
    info("rank %d (%d reads) ...\t\t(%d samples available)", rank, reads,
         int(names.size()));

  start = now();

  if (c.bloomy_fmt) {
    do_reads_ft(rank, &names[0], reads);
  } else {
    do_reads(rank, &names[0], reads);
  }

  m.latencies->push_back(now() - start);

  m.partitions++;
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  std::vector<int> ranks;
  int nranks;
  int ch;

  argv0 = argv[0];
  memset(cf, 0, sizeof(cf));
  tp = NULL;

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  memset(&r, 0, sizeof(r));
  memset(&g, 0, sizeof(g));

  g.timeout = DEF_TIMEOUT;

  while ((ch = getopt(argc, argv, "ar:d:j:t:ickv")) != -1) {
    switch (ch) {
      case 'a':
        g.a = 1;
        break;
      case 'r':
        g.r = atoi(optarg);
        if (g.r < 0) usage("bad rank number");
        break;
      case 'd':
        g.d = atoi(optarg);
        if (g.d < 0) usage("bad depth");
        break;
      case 'j':
        r.bg = atoi(optarg);
        if (r.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      case 'i':
        r.nobf = 1;
        break;
      case 'c':
        r.crc32c = 1;
        break;
      case 'k':
        r.paranoid = 1;
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
  get_manifest(g.in, &c);

  printf("\n%s\n==options:\n", argv0);
  printf("\tqueries: %d x %d (ranks x reads)\n", g.r, g.d);
  printf("\tnum bg threads: %d (reader thread pool)\n", r.bg);
  printf("\tanti-shuffle: %d\n", g.a);
  printf("\tinfodir: %s\n", g.in);
  printf("\tplfsdir: %s\n", g.dirname);
  printf("\ttimeout: %d s\n", g.timeout);
  printf("\tignore bloom filters: %d\n", r.nobf);
  printf("\tverify crc32: %d\n", r.crc32c);
  printf("\tparanoid checks: %d\n", r.paranoid);
  printf("\tverbose: %d\n", g.v);
  printf("\n==dir manifest\n");
  printf("\tio engine: %d\n", c.io_engine);
  printf("\tforce leveldb format: %d\n", c.force_leveldb_format);
  printf("\tunordered storage: %d\n", c.unordered_storage);
  printf("\tnum epochs: %d\n", c.num_epochs);
  printf("\tkey size: %d bytes\n", c.key_size);
  printf("\tvalue size: %d bytes\n", c.value_size);
  printf("\tmemtable size: %s\n", c.memtable_size);
  printf("\tfilter bits per key: %s\n", c.filter_bits_per_key);
  printf("\tskip crc32c: %d\n", c.skip_crc32c);
  printf("\tbypass shuffle: %d\n", c.bypass_shuffle);
  printf("\tlg parts: %d\n", c.lg_parts);
  printf("\tcomm sz: %d\n", c.comm_sz);
  printf("\tparticle id size: %d\n", c.particle_id_size);
  printf("\tparticle size: %d\n", c.particle_size);
  printf("\tbloomy fmt: %d\n", c.bloomy_fmt);
  printf("\twisc fmt: %d\n", c.wisc_fmt);
  printf("\n");

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  memset(&m, 0, sizeof(m));
  m.latencies = new std::vector<uint64_t>;
  m.table_seeks[MIN] = ULONG_LONG_MAX;
  m.seeks[MIN] = ULONG_LONG_MAX;
  for (int i = 0; i < c.comm_sz; i++) {
    ranks.push_back(i);
  }
  std::random_shuffle(ranks.begin(), ranks.end());
  nranks = (g.a || c.bypass_shuffle) ? c.comm_sz : g.r;
  if (g.v) info("start queries (%d ranks) ...", std::min(nranks, c.comm_sz));
  for (int i = 0; i < nranks && i < c.comm_sz; i++) {
    run_queries(ranks[i]);
  }
  report();

  if (tp) deltafs_tp_close(tp);
  if (c.memtable_size) free(c.memtable_size);
  if (c.filter_bits_per_key) free(c.filter_bits_per_key);
  delete m.latencies;

  if (g.v) info("all done!");
  if (g.v) info("bye");

  exit(0);
}
