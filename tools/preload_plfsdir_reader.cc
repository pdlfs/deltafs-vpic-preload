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
 * a benchmark program for evaluating plfsdir read performance.
 */

#include "preload_plfsdir_reader.h"

#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
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
static struct plfsdir_core_mon x;    /* plfsdir internal monitoring stats */
static struct plfsdir_mon m;         /* plfsdir monitoring stats */
static struct plfsdir_reader_conf r; /* plfsdir reader conf */
static struct plfsdir_conf c;        /* plfsdir conf */

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
  char* dirinfo; /* path to dir info */
  char* dirname; /* dir name (path to dir storage) */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

/*
 * ms: measurements
 */
struct ms {
  std::vector<uint64_t>* latencies;
  uint64_t partitions;
} z;

/*
 * report: print performance measurements
 */
static void report() {
  if (m.ops == 0) return;
  printf("\n");
  printf("=== Query Results ===\n");
  printf("[R] Total Epochs: %d\n", c.num_epochs);
  printf("[R] Total Data Partitions: %d (%lu queried)\n", c.comm_sz,
         z.partitions);
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
           double(x.total_table_seeks) / m.ops, x.min_table_seeks,
           x.max_table_seeks);
  if (c.io_engine == DELTAFS_PLFSDIR_DEFAULT)
    printf("[R] SST Data Blocks Fetched Per Query: %.3f (min: %lu, max: %lu)\n",
           double(x.total_seeks) / m.ops, x.min_seeks, x.max_seeks);
  printf("[R] Total Under Storage Seeks: %lu\n", m.under_seeks);
  printf("[R] Total Under Data Read: %lu bytes\n", m.under_bytes);
  printf("[R] Total Under Files Opened: %lu\n", m.under_files);
  std::vector<uint64_t>* const lat = z.latencies;
  if (lat && lat->size() != 0) {
    std::sort(lat->begin(), lat->end());
    uint64_t sum = 0;
    std::vector<uint64_t>::iterator it = lat->begin();
    for (; it != lat->end(); ++it) sum += *it;
    printf(
        "[R] Latency Per Rank (Partition): "
        "%.3f (med: %3f, min: %.3f, max %.3f) ms\n",
        double(sum) / z.partitions / 1000,
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
 * getnames: load names from input
 */
static void getnames(int rank, std::vector<std::string>* results) {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  results->clear();

  snprintf(fname, sizeof(fname), "%s/NAMES-%07d.txt", g.dirinfo, rank);
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
 * runqueries: run queries against a given partition
 */
static void runqueries(const struct plfsdir_stats* s, int rank) {
  std::vector<std::string> names;
  getnames((g.a || c.bypass_shuffle) ? 0 : rank, &names);
  std::random_shuffle(names.begin(), names.end());
  uint64_t start;

  const int reads = std::min(g.d, int(names.size()));

  if (g.v)
    info("rank %d (%d reads) ...\t\t(%d samples available)", rank, reads,
         int(names.size()));

  start = now();

  if (c.bloomy_fmt) {
    filterreadnames(s, rank, &names[0], reads);
  } else {
    readnames(s, rank, &names[0], reads);
  }

  z.latencies->push_back(now() - start);

  z.partitions++;
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  int ch;
  argv0 = argv[0];
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

  if (argc != 2) /* dirname and dirinfo must be specified via cmd */
    usage("bad args");
  g.dirname = argv[0];
  g.dirinfo = argv[1];

  if (access(g.dirname, R_OK) != 0)
    complain("cannot access %s: %s", g.dirname, strerror(errno));
  if (access(g.dirinfo, R_OK) != 0)
    complain("cannot access %s: %s", g.dirinfo, strerror(errno));

  memset(&c, 0, sizeof(c));
  getmanifest(g.dirinfo, &c);

  printf("\n%s\n==options:\n", argv0);
  printf("\tqueries: %d x %d (ranks x reads)\n", g.r, g.d);
  printf("\tnum bg threads: %d (reader thread pool)\n", r.bg);
  printf("\tanti-shuffle: %d\n", g.a);
  printf("\tinfodir: %s\n", g.dirinfo);
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

  memset(&z, 0, sizeof(z));
  z.latencies = new std::vector<uint64_t>;
  memset(&m, 0, sizeof(m));
  memset(&x, 0, sizeof(x));
  x.min_table_seeks = ULONG_LONG_MAX;
  x.min_seeks = ULONG_LONG_MAX;

  if (r.bg) tp = deltafs_tp_init(r.bg);
  if (r.bg && !tp) complain("fail to init thread pool");

  plfsdir_stats s;
  memset(&s, 0, sizeof(s));
  s.tp = tp;
  s.dirname = g.dirname;
  s.v = g.v;

  s.r = &r;
  s.c = &c;
  s.x = &x;
  s.m = &m;

  std::vector<int> ranks;
  int nranks;
  for (int i = 0; i < c.comm_sz; i++) {
    ranks.push_back(i);
  }
  std::random_shuffle(ranks.begin(), ranks.end());
  nranks = (g.a || c.bypass_shuffle) ? c.comm_sz : g.r;
  if (g.v) info("start queries (%d ranks) ...", std::min(nranks, c.comm_sz));
  for (int i = 0; i < nranks && i < c.comm_sz; i++) {
    runqueries(&s, ranks[i]);
  }
  report();

  if (tp) deltafs_tp_close(tp);
  if (c.memtable_size) free(c.memtable_size);
  if (c.filter_bits_per_key) free(c.filter_bits_per_key);
  delete z.latencies;

  if (g.v) info("all done!");
  if (g.v) info("bye");

  exit(0);
}
