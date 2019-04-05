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
 * preload_reader.cc
 *
 * a simple reader program for reading data out of a plfsdir.
 */

#include "preload_plfsdir_reader.h"

#include <assert.h>
#include <errno.h>
#include <getopt.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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
  char* dirinfo; /* path to dir info */
  char* dirname; /* dir name (path to dir storage) */
  char* dirdest; /* output dir */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

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
  fprintf(stderr, "usage: %s [options] plfsdir infodir destdir fname\n", argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-j num    number of background worker threads\n");
  fprintf(stderr, "\t-t sec    timeout (alarm), in seconds\n");
  fprintf(stderr, "\t-i        ignore bloom filters\n");
  fprintf(stderr, "\t-c        verify crc32c (for both data and indexes)\n");
  fprintf(stderr, "\t-k        force paranoid checks\n");
  fprintf(stderr, "\t-v        be verbose\n");
  exit(1);
}

#include <pdlfs-common/xxhash.h>

/*
 * output stats
 */
static struct {
  uint64_t nfiles; /* total files copied out */
  uint64_t nbytes; /* total bytes written */
} z;
static FILE* out;
static void consume(char* buf, size_t sz) {
  if (sz != 0) {
    fwrite(buf, 1, sz, out);
    z.nbytes += sz;
  }
}

static void read(const struct plfsdir_stats* s, char* target) {
  std::string path, fname;
  int rank;

  fname = target;
  path = std::string(g.dirdest) + "/" + fname;
  out = fopen(path.c_str(), "w");
  if (!out) {
    complain("cannot create output file %s: %s", target, strerror(errno));
  }
  rank = pdlfs::xxhash32(fname.data(), fname.length(), 0) % c.comm_sz;
  z.nfiles++;

  if (c.bloomy_fmt) {
    filterreadnames(s, rank, &fname, 1);
  } else {
    readnames(s, rank, &fname, 1);
  }

  fflush(out);
  fclose(out);
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

  while ((ch = getopt(argc, argv, "j:t:ickv")) != -1) {
    switch (ch) {
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

  if (argc < 3) /* dirname, dirinfo, and dirdest are required */
    usage("bad args");

  g.dirname = argv[0];
  g.dirinfo = argv[1];
  g.dirdest = argv[2];

  if (access(g.dirname, R_OK) != 0)
    complain("cannot access %s: %s", g.dirname, strerror(errno));
  if (access(g.dirinfo, R_OK) != 0)
    complain("cannot access %s: %s", g.dirinfo, strerror(errno));
  if (access(g.dirdest, W_OK) != 0)
    complain("cannot access %s: %s", g.dirdest, strerror(errno));

  memset(&c, 0, sizeof(c));
  getmanifest(g.dirinfo, &c);

  if (g.v) {
    printf("\n%s\n==options:\n", argv0);
    printf("\tnum bg threads: %d (reader thread pool)\n", r.bg);
    printf("\tplfsdir: %s\n", g.dirname);
    printf("\tinfodir: %s\n", g.dirinfo);
    printf("\tdestdir: %s\n", g.dirdest);
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
  }

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  memset(&m, 0, sizeof(m));
  memset(&x, 0, sizeof(x));
  x.min_table_seeks = ULONG_LONG_MAX;
  x.min_seeks = ULONG_LONG_MAX;

  if (r.bg) tp = deltafs_tp_init(r.bg);
  if (r.bg && !tp) complain("fail to init thread pool");

  plfsdir_stats s;
  memset(&s, 0, sizeof(s));
  s.tp = tp;
  s.consume = consume;
  s.dirname = g.dirname;
  s.v = g.v;

  s.r = &r;
  s.c = &c;
  s.x = &x;
  s.m = &m;

  memset(&z, 0, sizeof(z));
  for (int a = 3; a < argc; a++) {
    read(&s, argv[a]);
  }
  if (g.v) {
    info("\n---");
    info("total files written: %lu", z.nfiles);
    info("total bytes written: %lu", z.nbytes);
  }

  if (tp) deltafs_tp_close(tp);
  if (c.memtable_size) free(c.memtable_size);
  if (c.filter_bits_per_key) free(c.filter_bits_per_key);

  if (g.v) info("all done!");
  if (g.v) info("bye");

  exit(0);
}
