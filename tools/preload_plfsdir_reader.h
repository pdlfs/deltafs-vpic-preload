/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * preload_plfsdir_reader.h
 *
 * data structures and utility functions internal to plfsdir reader programs
 */

#pragma once

#include <deltafs/deltafs_api.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <string>

/*
 * plfsdir_tp_t: plfsdir thread pool handle type
 */
typedef deltafs_tp_t plfsdir_tp_t;

/*
 * vcomplain/complain about something and exit.
 */
static inline void vcomplain(const char* format, va_list ap) {
  fprintf(stderr, "!!! ERROR !!! ");
  vfprintf(stderr, format, ap);
  fprintf(stderr, "\n");
  exit(1);
}

static inline void complain(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vcomplain(format, ap);
  va_end(ap);
}

/*
 * print info messages.
 */
static inline void vinfo(const char* format, va_list ap) {
  vprintf(format, ap);
  printf("\n");
}

static inline void info(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vinfo(format, ap);
  va_end(ap);
}

/*
 * helper functions for parsing plfsdir manifest lines
 */
static inline bool parse_manifest_int(char* ch, const char* prefix,
                                      int* result) {
  size_t n = strlen(prefix);
  if (strncmp(ch, prefix, n) == 0) {
    *result = atoi(ch + n);
    return true;
  } else {
    return false;
  }
}

static inline bool parse_manifest_string(char* ch, const char* prefix,
                                         char** result) {
  size_t n = strlen(prefix);
  if (strncmp(ch, prefix, n) == 0) {
    *result = strdup(ch + n);
    if ((*result)[0] != 0 && (*result)[strlen(*result) - 1] == '\n')
      (*result)[strlen(*result) - 1] = 0;
    return true;
  } else {
    return false;
  }
}

/*
 * end helper functions
 */

/* plfsdir_reader_conf: how to read from a plfsdir */
struct plfsdir_reader_conf {
  int bg; /* number of background worker threads for reading data in parallel */
  int nobf;     /* ignore bloom filters */
  int crc32c;   /* verify checksums */
  int paranoid; /* paranoid checks */
};

/* plfsdir_conf: how a plfsdir is configured */
struct plfsdir_conf {
  int num_epochs;
  int key_size;
  int value_size;
  char* filter_bits_per_key;
  char* memtable_size;
  int lg_parts;
  int skip_crc32c;
  int bypass_shuffle;
  int force_leveldb_format;
  int unordered_storage;
  int io_engine;
  int comm_sz;
  int particle_id_size;
  int particle_size;
  int bloomy_fmt;
  int wisc_fmt;
};

/*
 * getmanifest: retrieve dir manifest from *dirinfo and store it to *c
 */
static inline void getmanifest(const char* dirinfo, struct plfsdir_conf* c) {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  snprintf(fname, sizeof(fname), "%s/MANIFEST", dirinfo);
  f = fopen(fname, "r");
  if (!f) {
    complain("error opening %s: %s", fname, strerror(errno));
  }

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
    if (parse_manifest_int(ch, "num_epochs=", &c->num_epochs) ||
        parse_manifest_int(ch, "key_size=", &c->key_size) ||
        parse_manifest_int(ch, "value_size=", &c->value_size) ||
        parse_manifest_string(
            ch, "filter_bits_per_key=", &c->filter_bits_per_key) ||
        parse_manifest_string(ch, "memtable_size=", &c->memtable_size) ||
        parse_manifest_int(ch, "lg_parts=", &c->lg_parts) ||
        parse_manifest_int(ch, "skip_checksums=", &c->skip_crc32c) ||
        parse_manifest_int(ch, "bypass_shuffle=", &c->bypass_shuffle) ||
        parse_manifest_int(ch,
                           "force_leveldb_format=", &c->force_leveldb_format) ||
        parse_manifest_int(ch, "unordered_storage=", &c->unordered_storage) ||
        parse_manifest_int(ch, "io_engine=", &c->io_engine) ||
        parse_manifest_int(ch, "comm_sz=", &c->comm_sz) ||
        parse_manifest_int(ch, "particle_id_size=", &c->particle_id_size) ||
        parse_manifest_int(ch, "particle_size=", &c->particle_size)) {
    } else if (strcmp(ch, "fmt=bloomy\n") == 0) {
      c->bloomy_fmt = 1;
    } else if (strcmp(ch, "fmt=wisc\n") == 0) {
      c->wisc_fmt = 1;
    }
  }

  if (ferror(f)) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  if (c->key_size == 0 || c->comm_sz == 0) {
    complain("bad manifest: key_size or comm_sz is 0?!");
  }

  fclose(f);
}

/*
 * genconf: generate plfsdir conf and write it into *buf
 */
static inline char* genconf(const struct plfsdir_reader_conf* r,
                            const struct plfsdir_conf* c, int rank, char* buf,
                            size_t bufsize) {
  int n;

  n = snprintf(buf, bufsize, "rank=%d", rank);
  n += snprintf(buf + n, bufsize - n, "&value_size=%d", c->value_size);
  n += snprintf(buf + n, bufsize - n, "&key_size=%d", c->key_size);

#ifndef NDEBUG
  n += snprintf(buf + n, bufsize - n, "&memtable_size=%s", c->memtable_size);
  n += snprintf(buf + n, bufsize - n, "&bf_bits_per_key=%s",
                c->filter_bits_per_key);
#endif

  if (c->io_engine == DELTAFS_PLFSDIR_DEFAULT) {
    n += snprintf(buf + n, bufsize - n, "&num_epochs=%d", c->num_epochs);
    n += snprintf(buf + n, bufsize - n, "&skip_checksums=%d", c->skip_crc32c);
    n += snprintf(buf + n, bufsize - n, "&verify_checksums=%d", r->crc32c);
    n += snprintf(buf + n, bufsize - n, "&paranoid_checks=%d", r->paranoid);
    n += snprintf(buf + n, bufsize - n, "&parallel_reads=%d", r->bg != 0);
    n += snprintf(buf + n, bufsize - n, "&ignore_filters=%d", r->nobf);
    snprintf(buf + n, bufsize - n, "&lg_parts=%d", c->lg_parts);
  }

  return buf;
}

/* plfsdir_core_mon: plfsdir's internal seek stats */
struct plfsdir_core_mon {
  uint64_t total_table_seeks; /* total num of table touched over all read ops */
  uint64_t min_table_seeks;   /* min per read */
  uint64_t max_table_seeks;   /* max per read */
  uint64_t total_seeks;       /* total data block seeks */
  uint64_t min_seeks;         /* min per read */
  uint64_t max_seeks;         /* max per read */
};

/* plfsdir_mon: monitoring stats */
struct plfsdir_mon {
  uint64_t under_bytes; /* total amount of underlying data retrieved */
  uint64_t under_files; /* total amount of underlying files opened */
  uint64_t under_seeks; /* total amount of underlying storage seeks */
  uint64_t extra_ops;   /* num extra read ops */
  uint64_t extra_okops; /* num extra read ops that return non-empty data */
  uint64_t extra_bytes; /* total extra amount of data read */
  uint64_t ops;         /* num read ops */
  uint64_t okops;       /* num read ops that return non-empty data */
  uint64_t bytes;       /* total amount of data read */
};

/* plfsdir_stats: read stats */
struct plfsdir_stats {
  plfsdir_tp_t* tp;
  void (*consume)(char*, size_t);
  const struct plfsdir_reader_conf* r;
  const struct plfsdir_conf* c;
  struct plfsdir_core_mon* x;
  struct plfsdir_mon* m;
  const char* dirname;
  int v;
};

static inline void onemore(const struct plfsdir_stats* s, int rank, off_t off) {
  size_t sz;
  deltafs_plfsdir_t* dir;
  char conf[20];
  ssize_t n;

  snprintf(conf, sizeof(conf), "rank=%d", rank);
  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, DELTAFS_PLFSDIR_NOTHING);
  if (!dir) {
    complain("fail to create plfsdir handle");
  }

  deltafs_plfsdir_enable_io_measurement(dir, 1);
  if (deltafs_plfsdir_open(dir, s->dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));
  if (deltafs_plfsdir_io_open(dir, s->dirname) != 0)
    complain("error opening plfsdir io: %s", strerror(errno));

  sz = s->c->particle_size;
  char* buf = static_cast<char*>(malloc(sz));
  n = deltafs_plfsdir_io_pread(dir, buf, sz, off);
  if (n < 0 || (size_t)n != sz)
    complain("error reading extra data: %s", strerror(errno));
  if (s->consume) {
    s->consume(buf, sz);
  }

  free(buf);

  s->m->under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  s->m->under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  s->m->under_seeks +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");
  s->m->extra_bytes += n;
  if (n != 0) s->m->extra_okops++;
  s->m->extra_ops++;

  deltafs_plfsdir_free_handle(dir);
}

static inline void readone(const struct plfsdir_stats* s,
                           deltafs_plfsdir_t* dir, const char* fname) {
  size_t table_seeks;
  size_t seeks;
  size_t sz;
  uint64_t off;
  int rank;

  table_seeks = seeks = 0;

  if (s->v) info("reading %s ...", fname);
  char* buf = static_cast<char*>(
      deltafs_plfsdir_read(dir, fname, -1, &sz, &table_seeks, &seeks));
  if (!buf) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  if (s->c->wisc_fmt) { /* need one more read for wisc formatted dirs */
    if (sz != 12) complain("bad record size: %s", fname);
    memcpy(&rank, buf, 4);
    memcpy(&off, buf + 4, 8);
    onemore(s, rank, off);
  } else if (s->consume) { /* directly consume data */
    s->consume(buf, sz);
  }

  free(buf);

  s->x->total_table_seeks += table_seeks;
  s->x->min_table_seeks =
      std::min<uint64_t>(table_seeks, s->x->min_table_seeks);
  s->x->max_table_seeks =
      std::max<uint64_t>(table_seeks, s->x->max_table_seeks);
  s->x->total_seeks += seeks;
  s->x->min_seeks = std::min<uint64_t>(seeks, s->x->min_seeks);
  s->x->max_seeks = std::max<uint64_t>(seeks, s->x->max_seeks);
  s->m->bytes += sz;
  if (sz != 0) s->m->okops++;
  s->m->ops++;
}

static inline void readnames(const struct plfsdir_stats* s, int rank,
                             std::string* fnames, int nnames) {
  deltafs_plfsdir_t* dir;

  char* conf = static_cast<char*>(malloc(500));
  genconf(s->r, s->c, rank, conf, 500);
  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, s->c->io_engine);
  if (!dir) {
    complain("fail to create plfsdir handle");
  }

  deltafs_plfsdir_enable_io_measurement(dir, 1);
  if (s->tp) deltafs_plfsdir_set_thread_pool(dir, s->tp);
  deltafs_plfsdir_force_leveldb_fmt(dir, s->c->force_leveldb_format);
  deltafs_plfsdir_set_unordered(dir, s->c->unordered_storage);
  deltafs_plfsdir_set_fixed_kv(dir, 1);

  if (deltafs_plfsdir_open(dir, s->dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));
  for (int i = 0; i < nnames; i++) {
    readone(s, dir, fnames[i].c_str());
  }

  s->m->under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  s->m->under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  s->m->under_seeks +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");

  deltafs_plfsdir_free_handle(dir);

  free(conf);
}

static inline void filterreadnames(const struct plfsdir_stats* s, int rank,
                                   std::string* fnames, int nnames) {
  deltafs_plfsdir_t* dir;
  char conf[20];
  size_t n;

  snprintf(conf, sizeof(conf), "rank=%d", rank);
  dir = deltafs_plfsdir_create_handle(conf, O_RDONLY, DELTAFS_PLFSDIR_NOTHING);
  if (!dir) {
    complain("fail to create plfsdir handle");
  }

  deltafs_plfsdir_enable_io_measurement(dir, 1);
  if (deltafs_plfsdir_open(dir, s->dirname) != 0)
    complain("error opening plfsdir: %s", strerror(errno));
  if (deltafs_plfsdir_filter_open(dir, s->dirname) != 0)
    complain("error opening plfsdir filter: %s", strerror(errno));

  for (int i = 0; i < nnames; i++) {
    int* possible_ranks =
        deltafs_plfsdir_filter_get(dir, fnames[i].data(), fnames[i].size(), &n);
    if (possible_ranks) {
      for (size_t j = 0; j < n; j++) {
        readnames(s, possible_ranks[j], &fnames[i], 1);
      }
    }
    free(possible_ranks);
  }

  s->m->under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  s->m->under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  s->m->under_seeks +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");

  deltafs_plfsdir_free_handle(dir);
}
