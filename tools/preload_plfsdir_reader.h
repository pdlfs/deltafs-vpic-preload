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
 * preload_plfsdir_reader.h
 *
 * data structures and utility functions internal to plfsdir reader programs
 */

#pragma once

#include <deltafs/deltafs_api.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * plfsdir_tp_t: plfsdir thread pool handle type
 */
typedef deltafs_tp_t plfsdir_tp_t;

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
 * get_manifest: retrieve dir manifest from *dirinfo and store it to *c
 */
static inline void get_manifest(const char* dirinfo, struct plfsdir_conf* c) {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  snprintf(fname, sizeof(fname), "%s/MANIFEST", dirinfo);
  f = fopen(fname, "r");
  if (!f) {
    fprintf(stderr, "error opening %s: %s", fname, strerror(errno));
    exit(1);
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
    fprintf(stderr, "error reading %s: %s", fname, strerror(errno));
    exit(1);
  }

  if (c->key_size == 0 || c->comm_sz == 0) {
    fprintf(stderr, "bad manifest: key_size or comm_sz is 0?!");
    exit(1);
  }

  fclose(f);
}

/*
 * gen_conf: generate plfsdir conf and write it into *buf
 */
static inline char* gen_conf(const struct plfsdir_reader_conf* r,
                             const struct plfsdir_conf* c, int myrank,
                             char* buf, size_t bufsize) {
  int n;

  n = snprintf(buf, bufsize, "rank=%d", myrank);
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
