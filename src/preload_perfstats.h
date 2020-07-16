#pragma once

#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <time.h>

#include "common.h"

#define PERFSTATS_MEM_SIZE 512
#define PERFSTATS_CAPTURE_FREQ 10

typedef struct bd_stats {
  long unsigned rd_ios;
  long unsigned rd_merges;
  long unsigned rd_secs;
  long unsigned rd_ticks;

  long unsigned wr_ios;
  long unsigned wr_merges;
  long unsigned wr_secs;
  long unsigned wr_ticks;

  unsigned in_flight;
  unsigned io_ticks;
  unsigned time_queue;
} bd_stats_t;

typedef struct perfstats_stats {
  struct timespec stat_time;
  long long bytes_written;
  long long secs_written;
} perfstats_stats_t;

typedef struct perfstats_ctx {
  /* All timestamps are relative to this time */
  int my_rank;
  struct timespec start_time;
  FILE *output_file;

  perfstats_stats_t stats[PERFSTATS_MEM_SIZE];
  int stats_idx;

  pthread_t stats_thread;
  bool shutdown;

  char stats_fpath[PATH_MAX];

  pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;

  long long prop_bytes_written = 0;

  /*
   * Sysfs logging is enabled in very limited circumstances:
   * if libblkid is installed, and the partition containing the write
   * path supports sysfs block device extended attribtes
   */
  bool sysfs_enabled = false;

#ifdef PRELOAD_HAS_BLKID
  char sysfs_path[PATH_MAX];
  bd_stats_t prev_bd_stats;
#endif

} perfstats_ctx_t;

/**
 * @brief
 *
 * @param pctx
 * @param my_rank
 * @param dir_path
 * @param local_root
 *
 * @return
 */
int perfstats_init(perfstats_ctx_t *pctx, int my_rank, const char *dir_path,
                   const char *local_root);

/**
 * @brief
 *
 * @param pctx
 *
 * @return
 */
int perfstats_destroy(perfstats_ctx_t *pctx);
