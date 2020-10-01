#pragma once

#include <assert.h>
#include <limits.h>
#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <time.h>

#include "common.h"
#include "pdlfs-common/mutexlock.h"
#include "range_common.h"
#include "rtp/rtp.h"
#include "stat.h"

#define PERFSTATS_MEM_SIZE 1
#define PERFSTATS_CAPTURE_FREQ 10

namespace pdlfs {
typedef struct perfstats_stats {
  struct timespec stat_time;
  long long bytes_written;
  long long secs_written;
} perfstats_stats_t;

typedef struct {
  uint64_t bytes_written = 0;
} stat_hooks_t;

typedef struct perfstats_ctx {
  /* All timestamps are relative to this time */
  int my_rank;
  struct timespec start_time;
  FILE* output_file;

  pthread_t stats_thread;
  bool shutdown;

  char stats_fpath[PATH_MAX];

  port::Mutex worker_mtx;
  stat_hooks_t stat_hooks;
  std::vector<StatLogger*> all_loggers_;
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
int perfstats_init(perfstats_ctx_t* pctx, int my_rank, const char* dir_path,
                   const char* local_root);

/**
 * @brief
 *
 * @param pctx
 *
 * @return
 */
int perfstats_destroy(perfstats_ctx_t* pctx);

/**
 * @brief
 *
 * @param pctx
 * @param pvt_ctx
 * @param rctx
 *
 * @return
 */
int perfstats_log_reneg(perfstats_ctx_t* pctx, pivot_ctx_t* pvt_ctx,
                        reneg_ctx_t rctx);

/**
 * @brief
 *
 * @param pctx
 * @param pvt_ctx
 * @param my_rank
 * @return
 */
int perfstats_log_aggr_bin_count(perfstats_ctx_t* pctx, pivot_ctx_t* pvt_ctx,
                                 int my_rank);

/**
 * @brief
 *
 * @param pctx
 * @param pivots
 * @param num_pivots
 * @return
 */
int perfstats_log_mypivots(perfstats_ctx_t* pctx, float* pivots,
                           int num_pivots);

/**
 * @brief 
 *
 * @param pctx
 * @param event_label
 * @param event_desc
 *
 * @return 
 */
int perfstats_log_eventstr(perfstats_ctx_t* pctx, const char* event_label,
                           const char* event_desc);

/**
 * @brief 
 *
 * @param pctx
 * @param fmt
 * @param ...
 *
 * @return 
 */
int perfstats_printf(perfstats_ctx_t* pctx, const char* fmt, ...);
}  // namespace pdlfs