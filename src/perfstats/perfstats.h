#pragma once

#include <limits.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>

#include <vector>

#include <pdlfs-common/mutexlock.h>

#define PERFSTATS_CAPTURE_FREQ 10

namespace pdlfs {
namespace carp {
class Carp;       /* forward decl for pointer */
};
};

namespace pdlfs {

typedef struct perfstats_ctx {
  /* All timestamps are relative to this time */
  int my_rank;
  struct timespec start_time;
  FILE* output_file;

  pthread_t stats_thread;
  bool shutdown;

  char stats_fpath[PATH_MAX];

  port::Mutex worker_mtx;
  uint64_t bytes_written;
} perfstats_ctx_t;

/**
 * @brief
 *
 * @param perf_ctx
 * @param my_rank
 * @param dir_path
 * @param local_root
 *
 * @return
 */
int perfstats_init(perfstats_ctx_t* perf_ctx, int my_rank, const char* dir_path,
                   const char* local_root);

/**
 * @brief
 *
 * @param perf_ctx
 *
 * @return
 */
int perfstats_destroy(perfstats_ctx_t* perf_ctx);

/**
 * @brief
 *
 * @param perf_ctx
 * @param carp
 * @param my_rank
 * @param round_num
 *
 * @return
 */
void perfstats_log_reneg(perfstats_ctx_t* perf_ctx, pdlfs::carp::Carp* carp,
                         int my_rank, int round_num);

/**
 * @brief
 *
 * @param perf_ctx
 * @param carp
 * @param my_rank
 * @return
 */
void perfstats_log_aggr_bin_count(perfstats_ctx_t* perf_ctx,
                                  pdlfs::carp::Carp* carp, int my_rank);

/**
 * @brief
 *
 * @param perf_ctx
 * @param pivots
 * @param num_pivots
 * @param pivot_label
 * @return
 */
void perfstats_log_mypivots(perfstats_ctx_t* perf_ctx, double* pivots,
                            int num_pivots, const char* pivot_label);

/**
 * @brief
 *
 * @param perf_ctx
 * @param pivots
 * @param num_pivots
 * @param pivot_label
 * @return
 */
void perfstats_log_vec(perfstats_ctx_t* perf_ctx, std::vector<uint64_t>& vec,
                       const char* pivot_label);

/**
 * @brief
 *
 * @param perf_ctx
 * @param fmt
 * @param ...
 *
 * @return
 */
void perfstats_printf(perfstats_ctx_t* perf_ctx, const char* fmt, ...);
}  // namespace pdlfs
