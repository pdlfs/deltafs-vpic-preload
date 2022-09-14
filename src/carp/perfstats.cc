
#include <assert.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <mpi.h>

#include "perfstats.h"

#include "../carp/carp.h"

/* Local Definitions */
namespace {
uint64_t get_timestamp(pdlfs::perfstats_ctx_t* perf_ctx) {
  struct timespec stat_time;
  clock_gettime(CLOCK_MONOTONIC, &(stat_time));

  uint64_t time_delta_ms =
      (stat_time.tv_sec - perf_ctx->start_time.tv_sec) * 1e3 +
      (stat_time.tv_nsec - perf_ctx->start_time.tv_nsec) / 1e6;

  return time_delta_ms;
}
float get_norm_std(uint64_t* arr, size_t sz) {
  double norm_arr[sz];

  uint64_t sum_x = 0;
  for (size_t i = 0; i < sz; i++) {
    sum_x += arr[i];
  }

  double arr_mean = sum_x * 1.0 / sz;
  double norm_x2 = 0, norm_x = 0;
  for (size_t i = 0; i < sz; i++) {
    double norm_val = arr[i] / arr_mean;
    norm_x += norm_val;
    norm_x2 += (norm_val * norm_val);
  }

  double var = (norm_x2 - norm_x) / sz;
  double std = sqrt(var);

  return std;
}
}  // namespace

namespace pdlfs {

/* Internal Declarations */
void* perfstats_worker(void* arg);

int perfstats_init(perfstats_ctx_t* perf_ctx, int my_rank, const char* dir_path,
                   const char* local_root) {
  assert(perf_ctx != NULL);

  perf_ctx->my_rank = my_rank;
  perf_ctx->shutdown = true;
  int rv = 0;

  clock_gettime(CLOCK_MONOTONIC, &(perf_ctx->start_time));

  snprintf(perf_ctx->stats_fpath, PATH_MAX, "%s/vpic-perfstats.log.%d",
           dir_path, my_rank);
  FILE* output_file = fopen(perf_ctx->stats_fpath, "w");

  if (output_file == NULL) {
    rv = -1;
    logf(LOG_ERRO, "perfstats_init: failed to open file: %s\n",
         perf_ctx->stats_fpath);
    return rv;
  }

  setvbuf(output_file, NULL, _IOLBF, 0);

  perf_ctx->output_file = output_file;

  fprintf(perf_ctx->output_file, "Timestamp (ms),Stat Type, Stat Value\n");

  rv = pthread_create(&(perf_ctx->stats_thread), NULL, perfstats_worker,
                      static_cast<void*>(perf_ctx));

  if (rv) {
    logf(LOG_ERRO, "perfstats_init: failed to create pthread");
    return rv;
  }
  perf_ctx->shutdown = false;

  return 0;
}

int perfstats_destroy(perfstats_ctx_t* perf_ctx) {
  if (!perf_ctx->shutdown) {
    perf_ctx->shutdown = true;   /* tell worker thread to shut down */
    pthread_join(perf_ctx->stats_thread, NULL);
  }

  if (perf_ctx->output_file)
    fclose(perf_ctx->output_file);
  perf_ctx->output_file = NULL;

  return 0;
}

/* BEGIN Internal Definitions */
void* perfstats_worker(void* arg) {
  perfstats_ctx_t* perf_ctx = static_cast<perfstats_ctx_t*>(arg);
  uint64_t timestamp;

  perf_ctx->worker_mtx.Lock();
  while (1) {
    if (perf_ctx->shutdown)
      break;

    timestamp = ::get_timestamp(perf_ctx);
    fprintf(perf_ctx->output_file, "%lu,LOGICAL_BYTES_WRITTEN,%lu\n",
            timestamp, perf_ctx->bytes_written);

    perf_ctx->worker_mtx.Unlock();  /* drop lock during sleep */
    usleep(1e6 / PERFSTATS_CAPTURE_FREQ);
    perf_ctx->worker_mtx.Lock();
  }
  perf_ctx->worker_mtx.Unlock();
  if (perf_ctx->my_rank == 0)
    logf(LOG_INFO, "perfstats_worker: done, shutting down");

  return NULL;
}

void perfstats_log_reneg(perfstats_ctx_t* perf_ctx, pdlfs::carp::Carp* carp,
                         int my_rank, int round_num) {
  int i;
  uint64_t timestamp = ::get_timestamp(perf_ctx);
  MutexLock ml(&perf_ctx->worker_mtx);

  fprintf(perf_ctx->output_file, "%lu,RENEG_COUNTS,RANK%d_R%d: ", timestamp,
          my_rank, round_num);
  for (i = 0 ; i < carp->rank_counts_.size() ; i++) {
    fprintf(perf_ctx->output_file, "%.lf ", carp->rank_counts_[i]);
  }
  fprintf(perf_ctx->output_file, ": OOB (%zu)\n", carp->OobSize());

  fprintf(perf_ctx->output_file, "%lu,RENEG_PIVOTS,", timestamp);
  for (i = 0 ; i < carp->my_pivot_count_ ; i++) {
    fprintf(perf_ctx->output_file, "%.4lf ", carp->my_pivots_[i]);
  }
  fprintf(perf_ctx->output_file, "\n");
}

void perfstats_log_aggr_bin_count(perfstats_ctx_t* perf_ctx,
                                  pdlfs::carp::Carp* carp, int my_rank) {
  std::vector<uint64_t>& cnt_vec = carp->rank_counts_aggr_;
  size_t bin_sz = cnt_vec.size();
  uint64_t send_buf[bin_sz], recv_buf[bin_sz];

  std::copy(cnt_vec.begin(), cnt_vec.end(), send_buf);
  MPI_Reduce(send_buf, recv_buf, bin_sz, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD); /* XXX: send directly from cnt_vec[] ? */

  if (my_rank != 0) return;

  uint64_t timestamp = get_timestamp(perf_ctx);
  float aggr_std_val = get_norm_std(recv_buf, bin_sz);
  int i;

  perf_ctx->worker_mtx.Lock();

  fprintf(perf_ctx->output_file, "%lu,RENEG_AGGR_BINCNT,", timestamp);
  for (i = 0 ; i < bin_sz ; i++) {
    fprintf(perf_ctx->output_file, "%lu ", recv_buf[i]);
  }
  fprintf(perf_ctx->output_file, "\n");
  fprintf(perf_ctx->output_file, "%lu,RENEG_AGGR_STD,%.2f\n",
          timestamp, aggr_std_val);
  
  perf_ctx->worker_mtx.Unlock();

  if (my_rank == 0) {
    logf(LOG_INFO, "[perfstats] normalized load stddev: %.3f\n", aggr_std_val);
  }
}

void perfstats_log_mypivots(perfstats_ctx_t* perf_ctx, double* pivots,
                            int num_pivots, const char* pivot_label) {
  int i;
  uint64_t timestamp = ::get_timestamp(perf_ctx);
  MutexLock ml(&perf_ctx->worker_mtx);

  fprintf(perf_ctx->output_file, "%lu,%s,", timestamp, pivot_label);
  for (i = 0 ; i < num_pivots ; i++) {
    fprintf(perf_ctx->output_file, "%.4lf ", pivots[i]);
  }
  fprintf(perf_ctx->output_file, "\n");
}

void perfstats_log_vec(perfstats_ctx_t* perf_ctx, std::vector<uint64_t>& vec,
                      const char* vlabel) {
  int i;
  uint64_t timestamp = ::get_timestamp(perf_ctx);
  MutexLock ml(&perf_ctx->worker_mtx);

  fprintf(perf_ctx->output_file, "%lu,%s,", timestamp, vlabel);
  for (i = 0 ; i < vec.size() ; i++) {
    fprintf(perf_ctx->output_file, "%lu ", vec[i]);
  }
  fprintf(perf_ctx->output_file, "\n");
}

void perfstats_printf(perfstats_ctx_t* perf_ctx, const char* fmt, ...) {
  uint64_t timestamp = get_timestamp(perf_ctx);
  MutexLock ml(&perf_ctx->worker_mtx);

  va_list ap;
  va_start(ap, fmt);
  vfprintf(perf_ctx->output_file, fmt, ap);
  va_end(ap);
  fprintf(perf_ctx->output_file, "\n");
}
}  // namespace pdlfs
   /* END Internal Definitions */
