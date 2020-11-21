#include "perfstats/perfstats.h"

#include <math.h>
#include <mpi.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "perfstats/stat.h"
#include "preload_internal.h"

#ifdef PRELOAD_HAS_BLKID
#include "perfstats/stat_blkid.h"
#endif

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

int perfstats_log_once(perfstats_ctx_t* perf_ctx);

int perfstats_log_stat(perfstats_ctx_t* perf_ctx, Stat& s);

int perfstats_log_hooks(perfstats_ctx_t* perf_ctx, uint64_t timestamp);

int perfstats_generate_header(perfstats_ctx_t* perf_ctx);

int perfstats_init(perfstats_ctx_t* perf_ctx, int my_rank, const char* dir_path,
                   const char* local_root) {
  assert(perf_ctx != NULL);

  perf_ctx->my_rank = my_rank;
  perf_ctx->shutdown = false;
  int rv = 0;

  clock_gettime(CLOCK_MONOTONIC, &(perf_ctx->start_time));

  snprintf(perf_ctx->stats_fpath, PATH_MAX, "%s/vpic-perfstats.log.%d",
           dir_path, my_rank);
  FILE* output_file = fopen(perf_ctx->stats_fpath, "w");

  if (output_file == NULL) {
    rv = -1;
    logf(LOG_ERRO, "perfstats_init: failed to open file");
    return rv;
  }

  setvbuf(output_file, NULL, _IOLBF, 0);

  perf_ctx->output_file = output_file;

  perfstats_generate_header(perf_ctx);

  rv = pthread_create(&(perf_ctx->stats_thread), NULL, perfstats_worker,
                      static_cast<void*>(perf_ctx));

#ifdef PRELOAD_HAS_BLKID
  StatBlkid* statBlkid = new StatBlkid(local_root);
  perf_ctx->all_loggers_.push_back(statBlkid);
#endif

  if (rv) {
    logf(LOG_ERRO, "perfstats_init: failed to create pthread");
    return rv;
  }

  return 0;
}

int perfstats_destroy(perfstats_ctx_t* perf_ctx) {
  /* Tell worker thread to shut down */
  perf_ctx->shutdown = true;
  pthread_join(perf_ctx->stats_thread, NULL);

  fclose(perf_ctx->output_file);
  perf_ctx->output_file = NULL;

  for (uint32_t sidx = 0; sidx < perf_ctx->all_loggers_.size(); sidx++) {
    delete perf_ctx->all_loggers_[sidx];
  }

  return 0;
}

/* BEGIN Internal Definitions */
void* perfstats_worker(void* arg) {
  perfstats_ctx_t* perf_ctx = static_cast<perfstats_ctx_t*>(arg);

  while (true) {
    perf_ctx->worker_mtx.Lock();

    if (perf_ctx->shutdown) {
      if (perf_ctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: shutting down");
      }

      if (perf_ctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: done");
      }

      perf_ctx->worker_mtx.Unlock();
      break;
    }

    /* do logging */
    perfstats_log_once(perf_ctx);

    perf_ctx->worker_mtx.Unlock();
    usleep(1e6 / PERFSTATS_CAPTURE_FREQ);
  }

  return NULL;
}

int perfstats_log_stat(perfstats_ctx_t* perf_ctx, Stat& s) {
  perf_ctx->worker_mtx.AssertHeld();
  s.Serialize(perf_ctx->output_file);

  return 0;
}

int perfstats_log_once(perfstats_ctx_t* perf_ctx) {
  perf_ctx->worker_mtx.AssertHeld();

  uint64_t timestamp = ::get_timestamp(perf_ctx);
  perfstats_log_hooks(perf_ctx, timestamp);

  /* construct with random parameters; TODO: add better constructor */
  Stat s(StatType::V_INT, "");

  for (uint32_t sidx = 0; sidx < perf_ctx->all_loggers_.size(); sidx++) {
    perf_ctx->all_loggers_[sidx]->LogOnce(timestamp, s);
    perfstats_log_stat(perf_ctx, s);
  }

  return 0;
}

int perfstats_generate_header(perfstats_ctx_t* perf_ctx) {
  const char* header_str = "Timestamp (ms),Stat Type, Stat Value\n";
  fwrite(header_str, strlen(header_str), 1, perf_ctx->output_file);
  return 0;
}

int perfstats_log_hooks(perfstats_ctx_t* perf_ctx, uint64_t timestamp) {
  perf_ctx->worker_mtx.AssertHeld();

  const char* lbw_label = "LOGICAL_BYTES_WRITTEN";
  Stat logical_bytes_written(StatType::V_UINT64, lbw_label);
  logical_bytes_written.SetValue(timestamp, perf_ctx->stat_hooks.bytes_written);

  perfstats_log_stat(perf_ctx, logical_bytes_written);
  return 0;
}

int perfstats_log_reneg(perfstats_ctx_t* perf_ctx, pivot_ctx_t* pvt_ctx,
                        rtp_ctx_t rctx) {
  uint64_t timestamp = ::get_timestamp(perf_ctx);

  const char* const kRenegMassLabel = "RENEG_COUNTS";
  Stat massStat(StatType::V_STR, kRenegMassLabel);

  const char* const kRenegPivotsLabel = "RENEG_PIVOTS";
  Stat pivotStat(StatType::V_STR, kRenegPivotsLabel);

  int buf_sz = STAT_BUF_MAX, buf_idx = 0;
  char buf[buf_sz];

  buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx,
                      "RANK%d_R%d: ", rctx->my_rank, rctx->round_num);

  std::vector<float>& counts = pvt_ctx->rank_bin_count;
  buf_idx +=
      print_vector(buf + buf_idx, buf_sz - buf_idx, counts, counts.size(),
                   /* truncate */ false);

  buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx, ": OOB (%zu)",
                      pvt_ctx->oob_buffer->Size());

  massStat.SetValue(timestamp, buf);

  print_vector(buf, buf_sz, pvt_ctx->my_pivots, pvt_ctx->my_pivot_count,
               /* truncate */ false);
  pivotStat.SetValue(timestamp, buf);

  perf_ctx->worker_mtx.Lock();
  perfstats_log_stat(perf_ctx, massStat);
  perfstats_log_stat(perf_ctx, pivotStat);
  perf_ctx->worker_mtx.Unlock();

  return 0;
}

int perfstats_log_aggr_bin_count(perfstats_ctx_t* perf_ctx,
                                 pivot_ctx_t* pvt_ctx, int my_rank) {
  int rv = 0;
  std::vector<uint64_t>& cnt_vec = pvt_ctx->rank_bin_count_aggr;
  size_t bin_sz = cnt_vec.size();
  uint64_t send_buf[bin_sz], recv_buf[bin_sz];

  std::copy(cnt_vec.begin(), cnt_vec.end(), send_buf);
  MPI_Reduce(send_buf, recv_buf, bin_sz, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);

  if (my_rank != 0) return rv;

  size_t buf_sz = STAT_BUF_MAX;
  char buf[buf_sz];
  const char* const kAggrCountLabel = "RENEG_AGGR_BINCNT";
  Stat aggr_count(StatType::V_STR, kAggrCountLabel);

  print_vector(buf, buf_sz, recv_buf, bin_sz, false);

  uint64_t timestamp = get_timestamp(perf_ctx);
  aggr_count.SetValue(timestamp, buf);

  const char* const kAggrStdLabel = "RENEG_AGGR_STD";
  Stat aggr_std(StatType::V_FLOAT, kAggrStdLabel);
  float aggr_std_val = get_norm_std(recv_buf, bin_sz);
  aggr_std.SetValue(timestamp, aggr_std_val);

  if (my_rank == 0) {
    logf(LOG_INFO, "[perfstats] normalized load stddev: %.3f\n", aggr_std_val);
  }

  perf_ctx->worker_mtx.Lock();
  perfstats_log_stat(perf_ctx, aggr_count);
  perfstats_log_stat(perf_ctx, aggr_std);
  perf_ctx->worker_mtx.Unlock();

  return rv;
}

int perfstats_log_mypivots(perfstats_ctx_t* perf_ctx, double* pivots,
                           int num_pivots, const char* pivot_label) {
  int rv = 0;

  size_t buf_sz = STAT_BUF_MAX;
  char buf[buf_sz];
  print_vector(buf, buf_sz, pivots, num_pivots, false);

  uint64_t timestamp = get_timestamp(perf_ctx);
  Stat pivot_stat(StatType::V_STR, pivot_label);
  pivot_stat.SetValue(timestamp, buf);

  perf_ctx->worker_mtx.Lock();
  perfstats_log_stat(perf_ctx, pivot_stat);
  perf_ctx->worker_mtx.Unlock();

  return rv;
}

int perfstats_log_carp(perfstats_ctx_t* perf_ctx) {
#define PERFLOG(a, b) \
  perfstats_log_eventstr(perf_ctx, a, std::to_string(b).c_str())
  PERFLOG("CARP_ENABLED", pctx.carp_on);
  PERFLOG("CARP_NUM_PIVOTS", pctx.carp_on);
  PERFLOG("CARP_DYNAMIC_ENABLED", pctx.carp_dynamic_reneg);
  PERFLOG("CARP_RENEG_INTERVAL", pctx.carp_reneg_intvl);
#undef PERFLOG

  return 0;
}

int perfstats_log_eventstr(perfstats_ctx_t* perf_ctx, const char* event_label,
                           const char* event_desc) {
  int rv = 0;

  uint64_t timestamp = get_timestamp(perf_ctx);

  Stat str_stat(StatType::V_STR, event_label);
  str_stat.SetValue(timestamp, event_desc);

  perf_ctx->worker_mtx.Lock();
  perfstats_log_stat(perf_ctx, str_stat);
  perf_ctx->worker_mtx.Unlock();
  return rv;
}

int perfstats_printf(perfstats_ctx_t* perf_ctx, const char* fmt, ...) {
  int rv = 0;
  uint64_t timestamp = get_timestamp(perf_ctx);

  perf_ctx->worker_mtx.Lock();

  va_list ap;
  va_start(ap, fmt);
  vfprintf(perf_ctx->output_file, fmt, ap);
  va_end(ap);
  fprintf(perf_ctx->output_file, "\n");

  perf_ctx->worker_mtx.Unlock();

  return 0;
}
}  // namespace pdlfs
   /* END Internal Definitions */
