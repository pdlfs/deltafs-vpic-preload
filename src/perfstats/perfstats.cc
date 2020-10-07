#include "perfstats/perfstats.h"

#include <math.h>
#include <mpi.h>
#include <stdarg.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "perfstats/stat.h"

#ifdef PRELOAD_HAS_BLKID
#include "perfstats/stat_blkid.h"
#endif

/* Local Definitions */
namespace {
uint64_t get_timestamp(pdlfs::perfstats_ctx_t* pctx) {
  struct timespec stat_time;
  clock_gettime(CLOCK_MONOTONIC, &(stat_time));

  uint64_t time_delta_ms = (stat_time.tv_sec - pctx->start_time.tv_sec) * 1e3 +
                           (stat_time.tv_nsec - pctx->start_time.tv_nsec) / 1e6;

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

int perfstats_log_once(perfstats_ctx_t* pctx);

int perfstats_log_stat(perfstats_ctx_t* pctx, Stat& s);

int perfstats_log_hooks(perfstats_ctx_t* pctx, uint64_t timestamp);

int perfstats_generate_header(perfstats_ctx_t* pctx);

int perfstats_init(perfstats_ctx_t* pctx, int my_rank, const char* dir_path,
                   const char* local_root) {
  assert(pctx != NULL);

  pctx->my_rank = my_rank;
  pctx->shutdown = false;
  int rv = 0;

  clock_gettime(CLOCK_MONOTONIC, &(pctx->start_time));

  snprintf(pctx->stats_fpath, PATH_MAX, "%s/vpic-perfstats.log.%d", dir_path,
           my_rank);
  FILE* output_file = fopen(pctx->stats_fpath, "w");

  if (output_file == NULL) {
    rv = -1;
    logf(LOG_ERRO, "perfstats_init: failed to open file");
    return rv;
  }

  setvbuf(output_file, NULL, _IOLBF, 0);

  pctx->output_file = output_file;

  perfstats_generate_header(pctx);

  rv = pthread_create(&(pctx->stats_thread), NULL, perfstats_worker,
                      static_cast<void*>(pctx));

#ifdef PRELOAD_HAS_BLKID
  StatBlkid* statBlkid = new StatBlkid(local_root);
  pctx->all_loggers_.push_back(statBlkid);
#endif

  if (rv) {
    logf(LOG_ERRO, "perfstats_init: failed to create pthread");
    return rv;
  }

  return 0;
}

int perfstats_destroy(perfstats_ctx_t* pctx) {
  /* Tell worker thread to shut down */
  pctx->shutdown = true;
  pthread_join(pctx->stats_thread, NULL);

  fclose(pctx->output_file);
  pctx->output_file = NULL;

  for (uint32_t sidx = 0; sidx < pctx->all_loggers_.size(); sidx++) {
    delete pctx->all_loggers_[sidx];
  }

  return 0;
}

/* BEGIN Internal Definitions */
void* perfstats_worker(void* arg) {
  perfstats_ctx_t* pctx = static_cast<perfstats_ctx_t*>(arg);

  while (true) {
    pctx->worker_mtx.Lock();

    if (pctx->shutdown) {
      if (pctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: shutting down");
      }

      if (pctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: done");
      }

      pctx->worker_mtx.Unlock();
      break;
    }

    /* do logging */
    perfstats_log_once(pctx);

    pctx->worker_mtx.Unlock();
    usleep(1e6 / PERFSTATS_CAPTURE_FREQ);
  }

  return NULL;
}

int perfstats_log_stat(perfstats_ctx_t* pctx, Stat& s) {
  pctx->worker_mtx.AssertHeld();
  s.Serialize(pctx->output_file);

  return 0;
}

int perfstats_log_once(perfstats_ctx_t* pctx) {
  pctx->worker_mtx.AssertHeld();

  uint64_t timestamp = ::get_timestamp(pctx);
  perfstats_log_hooks(pctx, timestamp);

  /* construct with random parameters; TODO: add better constructor */
  Stat s(StatType::V_INT, "");

  for (uint32_t sidx = 0; sidx < pctx->all_loggers_.size(); sidx++) {
    pctx->all_loggers_[sidx]->LogOnce(timestamp, s);
    perfstats_log_stat(pctx, s);
  }

  return 0;
}

int perfstats_generate_header(perfstats_ctx_t* pctx) {
  const char* header_str = "Timestamp (ms),Stat Type, Stat Value\n";
  fwrite(header_str, strlen(header_str), 1, pctx->output_file);
  return 0;
}

int perfstats_log_hooks(perfstats_ctx_t* pctx, uint64_t timestamp) {
  pctx->worker_mtx.AssertHeld();

  const char* lbw_label = "LOGICAL_BYTES_WRITTEN";
  Stat logical_bytes_written(StatType::V_UINT64, lbw_label);
  logical_bytes_written.SetValue(timestamp, pctx->stat_hooks.bytes_written);

  perfstats_log_stat(pctx, logical_bytes_written);
  return 0;
}

int perfstats_log_reneg(perfstats_ctx_t* pctx, pivot_ctx_t* pvt_ctx,
                        reneg_ctx_t rctx) {
  uint64_t timestamp = ::get_timestamp(pctx);

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
                      pvt_ctx->oob_buffer.Size());

  massStat.SetValue(timestamp, buf);

  print_vector(buf, buf_sz, pvt_ctx->my_pivots, pvt_ctx->my_pivot_count,
               /* truncate */ false);
  pivotStat.SetValue(timestamp, buf);

  pctx->worker_mtx.Lock();
  perfstats_log_stat(pctx, massStat);
  perfstats_log_stat(pctx, pivotStat);
  pctx->worker_mtx.Unlock();

  return 0;
}

int perfstats_log_aggr_bin_count(perfstats_ctx_t* pctx, pivot_ctx_t* pvt_ctx,
                                 int my_rank) {
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

  uint64_t timestamp = get_timestamp(pctx);
  aggr_count.SetValue(timestamp, buf);

  const char* const kAggrStdLabel = "RENEG_AGGR_STD";
  Stat aggr_std(StatType::V_FLOAT, kAggrStdLabel);
  aggr_std.SetValue(timestamp, get_norm_std(recv_buf, bin_sz));

  pctx->worker_mtx.Lock();
  perfstats_log_stat(pctx, aggr_count);
  perfstats_log_stat(pctx, aggr_std);
  pctx->worker_mtx.Unlock();

  return rv;
}

int perfstats_log_mypivots(perfstats_ctx_t* pctx, float* pivots,
                           int num_pivots, const char *pivot_label) {
  int rv = 0;

  size_t buf_sz = STAT_BUF_MAX;
  char buf[buf_sz];
  print_vector(buf, buf_sz, pivots, num_pivots, false);

  uint64_t timestamp = get_timestamp(pctx);
  Stat pivot_stat(StatType::V_STR, pivot_label);
  pivot_stat.SetValue(timestamp, buf);

  pctx->worker_mtx.Lock();
  perfstats_log_stat(pctx, pivot_stat);
  pctx->worker_mtx.Unlock();

  return rv;
}

int perfstats_log_eventstr(perfstats_ctx_t* pctx, const char* event_label,
                           const char* event_desc) {
  int rv = 0;

  uint64_t timestamp = get_timestamp(pctx);

  Stat str_stat(StatType::V_STR, event_label);
  str_stat.SetValue(timestamp, event_desc);

  pctx->worker_mtx.Lock();
  perfstats_log_stat(pctx, str_stat);
  pctx->worker_mtx.Unlock();
  return rv;
}

int perfstats_printf(perfstats_ctx_t* pctx, int rank, const char* fmt, ...) {
  int rv = 0;
  uint64_t timestamp = get_timestamp(pctx);

  pctx->worker_mtx.Lock();

  va_list ap;
  va_start(ap, fmt);
  vfprintf(pctx->output_file, fmt, ap);
  va_end(ap);

  pctx->worker_mtx.Unlock();

  fprintf(stderr, "\n");

  return 0;
}
}  // namespace pdlfs
   /* END Internal Definitions */
