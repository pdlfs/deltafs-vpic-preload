#include "perfstats/perfstats.h"

#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "perfstats/stat.h"

#ifdef PRELOAD_HAS_BLKID
#include "perfstats/stat_blkid.h"
#endif

namespace pdlfs {

/* Internal Declarations */
void *perfstats_worker(void *arg);

int perfstats_log_once(perfstats_ctx_t *pctx);

int perfstats_log_stat(perfstats_ctx_t *pctx, Stat &s);

int perfstats_log_hooks(perfstats_ctx_t *pctx, uint64_t timestamp);

int perfstats_generate_header(perfstats_ctx_t *pctx);

int perfstats_flush(perfstats_ctx_t *pctx);

int perfstats_init(perfstats_ctx_t *pctx, int my_rank, const char *dir_path,
                   const char *local_root) {
  assert(pctx != NULL);

  pctx->my_rank = my_rank;
  pctx->shutdown = false;
  int rv = 0;

  clock_gettime(CLOCK_MONOTONIC, &(pctx->start_time));

  snprintf(pctx->stats_fpath, PATH_MAX, "%s/vpic-perfstats.log.%d", dir_path,
           my_rank);
  FILE *output_file = fopen(pctx->stats_fpath, "w");

  if (output_file == NULL) {
    rv = -1;
    logf(LOG_ERRO, "perfstats_init: failed to open file");
    return rv;
  }

  pctx->output_file = output_file;
  pctx->buffered_stats_.reserve(kStatArrSize);

  perfstats_generate_header(pctx);

  rv = pthread_create(&(pctx->stats_thread), NULL, perfstats_worker,
                      static_cast<void *>(pctx));

#ifdef PRELOAD_HAS_BLKID
  StatBlkid *statBlkid = new StatBlkid(local_root);
  pctx->all_loggers_.push_back(statBlkid);
#endif

  if (rv) {
    logf(LOG_ERRO, "perfstats_init: failed to create pthread");
    return rv;
  }

  return 0;
}

int perfstats_destroy(perfstats_ctx_t *pctx) {
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
void *perfstats_worker(void *arg) {
  perfstats_ctx_t *pctx = static_cast<perfstats_ctx_t *>(arg);

  while (true) {
    pctx->worker_mtx.Lock();

    if (pctx->shutdown) {
      if (pctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: shutting down");
      }

      perfstats_flush(pctx);
      break;
    }

    /* do logging */
    perfstats_log_once(pctx);

    pctx->worker_mtx.Unlock();
    usleep(1e6 / PERFSTATS_CAPTURE_FREQ);
  }

  return NULL;
}

int perfstats_log_stat(perfstats_ctx_t *pctx, Stat &s) {
  pctx->worker_mtx.AssertHeld();

  pctx->buffered_stats_.push_back(s);
  if (pctx->buffered_stats_.size() >= kStatArrSize) {
    perfstats_flush(pctx);
  }
  return 0;
}

int perfstats_log_once(perfstats_ctx_t *pctx) {
  pctx->worker_mtx.AssertHeld();

  struct timespec stat_time;
  clock_gettime(CLOCK_MONOTONIC, &(stat_time));

  uint64_t time_delta_ms = (stat_time.tv_sec - pctx->start_time.tv_sec) * 1e3 +
                           (stat_time.tv_nsec - pctx->start_time.tv_nsec) / 1e6;

  perfstats_log_hooks(pctx, time_delta_ms);
  /* construct with random parameters; TODO: add better constructor */
  Stat s(StatType::V_INT, "");

  for (uint32_t sidx = 0; sidx < pctx->all_loggers_.size(); sidx++) {
    pctx->all_loggers_[sidx]->LogOnce(time_delta_ms, s);
    perfstats_log_stat(pctx, s);
  }

  return 0;
}

int perfstats_generate_header(perfstats_ctx_t *pctx) {
  const char *header_str = "Timestamp (ms),Stat Type, Stat Value";
  fwrite(header_str, strlen(header_str), 1, pctx->output_file);
  return 0;
}

int perfstats_flush(perfstats_ctx_t *pctx) {
  pctx->worker_mtx.AssertHeld();

  for (uint32_t sidx = 0; sidx < pctx->buffered_stats_.size(); sidx++) {
    pctx->buffered_stats_[sidx].Serialize(pctx->output_file);
  }

  pctx->buffered_stats_.clear();
  return 0;
}

int perfstats_log_hooks(perfstats_ctx_t *pctx, uint64_t timestamp) {
  pctx->worker_mtx.AssertHeld();

  const char *lbw_label = "LOGICAL_BYTES_WRITTEN";
  Stat logical_bytes_written(StatType::V_UINT64, lbw_label);
  logical_bytes_written.SetValue(timestamp, pctx->stat_hooks.bytes_written);

  perfstats_log_stat(pctx, logical_bytes_written);
  return 0;
}

int perfstats_log_reneg(perfstats_ctx_t *pctx, pivot_ctx_t *pvt_ctx,
                        reneg_ctx_t rctx) {
  struct timespec stat_time;
  clock_gettime(CLOCK_MONOTONIC, &(stat_time));

  uint64_t time_delta_ms = (stat_time.tv_sec - pctx->start_time.tv_sec) * 1e3 +
                           (stat_time.tv_nsec - pctx->start_time.tv_nsec) / 1e6;

  const char* const kRenegLabel = "RENEG_BEGIN";
  Stat s(StatType::V_STR, kRenegLabel);

  int buf_sz = STAT_BUF_MAX, buf_idx = 0;
  char buf[buf_sz];

  buf_idx += snprintf(buf + buf_idx, buf_sz - buf_idx,
                      "RANK%d_R%d: ", rctx->my_rank, rctx->round_num);

  std::vector<float> &counts = pvt_ctx->rank_bin_count;
  buf_idx +=
      print_vector(buf + buf_idx, buf_sz - buf_idx, counts, counts.size(),
                   /* truncate */ false);

  logf(LOG_DBG2, "[Perfstats] %s\n", buf);

  s.SetValue(time_delta_ms, buf);

  pctx->worker_mtx.Lock();
  perfstats_log_stat(pctx, s);
  pctx->worker_mtx.Unlock();

  return 0;
}
}  // namespace pdlfs
/* END Internal Definitions */
