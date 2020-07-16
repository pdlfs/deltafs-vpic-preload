#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef PRELOAD_HAS_BLKID
#include <blkid/blkid.h>
#endif

#include "preload_perfstats.h"

static int stat_counter;

/* BEGIN Internal Declarations */
int get_sysfs_path_for_bd(const char *dev_name, char *sys_path_buf,
                          int sys_path_buf_len);

void *perfstats_worker(void *arg);

int perfstats_log_once(perfstats_ctx_t *pctx, struct perfstats_stats &stats);

int perfstats_generate_header(perfstats_ctx_t *pctx);

int perfstats_serialize_stat(perfstats_ctx_t *pctx, perfstats_stats_t &stat,
                             char *buf, int buf_len);

int perfstats_flush(perfstats_ctx_t *pctx);
/* END Internal Declarations */

int get_sysfs_path_for_bd(const char *dev_name, char *sys_path_buf,
                          int sys_path_buf_len) {
  if (dev_name == NULL) {
    return -1;
  }

  int devname_len = strlen(dev_name);
  const int devname_len_max = 64;

  if (devname_len >= devname_len_max) {
    return -1;
  }

  char part_name[devname_len_max];
  sscanf(dev_name, "/dev/%s", part_name);

  snprintf(sys_path_buf, sys_path_buf_len, "/sys/class/block/%s/stat",
           part_name);

  return 0;
}

int get_stats(const char *sysfs_path, bd_stats_t &bds) {
  if (sysfs_path == NULL) {
    return -1;
  }

  FILE *sys_fp;
  if ((sys_fp = fopen(sysfs_path, "r")) == NULL) {
    return -1;
  }

  int num_scanned =
      fscanf(sys_fp, "%lu %lu %lu %lu %lu %lu %lu %lu %u %u %u", 
          &bds.rd_ios, &bds.rd_merges, &bds.rd_secs, &bds.rd_ticks, 
          &bds.wr_ios, &bds.wr_merges, &bds.wr_secs, &bds.wr_ticks, 
          &bds.in_flight, &bds.io_ticks, &bds.time_queue);

  fclose(sys_fp);

  if (num_scanned != 11) {
    /* Extended statistics not supported */
    return -1;
  }

  return 0;
}

int init_blkid_stats(const char *write_path, char *sysfs_path) {
  struct stat s;

  if (lstat(write_path, &s)) {
    printf("Unable to stat\n");
    return EXIT_FAILURE;
  }

  dev_t dev_id = s.st_dev;

  char *dev_name = blkid_devno_to_devname(dev_id);
  if (dev_name == NULL) {
    return -1;
  }

  int rv = get_sysfs_path_for_bd(dev_name, sysfs_path, PATH_MAX);

  return rv;
}

int perfstats_init(perfstats_ctx_t *pctx, int my_rank, const char *dir_path,
                   const char *local_root) {
  assert(pctx != NULL);

  pctx->my_rank = my_rank;
  pctx->stats_idx = 0;
  pctx->shutdown = false;

  clock_gettime(CLOCK_MONOTONIC, &(pctx->start_time));

  int rv = 0;

  snprintf(pctx->stats_fpath, PATH_MAX, "%s/vpic-perfstats.log.%d", dir_path,
           my_rank);
  FILE *output_file = fopen(pctx->stats_fpath, "w");

  if (output_file == NULL) {
    rv = -1;
    logf(LOG_ERRO, "perfstats_init: failed to open file");
    return rv;
  }

#ifdef PRELOAD_HAS_BLKID
  rv = init_blkid_stats(local_root, pctx->sysfs_path);

  if (rv) {
    logf(LOG_ERRO,
         "BLKID enabled but failed to initialize. Continuing anyway.\n");
  } else {
    if (pctx->my_rank == 0) {
      logf(LOG_INFO, "[Perfstats] Local_root matched to sysfs: %s\n",
          pctx->sysfs_path);
    }

    bd_stats_t bds_test;
    rv = get_stats(pctx->sysfs_path, bds_test);
    if (!rv) pctx->sysfs_enabled = true;
  }
#endif

  pctx->output_file = output_file;

  perfstats_generate_header(pctx);

  rv = pthread_create(&(pctx->stats_thread), NULL, perfstats_worker,
                      static_cast<void *>(pctx));

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

  return 0;
}

/* BEGIN Internal Definitions */
void *perfstats_worker(void *arg) {
  perfstats_ctx_t *pctx = static_cast<perfstats_ctx_t *>(arg);

  while (true) {
    pthread_mutex_lock(&(pctx->worker_mutex));
    if (pctx->shutdown) {
      if (pctx->my_rank == 0) {
        logf(LOG_INFO, "perfstats_worker: shutting down");
      }

      perfstats_flush(pctx);
      pctx->stats_idx = 0;

      break;
    }

    if (pctx->stats_idx == PERFSTATS_MEM_SIZE) {
      perfstats_flush(pctx);
      pctx->stats_idx = 0;
    }

    /* do logging */
    perfstats_log_once(pctx, pctx->stats[pctx->stats_idx]);
    pctx->stats_idx++;

    pthread_mutex_unlock(&(pctx->worker_mutex));
    usleep(1e6 / PERFSTATS_CAPTURE_FREQ);
  }

  return NULL;
}

int perfstats_log_once(perfstats_ctx_t *pctx, struct perfstats_stats &stats) {
  clock_gettime(CLOCK_MONOTONIC, &(stats.stat_time));

  stats.bytes_written = pctx->prop_bytes_written;

  if (pctx->sysfs_enabled) {
    bd_stats_t bd_stats;
    int rv = get_stats(pctx->sysfs_path, bd_stats);
    stats.secs_written = bd_stats.wr_secs;
  }
}

int perfstats_generate_header(perfstats_ctx_t *pctx) {
  const char *header_str;

  if (pctx->sysfs_enabled) {
    header_str = "Timestamp (ms),Logical Bytes Written,Disk Sectors Written\n";
  } else {
      header_str = "Timestamp (ms),Logical Bytes Written\n";
  }

  fwrite(header_str, strlen(header_str), 1, pctx->output_file);
}

int perfstats_serialize_stat(perfstats_ctx_t *pctx, perfstats_stats_t &stat,
                             char *buf, int buf_len) {
  /* lock->assertheld */
  int rv = 0;

  long long time_delta_ms =
      (stat.stat_time.tv_sec - pctx->start_time.tv_sec) * 1e3 +
      (stat.stat_time.tv_nsec - pctx->start_time.tv_nsec) / 1e6;

  if (pctx->sysfs_enabled) {
    rv = snprintf(buf, buf_len, "%lld,%lld,%lld\n", 
        time_delta_ms, stat.bytes_written, stat.secs_written);
  } else {
    rv = snprintf(buf, buf_len, "%lld,%lld\n", time_delta_ms, stat.bytes_written);
  }

  return rv;
}

int perfstats_flush(perfstats_ctx_t *pctx) {
  /* lock->assertheld */
  int ntoflush = pctx->stats_idx;
  assert(ntoflush <= PERFSTATS_MEM_SIZE);

  for (int sidx = 0; sidx < ntoflush; sidx++) {
    char buf[1024];
    int nbytes = perfstats_serialize_stat(pctx, pctx->stats[sidx], buf, 1024);
    fwrite(buf, nbytes, 1, pctx->output_file);
  }
}
/* END Internal Definitions */
