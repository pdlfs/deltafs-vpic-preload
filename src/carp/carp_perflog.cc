/*
 * carp_perflog.cc  perflog related carp routines
 */

#include <assert.h>
#include <mpi.h>

#include "carp.h"

#define PERFLOG_CAPTURE_FREQ 10 /* currently hardwired */

namespace pdlfs {
namespace carp {

/*
 * get_timestamp: generate a timestamp relative to a start time
 */
static uint64_t get_timestamp(struct timespec* start_time) {
  struct timespec stat_time;
  clock_gettime(CLOCK_MONOTONIC, &stat_time);

  uint64_t time_delta_ms = (stat_time.tv_sec - start_time->tv_sec) * 1e3 +
                           (stat_time.tv_nsec - start_time->tv_nsec) / 1e6;

  return time_delta_ms;
}

/*
 * get_norm_std: generate std of uint64 array
 */
static float get_norm_std(uint64_t* arr, size_t sz) {
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

/*
 * Carp::PerflogStartup: init perflog (called from carp ctor)
 */
void Carp::PerflogStartup() {
  std::string logfile;
  char rankstr[16];
  FILE* fp;
  int rv;

  assert(options_.enable_perflog && options_.log_home != NULL);
  snprintf(rankstr, sizeof(rankstr), "%d", options_.my_rank);
  logfile = options_.log_home;
  logfile = logfile + "/vpic-perfstats.log." + rankstr;

  fp = fopen(logfile.c_str(), "w");
  if (fp == NULL) {
    flog(LOG_ERRO, "PerflogStartup: fopen failed: %s", logfile.c_str());
    flog(LOG_ERRO, "PerflogStartup: log disabled");
    return;
  }
  setvbuf(fp, NULL, _IOLBF, 0);

  perflog_.fp = fp;
  rv = pthread_create(&perflog_.thread, NULL, Carp::PerflogMain, this);
  if (rv) {
    perflog_.fp = NULL;
    fprintf(fp, "PerflogStartup: failed to create pthread!\n");
    flog(LOG_ERRO, "PerflogStartup: failed to create pthread!");
    return;
  }
}

/*
 * Carp::PerflogMain: main routine of perflog thread
 */
void* Carp::PerflogMain(void* arg) {
  Carp* carp = static_cast<Carp*>(arg);
  FILE* sfp = carp->perflog_.fp; /* safe local copy of fp, use for I/O */
  uint64_t timestamp;

  carp->perflog_.mtx.Lock();
  if (carp->perflog_.fp) /* to be safe */
    fprintf(sfp, "Timestamp (ms),Stat Type, Stat Value\n");

  while (1) {
    uint64_t bewrbytes;
    if (carp->perflog_.fp == NULL) /* signals us to shutdown */
      break;

    timestamp = get_timestamp(&carp->start_time_);
    carp->mutex_.Lock();
    bewrbytes = carp->backend_wr_bytes_;
    carp->mutex_.Unlock();
    fprintf(sfp, "%lu,LOGICAL_BYTES_WRITTEN,%lu\n", timestamp, bewrbytes);

    carp->perflog_.mtx.Unlock(); /* drop lock during sleep */
    usleep(1e6 / PERFLOG_CAPTURE_FREQ);
    carp->perflog_.mtx.Lock();
  }

  carp->perflog_.mtx.Unlock();
  if (carp->options_.my_rank == 0)
    flog(LOG_INFO, "perfstats_worker: done, shutting down");

  return NULL;
}

/*
 * Carp::PerflogDestroy: stop perflog if running.  called from
 * Carp dtor and sets perflog_.fp to NULL (so all perflog calls
 * should be complete before you distruct the carp object).
 */
void Carp::PerflogDestroy() {
  FILE* fp = perflog_.fp;

  if (fp) {
    perflog_.fp = NULL;                  /* tell worker to shutdown now */
    pthread_join(perflog_.thread, NULL); /* wait for shutdown to complete */
    fclose(fp);
  }
}

/*
 * Carp::PerflogReneg: log a reneg operation
 */
void Carp::PerflogReneg(int round_num) {
  uint64_t timestamp = get_timestamp(&start_time_);
  MutexLock ml(&perflog_.mtx);
  mutex_.AssertHeld();    /* accessing carp fields */
  assert(perflog_.fp);

  fprintf(perflog_.fp, "%lu,RENEG_COUNTS,RANK%d_R%d: ", timestamp,
          options_.my_rank, round_num);

  int rankcntsz = bins_.Size();
  for (int i = 0; i < rankcntsz; i++) {
    fprintf(perflog_.fp, "%" PRIu64 " ", bins_.Weight(i));
  }
  fprintf(perflog_.fp, ": OOB (%zu)\n", this->OobSize());
}

/*
 * Carp::PerflogAggrBinCount: log bin count (a MPI collective call)
 */
void Carp::PerflogAggrBinCount() {
  size_t rankcntaggrsz = bins_.Size();
  uint64_t send_buf[rankcntaggrsz], recv_buf[rankcntaggrsz];

  assert(perflog_.fp);
  for (size_t i = 0 ; i < rankcntaggrsz ; i++) {
    send_buf[i] = bins_.AggrCount(i);
  }
  MPI_Reduce(send_buf, recv_buf, rankcntaggrsz, MPI_UNSIGNED_LONG_LONG, MPI_SUM,
             0, MPI_COMM_WORLD); /* XXX: snd directly from rank_counts_aggr_? */
  if (options_.my_rank != 0) return;

  uint64_t timestamp = get_timestamp(&start_time_);
  float aggr_std_val = get_norm_std(recv_buf, rankcntaggrsz);

  perflog_.mtx.Lock();

  fprintf(perflog_.fp, "%lu,RENEG_AGGR_BINCNT,", timestamp);
  for (size_t i = 0; i < rankcntaggrsz; i++) {
    fprintf(perflog_.fp, "%lu ", recv_buf[i]);
  }
  fprintf(perflog_.fp, "\n");
  fprintf(perflog_.fp, "%lu,RENEG_AGGR_STD,%.2f\n", timestamp, aggr_std_val);

  perflog_.mtx.Unlock();

  if (options_.my_rank == 0) {
    flog(LOG_INFO, "[perfstats] normalized load stddev: %.3f", aggr_std_val);
  }
}

/*
 * Carp::PerflogMyPivots: log my pivots
 */
void Carp::PerflogMyPivots(Pivots* pivots, const char* lab) {
  uint64_t timestamp = get_timestamp(&start_time_);
  MutexLock ml(&perflog_.mtx);

  assert(perflog_.fp);
  fprintf(perflog_.fp, "%lu,%s,", timestamp, lab);
  for (size_t i = 0; i < pivots->Size(); i++) {
    fprintf(perflog_.fp, "%.4lf ", (*pivots)[i]);
  }
  fprintf(perflog_.fp, "\n");
}

/*
 * Carp::PerflogPivots: logs pivots and rank_count_aggr_
 */
void Carp::PerflogPivots(Pivots &pivots) {
  uint64_t timestamp = get_timestamp(&start_time_);
  MutexLock ml(&perflog_.mtx);
  assert(perflog_.fp);
  mutex_.AssertHeld();    /* accessing carp fields */

  fprintf(perflog_.fp, "%lu,RENEG_PIVOTS_E%d,", timestamp, epoch_);
  for (size_t i = 0 ; i < pivots.Size() ; i++) {
    fprintf(perflog_.fp, "%.4lf ", pivots[i]);
  }
  fprintf(perflog_.fp, "\n");

  int rankcntaggrsz = bins_.Size();
  fprintf(perflog_.fp, "%lu,RENEG_BINCNT_E%d,", timestamp, epoch_);
  for (int i = 0 ; i < rankcntaggrsz ; i++) {
    fprintf(perflog_.fp, "%lu ", bins_.AggrCount(i));
  }
  fprintf(perflog_.fp, "\n");
}

/*
 * Carp::PerflogVPrintf: vprintf to perflog
 */
void Carp::PerflogVPrintf(const char* fmt, va_list ap) {
  MutexLock ml(&perflog_.mtx);

  assert(perflog_.fp);
  vfprintf(perflog_.fp, fmt, ap);
  fprintf(perflog_.fp, "\n");
}

} /* namespace carp */
} /* namespace pdlfs */
