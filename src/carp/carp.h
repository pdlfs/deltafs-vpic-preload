//
// Created by Ankush J on 3/5/21.
//

#pragma once


#include <pdlfs-common/env.h>
#include <pdlfs-common/mutexlock.h>
#include <pdlfs-common/status.h>

#include "../preload_shuffle.h"

#include "carp_utils.h"
#include "oob_buffer.h"
#include "policy.h"
#include "range_common.h"
#include "shuffle_write_range.h"

/* XXX: temporarily for range-utils, refactor ultimately */
#include "rtp.h"

namespace pdlfs {
namespace carp {

/*
 * CarpOptions: used to init carp via the Carp constructor.  allocated
 * and loaded by preload_init_carpopts() -- see that function for default
 * values.  for preload config via environment variables, the variable
 * name is listed below.
 */
struct CarpOptions {
  int index_attr_size;       /* sizeof indexed attr, default=sizeof(float) */
                             /* note: currenly only float is supported */
                             /* (PRELOAD_Particle_indexed_attr_size) */
  int index_attr_offset;     /* offset in particle buf of indexed attr */
                             /* default: 0 */
                             /* (PRELOAD_Particle_indexed_attr_offset) */
  uint32_t oob_sz;           /* max #particles in oob buf (RANGE_Oob_size) */
  const char* reneg_policy;  /* InvocationDynamic, InvocationPeriodic (def), */
                             /* InvocationOnce. (RANGE_Reneg_policy) */
  uint64_t reneg_intvl;      /* periodic: reneg every reneg_intvl writes */
                             /*   (RANGE_Reneg_interval) */
  uint32_t dynamic_intvl;    /* stat trig: invoke every dynamic_intvl calls */
                             /*   (RANGE_Reneg_interval) */
  float dynamic_thresh;      /* stat trig: evaltrig if load_skew > trigger */
                             /*   (RANGE_Dynamic_threshold) */
  int rtp_pvtcnt[4];         /* # of RTP pivots gen'd by each stage */
                             /* RANGE_Pvtcnt_s{1,2,3} */
  Env* env;                  /* stat: for GetFileSize() in StatFiles() */
                             /* normally set to Env::Default() */
  shuffle_ctx_t* sctx;       /* shuffle context */
  uint32_t my_rank;          /* my MPI rank */
  uint32_t num_ranks;        /* MPI world size */
  int enable_perflog;        /* non-zero to enable perflog */
  const char *log_home;      /* where to put perflog (if enabled) */
  std::string mount_path;    /* mount_path (set from preload MPI_Init) */
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
        backend_wr_bytes_(0),
        rtp_(this, options_),
        epoch_(0),
        cv_(&mutex_),
        range_min_(0),
        range_max_(0),
        rank_bins_(options_.num_ranks + 1, 0),
        rank_counts_(options_.num_ranks, 0),
        rank_counts_aggr_(options_.num_ranks, 0),
        oob_buffer_(options.oob_sz),
        my_pivot_count_(0),
        my_pivot_width_(0),
        policy_(nullptr) {
    MutexLock ml(&mutex_);
    // necessary to set mts_state_ to READY
    Reset();
    clock_gettime(CLOCK_MONOTONIC, &start_time_);
    perflog_.fp = NULL;

#define POLICY_IS(s) (strncmp(options_.reneg_policy, s, strlen(s)) == 0)

    if (options_.reneg_policy == nullptr) {
      policy_ = new InvocationPeriodic(*this, options_);
    } else if(POLICY_IS("InvocationDynamic")) {
      policy_ = new InvocationDynamic(*this, options_);
    } else if (POLICY_IS("InvocationPeriodic")) {
      policy_ = new InvocationPeriodic(*this, options_);
    } else if (POLICY_IS("InvocationOnce")) {
      policy_ = new InvocationOnce(*this, options_);
    } else {
      policy_ = new InvocationPeriodic(*this, options_);
    }

    if (options_.enable_perflog) {
      this->PerflogStartup();
    }
  }

  ~Carp() {
    PivotUtils::LogPivots(this, options_.rtp_pvtcnt[1]);
    this->PerflogDestroy();
  }

  Status Serialize(const char* fname, unsigned char fname_len, char* data,
                   unsigned char data_len, unsigned char extra_data_len,
                   particle_mem_t& p);

  Status AttemptBuffer(particle_mem_t& p, bool& shuffle, bool& flush);

  /* AdvanceEpoch is also called before the first epoch,
   * so it can't be assumed that the first epoch is e0
   */
  void AdvanceEpoch() {
    MutexLock ml(&mutex_);
    MainThreadState cur_state = mts_mgr_.GetState();
    assert(cur_state != MainThreadState::MT_BLOCK);
    policy_->AdvanceEpoch();
    epoch_++;
  }

  OobFlushIterator OobIterator() { return OobFlushIterator(oob_buffer_); }

  size_t OobSize() const { return oob_buffer_.Size(); }

  void UpdateState(MainThreadState new_state) {
    mutex_.AssertHeld();
    mts_mgr_.UpdateState(new_state);
  }

  MainThreadState GetCurState() {
    mutex_.AssertHeld();
    return mts_mgr_.GetState();
  }

  MainThreadState GetPrevState() {
    mutex_.AssertHeld();
    return mts_mgr_.GetPrevState();
  }

  Status HandleMessage(void* buf, unsigned int bufsz, int src, uint32_t type) {
    return rtp_.HandleMessage(buf, bufsz, src, type);
  }

  int NumRounds() const { return rtp_.NumRounds(); }

  void BackendWriteCounter(uint64_t b) {
    backend_wr_bytes_ += b;  /* stat cnt, XXX assume ok w/o lock */
  }

  void LogReneg(int round_num) {
    if (PerflogOn()) PerflogReneg(round_num);
  }
  void LogAggrBinCount() {
    if (PerflogOn()) PerflogAggrBinCount();
  }
  void LogMyPivots(double *pivots, int num_pivots, const char *lab) {
    if (PerflogOn()) PerflogMyPivots(pivots, num_pivots, lab);
  }
  void LogVec(std::vector<uint64_t>& vec, const char *vlabel) {
    if (PerflogOn()) PerflogVec(vec, vlabel);
  }
  void LogPrintf(const char* fmt, ...) {
    if (PerflogOn()) {
      va_list ap;
      va_start(ap, fmt);
      PerflogVPrintf(fmt, ap);
      va_end(ap);
    }
  }

 private:
  void AssignShuffleTarget(particle_mem_t& p) {
    int dest_rank = policy_->ComputeShuffleTarget(p);
    if (dest_rank >= 0 and dest_rank < options_.num_ranks) {
      p.shuffle_dest = dest_rank;
      rank_counts_[dest_rank]++;
      rank_counts_aggr_[dest_rank]++;
    } else {
      p.shuffle_dest = -1;
    }
  }

  void MarkFlushableBufferedItems() {
    std::vector<particle_mem_t>& oob_vec = oob_buffer_.buf_;
    for (size_t i = 0; i < oob_vec.size(); i++) {
      AssignShuffleTarget(oob_vec[i]);
    }
  }

  /* Not called directly - up to the invocation policy */
  void Reset() {
    mutex_.AssertHeld();
    mts_mgr_.Reset();
    range_min_ = 0;
    range_max_ = 0;
    std::fill(rank_counts_.begin(), rank_counts_.end(), 0);
    std::fill(rank_counts_aggr_.begin(), rank_counts_aggr_.end(), 0);
    oob_buffer_.Reset();
  }

  void PerflogStartup();
  static void *PerflogMain(void *arg);
  void PerflogDestroy();
  int PerflogOn() { return perflog_.fp != NULL; }
  void PerflogReneg(int round_num);
  void PerflogAggrBinCount();
  void PerflogMyPivots(double *pivots, int num_pivots, const char *lab);
  void PerflogVec(std::vector<uint64_t>& vec, const char *vlabel);
  void PerflogVPrintf(const char* fmt, va_list ap);

 private:
  const CarpOptions& options_;    /* configuration options */
  MainThreadStateMgr mts_mgr_;
  struct timespec start_time_;    /* time carp object was created */
  uint64_t backend_wr_bytes_;     /* total #bytes written to backend */

  struct perflog_state {
    port::Mutex mtx;              /* mutex for perfstats */
    FILE *fp;                     /* open FILE we write stats to */
    pthread_t thread;             /* profiling thread making periodic output */
  } perflog_;

  RTP rtp_;
  int epoch_;

 public:
  /* XXX: temporary, refactor RTP/perfstats as friend classes */
  port::Mutex mutex_;
  port::CondVar cv_;

  double range_min_;
  double range_max_;

  std::vector<float> rank_bins_;
  std::vector<float> rank_counts_;
  std::vector<uint64_t> rank_counts_aggr_;

  OobBuffer oob_buffer_;

  double my_pivots_[pdlfs::kMaxPivots];
  size_t my_pivot_count_;
  double my_pivot_width_;

 private:
  friend class InvocationPolicy;
  InvocationPolicy* policy_;

  friend class PivotUtils;
};
}  // namespace carp
}  // namespace pdlfs
