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
 *
 * the reneg_policy controls when we start a reneg.  the options are:
 *   InvocationPeriodic: trigger if oob full on any rank or if rank 0
 *                       has written reneg_intvl times in the current epoch.
 *                       this is the default policy.
 *
 *   InvocationDynamic: trigger if oob full or the stat_trigger fires.
 *                      the stat trigger is invoked on rank 0 every
 *                      dynamic_intvl writes.  when it fires it serially
 *                      stats the backing files to compute a skew.
 *                      if the skew is > dynamic_thresh, then rank 0
 *                      will trigger a reneg.
 *
 *   InvocationOnce: rank 0 triggers the first time its oob is full
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
                             /* RANGE_Pvtcnt_s{1,2,3}; pvtcnt[0] not used */
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
    mutex_.Lock();
    LogPivots(options_.rtp_pvtcnt[1]);  /* expects mutex_ to be held */
    mutex_.Unlock();
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

  Status HandleMessage(void* buf, unsigned int bufsz, int src, uint32_t type) {
    return rtp_.HandleMessage(buf, bufsz, src, type);
  }

  int NumRounds() const { return rtp_.NumRounds(); }

  /*
   * BackendWriteCounter is called from preload_write() to track
   * the number of bytes written to the backend so it can be reported
   * as a stat (e.g. by the perflog profile thread).  We need this
   * hook for preload_write() as it does not make any calls to carp
   * (it passes data directly to the backend).  Caller should not
   * be holding mutex_, as we grab it.
   */
  void BackendWriteCounter(uint64_t b) {
    MutexLock ml(&mutex_);
    backend_wr_bytes_ += b;
  }

  /*
   * Top-level Log*() perflog functions.  Centralizes checking
   * if perflog is on and if so, pass control to the corresponding
   * Perflog*() function.
   */

  /* called from HandlePivotBroadcast() w/caller holding mutex_ */
  void LogReneg(int round_num) {
    if (PerflogOn()) PerflogReneg(round_num);
  }

  /* collective call at shutdown from preload MPI_Finalize() wrapper */
  void LogAggrBinCount() {
    if (PerflogOn()) PerflogAggrBinCount();
  }

  /* called from UpdatePivots w/caller holding mutex_ */
  void LogMyPivots(double *pivots, int num_pivots, const char *lab) {
    if (PerflogOn()) PerflogMyPivots(pivots, num_pivots, lab);
  }

  /* LogPivots logs both my_pivots_ and rank_counts_aggr_ */
  /* called from HandleBegin w/caller holding mutex_ */
  /* also called from carp dtor during shutdown */
  void LogPivots(int pvtcnt) {
    if (PerflogOn()) PerflogPivots(pvtcnt);
  }

  /* called from HandleBegin and HandlePivots */
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

  /* internal (private) perflog rountes (only call if perlog enabled) */
  void PerflogStartup();                           // open perflog
  static void *PerflogMain(void *arg);             // periodic thread main
  void PerflogDestroy();                           // shut perflog down
  int PerflogOn() { return perflog_.fp != NULL; }  // is perflog on?
  /* internal interface for top-level Log* calls, see above */
  void PerflogReneg(int round_num);
  void PerflogAggrBinCount();
  void PerflogMyPivots(double *pivots, int num_pivots, const char *lab);
  void PerflogPivots(int pvtcnt);
  void PerflogVPrintf(const char* fmt, va_list ap);

 private:
  /* these values do not change after ctor */
  const CarpOptions& options_;    /* ref to config options from ctor */
  struct timespec start_time_;    /* time carp object was created */

  /* protected by mutex_ */
  MainThreadStateMgr mts_mgr_;    /* to block shuffle write during reneg */
  uint64_t backend_wr_bytes_;     /* total #bytes written to backend */
  int epoch_;                     /* current epoch#, update w/AdvanceEpoch() */

  struct perflog_state {
    /*
     * perflog is only enabled if the enable_perflog option is set.
     * if enabled, perflog creates a profiling thread to generate periodic
     * output.  the profiling thread exits when fp is set to null by dtor.
     */
    FILE *fp;                     /* open log: only updated by ctor/dtor */
    port::Mutex mtx;              /* serialize threads writing to fp */
    pthread_t thread;             /* profiling thread making periodic output */
  } perflog_;

  RTP rtp_;

 public:
  /* XXX: temporary, refactor RTP as friend class */
  port::Mutex mutex_;             /* protects fields in Carp class */
  port::CondVar cv_;              /* tied to mutex_ (above).  RTP InitRound */
                                  /* uses cv_ to wait for MT_READY state */

  /* XXX: redundant with OOB range_min_/range_max_?  do we need both? */
  double range_min_;
  double range_max_;

  /* protected by mutex_ */
  /*
   * note: rank "r"'s bin range starts at rank_bins_[r] (inclusive)
   * and ends at rank_bins[r+1] (exclusive).  points less than
   * rank_bins_[0] or greater than rank_bins_[nranks] are out of
   * bounds.  for bootstrapping, all values of rank_bins_[] are set to 0
   * putting everything out of bounds.
   *
   * each time we assign a particle to rank "r" we increment both
   * rank_counts_[r] and rank_counts_aggr_[r].  rank_counts_[]
   * is reset to zero by UpdatePivots(), rank_counts_aggr_[] is not.
   *
   * rank_bins_[] and rank_counts_[] are used by the reneg protocol
   * to calculate a new set of pivots.  rank_counts_aggr_[] is only
   * used for perflog reporting (not used in reneg protocol).
   */
  std::vector<float> rank_bins_;            /* defines each rank's bin range */
  std::vector<float> rank_counts_;          /* cnt #times we assign to rank */
  std::vector<uint64_t> rank_counts_aggr_;  /* cnt, !cleared by pivot upd */

  OobBuffer oob_buffer_;                    /* out of bounds data */

  double my_pivots_[pdlfs::kMaxPivots];     /* my reneg calculated pivots */
  size_t my_pivot_count_;                   /* pivot size ( <= kMaxPivots) */
  double my_pivot_width_;

 private:
  friend class InvocationPolicy;
  InvocationPolicy* policy_;

  friend class PivotUtils;
};
}  // namespace carp
}  // namespace pdlfs
