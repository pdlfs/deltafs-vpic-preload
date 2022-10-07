//
// Created by Ankush J on 3/5/21.
//

#pragma once


#include <pdlfs-common/env.h>
#include <pdlfs-common/mutexlock.h>
#include <pdlfs-common/status.h>

#include "../preload_shuffle.h"

#include "carp_containers.h"
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
 *
 * InvocationInterEpoch: trigger once every reneg_intvl epochs.
 * we buffer data in OOB buffer until the first reneg of the epoch.
 * only triggers on rank 0.  this is the default policy.
 *
 * InvocationIntraEpoch: trigger every reneg_intvl writes OR if OOB full.
 */
struct CarpOptions {
  int index_attr_size;      /* sizeof indexed attr, default=sizeof(float) */
                            /* note: currenly only float is supported */
                            /* (PRELOAD_Particle_indexed_attr_size) */
  int index_attr_offset;    /* offset in particle buf of indexed attr */
                            /* default: 0 */
                            /* (PRELOAD_Particle_indexed_attr_offset) */
  uint32_t oob_sz;          /* max #particles in oob buf (RANGE_Oob_size) */
  const char* reneg_policy; /* InvocationIntraEpoch, */
                            /* InvocationInterEpoch (def) */
                            /*   (RANGE_Reneg_policy) */
  uint64_t reneg_intvl;     /* periodic: reneg every reneg_intvl writes */
                            /*   (RANGE_Reneg_interval) */
  int rtp_pvtcnt[4];        /* # of RTP pivots gen'd by each stage */
                            /* RANGE_Pvtcnt_s{1,2,3} */
  Env* env;                 /* stat: for GetFileSize() in StatFiles() */
                            /* normally set to Env::Default() */
  shuffle_ctx_t* sctx;      /* shuffle context */
  uint32_t my_rank;         /* my MPI rank */
  uint32_t num_ranks;       /* MPI world size */
  int enable_perflog;       /* non-zero to enable perflog */
  const char* log_home;     /* where to put perflog (if enabled) */
  std::string mount_path;   /* mount_path (set from preload MPI_Init) */
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
        backend_wr_bytes_(0),
        rtp_(this, options_),
        epoch_(0),
        cv_(&mutex_),
        bins_(options.num_ranks),
        oob_buffer_(options.oob_sz),
        pivots_(options.num_ranks),
        policy_(nullptr) {
    MutexLock ml(&mutex_);
    // necessary to set mts_state_ to READY
    Reset();
    clock_gettime(CLOCK_MONOTONIC, &start_time_);
    perflog_.fp = NULL;

#define POLICY_IS(s) \
  ((s) != nullptr) and (strncmp(options_.reneg_policy, s, strlen(s)) == 0)

    if (POLICY_IS(pdlfs::kRenegPolicyIntraEpoch)) {
      policy_ = new InvocationIntraEpoch(*this, options);
    } else {
      policy_ = new InvocationInterEpoch(*this, options);
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

  //
  // Only for benchmarks, not for actual data runs
  //
  Status ForceRenegotiation();

  //
  // AdvanceEpoch is also called before the first epoch,
  // so it can't be assumed that the first epoch is e0
  //
  void AdvanceEpoch() {
    MutexLock ml(&mutex_);
    MainThreadState cur_state = mts_mgr_.GetState();
    assert(cur_state != MainThreadState::MT_BLOCK);
    policy_->AdvanceEpoch();
    epoch_++;
  }

  OobFlushIterator OobIterator() { return OobFlushIterator(oob_buffer_); }

  size_t OobSize() const { return oob_buffer_.Size(); }

  //
  // Return currently negotiated range, held by oob_buffer
  //
  InclusiveRange GetRange() const { return oob_buffer_.range_; }

  void UpdateRange(Range range) {
    oob_buffer_.SetRange(InclusiveRange(range.rmin(), range.rmax()));
  }

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

  void BackendWriteCounter(uint64_t b) {
    backend_wr_bytes_ += b; /* stat cnt, XXX assume ok w/o lock */
  }

  void LogReneg(int round_num) {
    if (PerflogOn()) PerflogReneg(round_num);
  }
  void LogAggrBinCount() {
    if (PerflogOn()) PerflogAggrBinCount();
  }
  void LogMyPivots(double* pivots, int num_pivots, const char* lab) {
    if (PerflogOn()) PerflogMyPivots(pivots, num_pivots, lab);
  }
  void LogVec(std::vector<uint64_t>& vec, const char* vlabel) {
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

  void CalculatePivots(const size_t num_pivots) {
    mutex_.AssertHeld();
    assert(mts_mgr_.GetState() == MainThreadState::MT_BLOCK);

    pivots_.Resize(num_pivots);
    pivots_.FillZeros();

    PivotCalcCtx pvt_ctx;
    PopulatePivotCalcState(&pvt_ctx);
    PivotUtils::CalculatePivots(&pvt_ctx, &pivots_, num_pivots);

    if (pivots_.PivotWidth() < 1e-3) {  // arbitrary limit for null pivots
      pivots_.MakeUpEpsilonPivots();
    }

    pivots_.AssertMonotonicity();
  }

 private:
  void PopulatePivotCalcState(PivotCalcCtx* pvt_ctx) {
    pvt_ctx->SetBins(&bins_);
    oob_buffer_.GetPartitionedProps(pvt_ctx->oob_left_, pvt_ctx->oob_right_);
  }

  void AssignShuffleTarget(particle_mem_t& p) {
    int dest_rank = policy_->ComputeShuffleTarget(p);
    if (dest_rank >= 0 and dest_rank < options_.num_ranks) {
      p.shuffle_dest = dest_rank;
      bins_.IncrementBin(dest_rank);
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
    bins_.Reset();
    oob_buffer_.Reset();
  }

  void PerflogStartup();
  static void* PerflogMain(void* arg);
  void PerflogDestroy();
  int PerflogOn() { return perflog_.fp != NULL; }
  void PerflogReneg(int round_num);
  void PerflogAggrBinCount();
  void PerflogMyPivots(double* pivots, int num_pivots, const char* lab);
  void PerflogVec(std::vector<uint64_t>& vec, const char* vlabel);
  void PerflogVPrintf(const char* fmt, va_list ap);

 private:
  /* these values do not change after ctor */
  const CarpOptions& options_;   /* ref to config options from ctor */
  struct timespec start_time_;   /* time carp object was created */

  /* protected by mutex_ */
  MainThreadStateMgr mts_mgr_;   /* to block shuffle write during reneg */
  uint64_t backend_wr_bytes_;    /* total #bytes written to backend */
  int epoch_;                    /* current epoch#, update w/AdvanceEpoch() */

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
  /* XXX: temporary, refactor RTP/perfstats as friend classes */
  port::Mutex mutex_;
  port::CondVar cv_;

  OrderedBins bins_;
  OobBuffer oob_buffer_;
  Pivots pivots_;

 private:
  friend class InvocationPolicy;
  InvocationPolicy* policy_;

  friend class PivotUtils;
};
}  // namespace carp
}  // namespace pdlfs
