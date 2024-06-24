//
// Created by Ankush J on 3/5/21.
//

#pragma once


#include <pdlfs-common/env.h>
#include <pdlfs-common/mutexlock.h>
#include <pdlfs-common/status.h>

#include "../preload_shuffle.h"

#include "oob_buffer.h"
#include "pivots.h"
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
  uint32_t oob_sz;          /* max #particles in oob buf (RANGE_Oob_size) */
  const char* reneg_policy; /* InvocationIntraEpoch, */
                            /* InvocationInterEpoch (def) */
                            /*   (RANGE_Reneg_policy) */
  uint64_t reneg_intvl;     /* periodic: reneg every reneg_intvl writes */
                            /*   (RANGE_Reneg_interval) */
  int rtp_pvtcnt[4];        /* # of RTP pivots gen'd by each stage */
                            /* RANGE_Pvtcnt_s{1,2,3}; pvtcnt[0] not used */
  shuffle_ctx_t* sctx;      /* shuffle context */
  uint32_t my_rank;         /* my MPI rank */
  uint32_t num_ranks;       /* MPI world size */
  int enable_perflog;       /* non-zero to enable perflog */
  const char* log_home;     /* where to put perflog (if enabled) */
};

class Carp {
 public:
  Carp(const CarpOptions& options)
      : options_(options),
        cv_(&mutex_),
        backend_wr_bytes_(0),
        epoch_(0),
        rtp_(this, options_),
        bins_(options.num_ranks),
        oob_buffer_(options.oob_sz),
        policy_(nullptr) {
    MutexLock ml(&mutex_);
    // necessary to set mts_state_ to READY
    Reset();
    clock_gettime(CLOCK_MONOTONIC, &start_time_);
    perflog_.fp = NULL;

    const char *renegpolicy = (options_.reneg_policy) ? options_.reneg_policy
                                                      : CARP_DEF_RENEGPOLICY;
    if (strcmp(renegpolicy, "InvocationIntraEpoch") == 0) {
      policy_ = new InvocationIntraEpoch(*this, options);
    } else if (strcmp(renegpolicy, "InvocationInterEpoch") == 0) {
      policy_ = new InvocationInterEpoch(*this, options);
    } else {
      ABORT("unknown reneg_policy specified");
    }

    if (options_.enable_perflog) {
      this->PerflogStartup();
    }
  }

  ~Carp() {
    this->PerflogDestroy();
  }

  /* invocation policies can use this if desired (must be holding lock) */
  void Reset() {
    mutex_.AssertHeld();
    mts_mgr_.Reset();
    bins_.Reset();
    oob_buffer_.Reset();
  }

  /* no locking constraints (does not access protected data) */
  Status Serialize(const char* skey, unsigned char skey_len, char* svalue,
                   unsigned char svalue_len, unsigned char extra_data_len,
                   particle_mem_t& p);

  /* called from shuffle_write_range(), takes lock */
  Status AttemptBuffer(particle_mem_t& p, bool& shuffle, bool& flush);

  //
  // Only for benchmarks, not for actual data runs.  takes lock.
  //
  Status ForceRenegotiation();

  //
  // AdvanceEpoch is also called before the first epoch, so it can't
  // be assumed that the first epoch is e0.  takes lock.
  //
  void AdvanceEpoch() {
    MutexLock ml(&mutex_);
    MainThreadState cur_state = mts_mgr_.GetState();
    assert(cur_state != MainThreadState::MT_BLOCK);
    policy_->AdvanceEpoch();
    epoch_++;
  }

  /* called from AttemptBuffer/TriggerReneg functions (with lock held) */
  bool IsOobFull() { return oob_buffer_.IsFull(); }

  // OobReset and OobInsert are exposed for testing carp (called w/lock held)
  void OobReset() { oob_buffer_.Reset(); }
  void OobInsert(particle_mem_t& item) { oob_buffer_.Insert(item); }

  // flush OOB buffer.  if purge is false then we continue to buffer
  // items that are still OOB.  if purge is true then we send items
  // that are still OOB to one of the ranks on the end (i.e. OOB left
  // to rank 0, OOB right to rank N-1).   takes lock.
  void FlushOOB(bool purge, int epoch);

  // is value out of bins_ bounds?  called from policy w/lock held
  bool OutOfBounds(float prop) {
    return !bins_.InRange(prop);
  }

  //
  // ret current range.  called from UpdateBinsFromPivots w/lock held
  //
  Range GetInBoundsRange() { return bins_.GetRange(); }

  void UpdateState(MainThreadState new_state) {
    mutex_.AssertHeld();
    mts_mgr_.UpdateState(new_state);
  }

  MainThreadState GetCurState() {
    mutex_.AssertHeld();
    return mts_mgr_.GetState();
  }

  bool IsFirstBlock() {
    mutex_.AssertHeld();
    return mts_mgr_.FirstBlock();
  }

  // called from 3hop priority handler.  takes lock.
  Status HandleMessage(void* buf, unsigned int bufsz, int src, uint32_t type) {
    return rtp_.HandleMessage(buf, bufsz, src, type);
  }

  // only used by a diag flog during preload.cc MPI finalize...
  int NumRounds() {
    MutexLock ml(&mutex_);
    return rtp_.NumRounds();
  }

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
   * Perflog*() function.  Note that only the carp dtor can turn
   * off the perflog, so once it is on it will stay on for the
   * lifetime of the Carp object.
   */

  /* called from HandlePivotBroadcast() w/caller holding mutex_ */
  void LogReneg(int round_num) {
    if (PerflogOn()) PerflogReneg(round_num);
  }

  /* MPI collective call @ preload MPI_Finalize() wrapper.  takes lock. */
  void LogAggrBinCount() {
    if (PerflogOn()) PerflogAggrBinCount();
  }

  /* called from UpdatePivots w/caller holding mutex_ */
  void LogMyPivots(Pivots* pivots, const char* lab) {
    if (PerflogOn()) PerflogMyPivots(pivots, lab);
  }

  /* LogPivots logs both pivots and rank_counts_aggr_ */
  /* called from HandleBegin w/caller holding mutex_ */
  void LogPivots(Pivots& pivots) {
    if (PerflogOn()) PerflogPivots(pivots);
  }

  /* called from HandleBegin and HandlePivots, doesn't need carp lock */
  void LogPrintf(const char* fmt, ...) {
    if (PerflogOn()) {
      va_list ap;
      va_start(ap, fmt);
      PerflogVPrintf(fmt, ap);
      va_end(ap);
    }
  }

  /* pivots should already be sized to desired pivot count (call w/lock) */
  void CalculatePivots(Pivots& pivots) {
    mutex_.AssertHeld();
    assert(mts_mgr_.GetState() == MainThreadState::MT_BLOCK);
    ComboConsumer<float,uint64_t> cco(&bins_, &oob_buffer_);
    pivots.Calculate(cco);
  }

  /* called at the end of RTP round to update our pivots (call w/lock) */
  void UpdateBinsFromPivots(Pivots* pivots);

  /* only used by range_utils test (call w/lock) */
  void UpdateFromArrays(size_t nbins, const float* bins,
                        const uint64_t* weights) {
    mutex_.AssertHeld();
    bins_.UpdateFromArrays(nbins, bins, weights);
  }

  /* called from ComputeShuffleTarget/AssignShuffleTarget (call w/lock) */
  int SearchBins(float val, size_t& ret_bidx, bool force) {
    return bins_.SearchBins(val, ret_bidx, force);
  }

  /* allow RTP (and tests) to access our lock through these APIs */
  void Lock() { mutex_.Lock(); }
  void Unlock() { mutex_.Unlock(); }
  void AssertLockHeld() { mutex_.AssertHeld(); }
  void CVWait() { cv_.Wait(); }
  void CVSignal() { cv_.Signal(); }

 private:
  /*
   * attempt to assign p to a shuffle target.   if p is out of
   * bounds and purge is true (e.g. during an epoch flush), we
   * force an assignment to the rank on the end (OOB left=>rank 0,
   * OOB right=>rank N-1).  called w/lock by AttemptBuffer, FlushOOB.
   * returns 0 if assigned, -1 if OOB left, +1 if OOB right.
   */
  int AssignShuffleTarget(particle_mem_t& p, bool purge) {
    int rv, dest_rank;
    assert(p.shuffle_dest == -1);
    rv = policy_->ComputeShuffleTarget(p, dest_rank);
    if (rv && purge) {
      dest_rank = (rv < 0) ? 0 : bins_.Size() - 1;
      rv = 0;     /* assigned dest via purge */
    }
    if (rv == 0) {
      p.shuffle_dest = dest_rank;
      bins_.IncrementBin(dest_rank);
    }
    return rv;
  }

  /* internal (private) perflog rountes (only call if perlog enabled) */
  void PerflogStartup();                           // open perflog
  static void *PerflogMain(void *arg);             // periodic thread main
  void PerflogDestroy();                           // shut perflog down
  int PerflogOn() { return perflog_.fp != NULL; }  // is perflog on?
  /* internal interface for top-level Log* calls, see above */
  void PerflogReneg(int round_num);
  void PerflogAggrBinCount();
  void PerflogMyPivots(Pivots* pivots, const char* lab);
  void PerflogPivots(Pivots &pivots);
  void PerflogVPrintf(const char* fmt, va_list ap);

  /* private Carp data members */

  /* these values do not change after ctor */
  const CarpOptions& options_;   /* ref to config options from ctor */
  struct timespec start_time_;   /* time carp object was created */

  struct perflog_state {
    /*
     * perflog is only enabled if the enable_perflog option is set.
     * if enabled, perflog creates a profiling thread to generate periodic
     * output.  the profiling thread exits when fp is set to null by dtor.
     */
    FILE *fp;                     /* open log: only updated by ctor/dtor */
    port::Mutex mtx;              /* protect/serialize access to perflog fp */
    pthread_t thread;             /* profiling thread making periodic output */
  } perflog_;

  port::Mutex mutex_;             /* Carp lock: protects remaining members */
  port::CondVar cv_;              /* tied to mutex_ (above). RTP InitRound */
                                  /* uses cv_ to wait for MT_READY state */
  MainThreadStateMgr mts_mgr_;    /* to block shuffle write during reneg */
  uint64_t backend_wr_bytes_;     /* total #bytes written to backend */
  int epoch_;                     /* current epoch#, update w/AdvanceEpoch() */
  RTP rtp_;                       /* RTP protocol state */
  OrderedBins bins_;              /* our current bins (defines bounds) */
  OobBuffer oob_buffer_;          /* out of bounds data */
  InvocationPolicy* policy_;      /* our policy */
};
}  // namespace carp
}  // namespace pdlfs
