//
// Created by Ankush J on 3/5/21.
//

#include "carp.h"

#include "rtp.h"

namespace pdlfs {
namespace carp {

Status Carp::Serialize(const char* skey, unsigned char skey_len, char* svalue,
                       unsigned char svalue_len, unsigned char extra_data_len,
                       particle_mem_t& p) {
  Status s = Status::OK();
  char* bp;

  memcpy(&p.indexed_prop, skey, sizeof(p.indexed_prop));
  p.buf_sz = skey_len + svalue_len + extra_data_len;
  assert(p.buf_sz <= sizeof(p.buf));
  bp = p.buf;
  memcpy(bp, skey, skey_len);
  bp += skey_len;
  memcpy(bp, svalue, svalue_len);
  if (extra_data_len) {
    memset(bp + svalue_len, 0, extra_data_len);
  }
  p.shuffle_dest = -1; /* default to -1 (i.e. 'unknown') */

  flog(LOG_DBG2, "Carp::Serialize: bufsz: %d", p.buf_sz);
  return s;
}

Status Carp::AttemptBuffer(particle_mem_t& p, bool& shuffle, bool& flush) {
  MutexLock ml(&mutex_);
  Status s = Status::OK();

  bool can_buf = policy_->BufferInOob(p);
  /* shuffle = true if we can't buffer */
  shuffle = !can_buf;

  if (can_buf) {
    /* XXX: check RV */
    int rv = oob_buffer_.Insert(p);
    if (rv < 0) {
      flog(LOG_ERRO, "OOB Insert failed: %d", options_.my_rank);
      ABORT("Oob insert failed!");
    }
  }

  /* reneg = true iff
   * 1. Trigger returns true
   * 2. OOB is full
   * 3. Reneg is ongoing
   */

  bool reneg_ongoing = (mts_mgr_.GetState() != MainThreadState::MT_READY);
  bool reneg = policy_->TriggerReneg() || reneg_ongoing;

  /* Attempt a flush if reneg happened */
  flush = reneg;

  if (reneg) {
    rtp_.InitRound();
    MarkFlushableBufferedItems();
  }

  if (shuffle) {
    AssignShuffleTarget(p);
    if (p.shuffle_dest == -1) {
      ABORT("Invalid shuffle target");
    }
  }

  return s;
}

Status Carp::ForceRenegotiation() {
  MutexLock ml(&mutex_);
  Status s = Status::OK();
  rtp_.InitRound();
  rtp_.PrintStats();
  return s;
}

/* called at the end of RTP round to update our pivots */
void Carp::UpdateBinsFromPivots(Pivots* pivots) {
  mutex_.AssertHeld();
  this->LogMyPivots(pivots, "RENEG_AGGR_PIVOTS");

  Range carp_range = this->GetInBoundsRange();
  Range pivot_bounds =pivots->GetPivotBounds();
  if (!carp_range.IsSet()) {
    assert(pivot_bounds.IsSet());
  } else {
    assert(float_lte(pivot_bounds.rmin(), carp_range.rmin()));
    assert(float_gte(pivot_bounds.rmax(), carp_range.rmax()));
  }

  this->UpdateInBoundsRange(pivot_bounds);
  pivots->InstallInOrderedBins(&bins_);

#ifdef DELTAFS_PLFSDIR_RANGEDB
  // make safe to invoke CARP-RTP without a properly initiialized
  // storage backend, such as for benchmarks
  if (pctx.plfshdl != NULL) {
    Range our_bin = bins_.GetBin(pctx.my_rank);
    deltafs_plfsdir_range_update(pctx.plfshdl, our_bin.rmin(), our_bin.rmax());
  }
#else
  ABORT("linked deltafs does not support rangedb");
#endif
}

}  // namespace carp
}  // namespace pdlfs
