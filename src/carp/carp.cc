//
// Created by Ankush J on 3/5/21.
//

#include "carp.h"

#include "rtp.h"

namespace pdlfs {
namespace carp {

// copy skey and svalue into p in order to serialize it
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

// determine if p is OOB.  if OOB: insert in buffer for later sending,
// else set shuffle_dest so it can be sent now.  also, let caller know
// if OOB buffer should be flushed (can happen in either case).
Status Carp::AttemptBuffer(particle_mem_t& p, bool& is_oob,
                           bool& need_oob_flush) {
  MutexLock ml(&mutex_);
  Status s = Status::OK();

  /* first determine if "p" is OOB or not */
  is_oob = policy_->BufferInOob(p);

  /* if its oob, "p" gets copied off to OOB buffer with shuffle_dest==-1 */
  if (is_oob) {

    oob_buffer_.Insert(p);

    /* we are done with "p" -- processing will complete when oob is flushed */
  }

  /*
   * we ask the caller to flush OOB if RTP is already in progress or
   * we decide to trigger RTP now.   also, if RTP is engaged, we want to
   * block until it completes.
   */
  if (mts_mgr_.GetState() != MainThreadState::MT_READY ||
     policy_->TriggerReneg()) {
    need_oob_flush = true;
    /* starts RTP (if not running) and blocks until it's done */
    rtp_.InitRound();   /* block here in CV wait on Carp::mutex_ */
  } else {
    need_oob_flush = false;
  }

  /* if we didn't pass "p" off to the oob buffer, then set shuffle_dest */
  if (!is_oob) {
    AssignShuffleTarget(p, false);
    if (p.shuffle_dest == -1) {
      ABORT("Invalid shuffle target");
    }
  }

  return s;
}

Status Carp::ForceRenegotiation() {  /* only used by RTP benchmark */
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

  pivots->InstallInOrderedBins(&bins_);  /* this updates bounds too */

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

/*
 * FlushOOB: attempt to flush out items in the OOB buffer by assigning
 * them to a rank and sending them.  we expect carp to be unlocked
 * (since we take the lock).  the assignemnt to a rank can fail
 * if the item is still out of bounds.  if purge is false, we'll
 * keep the item in the OOB buffer (the next RTP op should assign
 * it a rank).   if purge is true, we'll send it to one of the ranks
 * on the end (i.e. OOB left items go to rank 0, OOB right items
 * go to rank N-1).  purge is set to true for epoch flushes.  epoch is
 * passed through to shuffle enqueue (XXX but XN shuffle doesn't use
 * epoch, so the value doesn't matter).
 */
void Carp::FlushOOB(bool purge, int epoch) {
  std::vector<particle_mem_t> swpbuf;

  /* lock and extract current OOB list */
  mutex_.Lock();
  if (oob_buffer_.Size() > 0) {
    oob_buffer_.SwapList(swpbuf);
  }

  /* attempt to assign dest rank for any unassigned entries */
  for (size_t lcv = 0 ; lcv < swpbuf.size() ; lcv++) {

    if (swpbuf[lcv].shuffle_dest != -1)   /* dest already assigned? */
      continue;

    if (this->AssignShuffleTarget(swpbuf[lcv], purge) != 0) {
      oob_buffer_.Insert(swpbuf[lcv]);  /* still OOB, put back in oob list */
    }

  }

  /* unlock.  we now have exclusive access to items we need to send */
  mutex_.Unlock();

  /* now that we've unlocked: send all items that have a valid dest */

  /*
   * NOTES:
   *  [1] RTP ctor already ensured ctx->type == SHUFFLE_XN, NN not supported
   *  [2] skipping native_write RPC bypass trick when shuffle_dest==my_rank
   *      since we'd have to unserialize to get key/value info and XN shuffle
   *      already bypasses Mercury when shuffle_dest==my_rank.
   */
  xn_ctx_t *xn = static_cast<xn_ctx_t*>(pctx.sctx.rep);
  int srcrank = shuffle_rank(&pctx.sctx);
  for (size_t lcv = 0 ; lcv < swpbuf.size() ; lcv++) {
    if (swpbuf[lcv].shuffle_dest == -1)   /* skip unknown dests */
      continue;
    /* note: we can block in shuffle enqueue if flow control is applied */
    xn_shuffle_enqueue(xn, swpbuf[lcv].buf, swpbuf[lcv].buf_sz,
                       epoch, swpbuf[lcv].shuffle_dest, srcrank);
  }

  /* done! */
}

}  // namespace carp
}  // namespace pdlfs
